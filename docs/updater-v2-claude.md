# Updater V2 Design

Based on review of `crates/catchup_worker` as it stands on the `updater` branch.

## Summary of what works and what doesn't

The catchup control plane is in good shape: durable segments, cursor-based resume, dead-letter tracking, structured retries, and reasonable test coverage. The realtime path is a stub. `SyncService::realtime_update` spawns legacy `firebase_worker` tasks in `WorkerMode::Updater`, drops the handles immediately, and the whole thing runs sequentially after catchup in `main.rs`. It will not survive a production workload.

---

## Issue 1: MaxID is not a freshness cursor

`/maxitem.json` tells you the highest ID ever assigned. It catches new items. It does not catch:

- Score changes on existing stories
- `descendants` counter updates as comments are added
- `dead` / `deleted` flag changes
- New `kids` entries added to an existing story or comment

The Firebase SSE stream at `/updates.json` pushes batches of item IDs whenever *any* of these mutations occur. This is the right mechanism. The old brute-force (pull last 1M items) was compensating for not having the SSE path working correctly.

**The disconnect problem**: when the SSE connection drops, we lose a window of update events. We don't know which specific items mutated during that window. The backstop is a startup rescan — but the rescan window must be computed from when we *last had a live connection*, not from `now()`.

**Why `now() - N_days` is wrong**: if the server was down for 4 days and the mutation window is 3 days, items created in the range `[last_connected - 3d, last_connected]` were still actively receiving updates when the outage started and continued to receive them while we were down. A window anchored at `now()` doesn't reach them. The correct window is `last_connected_at - N_days` to `now()`. Down 4 days with N=3 → rescan 7 days. Down 30 days → rescan 33 days. No artificial cap: at 2500 RPS, 30 days of items (~4M) takes ~27 minutes, which is fine.

**Persisting last connection time**: store `last_sse_event_at TIMESTAMPTZ` in a small `updater_state` table (single row). Update it on a timer (every 60 seconds) while SSE is connected, not on every event. On startup, read it and compute `rescan_from = last_sse_event_at - N_days`. If no row exists (first run), fall back to `now() - N_days`.

**Do we need a periodic rescan loop while the service is running?** No. Restart is the trigger. If the service has been up a long time without a restart, schedule a nightly systemd restart — don't add a scheduling loop inside the service.

**Do we need a continuous CatchupLoop for new items?** No. New items appear in `updates.json` — HN pushes them through SSE. New items not yet in the DB are also caught by the frontier-to-maxitem pass in the startup rescan.

**On the right value of N**: at current HN velocity (~130k items/day), 3 days covers the overwhelming majority of mutation activity (front page lifetime ~24h, most comments appear in day 1). A time-based window is more stable than an ID-count window as HN velocity changes. To validate empirically: add `last_fetched_at TIMESTAMPTZ` to `items` and track `now() - to_timestamp(items.time)` for items touched by realtime workers. A histogram of that gives the actual p95/p99 mutation window. Start with 3 days, tune from data.

---

## Issue 2: Catchup and realtime must be concurrent

The current `main.rs` flow is:

```
catchup() → wait for completion → start realtime listeners
```

This is wrong. On a fresh install, catchup takes ~5 hours. During those 5 hours new items are being created and the SSE stream is not being consumed. The fix: run them concurrently under a shared `CancellationToken`. They share the global rate limiter so they don't stack additively against Firebase's limits. Since realtime events come in small batches and catchup workers compete for the same RPS budget, no additional priority mechanism is needed in practice.

---

## Issue 3: Process architecture

**Keep `catchup_only` as-is.** It is a clean, well-wired entrypoint with good CLI ergonomics and test coverage. It serves the bulk-ingest use case and the manual-rescan use case. Don't merge it into the service binary.

**The service binary is the updater.** It does exactly three things, all started concurrently at launch:

1. **SSE listener** (one task): connects to `/updates.json` immediately on startup, pushes item IDs to a bounded update channel, handles reconnection with gap fill. Starts first; no waiting.

2. **Update worker pool** (N tasks): drains the update channel, fetches items, upserts. Supervised — hold handles, restart on exit.

3. **Startup rescan** (one background task): concurrently with SSE, rescans all items with `time >= last_sse_event_at - N_days` using the existing orchestrator. Window stretches to cover the full outage period — down 4 days with N=3 means a 7-day rescan (~910k items, ~6 minutes at 2500 RPS). Exits when done; does not loop.

SSE starts immediately — the rescan runs behind it. Any items that come through SSE during the rescan window get upserted and will simply be upserted again if the rescan also touches them. Idempotent either way. The mental model is simple enough to hold in your head.

---

## Issue 4: Multi-worker

**For a single-server personal archive: don't build multi-worker in v2.**

The database and the worker are co-located. An outage that takes one down takes both down. Multi-box failover doesn't buy much here. The correctness model (startup rescan on restart) already handles the worker dying cleanly.

**The path to multi-worker is one SQL change.** Add `FOR UPDATE SKIP LOCKED` to the subquery in `claim_next_pending_segment`. That makes the segment table safe for concurrent claimants without any other changes. Do this now — it costs nothing and future-proofs the claiming logic.

Redis is unnecessary. `ingest_segments` *is* the work queue. Advisory locks for warm standby are unnecessary for a personal archive where DB and process live on the same box.

---

## SSE reconnection design

The current `listen_to_updates` exits with `Ok(())` when the stream ends. The reconnection loop needs to do three things:

**1. Reconnect with backoff.** Outer retry loop, exponential backoff (500ms → 1s → 2s → 4s → cap at 30s), reset on successful event receipt.

**2. Gap fill on reconnect.** Before connecting, record `max_id_before`. On reconnect, call `get_max_id()`. If `max_id_now > max_id_before`, push the range `(max_id_before, max_id_now]` directly into the update channel. This handles new items created during the blind window. This is not a named loop — it's ~10 lines in the reconnect handler. Mutations to *existing* items during the disconnect window are covered by the startup rescan on next restart.

**3. Last-event timestamp.** Record `last_event_at` on every successful SSE event. Drives the staleness metric. A stale timestamp is the primary signal the listener is broken.

---

## Update channel design

Replace `flume::unbounded` with `flume::bounded(4096)`.

When full, `send_async` blocks the SSE listener — this is intentional backpressure. If Firebase disconnects us because we stopped reading, we reconnect and gap fill covers any new IDs. Do not silently drop items; a dropped ID is a missed update until next startup rescan.

Increment a `realtime_queue_overflow_total` counter when the channel is at capacity and the listener would block for longer than a threshold, so you can see if this is happening in practice.

The update workers should deduplicate in-flight IDs. SSE can push the same ID multiple times rapidly (score churn). A `HashSet<i64>` over currently-queued IDs is sufficient. This is an optimization, not a correctness requirement.

---

## Update worker design

Replace the legacy `firebase_worker::worker(mode=Updater)` path. The legacy path has a double-count metric bug, no batching, no retry logic, and unsupervised handles.

For realtime, segment state machinery is not needed — these are individual item fetches. A simple per-item retry loop is sufficient:

```
loop {
    id = recv from channel
    fetch with retry (3 attempts, 200ms / 500ms / 1s)
    upsert
    emit metric
}
```

Spawn N workers (default 8 is plenty — the bottleneck is Firebase RPS, not concurrency). Hold `JoinHandle`s. If a worker exits with error, log and restart after a short delay. If all workers die, exit the process and let systemd restart it.

---

## Monitoring

### Page on these

| Metric | Alert |
|--------|-------|
| `realtime_listener_connected` (gauge 0/1) | < 1 for > 60s |
| `realtime_last_event_age_seconds` (gauge) | > 300s — keepalives stopped |
| `realtime_worker_alive_count` (gauge) | < 1 |
| `catchup_dead_letter_segments_total` (counter) | rate > 0 |

### Ticket on these

| Metric | Alert |
|--------|-------|
| `realtime_queue_depth` (gauge) | > 2000 sustained |
| `realtime_queue_overflow_total` (counter) | rate > 0 |
| `realtime_reconnects_total` (counter) | > 6/hour |
| `catchup_frontier_lag` = `maxitem - frontier_id` (gauge) | > 50000 |

### Dashboard only

- `realtime_items_updated_total` — update worker throughput
- `realtime_update_age_seconds` histogram — age of items at time of SSE event (drives window sizing)
- `realtime_items_actually_changed` — items where the fetched payload differed from what was stored (rescan/realtime effectiveness; requires a comparison step in the upsert path or a DB trigger)
- `startup_rescan_duration_seconds` — how long the startup rescan took

The `realtime_update_age_seconds` histogram is what you use to decide whether 3 days is the right rescan window. The `realtime_items_actually_changed` count tells you whether the rescan is doing real work or just burning RPS confirming things are already current.

---

## Implementation order

1. **`FOR UPDATE SKIP LOCKED`** in `claim_next_pending_segment` — small, safe, do it first.

2. **Fix SSE reconnection** (`firebase_listener/listener.rs`) — outer retry loop, backoff, gap fill on reconnect, `last_event_at` tracking.

3. **Fix update worker supervision** (`sync_service/mod.rs`) — hold handles, restart on exit, replace `flume::unbounded` with `flume::bounded`. Keep the legacy worker logic for now; wrap it correctly first.

4. **Concurrent startup in `main.rs`** — startup rescan then launch SSE listener + update workers concurrently with catchup, under shared `CancellationToken`.

5. **Port update workers to `IngestWorker`** — replace legacy `firebase_worker(mode=Updater)` with per-item retry loop. Fix the double-count metric.

6. **Observability** — add the metrics above; add `last_fetched_at` to `items`; add `updater_state` table with `last_sse_event_at`, written on a 60-second timer while SSE is connected.

Items 1-4 fix the critical supervision and concurrency gaps. Items 5-6 are correctness and polish.

---

## What not to build in v2

- **A periodic freshness replay loop running inside the service**: restart achieves the same thing. A nightly `systemd` restart or cron `catchup_only` invocation is simpler and more transparent than a scheduled loop inside a long-lived daemon.
- **A continuous CatchupLoop for frontier advancement**: new items come through SSE; restart covers gaps.
- **Redis**: unnecessary.
- **Advisory locks / warm standby**: unnecessary for co-located DB + worker.
- **Multi-active workers**: the `FOR UPDATE SKIP LOCKED` fix (step 1) is all the preparation needed if this ever matters.
