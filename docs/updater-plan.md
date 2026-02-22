## Updater V2 Tonight Plan: Correctness First, Thin CLI Unification, Two Units

### Summary
Ship a production-viable updater tonight by sequencing work into **must-have correctness** first, then **thin subcommand unification**, then **basic observability/alerts**.  
Given your choices, we will:
1. Use **one binary with subcommands**.
2. Run it as **two systemd units** (long-running updater + optional one-shot catchup).
3. Add only **minimal critical schema** (`updater_state`), and **skip `items.last_fetched_at`** for now.

---

## Decisions (locked)
1. **Binary + units**
- Implement subcommands in `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/src/main.rs`.
- Use two units:
  - `catchup_worker updater ...` (long-running)
  - `catchup_worker catchup ...` (oneshot/manual/replay job)

2. **Persistence for last-seen**
- Add new table via migration in `/Users/ritsuko/projects/data/search-hn/crates/hn_core/migrations/...`:
  - `updater_state` with single-row semantics and `last_sse_event_at TIMESTAMPTZ`.
- Do **not** add `items.last_fetched_at` tonight.

3. **Realtime reuse vs replace**
- Reuse:
  - `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/lib/firebase_listener/listener.rs` (SSE and maxitem client surface)
  - `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/lib/sync_service/ingest_worker.rs` (`process_realtime_item`)
- Replace for realtime runtime:
  - Stop using legacy updater mode in `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/lib/sync_service/firebase_worker.rs` for service path.
  - Add supervised realtime worker pool in `SyncService`.

---

## Work Parceling (tonight execution order)

### Parcel 1 (P0): Correctness/Survivability (ship blocker)
1. Add `FOR UPDATE SKIP LOCKED` claim logic in `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/lib/segment_manager/ops.rs` for future-safe claimers.
2. Add migration for `updater_state(last_sse_event_at, updated_at)` (single-row, primary key fixed to `1`).
3. Harden SSE listener:
- outer reconnect loop with exponential backoff (500ms → 30s cap)
- track `max_id_before_disconnect`
- on reconnect, `get_max_id` and enqueue `(before, now]` as gap-fill
- keep `last_event_at` in memory and persist to `updater_state` at 60s cadence while connected.
4. Switch realtime channel to bounded (`4096`) and expose queue pressure metrics.
5. Run catchup startup rescan **concurrently** with realtime startup (shared shutdown token), not sequentially.

### Parcel 2 (P0): Thin subcommand unification + deploy shape
1. Convert service CLI to subcommands in `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/src/main.rs`:
- `updater` subcommand: long-running SSE + workers + startup rescan
- `catchup` subcommand: existing catchup-only knobs
2. Keep compatibility shim for existing `catchup_only` temporarily (calls shared catchup path) to avoid deployment breakage tonight.
3. Add/adjust unit files in `/Users/ritsuko/projects/data/search-hn/infra/systemd/`:
- `search-hn-updater.service` (Restart=on-failure)
- `search-hn-catchup.service` (Type=oneshot)

### Parcel 3 (P1): Realtime worker runtime cleanup
1. In `SyncService`, supervise realtime workers (hold handles, restart crashed worker with delay).
2. Use `IngestWorker::process_realtime_item` for per-ID processing and retries.
3. Remove double-count metric behavior from legacy realtime path by no longer using it in updater runtime.

### Parcel 4 (P1): Basic observability and alertability
Add minimal must-have metrics/logs:
- `realtime_listener_connected` gauge
- `realtime_last_event_age_seconds` gauge
- `realtime_worker_alive_count` gauge
- `realtime_queue_depth` gauge
- `realtime_queue_overflow_total` counter
- `realtime_reconnects_total` counter
- `realtime_items_updated_total` counter
- `catchup_frontier_lag` gauge (`maxitem - frontier_id`)
Structured log events:
- listener connect/disconnect/reconnect attempt/success
- gap-fill range enqueued (`from_id`, `to_id`, count)
- worker crash/restart
- startup rescan window (`rescan_from`, `rescan_to`, duration)

---

## Public Interface / API Changes
1. **CLI**
- `catchup_worker updater [flags]`
- `catchup_worker catchup [existing catchup flags]`
- Compatibility: existing `catchup_only` remains temporarily.
2. **DB schema**
- New table `updater_state` (single row with `last_sse_event_at`).
3. **Metrics surface**
- New realtime health/queue/reconnect metrics listed above.
4. **Systemd**
- New recommended dedicated updater unit; catchup as explicit separate unit/job.

---

## Testing and Acceptance Criteria

### Unit tests
1. SSE reconnect backoff resets after successful event.
2. Gap-fill enqueues exact `(before, now]` IDs.
3. `updater_state` persistence throttled to cadence, not per event.
4. Realtime worker supervisor restarts crashed worker and updates alive gauge.
5. `claim_next_pending_segment` with `SKIP LOCKED` still claims correctly under contention.

### Integration tests
1. Start updater with mocked SSE that disconnects mid-stream:
- updater reconnects
- gap-fill fires
- no permanent stall
2. Simulate bounded queue saturation:
- listener backpressure observed
- no silent drop path
3. Cold start with historical data:
- catchup startup rescan runs concurrently with listener/workers
- realtime events still processed during rescan.

### Tonight done-definition
- Updater process remains healthy through forced SSE disconnect test.
- Metrics endpoint shows connected/age/queue/alive/reconnect signals.
- Restart of updater process computes startup rescan from persisted `last_sse_event_at` and runs.

---

## Assumptions and Defaults
1. Rescan freshness window default: **3 days**.
2. Startup rescan trigger: process start only (no in-process periodic loop).
3. Nightly resilience: systemd-managed restarts acceptable operationally.
4. No `items.last_fetched_at` tonight; revisit after stability.
5. Full-table scans for window lookup are acceptable for now (current environment notes indicate ~20s full scan).

