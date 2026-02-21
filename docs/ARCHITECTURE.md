# Search-HN Mirroring Architecture

This is the canonical architecture doc for the Hacker News mirror pipeline in
`crates/catchup_worker`.

It replaces `docs/v1-gaps.md` as the day-to-day reference.

## Scope and goals

- Correct, restart-safe mirroring of HN items into Postgres.
- Fast enough for personal archive scale.
- Durable, inspectable ingest state in Postgres.
- Keep operational model simple: one process, one database, explicit failure
  states.

## Current system shape

Two data paths exist:

- Catchup path: durable segment orchestration in Postgres.
- Realtime path: SSE listener + update workers (currently still legacy/stub in
  key places).

## Core design decisions

1. Keep Rust + Postgres + Diesel.
2. Use Postgres as both data store and ingest control plane.
3. Model progress with durable segment state, not `MAX(id)` on `items`.
4. Prefer explicit terminal/non-terminal outcomes over implicit success.

## Data model

Primary mirrored tables:

- `items`
- `kids`

Control-plane tables:

- `ingest_segments`
- `ingest_exceptions`

`ingest_segments` tracks window planning and segment lifecycle:

- `pending`
- `in_progress`
- `done`
- `retry_wait`
- `dead_letter`

`ingest_exceptions` stores per-item non-happy-path outcomes for replay/audit.

## Catchup architecture (implemented)

The catchup flow is orchestrated by `CatchupOrchestrator` and `IngestWorker`.

High-level flow:

1. Read upstream max ID (`/maxitem.json`).
2. Compute planning window from frontier and optional CLI bounds.
3. Requeue interrupted work (`in_progress` -> `pending`) and reactivate
   retryables (`retry_wait` -> `pending`).
4. Enqueue uncovered segments for the requested window.
5. Claim pending segments and execute bounded worker concurrency.
6. Persist per-segment outcomes (`done`, `retry_wait`, `dead_letter`) and
   per-item exceptions.
7. Recompute/report frontier and run summary metrics.

## Failure model

Per-segment attempts are classified into:

- Success (`done`)
- Retryable failure (`retry_wait`)
- Fatal failure (`dead_letter`)

Important policy:

- Dead letters are durable and auditable.
- Replay is explicit (not silent).
- Process should fail clearly on fatal conditions instead of silently muddling.

## Realtime architecture (target)

The intended updater service model is:

1. SSE listener starts immediately and stays connected with reconnect/backoff.
2. Bounded update channel applies backpressure.
3. Supervised realtime worker pool drains updates and upserts items.
4. Startup rescan runs concurrently using a window anchored at last known live
   SSE connection time.

This target design is captured in `docs/updater-v2-claude.md`.

## Known remaining gaps (as of 2026-02-21)

1. Realtime still runs after catchup instead of concurrently in
   `crates/catchup_worker/src/main.rs`.
2. Realtime channel is still unbounded (`flume::unbounded`) in
   `crates/catchup_worker/src/main.rs`.
3. `SyncService::realtime_update` still spawns legacy updater workers and does
   not supervise/restart handles in
   `crates/catchup_worker/lib/sync_service/mod.rs`.
4. SSE listener lacks robust outer reconnect loop + gap-fill behavior in
   `crates/catchup_worker/lib/firebase_listener/listener.rs`.
5. Legacy updater worker path is still active in
   `crates/catchup_worker/lib/sync_service/firebase_worker.rs` (including known
   metric double-count behavior and panic path).
6. `claim_next_pending_segment` still needs `FOR UPDATE SKIP LOCKED` for future
   multi-claimer safety in
   `crates/catchup_worker/lib/segment_manager/ops.rs`.
7. Updater durability/observability additions from v2 are still pending:
   `updater_state.last_sse_event_at` and `items.last_fetched_at`.

## Operational stance

- Single-box personal archive remains the baseline.
- Multi-active workers are not required now.
- If multi-claimers become necessary later, the segment claim query should be
  hardened with `FOR UPDATE SKIP LOCKED`.

## Source docs

- Active realtime/update plan: `docs/updater-v2-claude.md`
- Historical gap analysis (archived): `docs/v1-gaps.md`
