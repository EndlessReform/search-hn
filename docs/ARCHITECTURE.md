# Search-HN Mirroring Architecture

Canonical architecture reference for the Hacker News mirroring pipeline in
`/Users/ritsuko/projects/data/search-hn/crates/catchup_worker`.

## Goals

- Keep mirrored `items`/`kids` data fresh with restart-safe behavior.
- Keep failure modes explicit and auditable.
- Keep operations simple for a single-box personal archive.

## Runtime Modes

### 1. `updater` (long-running)

`catchup_worker updater` runs three concurrent tasks:

1. SSE listener (`updates.json`) with reconnect, exponential backoff, and reconnect gap-fill using
   `maxitem`.
2. Supervised realtime worker pool consuming a bounded queue and ingesting per-ID updates.
3. Startup replay window (catchup orchestration) anchored at
   `updater_state.last_sse_event_at - startup_rescan_days`.

### 2. `catchup` (one-shot)

`catchup_worker catchup` runs durable segment orchestration over a bounded planning window and exits.

`catchup_only` remains as a compatibility wrapper around the same one-shot catchup flow.

## Data Plane and Control Plane

Primary mirrored tables:

- `items`
- `kids`

Catchup control-plane tables:

- `ingest_segments` (`pending`, `in_progress`, `done`, `retry_wait`, `dead_letter`)
- `ingest_exceptions`

Updater durability table:

- `updater_state` (single-row state including `last_sse_event_at`)

Shared ingest dead-letter audit table:

- `ingest_dlq_items`

## Catchup Flow

1. Resolve target window (`frontier`, optional `start/end/limit`, optional replay forcing).
2. Recover and reactivate durable work (`in_progress -> pending`, `retry_wait -> pending`).
3. Materialize uncovered ranges as segments.
4. Claim segments (`FOR UPDATE SKIP LOCKED` in Postgres mode).
5. Process segments via shared ingest core (fetch retry + persist retry).
6. Persist segment outcome (`done` / `retry_wait` / `dead_letter`) and exception rows.
7. Emit progress metrics and run summary.

## Realtime Flow

1. SSE listener pushes item IDs to bounded channel.
2. Worker supervisor runs N realtime ingest workers and restarts workers on crash/exit.
3. Each worker fetches and upserts one item with retry policy and shared persistence logic.
4. Retryable/fatal realtime failures are persisted to `ingest_dlq_items`.

## Failure and Recovery Semantics

- Retryable failures are explicit and durable (`retry_wait` or DLQ record).
- Fatal failures are explicit and durable (`dead_letter` or DLQ record).
- Process restarts are first-class recovery events.
- Startup replay window catches updates missed during downtime.

## Catchup Worker Modules

### Entry points

- `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/src/main.rs`
  - CLI subcommands: `updater`, `catchup`.
  - Updater orchestration and task lifecycle wiring.
- `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/src/bin/catchup_only.rs`
  - Compatibility wrapper for one-shot catchup.

### Orchestration and sync service

- `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/lib/sync_service/catchup_orchestrator.rs`
  - Durable catchup planning, claiming, and segment result persistence.
- `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/lib/sync_service/mod.rs`
  - Realtime worker supervision and shared SyncService API.
- `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/lib/sync_service/updater_state.rs`
  - Load/save `last_sse_event_at` and startup replay anchor helpers.
- `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/lib/sync_service/dlq.rs`
  - Shared DLQ persistence model/helpers.

### Ingest core and executors

- `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/lib/sync_service/ingest_worker/core.rs`
  - Shared micro-level fetch/persist retry and batch flush behavior.
- `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/lib/sync_service/ingest_worker/segment_executor.rs`
  - Segment scheduler semantics.
- `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/lib/sync_service/ingest_worker/realtime_executor.rs`
  - Realtime per-item scheduler semantics.
- `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/lib/sync_service/ingest_worker/{fetcher,persister,retry,error_mapping}.rs`
  - Fetch/persist adapters and shared retry/error mapping primitives.

### Firebase listener and segment store

- `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/lib/firebase_listener/listener.rs`
  - SSE client, reconnect loop, reconnect gap-fill, queue backpressure handling.
- `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/lib/segment_manager/ops.rs`
  - Segment state SQL operations (`claim`, `mark done/retry_wait/dead_letter`, replay helpers).

### Integration tests

- `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/tests/catchup_e2e.rs`
- `/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/tests/updater_e2e.rs`

## Operations

Recommended baseline units:

- `infra/systemd/search-hn-updater.service` (long-running updater)
- `infra/systemd/search-hn-catchup.service` (manual/one-shot catchup)
- `infra/systemd/search-hn-catchup.timer` (optional scheduled catchup sweeps)

See `/Users/ritsuko/projects/data/search-hn/infra/systemd/README.md` for deployment steps.

## Defaults and Assumptions

- Default startup replay window is 3 days (`--startup-rescan-days 3`).
- Realtime queue is bounded and backpressure-based (default `--channel-capacity 4096`).
- Startup replay is process-start triggered; no in-process periodic replay loop.
- `updater_state.last_sse_event_at` is the persisted replay anchor.
- `items.last_fetched_at` is intentionally not part of the current schema.

## Non-goals (Current)

- No Redis/distributed queue layer.
- No multi-active deployment complexity beyond durable segment claiming.
- No periodic replay scheduler embedded in the updater process.

## Verification Expectations

Core regressions should be caught by unit + integration coverage in
`/Users/ritsuko/projects/data/search-hn/crates/catchup_worker/tests`:

- SSE reconnect + gap-fill behavior.
- Bounded queue backpressure behavior.
- Realtime worker supervision/restart behavior.
- Startup replay anchor persistence and restart behavior.
- Durable catchup segment claim/progress/replay semantics.

## Historical docs

- Historical gap log: `/Users/ritsuko/projects/data/search-hn/docs/v1-gaps.md`
