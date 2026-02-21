# V1 Gap Analysis: HN Index Mirroring (`catchup_worker`)

This document captures the core findings from review of the current HN mirror implementation in `crates/catchup_worker/`, focusing on:

- stack fit (Rust + Diesel + Postgres)
- system design for initial catchup/healing
- correctness and algorithmic risk (catchup detection, resume handling, realtime ingest)

Assumptions for this design pass:

- one catchup process active at a time
- personal archive workload (correctness + speed > high availability)
- Postgres is fully under our control; HN Firebase is not

## 1) Stack Fit

## Verdict
Rust + Postgres is a strong fit for this workload (concurrent I/O, durable ingest, idempotent upserts).
Diesel is acceptable for typed DB access, but current reliability/correctness issues are materially more important than any ORM swap.

## Recommendation
Do **not** re-platform in v1. Keep the stack and focus engineering effort on control-plane correctness (planner/checkpoints/retries/supervision).

## 2) System Design Review: Initial Catchup + Healing

## Verdict
The current model is not just buggy, it is structurally under-specified.
It infers progress from `MAX(id)` and hands giant static ranges to detached workers, so failure semantics, dead-letter handling, and resume correctness are all ambiguous.

An upfront refactor is likely cheaper than continued patching.

## 2.1 Failure Semantics Are Not Explicit

Today, an ID is effectively "done" only if it was inserted/updated in `items`.
That is too weak for HN because some IDs can be unavailable (`null`/deleted/inaccessible at fetch time), and transient failures are expected.

We need explicit terminal and non-terminal outcomes per ID:

- `Succeeded`: item persisted (or updated) successfully.
- `TerminalMissing`: upstream confirmed no materialized item for this ID (expected for some IDs).
- `RetryableFailure`: timeout/5xx/network/db transient failure.
- `FatalFailure`: invariant/schema/programming failure that should stop the run.

Without this distinction:

- dead-letter is undefined
- retries are ad hoc
- contiguous progress ("how far are we truly healed?") cannot be computed correctly

## 2.2 Static Giant Range Fanout Is Unsafe

Current behavior combines:

- coarse range partitioning
- many workers (`200`) with little supervision
- no durable ownership/lease/progress per chunk

Risks:

- one worker can fail after doing 90% of a huge range, and restart semantics are unclear
- planner cannot rebalance slow/failed chunks
- operator cannot tell which subrange is stuck vs complete

Replace with a simple work queue:

- generate **small segments** (1k to 10k IDs each), write them to `ingest_segments` in Postgres
- pop pending segments in a loop with bounded concurrency (16 to 64 in-flight item fetches)
- each segment has durable status (`pending`, `in_progress`, `done`, `retry_wait`, `dead_letter`)
- each segment updates heartbeat/progress so stalled work is visible and resumable

This gives oversight without distributed complexity. Keep the implementation a straightforward loop over a work queue — not an actor system or framework.

## 2.3 Resume/Healing Must Be Cursor-Based, Not `MAX(id)`-Based

`MAX(id)` is not a correctness cursor when holes exist.

Use a durable frontier definition:

- `frontier_id`: largest ID such that every ID `<= frontier_id` is in a terminal state (`Succeeded` or `TerminalMissing`).

Track this in two control-plane tables in Postgres:

- `ingest_segments(segment_id, start_id, end_id, status, attempts, scan_cursor_id, unresolved_count, heartbeat_at, started_at, last_error)`
- `ingest_exceptions(segment_id, item_id, state, attempts, next_retry_at, last_error, updated_at)`

(`ingest_runs` is deferred — for a single-process personal archive, a `started_at` column on segments is sufficient lineage. Add run-level tracking later if historical audit is needed.)

Single-process restart algorithm:

1. Read `maxitem` from HN.
2. Compute frontier from sealed segments.
3. Enqueue segments for unresolved IDs in `[frontier_id + 1, maxitem]` plus known holes behind frontier.
4. Process segments with retries/backoff; write per-ID rows only for non-happy-path IDs (`RetryableFailure`, `DeadLetter`, optional `TerminalMissing` audit).
5. Periodically advance `frontier_id` from contiguous `sealed` segments.
6. Process is complete when no non-terminal IDs remain up to target.

This yields deterministic restart behavior after crashes.

## 2.4 Dead-Letter Policy (Practical for Personal Archive)

Recommended behavior:

- Retry retryable errors with bounded exponential backoff + jitter.
- After `N` attempts (example: 8), move ID/segment to `dead_letter`.
- Do **not** silently drop dead letters; keep them queryable in `ingest_exceptions`.
- Treat truly terminal missing IDs as complete coverage, not failures.
- Dead letters are replayable via a SQL `UPDATE ... SET state = 'pending'` for now. Build a `--replay-dead-letter` CLI command later once actual dead-letter patterns are observed.

This keeps correctness auditable while avoiding infinite retries.

## 2.5 Why Refactor Up Front Is Reasonable

Most currently observed bugs stem from missing control-plane invariants, not isolated code defects.
Refactoring now to a durable segment-state model should remove whole classes of issues:

- silent partial success
- ambiguous resume semantics
- no dead-letter accountability
- unobservable stuck ranges

Patching one bug at a time without these invariants will likely cause churn.

## 2.6 Where Failure/Dead-Letter Logic Should Live

This should be application logic in `catchup_worker`, with Postgres as the authoritative state store.

- Worker path (`firebase_worker`) should only fetch item data, classify error type, and persist item/kids.
- Orchestration path (new module under `sync_service`) should own retries, segment leasing, dead-letter transitions, and frontier advancement.
- Segment state layer (new module under `lib/segment_manager/`) should own all reads/writes for segment/exception state.

Do not hide retry/dead-letter semantics in ad hoc worker loops; keep policy centralized in the orchestrator.

## 2.7 Postgres vs KV and Scale (30 to 60M IDs)

Recommendation: use Postgres tables, not a separate KV store.

- We need transactional coupling between `items` upserts and ingest-state transitions.
- We need range queries (`frontier`, segment scans, "what is unresolved between X and Y"), which are relational.
- Operationally, one database is simpler and easier to inspect for a personal archive.

To avoid endless denormalization at 30 to 60M IDs, use a sparse state model:

- Do not store a per-ID success row for every ID.
- Treat "successfully persisted into `items` during segment scan" as implicit success.
- Store per-ID rows only in `ingest_exceptions` for retry/dead-letter (and optional terminal-missing audit).
- Track segment-level completeness with `scan_cursor_id` and `unresolved_count`; mark segment `sealed` only when unresolved reaches zero.

This keeps control-plane rows proportional to trouble cases, not total corpus size.
If audit depth is needed later, add an optional history table or periodic snapshots without changing the core execution model.

## 2.8 SSE Reconnection and Realtime Gap Detection

The realtime path uses Firebase SSE to stream item updates. SSE connections **will** drop — HN's Firebase endpoint is not under our control.

This needs handling in v1:

- Automatic reconnection with exponential backoff.
- On reconnect, compare the last-seen SSE event ID against current `maxitem` to detect a gap window.
- If a gap is detected, enqueue the missed range as catchup segments before resuming SSE consumption.

Without this, items published during a disconnect window are silently lost. This is where catchup and realtime intersect and the two paths need to share segment-enqueue logic.

## 2.9 Explicit Non-Goals for V1

To keep scope bounded:

- **Item re-scanning / freshness**: Items mutate after initial fetch (scores, new comments, deletions). Periodic re-scanning of previously-fetched items is out of scope for v1. The realtime SSE path handles updates to items that HN pushes, but we are not proactively polling for staleness. This can be a v2 concern.
- **Multi-process / distributed coordination**: Single process, single Postgres. No distributed locking or queue.
- **Run-level audit history**: No `ingest_runs` table in v1. Add later if needed.

## 3) Critical Implementation Bugs (Still Real)

These are concrete defects in the current codebase and should still be fixed, but they are downstream of the design gaps above.

Status as of 2026-02-07:
- Item #4 is completed (`items.parts` migrated to `BIGINT[]` and schema aligned).
- WP0 is completed.
- WP1 is completed (schema migration + DB access layer + DB tests).

## Critical

1. Catchup worker failures are effectively ignored.
- `tokio::spawn` workers return `Result<(), Error>`, but join handling treats `Ok(_)` as success without checking inner `Err`.
- Impact: catchup can report success while silently missing data.
- Code: `crates/catchup_worker/lib/sync_service/mod.rs`

2. Realtime workers are unsupervised and detached.
- `realtime_update` spawns workers and returns immediately; handles are dropped.
- Impact: worker crashes are invisible and not restarted.
- Code: `crates/catchup_worker/lib/sync_service/mod.rs`

3. Missing-range detection logic is incorrect for resume/catchup.
- SQL binds parameters that are not used.
- Query is not bounded by requested min/max.
- Returned ranges are artificially expanded (`start - 1`, `end + 1`), creating overlap/churn.
- Impact: incorrect gap healing behavior and potentially invalid resume semantics.
- Code: `crates/catchup_worker/lib/sync_service/ranges.rs`

## High

4. Schema/type mismatch for `parts`. (DONE 2026-02-07)
- Migration defines `items.parts` as `TEXT`.
- Diesel schema/model expects `Nullable<Array<Int8>>` / `Option<Vec<i64>>`.
- Impact: runtime breakage or data corruption risk.
- **This must be fixed via migration before the refactor begins** — it affects data integrity of the existing corpus.
- Implemented via: `crates/hn_core/migrations/2026-02-07-000001_fix_items_parts_type/up.sql`, `crates/hn_core/src/db/schema.rs`

5. No retry/backoff strategy for transient failures.
- Network/db errors bubble up and terminate workers.
- Impact: low resilience; realtime and catchup can degrade or stall after transient issues.
- Code: `crates/catchup_worker/lib/sync_service/firebase_worker.rs`

6. Unbounded realtime queue (`flume::unbounded`).
- Impact: potential unbounded memory growth under backlog/failure.
- Code: `crates/catchup_worker/src/main.rs`

## Medium

7. Catchup range math can overrun and is off-by-one.
- `catchup_amt` with inclusive range behaves like `n + 1`.
- `max_id` is not clamped against upstream `maxitem`.
- Impact: unnecessary/invalid fetch attempts and ambiguous operator expectations.
- Code: `crates/catchup_worker/lib/sync_service/mod.rs`

8. Panic path exists in normal worker code.
- `panic!("Perverse situation")` in catchup flow.
- Impact: process crash risk from edge cases.
- Code: `crates/catchup_worker/lib/sync_service/firebase_worker.rs`

9. Realtime metric double-count.
- `records_pulled` incremented in two places.
- Impact: monitoring distortion.
- Code: `crates/catchup_worker/lib/sync_service/firebase_worker.rs`

## 4) Implementation Plan (Work Packages)

Designed to be parallelized across 3-6 agents. Dependencies are noted; independent packages can run concurrently.

### WP0: Pre-requisite migration (do first) [DONE 2026-02-07]

Fix the `parts` column type mismatch: add a new Diesel migration to `ALTER TABLE items ALTER COLUMN parts TYPE INT8[] USING ...`. Update `schema.rs` and `models.rs` to match. This unblocks everything else.

Completed:
- Added migration: `crates/hn_core/migrations/2026-02-07-000001_fix_items_parts_type/`
- Synced schema: `crates/hn_core/src/db/schema.rs`

**Depends on**: nothing.
**Files**: `migrations/`, `lib/db/schema.rs`, `lib/db/models.rs`

### WP1: Control-plane schema + DB access layer [DONE 2026-02-07]

- Add migration for `ingest_segments` and `ingest_exceptions` tables.
- New module `segment_manager` with CRUD operations: create/query/update segments, record exceptions, compute frontier.
- Unit tests for the DB access layer (can use an isolated test database or transaction rollback).

Current status:
- Migration complete: `crates/hn_core/migrations/2026-02-07-000002_add_ingest_state_tables/`
- Diesel schema entries added: `crates/hn_core/src/db/schema.rs`
- DB access layer complete: `crates/catchup_worker/lib/segment_manager/`
- Tests complete: `crates/catchup_worker/lib/segment_manager/ops.rs` (colocated unit tests), `crates/catchup_worker/tests/sqlite_test_harness.rs`

**Depends on**: WP0.
**Files**: `migrations/`, `lib/segment_manager/`, `lib/db/mod.rs`

### WP2: Segment-based orchestrator

- New orchestration module replacing the current `SyncService::catchup` flow.
- Generates segments, writes them to Postgres, processes them in a bounded-concurrency loop.
- Calls into `firebase_worker` for actual fetches; classifies outcomes and writes segment state.
- Implements frontier advancement.
- Resume logic: on startup, pick up `in_progress`/`pending` segments from DB.

**Depends on**: WP1.
**Files**: `lib/sync_service/mod.rs` (major rewrite), new orchestrator module

### WP3: Retry + dead-letter semantics

- Error classification in `firebase_worker`: retryable vs terminal-missing vs fatal.
- Bounded exponential backoff + jitter per segment/item.
- After N attempts, write to `ingest_exceptions` with `state = dead_letter`.
- Remove the `panic!("Perverse situation")` path — return an error instead.

**Depends on**: WP1 (for `ingest_exceptions` table). Can be developed in parallel with WP2 and integrated.
**Files**: `lib/sync_service/firebase_worker.rs`, `lib/sync_service/error.rs`

### WP4: Realtime path fixes

- SSE reconnection with backoff in `firebase_listener`.
- Gap detection on reconnect: compare last-seen event vs `maxitem`, enqueue missed range as segments (reuse WP2 segment-enqueue logic).
- Supervised realtime workers: hold `JoinHandle`s, restart on crash.
- Bounded channel (`flume::bounded`) replacing `flume::unbounded`.
- Fix metric double-count.

**Depends on**: WP2 (for segment-enqueue reuse). Listener-level reconnection can start independently.
**Files**: `lib/firebase_listener/listener.rs`, `lib/sync_service/mod.rs`, `src/main.rs`

### WP5: Main entrypoint + CLI wiring

- Wire new orchestrator into `main.rs`.
- Remove hardcoded magic numbers (200 workers, 32 updaters) — move to config/CLI args.
- Ensure graceful shutdown propagates to orchestrator and realtime workers.
- Clamp `max_id` against upstream `maxitem`.

**Depends on**: WP2, WP4.
**Files**: `src/main.rs`, `lib/cli/mod.rs`, `lib/config/mod.rs`

## Implementation Risk: Diesel Async Transactions

Multiple work packages require transactional coupling between `items` upserts and segment state updates. `diesel-async` supports transactions but has sharp edges around connection pool management and error propagation. Expect friction here — budget time for getting transaction scoping right rather than treating it as trivial plumbing.

## 5) Testing Gaps

Current `catchup_worker` has no tests. Tests should be added as part of each work package, not as a separate phase.

Minimum coverage:

- **WP1**: unit tests for segment CRUD and frontier computation
- **WP2**: unit tests for segment planning; integration test for crash/resume mid-segment
- **WP3**: unit tests for error classification (`retryable` vs `terminal_missing` vs `fatal`); integration test for dead-letter creation
- **WP4**: unit test for gap detection on reconnect
- **WP5**: regression test for idempotent upsert behavior (`items` + `kids`)
