# V1 Gap Analysis: HN Index Mirroring (catchup_worker)

This document captures the core findings from review of the current HN mirror implementation in `crates/catchup_worker/`, focusing on:

- stack fit (Rust + Diesel + Postgres)
- correctness and algorithmic risk (catchup detection, resume handling, realtime ingest)

## 1) Stack Fit

## Verdict
Rust + Postgres is a strong fit for this workload (concurrent I/O, durable ingest, idempotent upserts).  
Diesel is acceptable for typed DB access, but current reliability/correctness issues are materially more important than any ORM swap.

## Recommendation
Do **not** re-platform in v1. Keep the stack and focus engineering effort on correctness, retry/supervision, and checkpointing.

## 2) Core Gaps and Bugs

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

4. Schema/type mismatch for `parts`.
- Migration defines `items.parts` as `TEXT`.
- Diesel schema/model expects `Nullable<Array<Int8>>` / `Option<Vec<i64>>`.
- Impact: runtime breakage or data corruption risk.
- Code: `crates/catchup_worker/migrations/2024-05-19-011100_create_hn_tables/up.sql`, `crates/catchup_worker/lib/db/schema.rs`, `crates/catchup_worker/lib/db/models.rs`

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

## 3) Resume/Catchup Design Gaps

Current resume behavior is inferred from DB shape (`MAX(id)` + ad-hoc gap scan), not from explicit ingestion state.

For robust resumability, add durable state:
- high-water mark (latest fully processed contiguous ID)
- job state/checkpoint (in-progress ranges + last successful flush)
- deterministic retry policy (bounded backoff, jitter, retryable classification)

This enables:
- precise restart after crash
- clear semantics for `catchup_start` / `catchup_amt`
- easier correctness auditing

## 4) Priority Fix Plan (Suggested)

1. Fix worker supervision and error propagation.
- Track join handles and fail/restart intentionally.
- Surface inner worker errors.

2. Correct and bound gap detection.
- Replace current query with a min/max constrained algorithm.
- Remove range expansion hacks.

3. Add retries/backoff + bounded realtime channel.
- Prevent single transient failures from killing ingest.
- Apply backpressure instead of unbounded buffering.

4. Resolve schema mismatch (`parts`) via migration + verification.
- Align SQL schema and Diesel model types.

5. Introduce explicit checkpointing for resume correctness.
- Move from inferred progress to stored progress.

## 5) Testing Gaps

Current catchup_worker has no tests.

Add at minimum:
- unit tests for range splitting and gap detection boundaries
- integration test for catchup resume after simulated crash
- integration test for transient fetch/db failure + retry behavior
- regression test for idempotent upsert behavior (item + kids)
