# Production rollout and cache evidence — September 7, 2026

Dated observations, not deployment instructions. See [current status](../../search-status.md)
and [cache operations](../../search-cache-operations.md) for the operational entry points.

## Migration validation — September 7, 2026

On the disposable PG17 host `searchhn-deploy-test`, a fresh database
`searchhn_prewarm_check_1788791283` and an explicitly non-superuser owner verified:
initial unprivileged installation fails with the expected superuser requirement;
superuser installation succeeds; the same up SQL succeeds as the unprivileged
owner afterward; down/up preserves the utility; warming a fixture table and its
primary-key index succeeds. Extension version was 1.2. The test database and role
were removed. Existing restored databases and production were untouched.

The rehearsal host's existing `admin` role was found to be elevated, so it was
not used as evidence of production's non-superuser behavior. These checks execute
the migration SQL transactionally through psql; they are not a claim of an
end-to-end Diesel CLI or production rollout test. The verification shell script
also passed `bash -n`.

## Post-change check — September 7, 2026

Live settings verified: 4 GB shared buffers, 12 GB effective cache estimate,
I/O timing on, and pg_textsearch preload retained. Worker ingestion and embedding
activity were healthy; 509,204 search rows were embedded with zero pending at the
check. The pg_prewarm 1.2 extension existed, but migration `20260907000013` was
**not yet recorded** in Diesel. This was the state at that check; the final audit below supersedes it.

Initial verification still found 874–1,357 ms first executions, dominated by
read waits. Repeating the operational warming brought all four query executions
to 32–38 ms with zero shared reads. A fresh hydroponics query still took 119 ms,
including 51 ms of read waits. Inspection found 1,348 MB of TOAST storage: the
original warming SQL omitted this out-of-line vector storage. The checked-in
`prewarm.sql` now warms both TOAST and its index as well as the main relations.
These cache reads were also performed live. A subsequently new ocean-exploration
query took 69 ms with only 4 ms of read waiting (1,098 shared reads), so this is
not a claim of universal zero misses or a fixed latency SLA. No further restart
or re-embedding was performed. The reason the earlier main-index pages were cold
was not established; there is no durable record here proving when the operator's
first warming completed relative to restart and replay.

## Final audit update

A subsequent read-only ledger query confirmed `20260907000013` applied alongside
`20260906000012`. The earlier pending-migration observation above is historical.
See [current status](../../search-status.md) for the latest consolidated handoff.

## Retained raw evidence

These JSON files were copied byte-for-byte from this session's ephemeral EXPLAIN
outputs. They contain the complete PostgreSQL plan, including the query vector;
no credentials. `SHA256SUMS` identifies the retained copies. Run its check from
repository root. All use 100 candidates/branch, ef_search=1000, RRF k=60 and weights
1 / 0.125. SQL timing excludes embedding HTTP. Labels describe observed first/repeat
access; no cache flush was used to create controlled cold conditions.

| File | Query and configuration | SQL ms |
| --- | --- | ---: |
| `compiler-cold.json` | How to build a programming language compiler; 128 MB buffers, timing off | 397.105 |
| `compiler-repeat.json` | Same query immediately repeated | 26.054 |
| `solar-io-timed.json` | Solar panels and home battery energy storage; 128 MB, session I/O timing on | 422.976 |
| `hydroponics-before-toast-warm.json` | Growing vegetables with hydroponics indoors; 4 GB, main relations warmed | 119.492 |
| `ocean-after-toast-warm.json` | Ocean exploration and deep sea submarines; 4 GB, TOAST also warmed | 69.419 |

The four-query 32–38 ms result and initial 874–1,357 ms result are summaries of
captured tool output, not retained raw plans: the profiling harness deleted those
scratch plans. Do not fabricate missing logs or describe this as a controlled
benchmark. New API responses can differ with BF16 serving; reproducing the text
query does not guarantee bit-identical embeddings.

## Operational handoff facts retained from September 6–7

The worker's DNS repair was verified earlier in the session: `/etc/resolv.conf`
points to `/run/systemd/resolve/stub-resolv.conf`, and inference/DB hostnames resolve.
The overnight note records removal of the temporary hosts override and no ACL
changes; those are dated observations, not instructions to redo DNS changes.
Original resolver backup: `/etc/resolv.conf.before-searchhn-dns-repair` on the worker.
The live batch audit remains unresolved; no new inference-host configuration was
inspected or deployed during cache acceptance. Production normal systemd PG17
restart was verified; the older OrbStack PID-tracking bypass remains test-only.

## Textual agent direct hybrid integration

Implemented locally on September 7, by the user's choice of direct database
access for this slice. The existing Textual `SearchRuntime` defaults to the new
production repository; Axum remains deferred. Headless and FastAPI use the same
Python repository. No production schema, grants, extensions, worker or service
configuration was changed. The local ignored `.env` now contains the production
`EMBEDDING_BASE_URL`; its existing production database setting was preserved.

Read-only application checks used production `readonly_hn_agent` and the deployed
embedding proxy. The live table comment matched
`pplx-0.6b-2c4d510dd4a7-vllm0.28.0-bf16-flash-mean-2048-tanh127-rne-v1`.

- [Initial application results](agent-hybrid.json): top-10 IDs, both branch ranks
  and RRF matched the SQL prototype using the **same captured query vector**;
  three pages were disjoint. Score, inclusive date, included/excluded domain,
  date sorting and date-only browsing produced constrained results. An injected
  inference timeout produced labelled keyword-only results with real BM25 SQL.
- [After limiting date browsing to `story_search`](agent-hybrid-final.json): same
  correctness checks, plus client-observed SQL timings. Date-only browsing dropped
  from 12.43 s in the initial run to 1.48 s here. These sequential observations are
  not a controlled benchmark and cache state was not reset.
- [Final plan-capture run](agent-hybrid-planned.json) and
  [full EXPLAIN JSON](agent-hybrid-plans.json): the unfiltered compiler query uses
  `story_search_embedding_hnsw` and `story_search_title_bm25`. Explicit filters use
  exact vector ranking; PostgreSQL chose scan-and-sort BM25 plans for the score
  and GitHub-domain examples, and BM25 index plans for the other text examples.
  These are EXPLAIN plans, **not EXPLAIN ANALYZE**; SQL timings are recorded
  separately and include client round trips.

Last-run request timings, including query embedding where applicable and initial
source payload retrieval, excluding the conversational model:

| Query | Constraint | ms |
| --- | --- | ---: |
| How to build a programming language compiler | none | 322 |
| Solar panels and home battery energy storage | score >= 500 | 3,717 |
| Growing vegetables with hydroponics indoors | 2024-01-01 through 2026-09-07 | 1,350 |
| database | github.com only | 3,861 |
| Ocean exploration and deep sea submarines | exclude youtube.com, sort date | 1,891 |
| date-only browse | September 6 | 1,151 |

The compiler request also measured 379 ms and 1,707 ms in earlier runs. These
observations do not establish a latency SLA. Broad filtered retrieval and source
joins remain an optimization opportunity; no indexes or denormalization were added.

Actual RRF example: story 2661452, “How do I create my own programming language
and a compiler for it”, ranked first in both branches. Its fusion score is
`1/(60+1) + 0.125/(60+1) = 0.018442622950819672`. Its HN score was 83 in the check;
the two scores have different meanings.

Local tests cover native coordinate validation, model/header/body recipe mismatch,
RRF arithmetic, runtime default and prompt, cache expiration/reset, live-page
payload refresh with a simulated deletion, stable keyword-only fallback and
filter-only inference avoidance. Existing tests cover tools, citations, CLI and
headless behavior. Production rows were not mutated to test deletion. No full
conversational-model evaluation, frozen quality benchmark, Rust web cutover or
service deployment is claimed.

Final local validation: **61 tests passed, 1 existing optional database integration
test skipped** (`uv run --package search-agent pytest packages/search-agent/tests -q`).
The production harness above independently exercised real database retrieval.
The existing local `.env` context was also constructed and verified against the
live recipe, confirming the default Textual startup configuration resolves to
`ProductionStoryRepository`.

Reproduce with [the read-only harness](../../../tools/search/validate-agent.py),
using `--output` for results and optional `--plans` for complete EXPLAIN JSON.
