# Sovereign story search — research handoff

Research closed 2026-09-06. Productionization is a separate conversation.
Start with the [Typst whitepaper](../whitepaper/search-hn.typ), which consolidates
notes 00–25 and supersedes their changing recommendations.

Selected starting point: Pplx 0.6B BF16 through stock vLLM, 1024 dimensions,
PostgreSQL pgvector + title-only pg_textsearch BM25, lexical RRF weight .125,
and provisional HNSW ef_search=1000. Filtered ANN, full-corpus behavior, and the
combined agentic recipe remain implementation acceptance checks.

## Archive and cleanup COMPLETE

- [x] Published `research-20260906-v4`: 8,987 files, 871,885,024 logical bytes.
- [x] Downloaded and SHA256-verified all 8,922 distinct blobs before deletion.
- [x] Removed completed local experiment trees and rejected embedding arrays.
- [x] Removed the dedicated scratch PostgreSQL container, volume and image.
- [x] Removed VM TEI containers/image, diagnostic CUDA image, rejected-model
  weights, reference/Nemotron environments, stopped FP32 control, and old tunnel.
- [x] Preserved selected Pplx weights, vLLM image/server, source, evals and traces.

The [closeout record](26-research-closeout.md) contains the exact TEI workaround,
release receipts, disk accounting, and retained scope. Original numbered notes
remain historical evidence; their earlier OPEN/pending statements are superseded
by this closeout. No production DB, unrelated data, or unit tests were removed.

## Historical implementation design

The following design is retained for review, not a statement that the proposed
production lifecycle is already implemented.

## Phase 0 — validate the remaining problem before extending the system

Initial [four-Luna miss audit](09-miss-audit.md) completed: all 18 residual questions
have answer evidence, but ten have weak title/URL identification and eight are
plausible. Seven have reasonable alternative answers; one is explicitly flagged
as an ambiguous target. These annotations do not alter scores or establish that
comment indexing is necessary. Eight cases already succeed in one new treatment.

Do this before more engine tuning or broader content indexing. The current
baseline is useful enough that additional work should be justified by observed
failure mechanisms, not by the availability of another retrieval technique.

1. **Audit the misses, with title/URL as the retrieval boundary.** Four Luna
   reviewers inspect all 18 questions missed by either final treatment. Separate
   a clear title-level retrieval/agent gap from an ambiguous target, unsupported
   question detail, and a clue absent from the indexed representation. Compare
   actual searches, returned alternatives and stopping behavior. Suggested queries
   informed by the known target are hypotheses, not demonstrated retrieval fixes.
   Finding the story and answering its comment-dependent detail are different
   requirements: reading comments after finding a story does not require indexing
   comments. Annotate separately; do not silently clean labels after seeing scores.
2. **Change only the embedding model next.** Keep corpus, questions, tool contract,
   prompt and retrieval settings fixed when comparing a few sovereign candidates
   with TE3. Run fresh trajectories for finalists. Do not bundle a model swap
   with prompt changes, richer document text and another fusion sweep.
3. **Keep dense-only a real candidate.** Hybrid helps entity-query efficiency here
   but not paraphrase efficiency or final exposure. Require it to earn its added
   work on the local model; neither commit to nor discard it on this small sample.
4. **Inspect quiet failures separately.** Both new treatments normally complete
   without target exposure on 13 questions. Check reasonable alternative answers,
   unsupported assertions, and premature stopping. Retrieval and answer correctness
   need separate scorecards; this audit is not a comprehensive answer grading run.
5. **Freeze this set as development/regression data.** We have inspected and tuned
   against it extensively. Gather a small untouched set of naturally phrased real
   searches for acceptance testing; retain synthetic question/style limitations.
   Question repairs belong in a new dataset version, with the old labels retained.
6. **Remove avoidable payload overhead before the next cost comparison.** The
   single-query tool response currently duplicates its results. Correct that in a
   separately recorded interface revision; do not rewrite the archived baseline.

Exit criterion: an evidence-backed miss taxonomy and a short list of genuinely
title-retrievable gaps worth testing. Do not use this audit as automatic authority
to index all comments or crawl article bodies. A locally served model, reliable
index maintenance and a dependable search endpoint are the next product milestone;
another ranking stage is not a milestone in itself.

The user-supplied Algolia screenshot is illustrative UI context, not evidence of
Algolia's indexing internals. No claims about its implementation are needed here.

## Requirements before abstractions

1. A client asks for stories using text, optional filters/sort, and pagination.
   It never selects an embedding provider/dimension or supplies a document vector.
2. Eligible source changes eventually become searchable without blocking ingestion
   on GPU work. Initial backfill and incremental maintenance follow the same rule.
3. Online queries remain responsive while backfill runs, and a GPU outage does not
   lose indexing work. Staleness/fallback must be observable rather than silent.
4. Model artifacts and indexed data remain usable without any external vendor.
5. Experiments remain reproducible: frozen labels, explicit corpus/prompt/index
   versions, preserved failed attempts, and separate infrastructure/model outcomes.

## Existing system and proposed authority

The homepage server is Rust/Axum (`crates/hn_app/src/main.rs`); a separate small
FastAPI wrapper exists in `packages/search-agent/src/search_agent/app.py`. The
experimental agent repository directly accesses PG and OpenAI. That last shortcut
must not dictate production ownership.

- **Axum/search server:** owns retrieval policy and the active index generation;
  sends synchronous query inference to the embedding server, queries PG and fuses
  rankings. Read access to vectors; no document-vector writes. TUI/headless clients
  share this API. A separate Python inference process is fine, not another public
  search authority.
- **Catchup/realtime ingestion:** owns source rows and records required work
  atomically with relevant updates. It does not invoke the GPU inside a DB
  transaction and does not write vector values.
- **Backfill/reconciler:** scans source eligibility and missing/stale representations,
  submitting the same work as ingestion. Not a separate embedding implementation.
- **Embedding worker:** sole ordinary writer of vectors and job completion state.
  Reads source/model configuration, claims jobs, calls inference, validates output
  and conditionally commits the result. Database grants should enforce the roles.
- **Inference server:** owns compute, not DB state; no database credentials.
- **Deployment/operator:** installs pinned model artifacts and authorizes building/
  activating a new index generation. Clients cannot change the active generation.

## Admission, freshness and removal

The experimental >=25-vote/two-year scope is provisional, not an irrevocable
production policy. Define eligibility once: story type, usable content, nondead/
nondeleted, admission score, and retention window. Search filters cannot recover
stories excluded from the embedded corpus. Consider admission below the normal
serving floor if lowering that floor later must work immediately.

An item arriving at score 1 does not need embedding under a 25-vote admission rule;
an observed update crossing the threshold does. Hash only the canonical embedded
content (initial candidate: title + URL, with explicit handling of text-only stories).
Title/URL changes invalidate that representation; votes/comment counts do not,
unless they are deliberately included in the text recipe. Dead/deleted stories
must stop serving promptly; retaining an unused vector is distinct from serving it.
Window aging needs filtering/cleanup, not inference. Avoid repeated work on score
oscillation when an identical vector already exists.

Realtime updates and startup/recovery rescans already exist. Verify that they
observe relevant score changes: reconciliation can repair missing vectors for
local rows, but cannot discover upstream changes that ingestion never fetched.

## Durable work and backfill

Start with a PG work table, not a broker. Coalesce desired work per story/index
generation; track desired content hash, availability, attempt count and lease.
Source write + enqueue should be one transaction at the shared persister boundary.
All other source writers must use the same mechanism or be covered explicitly;
do not assume the current separate upsert statements already offer this guarantee.

Claim a bounded batch in a short transaction; release DB locks before inference.
On completion, verify the generation, vector shape/finite values, current content
hash and eligibility. Atomically upsert a matching vector and acknowledge its job.
A newer desired hash must not be erased by an older worker's acknowledgement.
Expired claims are retryable; repeated failures remain visible for inspection.
Inference can be at-least-once; writes must be idempotent.

Backfill keyset-scans eligible IDs with durable progress and bounded queue growth.
Run incremental work concurrently at higher priority. After the scan, reconcile
missing/stale rows to close races and record coverage before activation. Backfill
and reconciliation call the same eligibility/text-version logic as ingestion.

## Query path, pagination and GPU contention

Cache query vectors server-side by session, exact query and index generation.
Cache the ranked candidate IDs for stable, bounded pagination; bind page tokens
to query, filters, sort, page size and generation. Expire sessions and bound memory.
Recheck deletion/serving eligibility when hydrating cached IDs, accepting shorter
pages when necessary. No duplicate document embedding work on a query cache miss.

Online query inference is synchronous, with priority over bounded document batches;
it does not wait in the durable backfill queue. Measure contention on the actual
3060 (confirm VRAM), not the 5090 used for the reranker. Start with one GPU runtime
and conservative batching. Define a timeout and explicit lexical-only fallback for
hybrid; dense-only should return an explicit failure rather than pretend fallback
is dense retrieval. Observe queue age, missing/stale coverage, inference latency,
fallback frequency and memory; no elaborate observability platform required.

Start with relevance order; date/vote sorting is opt-in. Specify whether an explicit
sort covers the full matching corpus or just the retrieved candidates—the spike
currently does the latter for textual queries. Carry pagination up to three pages
as an initial product bound, not as a constraint imposed by the database.

## Model lifecycle and sovereignty

Retain weights, tokenizer, model revision, license, preprocessing, query/document
prefixes, truncation, pooling, normalization, precision and dimension as one pinned
representation recipe. Dimensions alone cannot establish vector compatibility.
The server selects the active generation; inference verifies it serves that recipe.
Do not assume prefix shortening works for arbitrary local models just because it
worked for TE3. Benchmark each candidate's documented representation.

Build a new generation alongside the old one. After coverage and quality checks,
switch query encoder and document index together; keep old artifacts/index for
rollback and define behavior for sessions pinned to the old generation. Retention
and cleanup are operator actions, not automatic deletion during a migration.

## Next decisions / acceptance checks

- Agree on admission/retention, text-only stories, title/URL versus body, and
  acceptable source-update-to-searchable delay. These decide backfill work and size.
- Compare sovereign models, preferably within 0.125–1B, on frozen static
  queries, then fresh trajectories for finalists. Record quality, index size,
  query p50/p95 and backfill throughput on the actual 3060. Keep prompt changes
  separate; current keyword guidance is a known confounder.
- Validate extension packaging, upgrades, backups/restores and index rebuilds on
  the deployment architecture. A successful disposable container is not yet this.
- Test threshold crossings, edits racing inference, deletion during pagination,
  worker crash/retry, GPU outage, backfill restart and generation rollback before
  production. Review this document before implementing those responsibilities.

The research harness, local Perplexity server, and model bake-off are complete.
Durable artifacts and serving recipes are archived. The next stage is the
production implementation and acceptance checks described at the top of this
document; the research harness remains a reference for that work.
