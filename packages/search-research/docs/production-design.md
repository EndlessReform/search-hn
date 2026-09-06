# Production search design

Status: proposal for the implementation phase, following the completed research.
The [whitepaper](../whitepaper/search-hn.typ) explains the selected recipe;
[reproduction](reproduction.md) covers archived evidence and serving setup.

Start with Pplx 0.6B BF16 through stock vLLM, 1024 dimensions, PostgreSQL pgvector
and title-only pg_textsearch BM25, lexical RRF weight 0.125, and provisional HNSW
`ef_search=1000`. The sections below describe proposed ownership and lifecycle;
the research harness does not implement this production service.

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

## Implementation decisions and acceptance checks

- Set admission score, retention window, text-only story handling, and a target
  source-update-to-searchable delay. Confirm ingestion observes threshold changes.
- Evaluate the combined BF16/hybrid/HNSW recipe in the agent, retaining exact
  retrieval as a reference. Use fresh natural queries alongside the frozen
  regression set; the latter has already been used extensively for tuning.
- Measure full-corpus latency and concurrent query/backfill load on deployment
  hardware. Test vote/date/domain filters for candidate starvation and establish
  the required filtered ANN recovery before choosing final query plans.
- Validate extension packaging, upgrades, backup/restore, and index rebuilds on
  the deployment architecture.
- Test edits racing inference, threshold crossings, deletion during pagination,
  worker retry/crash, GPU outage, backfill restart, and generation rollback.
- Version tool/prompt changes separately, including removal of duplicated search
  payloads, so future agent comparisons have an interpretable baseline.

The miss audit found weak title/URL identification in ten of 18 residual cases
and reasonable alternative answers in seven. Broader relevance judgments and
fresh questions should guide any later decision to index comments or bodies.
