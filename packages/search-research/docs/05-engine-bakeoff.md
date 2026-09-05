# Ephemeral PostgreSQL vs DuckDB retrieval

Outputs: `data/pg-duckdb-bakeoff-20260904/`. This directory is durable and ignored
by Git, not tmpfs. The production PG mirror is not modified or used by the driver.

## Inputs and contract

- Exactly the existing 105,081-story title+URL corpus and 196 questions from
  `data/te3-large-baseline-20260904/`; no resampling or new document embeddings.
- Native exact cosine queries in both PostgreSQL/pgvector and DuckDB, at
  256/512/768/1024/1536/3072 dimensions. Prefix truncation followed by L2
  normalization, float32 storage, ascending story-ID tie break.
- Lexical candidates: DuckDB's existing Porter/English-stopword BM25 index,
  versus PG English stemming, explicit OR and built-in text ranking.
- Candidate depth 100, equal-weight reciprocal rank fusion, k=60. Recall/NDCG
  cutoffs 1/5/8/10/20. One known relevant story, other stories unjudged.
- PG lexical sweep: `ts_rank` and `ts_rank_cd`, each with normalization masks
  0/1/2/8/16/10. `ts_rank(...,1)` selected by best lexical NDCG@8 on these
  questions. This is exploratory in-sample tuning, not held-out validation.
  Original `ts_rank_cd(...,32)` results remain available separately.
- HNSW: 1536-dimensional float32 cosine, m=16, ef_construction=128,
  ef_search=100/200/400, compared with exact top-100 rankings. Its results are
  separate from the exact-search main comparison and trajectory replay.

## Frozen trajectory replay

All 4,604 consumed result lists from the three-model run are replayed against
both databases at 1536 dimensions, including domain/date/score constraints.
The 4,039 unique nonempty literal strings are embedded once; actual usage is
24,748 tokens, $0.00321724. The user explicitly approved this additional payload.
The original embedding rate limiter, durable shards and budget guard are reused.

Array-shaped Qwen strings are retained literally, not repaired. Genuine batched
queries are already expanded in the recorded lists. Filter-only and daily-top
requests retain score ordering and do not invent a semantic query. Domain fields
are derived identically from frozen URLs for both engines. The matched corpus
here is the frozen score>=10 two-year slice, not the original full live mirror.

`search_budget` counts ordered individual lists, including alternatives inside a
batch (same convention as the original query_pass@K), not model turns. We report
budgets 1/3/5/10/20/all and result cutoffs 8/20. All 196 cases per model remain in
the denominator, including no-search failures. The replay asks whether one of
the fixed searches would return the target; it does not generate new agent turns.

PG and DuckDB are evaluated concurrently, at most one request per engine. PG
exact search deliberately sorts `(distance)+0` to prevent an existing HNSW index
from turning this baseline into approximate retrieval. Identical query/filter
requests are cached, independently of target ID. Per-query timings are diagnostic:
the early replay prefix overlapped HNSW construction and ran serially before a
checkpointed switch to concurrent engines. Do not treat these as a controlled
throughput benchmark.

## Resources

OrbStack exposed 16 GB; PG capped at 10 GB and four CPUs, with 2 GB shared buffers,
2 GB maintenance memory, 128 MB work memory, JIT off, and two parallel query/
maintenance workers. Native DuckDB gets four threads with 4 GB vector-query and
2 GB lexical-query memory limits. No model inference runs locally.

The dedicated container is `searchhn-pg-bakeoff-20260904`; data uses the same-named
Docker volume. Port 55432 is bound only to loopback. Trust auth is for this public
data scratch instance only, not a deployment recommendation. Server version,
settings and corpus hash are recorded in `pg-config.json`; the image is pinned
by digest in `compose.bakeoff.yaml`.

## Reproduce

From repository root, with the original embedding artifacts present:

```sh
# For the already-created container:
docker start searchhn-pg-bakeoff-20260904
# On a fresh machine, instead:
docker compose -f packages/search-research/compose.bakeoff.yaml up -d

export OPENBLAS_NUM_THREADS=4
export VECLIB_MAXIMUM_THREADS=4
uv run --locked --package search-research python -m search_research.engine_data load
uv run --locked --package search-research python -m search_research.engine_bakeoff static
uv run --locked --package search-research python -m search_research.pg_lexical_sweep
# Requires OPENAI_API_KEY only for missing query embedding shards:
uv run --locked --package search-research python -m search_research.engine_data embed
uv run --locked --package search-research python -m search_research.engine_bakeoff replay
uv run --locked --package search-research python -m search_research.engine_ann
uv run --locked --package search-research python -m search_research.engine_report
uv run --locked --package search-research python -m search_research.engine_audit static
uv run --locked --package search-research python -m search_research.engine_audit replay
uv run --locked --package search-research python -m search_research.fusion_sweep
uv run --locked --package search-research pytest packages/search-research/tests
docker stop searchhn-pg-bakeoff-20260904
```

Static/replay journals resume completed cases and fsync each result. Loading is
idempotent for a complete table and fails clearly on a partial table. Run only
one driver against the shared DuckDB file at a time. The lexical and ANN sweeps
should each be run once per output directory: their raw journals append, while
their Parquet summaries describe the latest invocation. Reusing an existing ANN
index does not remeasure its build time.

## Artifacts

- `comparison.html`, `comparison.csv/parquet`: static comparisons, including
  tuned PG hybrid formed from the exact dense and selected lexical rankings.
- `static.jsonl`, `static-summary.csv/parquet`: original per-engine runs.
- `pg-lexical-sweep.csv/parquet/jsonl`: all 12 lexical configurations.
- `replay.jsonl`, `replay.parquet`, `replay-summary.csv`: per-search neighbors
  and cumulative target retrieval under each search budget.
- `ann-summary.csv`, `ann.parquet`, `ann-config.json`, `ann-plan.json`: indexed
  search quality, exact-neighbor overlap, index footprint and verified query plan.
- `trajectory-inputs.parquet`, `queries/*.npy`, `requests.jsonl`: frozen replay
  strings, embeddings and API usage receipts.

## Completed static results

Every case's dense Recall/NDCG at every tested cutoff reproduces the original
NumPy baseline in both native databases, across all six dimensions. Native
DuckDB hybrid also reproduces every original case metric.

At 1536 dimensions (196 questions):

| Method | PG Recall@8 | DuckDB Recall@8 |
| --- | ---: | ---: |
| Dense exact | 65.8% | 65.8% |
| Lexical (selected PG ranker) | 45.9% | 60.2% |
| Hybrid, equal RRF | 63.3% | 71.4% |
| Hybrid, lexical weight 0.5 | 69.4% | 74.0% |

The final row is an additional in-sample sensitivity sweep, not the preregistered
equal-weight baseline. We tested lexical weights 0/0.25/0.5/1/2 against dense
weight 1 using cached lists. At 3072 dimensions, weight 0.5 gives 69.9% PG vs
74.5% DuckDB. All weights are retained in `fusion-sweep.csv/parquet`. The
trajectory replay retains equal weights, avoiding a silent configuration change.

Lexical target eligibility before ranking: 181/196 PG vs 183/196 DuckDB. Thus
the large static lexical ranking gap is not chiefly a stemming/matching gap in
these new configurations. It does not isolate BM25 from all tokenizer effects,
but clearly locates the remaining weakness in the lexical branch rather than
the vector engine.

HNSW built in 143.8 seconds, index 817 MiB. ef_search=100/200/400 yielded
90.1%/96.0%/98.7% exact top-100 neighbor overlap, with Recall@8
63.3%/64.3%/65.3% versus 65.8% exact. At ef=400 the measured warm median was
7.8 ms (p95 9.5 ms); concurrent replay was active, so these are indicative
single-query timings rather than an isolated throughput study.

## Completed trajectory replay

All 4,604 lists have six results (two engines × three methods): 27,624 durable
records. PG and DuckDB dense retrieval agree on target hits at cutoffs 8 and 20
for every list. No query strings, including Qwen's encoded arrays, were repaired.

At cutoff 8 and a budget of 10 individual searches:

| Query-producing model | Dense, either engine | PG hybrid | DuckDB hybrid |
| --- | ---: | ---: | ---: |
| Luna | 158/196 (80.6%) | 155/196 (79.1%) | 163/196 (83.2%) |
| Gemma | 172/196 (87.8%) | 161/196 (82.1%) | 171/196 (87.2%) |
| Qwen | 178/196 (90.8%) | 165/196 (84.2%) | 169/196 (86.2%) |

Across all 588 cases, dense reaches 74.1% after one search, 84.4% after five,
86.4% after ten, and 87.2% using all recorded searches. Equal-weight hybrid
at ten reaches 81.8% PG / 85.5% DuckDB. Full per-model budgets and cutoff 20
are in replay-summary.csv and the HTML report.

Concrete rescue: Qwen's first ISS instrument query, a string containing three
JSON-array alternatives, puts the target at rank 2 under dense retrieval in
both engines (hybrid rank 2 PG / rank 3 DuckDB). That same literal string
missed in the original conjunctive FTS trajectory. Keyword-shaped queries
can carry useful semantic information without new agent prompting.

The Dark Hours paraphrase remains outside the top eight throughout all three
recorded trajectories, even with dense retrieval. The rewrite is not a universal
fix for opaque titles and article/comment-only clues.

## pg_textsearch follow-up: three fixed field variants

Pinned `pg_textsearch` 1.4.0 using the official PG17 ARM64 binary release,
SHA-256 `c084c942caa9d6e35a84aaff8b21e6c51afa4126030aabf1d5e76f03f4f2a320`.
Installed inside the existing scratch container only, using `shared_preload_libraries`.
The install script targets that exact container; it is not a production installer.
The same persistent Docker volume now requires those extension binaries on restart.
No vector or HNSW changes, new embeddings, or model calls.

All three indexes use English text configuration, k1=1.2 and b=0.75:

1. Combined: unchanged title+newline+URL input.
2. Title-only.
3. Title2-URL1: 2× title BM25 score + 1× URL BM25 score, from independent
   field indexes. This is explicit score combination, not BM25F or word repetition.
   The diagnostic scores the full corpus before selecting its top 100; no
   approximate candidate union. Its latency is not representative of native
   single-index top-K retrieval.

All 196 frozen questions run through each variant. Hybrid reuses the previously
measured exact dense rankings at 1536 and 3072 dimensions, with only the already
tested lexical weights 1 and 0.5, RRF constant 60 and candidate depth 100.
No additional sweep or trajectory rerun in this bounded follow-up.

```sh
bash packages/search-research/install-textsearch.sh
uv run --locked --package search-research python -m search_research.textsearch_bakeoff setup
uv run --locked --package search-research python -m search_research.textsearch_bakeoff run
uv run --locked --package search-research python -m search_research.engine_report
docker stop searchhn-pg-bakeoff-20260904
```

Outputs: `textsearch.jsonl`, `textsearch.parquet`, `textsearch-summary.csv`,
`textsearch-config.json`, `textsearch-plan.json`. The main HTML report includes
the new table; all earlier comparisons and trajectory results remain intact.

Results at 1536 dimensions, hybrid lexical weight 0.5 (dense weight 1):

| Lexical variant | Lexical Recall@8 | Lexical NDCG@8 | Hybrid Recall@8 | Hybrid NDCG@8 |
| --- | ---: | ---: | ---: | ---: |
| Earlier stock PG ranker | 45.9% | 0.365 | 69.4% | 0.541 |
| pg_textsearch combined | 60.7% | 0.503 | 70.4% | 0.580 |
| pg_textsearch title-only | 61.2% | 0.507 | 70.9% | 0.572 |
| pg_textsearch title2-URL1 | 61.2% | 0.507 | 70.9% | 0.572 |
| DuckDB BM25 combined | 60.2% | 0.514 | 74.0% | 0.587 |

Thus lexical Recall@8 reaches parity, but NDCG and hybrid metrics do not all
match. The best observed PG title variant remains six questions behind DuckDB
hybrid at 1536 dimensions, five behind at 3072 (71.9% vs 74.5%). Title-only
adds just one lexical hit versus combined; the 2x-title score adds none.

The boosted variant changes eight top-100 lists versus title-only but none of
the target ranks. URL-only audit: only 37/196 queries match any URL in the corpus.
For example, the PG parser produces the single token `openciv3.org` for
`https://openciv3.org/`, which does not match `OpenCiv3`. This identifies a concrete
tokenization difference to investigate later; no fourth variant was added here.
Run `python -m search_research.textsearch_audit` through UV to reproduce the audit;
results are saved in `textsearch-url-audit.parquet` and `textsearch-audit.json`.

Combined/title-only lexical medians were 2.5/2.2 ms; the exact full-scan weighted
diagnostic took 1.79 s median and bought no target-rank gain. Combined index size
is 27 MiB, title index 5.4 MiB, URL index 22.4 MiB. The saved EXPLAIN confirms
native combined search uses `textsearch_input_idx`. All 2,940 result records are
unique and complete (196 questions × 3 variants × 5 lexical/hybrid settings).
