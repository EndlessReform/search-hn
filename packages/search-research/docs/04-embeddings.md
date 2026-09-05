# Quick TE3-large dimension baseline

This is a disposable retrieval baseline, **not the final embedding candidate**.
The laptop drives OpenAI requests and does exact matrix search locally; no GPU
jobs or new agent trajectories are launched on melchior.

## Result (completed 2026-09-04)

All 105,081 stories and 196 questions embedded successfully. Returned usage:
3,521,099 tokens, **$0.457743** at the documented rate, with zero 429 responses.

| Dimensions | Recall@8 | NDCG@8 | Float32 vectors |
| --- | ---: | ---: | ---: |
| 256 | 52.6% | 0.429 | 103 MiB |
| 512 | 60.7% | 0.485 | 205 MiB |
| 768 | 62.8% | 0.493 | 308 MiB |
| 1,024 | 63.8% | 0.501 | 410 MiB |
| 1,536 | 65.8% | 0.529 | 616 MiB |
| 3,072 | 66.3% | 0.544 | 1,231 MiB |

For this quick baseline, 1,536 dimensions recovers 129/196 targets in the top
eight versus 130/196 at full size, with half the vector storage. This small
evaluation does not establish a universally optimal dimension or final model.
At full size, Recall@20 is 76.0%; either variant retrieves the target in the top
eight for 85/98 stories (86.7%). Exact search needed no ANN index. These are
direct-question retrieval metrics, not comparisons against multi-turn PG agents.

## Hybrid follow-up

| Dimensions | Hybrid Recall@8 | Hybrid NDCG@8 | Hybrid Recall@20 |
| --- | ---: | ---: | ---: |
| 256 | 65.3% | 0.519 | 77.0% |
| 512 | 68.9% | 0.557 | 78.6% |
| 768 | 70.4% | 0.566 | 79.6% |
| 1,024 | 71.4% | 0.565 | 80.1% |
| 1,536 | 71.4% | 0.573 | 79.6% |
| 3,072 | 71.9% | 0.580 | 79.6% |

`comparison.html` in the result directory puts the original cosine sweep and
BM25+dense hybrid in adjacent tables. BM25 alone reaches Recall@8 **60.2%** and
NDCG@8 **0.514**. Hybrid uses equal-weight reciprocal-rank fusion (`k=60`), taking
the top 100 candidates from each retriever. No parameters were tuned on these
results. DuckDB 1.5.5 uses built-in Porter stemming, English stopwords, lowercase
and accent normalization, with `ignore='[^a-z0-9]+'` to retain digits. Matching
is disjunctive, with BM25 `k=1.2`, `b=0.75`. Both retrievers see the same combined
title+URL text and the same questions. No API requests are made.

Reproduce locally:

```sh
OPENBLAS_NUM_THREADS=4 VECLIB_MAXIMUM_THREADS=4 \
  uv run --locked --package search-research python -m search_research.hybrid_baseline
```

The cosine ranks are recomputed and asserted identical to the original results.
`hybrid-summary.csv/json`, `hybrid-ranks.parquet`, `hybrid-neighbors.parquet`,
`hybrid-strata.csv` and `hybrid-config.json` capture the comparison and inputs.
Ranks outside the hybrid candidate union are recorded as missing, not guessed;
reported cutoffs are all <=20, below the candidate depth of 100. The BM25 index
is stored locally in `bm25.duckdb` and rebuilt by this command.

## Input accounting

The frozen corpus has 105,081 live stories with HN score >=10, from 2024-09-04
through 2026-09-04. Each input is exactly `title + newline + URL`; bodies and
comments are deliberately excluded. Queries are the 196 frozen plain-language
questions from the existing evaluation, without target metadata.

Tokenizer: `tiktoken.encoding_for_model("text-embedding-3-large")`. Corpus plus
queries total 3,521,099 input tokens, estimated at **$0.457743** at the documented
$0.13/million input tokens. Preparation refuses estimates above $2.50 including
a 15% reserve. Every HTTP attempt reserves its full estimated cost in a durable
journal, even if it fails; cumulative attempted cost cannot reach $2.50. This is
a conservative ceiling, not a claim that failed requests are always billed.

Published Tier 1: **3,000 RPM, 1,000,000 TPM**. Actual response headers confirmed
both limits. We use `aiolimiter`: 300 RPM with no multi-request burst, and 600k
TPM with at most a 20k-token initial burst, plus concurrency two. Batches have at
most 256 inputs/20k tokens. HTTP automatic retries are disabled; 429 retries
honor Retry-After and are budgeted. Other errors fail clearly; rerun to resume.

## Run

```sh
export HN_QUERY_DATABASE_URL=postgresql://readonly_hn_agent@searchhn-pg:5432/searchhn_test
# OPENAI_API_KEY is inherited, never written into an artifact.
uv run --locked --package search-research python -m search_research.embedding_baseline --prepare-only
OPENBLAS_NUM_THREADS=4 VECLIB_MAXIMUM_THREADS=4 \
  uv run --locked --package search-research python -m search_research.embedding_baseline
```

Defaults write to `data/te3-large-baseline-20260904/`. Once `corpus.parquet`
exists, restarting does not query PG. `manifest.json` hashes the corpus and eval
inputs. Completed `.npy` shards are fsynced and atomically renamed; restart skips
them. Do not edit the frozen corpus or question set in this directory. A lock
prevents concurrent drivers in the same directory.

## Dimension sweep and interpretation

Embed once at 3,072 dimensions. Test prefixes 256/512/768/1,024/1,536/3,072,
L2-normalizing **both documents and queries after truncation**. Exact cosine
search sees all corpus stories, with no ANN approximation or query expansion.
Prefix shortening and renormalization follow the official embedding guide.

- `dimensions.csv/json`: Recall@1/5/8/10/20, single-anchor NDCG at those cutoffs,
  either-question Recall@8, raw vector storage, and batch search timing.
- `ranks.parquet`: exact target rank for every question/dimension.
- `neighbors.parquet`: top 20 candidate IDs and scores for worked examples.
- `strata.csv`: question style and recent/older breakdowns.
- `requests.jsonl`: durable per-attempt reservations, returned usage and rate headers.

This is **single-shot retrieval**, not agent pass@k or proof of model context
exposure. The earlier PG run has a broader corpus and multi-turn trajectories;
its rates are not directly comparable. Latency is local batched matrix search,
including prefix preparation/rank calculation but excluding API embedding time,
not an isolated interactive-query latency benchmark. Other relevant stories are
unjudged. The vector payload is about 1.2 GiB at full size; working copies add RAM.

Sources, checked 2026-09-04:

- [Official model pricing and rate limits](https://developers.openai.com/api/docs/models/text-embedding-3-large)
- [Official guide: reducing embedding dimensions](https://developers.openai.com/api/docs/guides/embeddings)
