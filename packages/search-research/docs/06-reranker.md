# Qwen3-Reranker-0.6B: bounded candidate reranking

## Outcome (2026-09-04)

Cheap enough to run, but not a clear improvement over hybrid retrieval at eight
results. Top-30 reranking helps dense-only; top-50 is worse than top-30 here.
It does not compress the recall of twenty unreranked results into eight.

196 frozen questions, 98 target stories, same 105,081-document corpus. All methods
use exact 1536-dimensional TE3-large vectors. Hybrid means RRF k=60, dense weight
1, lexical weight 0.5; PG uses title-only pg_textsearch, DuckDB uses its existing
title+URL BM25 index. Reranker documents are title + newline + URL for every method.
No story IDs, scores, target labels, or comments enter the reranker.

| Retrieval | Rerank pool | Recall@5 | Recall@8 | NDCG@8 | Recall@20 |
|---|---:|---:|---:|---:|---:|
| Dense | none | 60.2% | 65.8% | .529 | 73.5% |
| Dense | 30 | 65.3% | 70.4% | .584 | 76.5% |
| Dense | 50 | 64.8% | 68.9% | .575 | 76.5% |
| PG hybrid | none | 62.8% | 70.9% | .572 | 78.6% |
| PG hybrid | 30 | 66.3% | 70.9% | .584 | 79.6% |
| PG hybrid | 50 | 65.8% | 69.9% | .576 | 77.6% |
| DuckDB hybrid | none | 64.8% | 74.0% | .587 | 79.1% |
| DuckDB hybrid | 30 | 66.8% | 71.9% | .591 | 79.6% |
| DuckDB hybrid | 50 | 66.3% | 69.9% | .578 | 78.1% |

At cutoff 8, top-30 reranking gains/loses 17/8 questions for dense, 10/10 for PG,
and 9/13 for DuckDB. There is one judged relevant target per question: recall is
single-question target-hit rate, and NDCG is discounted target rank. These are not
fresh agent pass@search-budget trajectories or full relevance judgments. The two
questions per story are correlated; small differences are exploratory, not a
significance claim. No instruction or score-fusion tuning was performed.

Worked examples (PG hybrid top 30):

- Origin developer question: target “Cursor launches Origin, GitHub alternative”
  moves 30 → 1; the separate Git-command question for that story moves 29 → 5.
- Chat Control scanning question: “EU Council forces Chat Control via fast-track”
  moves 28 → 2.
- Camera firmware question: “My security camera shipped a GitHub admin token in
  its login page” moves 6 → 24. Reranking also demotes good candidates.

## Cost and batching

Remote RTX 5090, Torch 2.10.0+cu128, Transformers 4.57.6, BF16, SDPA,
`use_cache=False`, only final-position logits materialized. Standard official
Qwen instruction and yes-minus-no logit score; no generated tokens.
Model revision: `e61197ed45024b0ed8a2d74b80b4d909f1255473`.

Three repetitions of 256 representative pairs per batch size, after warmup:

| Batch | Median server ms / 256 pairs | Peak Torch allocated MiB |
|---:|---:|---:|
| 8 | 458 | 1184 |
| 16 | 393 | 1223 |
| 32 | 344 | 1301 |
| 64 | 363 | 1459 |
| 128 | 419 | 1772 |

Batch 32 selected on server throughput (~745 pairs/s). Larger padding/batches did
not help this short-text sample. Peak allocation is Torch allocated memory, not
total process VRAM. BF16 scores vary with batch shape (mean per-pair spread .103,
maximum .5 logits across the sweep); the actual experiment uses batch 32
consistently, and near-tied rankings should not be assumed bitwise invariant.

Ten warmed individual queries each, batch 32:

| Candidates | Median server ms | Server p95 ms | Median laptop roundtrip ms |
|---:|---:|---:|---:|
| 30 | 41.9 | 42.2 | 46.9 |
| 50 | 74.5 | 76.3 | 79.8 |

These costs are additive to initial retrieval and exclude cold loading. This is
one client over persistent SSH, not a concurrent serving benchmark. 10,908 unique
question/document pairs across 196 questions took 16.2 seconds summed server
scoring time, excluding the sweep. Checkpoints share scores across candidate
methods/pools, without expanding any individual pool. No model API expense.
The worker exits at completion and releases VRAM; downloaded dependencies and
model weights remain cached on melchior.

## Reproduce

Run from the repository root on the laptop. Only model inference runs remotely;
there is no rsync or remote experiment driver. The SSH command starts a uv-managed
worker from the local source, communicates using JSONL, and ends on stdin EOF.

```sh
uv run --project packages/search-research python -m search_research.rerank_bakeoff --host melchior
uv run --project packages/search-research python -m search_research.rerank_bakeoff --report-only
uv run --project packages/search-research pytest packages/search-research/tests/test_rerank_bakeoff.py -q
```

Artifacts live in `data/pg-duckdb-bakeoff-20260904/reranker/`:
`scores.jsonl` (fsync per question, resume skips complete questions),
`timings.jsonl`, `runtime.jsonl`, `worker.log`, `metrics.parquet`, `summary.csv`.
Inputs are the existing frozen corpus/questions and `comparison.parquet` plus
`textsearch.parquet`. Keep these artifacts together; do not resume into this output
directory after changing inputs or model configuration. The driver reruns the
short timing sweep on resume. A clean remeasurement needs a separate artifact
directory (change `OUT`), not appending to the existing experiment.

Implementation: `src/search_research/rerank_bakeoff.py` (laptop),
`src/search_research/rerank_worker.py` (remote inference only).
Reference prompt: <https://huggingface.co/Qwen/Qwen3-Reranker-0.6B>.
