# Pplx BF16 HNSW accuracy on the two-year slice

Completed 2026-09-06. HNSW is the approximate nearest-neighbor (ANN) method;
PostgreSQL exact cosine is the reference. This test measures accuracy only and
makes no claim about latency or full-corpus performance.

## Fixed conditions

64,638 stories, original 196 questions, >=25 votes, original two-year window.
Cached Pplx BF16 1024-dimensional native int8 outputs are L2-normalized and loaded
into an isolated float32 pgvector table in the existing local scratch database.
No embedding calls, paid agent calls, or production DB changes.

One HNSW graph: m=16, ef_construction=128, serial build. Query ef_search is
100/200/400/800, iterative_scan off, candidate depth 100. All requests returned
100 candidates. No extra date/domain/score filters beyond the frozen admission
slice. Stable ID tie-breaking follows HNSW candidate selection; exact uses
(distance)+0 to prevent index use. EXPLAIN confirms exact versus HNSW paths at
all tested settings. PG exact reproduces every cached metric at all five cutoffs
for dense and lexical weights .125 and .25 (zero changed metric cells).

## Candidate recovery and target quality

Overlap is set intersection with the exact top-k divided by k, averaged over
196 queries. It is distinct from retrieving the question's known target.

| ef_search | Exact top-8 recovery | Exact top-20 recovery | Exact top-100 recovery | Worst top-100 recovery | Queries with perfect top-100 /196 |
|---|---:|---:|---:|---:|---:|
| 100 | 94.83% | 93.42% | 85.93% | 51% | 6 |
| 200 | 97.70% | 97.19% | 93.21% | 70% | 27 |
| 400 | 98.92% | 98.65% | 96.97% | 79% | 58 |
| 800 | 99.49% | 99.36% | 98.85% | 88% | 108 |

Hybrid uses fixed title-only PG BM25 candidates, dense weight 1, lexical .125,
and RRF k=60. The full artifacts also include lexical weight .25.

| Search | Dense hits@8 /196 | Dense hits@20 /196 | Hybrid hits@8 /196 | Hybrid hits@20 /196 | Hybrid nDCG@20 |
|---|---:|---:|---:|---:|---:|
| Exact | 135 | 153 | 143 | 158 | .62148 |
| HNSW 100 | 123 | 138 | 128 | 142 | .56283 |
| HNSW 200 | 129 | 145 | 135 | 149 | .58794 |
| HNSW 400 | 134 | 150 | 140 | 154 | .60948 |
| HNSW 800 | 135 | 151 | 141 | 155 | .61458 |

Relative to exact, hybrid .125 loses 16/9/4/3 targets at top 20 as ef increases,
with no gains. At 800, dense top-8's unchanged total hides one gain and one loss;
hybrid loses two with no gains. Approximation can improve a known target's rank
by omitting its competitors, so aggregate target totals do not imply exact
neighbor recovery. At .25, top-20 hits are 141/148/153/154 versus exact 157.

## Interpretation and limits

Search breadth clearly matters. Ef 100 or 200 sacrifices substantial measured
quality; 400 narrows the gap, and 800 is closest among tested settings but is not
quality-identical to exact. High average neighbor recovery can hide consequential
misses: 98.85% top-100 recovery still costs three hybrid target hits.

No setting is selected for production from this accuracy-only test. These are
results for one graph build and the smaller slice, without selective filters.
The full corpus and intended weaker host still need their own evaluation before
making the speed/quality decision. No further build-parameter grid or E2E run
was launched automatically.

## Reproduction, artifacts and cleanup

Run from repository root:

```sh
uv run --locked --package search-research python packages/search-research/tools/pplx_hnsw_accuracy.py
duckdb < packages/search-research/tools/pplx_hnsw_paired.sql
```

Artifacts: `data/pplx-vllm-gate-20260905/bf16-full/hnsw-accuracy/` contains
per-query journaled candidates, per-case rankings, overlap distributions,
aggregate/paired CSVs, plans, cached-control verification and model/index recipe.
The table comment records a matrix hash; reuse requires matching identity/count.

**Cleanup remains OPEN.** Add scratch table `pplx_bf16_hnsw_accuracy_20260906`
(and its HNSW index) plus these local artifacts to the existing archive/reap
checklist. Preserve unique results in verified Garage storage before deletion.
Existing model tables and serving configuration were not changed.
