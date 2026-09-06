# Pplx BF16 hybrid weighting: cached static sweep

Completed 2026-09-06. A small lexical contribution improves this development set;
retain weights .125 and .25 as the useful candidates. No E2E sessions, embedding
inference, database queries or paid calls were run. No serving defaults changed.

## Conditions and verification

Frozen 64,638-story corpus and 196 questions over 98 targets; Pplx BF16 native
1024-dimensional int8 outputs, prefix/L2 normalization, exact cosine search.
Title-only BM25 candidates are reused from the original cache. Dense weight 1,
RRF constant 60, top 100 candidates per leg; deterministic story-ID tie breaking.
Lexical weights: 0, .125, .25, .5, 1, 2, 4, plus a pure lexical control. Zero
weight excludes lexical-only candidates. Input hashes, query order, and all five
cutoffs of the dense, .5 hybrid and lexical native controls were verified.

| Lexical weight | Hits@8 /196 | nDCG@8 | Hits@20 /196 | nDCG@20 |
|---|---:|---:|---:|---:|
| 0 (dense) | 135 | .5769 | 153 | .6015 |
| .125 | 143 | .6012 | 158 | .6215 |
| .25 | 147 | .5950 | 157 | .6092 |
| .5 (previous hybrid) | 144 | .5918 | 157 | .6099 |
| 1 | 136 | .5833 | 154 | .6075 |
| 2 | 133 | .5852 | 140 | .5947 |
| 4 | 132 | .5797 | 139 | .5889 |
| Pure lexical | 124 | .5388 | 135 | .5533 |

## Paired interpretation

Relative to dense, .125 gains 10 targets and loses 2 at cutoff 8; at cutoff 20 it
adds 5 and loses none. Weight .25 gains 17 and loses 5 at cutoff 8, and gains 5 and
loses 1 at cutoff 20. This makes .125 the balanced provisional choice, with .25
worth retaining if top-8 target exposure is the main objective.

At .125, entity questions improve from 87 to 88 top-20 hits, paraphrases from 66
to 70; nDCG@20 improves for both (.6776 to .7038 and .5254 to .5391).
All five additional top-20 hits are in the recent cohort (94 to 99); older hits
remain 59, with improved nDCG. At lexical weights 2 and 4, paraphrase hits fall to
59 and 58, below dense's 66. A stronger lexical contribution is not helpful here.

A paired bootstrap with 5,000 resamples groups both question styles by target
story. For .125 versus dense, the 95% percentile intervals are +1.0 to +7.7
percentage points for recall@8, +0.5 to +5.1 points for recall@20, and +.0006 to
+.0405 for nDCG@20. These are exploratory, unadjusted intervals on an already
inspected development set, not independent confirmation after selecting among
weights. Against the previous .5 hybrid, all four intervals include zero.

No additional grid or E2E run is automatically triggered. The static evidence
supports a light lexical weight, but does not establish a precise optimum or an
agentic improvement. Only one target per question is judged; other relevant hits
are unjudged.

## Reproduction and artifacts

Run `uv run --locked --package search-research python packages/search-research/tools/pplx_hybrid_sweep.py`.
The [script](../tools/pplx_hybrid_sweep.py) saves rankings, top-100 candidate lists,
summary, style/cohort strata, paired comparisons and a recipe manifest under
`data/pplx-vllm-gate-20260905/bf16-full/hybrid-weight-sweep/`.

**Cleanup remains OPEN:** include these tuning artifacts in the existing verified
Garage archive before reaping local experiment data. Preserve originals and the
selected live serving recipe; no caches were deleted by this sweep.
