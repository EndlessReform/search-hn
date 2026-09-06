# Qwen 0.6B first bench — 2026-09-05

Same frozen 64,638 stories and original 196 questions as the second Luna run and
Perplexity benchmark. Native 1024 dimensions first, followed by cached prefixes
at 256/512/768/1024. Exact cosine and unchanged title-only BM25/RRF; no new agent
trajectories, query rewriting or instruction tuning.

**Cleanup remains OPEN:** follow the mandatory archive-and-reap checklist in
[the main design doc](00-design.md). This run's local cache is temporary working
storage. It has not yet been published to Garage.

## Frozen recipe

- Model: `Qwen/Qwen3-Embedding-0.6B`, revision
  `97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3`.
- TEI 1.9.3, same pinned Ampere image as Perplexity; FP16 `FlashQwen3` backend,
  last-token pooling, RTX 3060. See `compose.qwen.yaml` for the full launch recipe.
- The published query instruction is applied exactly once to questions, without a
  space after `Query:`. Documents are plain title + newline + URL. Prompt bytes
  are in `manifest.json` and verified against the pinned SentenceTransformer
  configuration. [Official model card](https://huggingface.co/Qwen/Qwen3-Embedding-0.6B/tree/97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3).
- Requests disable both normalization and truncation. Response floats are stored
  as float32, separately from FP16 compute precision. Prefixes of both query and
  document vectors are L2-normalized for scoring; no quantization experiment here.
- Server caps: 2048 batch tokens, 64 requests/client inputs, 128 concurrent inputs.
  Longest document is 352 tokens; longest prompted question is 52. All 2,315,387
  document tokens and 7,833 prompted question tokens fit without truncation.
- Five smoke inputs, including two instructed queries, match official
  SentenceTransformer CPU FP32/SDPA with minimum cosine **0.99999827**. Raw vector
  scales differ because the reference includes normalization; cosine is the
  relevant comparison. Singleton-versus-batch smoke checks also pass.

## Timing and quality

**Completed: full corpus in 395.54 seconds (6m 36s), 163.42 stories/s.** The 196
questions took another 1.28 seconds. All 4,040 document requests and 13 question
requests completed with zero errors/retries. The midpoint projected about 6m 37s,
so the run continued on the 3060 without needing a 5090.

First container launch through Ready was approximately 45 seconds; the weight
download itself took 35.95 seconds. This reused the already-pulled TEI image.
The seeded 256-document pilot selected batch size 16, around 169 stories/s and
a projected 6m 22s document pass. Batch 32/64 offered no clear improvement.

Qwen's end-to-end document throughput was **2.14× Perplexity's** on this setup.
This compares the supported TEI serving recipes: Qwen FP16/Flash Attention versus
Perplexity FP32. It does not isolate architecture from compute precision/backend.
Sampled GPU usage peaked at 1,315 MiB and 63°C. The document payload is 252.5 MiB
of float32; the full local run occupies approximately 283 MiB including small-file
overhead and evidence. Qwen has not been quantized; Perplexity's native int8
document payload is 63.1 MiB, so equal dimensions are not equal stored bytes.

### Native quality and controls

| Recipe | @8 target hits / 196 | @20 target hits / 196 | NDCG@20 |
| --- | ---: | ---: | ---: |
| Qwen-1024 dense | 137 | 149 | .5815 |
| Perplexity-1024 dense | 136 | 153 | .6002 |
| TE3-1024 dense | 132 | 151 | .5663 |
| TE3-1536 dense, historical control | 134 | 154 | .5873 |
| Qwen-1024 hybrid | 142 | 156 | .6206 |
| Perplexity-1024 hybrid | 144 | 157 | .6094 |
| TE3-1024 hybrid | 140 | 157 | .6057 |
| TE3-1536 hybrid, historical control | 142 | 160 | .6166 |

Qwen hybrid ranks the known targets better overall at @20, but Perplexity finds
one more target. Qwen dense loses four @20 hits to Perplexity while gaining one
at @8. This is a mixed comparison, not a clear quality winner. By style, Qwen's
dense @20 hits are 83/98 entity and 66/98 paraphrase; hybrid is 86/98 and 70/98.

### Cached dimension sweep

No additional inference. Each cell is target hits out of 196 / NDCG@20.

| Qwen dimensions | Dense | Hybrid |
| ---: | ---: | ---: |
| 256 | 119 / .4716 | 140 / .5474 |
| 512 | 138 / .5236 | 151 / .6007 |
| 768 | 146 / .5772 | 154 / .6174 |
| 1024 | 149 / .5815 | 156 / .6206 |

Keep Qwen-1024 as the candidate. Smaller prefixes lose recall; there is no need
to accept that now. Both native anchors reproduced exactly in the sweep; all
five cutoffs and dimension-specific strata are saved. Perplexity's corresponding
dimension table is in [its report](11-pplx-first-bench.md).

**Recommendation:** retain both native models for now. Qwen is substantially
faster to serve in this configuration and promising for hybrid ranking;
Perplexity retains better dense @20 coverage and a smaller native vector cache.
The next cheap discriminator is replaying recorded second-Luna search queries.
Reserve a fresh full agent loop for the resulting finalists. These 196 questions
are an inspected known-item development set with one labeled target per query,
not an independent test set or a fresh end-to-end success measurement.

## Reproduction and artifacts

All artifacts live under `data/sovereign-embeddings-20260905/qwen-1024/`: native
shards, manifest, request journal, progress/completion, pilot, GPU samples,
reference validation, source snapshot and scoring outputs. Historical Perplexity
and TE3 artifacts remain intact. Scoring verifies the frozen input hashes and
reproduces the archived TE3 metrics before writing the new comparison.

```sh
uv run --locked --package search-research python -m search_research.sovereign_run pilot --model qwen
uv run --locked --package search-research python -m search_research.sovereign_run embed --model qwen --batch-size 16
OPENBLAS_NUM_THREADS=4 VECLIB_MAXIMUM_THREADS=4 uv run --locked --package search-research python -m search_research.sovereign_score --root data/sovereign-embeddings-20260905/qwen-1024
OPENBLAS_NUM_THREADS=4 VECLIB_MAXIMUM_THREADS=4 uv run --locked --package search-research python -m search_research.sovereign_score --root data/sovereign-embeddings-20260905/qwen-1024 --dimension-sweep
```
