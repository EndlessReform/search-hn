# Reranker throughput and precision decision

**Historical baseline:** the current author-aware, popularity-boosted Luna rollout
and its repair loop are documented in [Current pipeline and handoff](pipeline-current.md).
The active run is `books-resolver-2025-heal-v2`, with no shortcut gate. Settings and
measurements below describe the earlier baseline unless explicitly reused there.

**2026-09-26: use native vLLM FP8 scoring with prefix caching for the next full
labeling run.** The tested configuration processes the random 128-reference slice
in **3.78 seconds**, versus **81.53 seconds** in the original Transformers BF16
runner: about **21.5× faster**. The resulting projection is **12.9 minutes of
reranking for 26,221 references**, plus startup and input handling. This is the
reranking stage only, not a forecast for both API labelers finishing.

The earlier 4–5 hour estimate described the untuned implementation and should not
be used as the expected full-run time. Increasing that implementation's batch size
did not materially help; native vLLM execution with prefix caching did.

## Selected configuration

- Pinned vLLM 0.23.0 container image:
  `sha256:f37691f675bb82f734f606de8af90e777d3f80a20b120e699fd43fd10e60b8d7`.
- `zeroentropy/zerank-2-reranker`, revision
  `5eae30d5ee3c6b2df2ef6d723bde45172d761c4c`.
- `runner=pooling`, `convert=classify`; copy the original `Yes` LM-head row into
  the classification head with `classifier_from_token=["Yes"]`,
  `method=no_post_processing`, `num_labels=1`.
- LAST pooling, activation disabled: retain raw logits, not a different probability
  or Yes/No scoring formulation. Model token 9454 is verified as `Yes`.
- Online FP8 quantization; BF16 remaining computation and KV cache.
- 16,384 maximum batched tokens; 256 concurrent sequence cap; chunked prefill;
  prefix caching enabled; 32,768 model context; GPU reservation .80.
- Feed windows of 128 references with every top-50 pair retained. Do not truncate
  comments. Preserve each pair score, candidate ID and full selector input.

The full calibration run will call both labelers rather than apply the old
raw+gap gate. Recalibrate that optional gate on the saved FP8 scores. The old BF16
threshold 10/2 accepted 55 proposals (54 credited); unchanged on FP8 it accepted
47 (46 credited). Selecting FP8 saves about eight projected reranking minutes
relative to native BF16, while changing the score distribution.

The user resumed the full run after tuning. Preparation is complete: 26,221
references and 558,083 candidate metadata records. The selected worker is running
on melchior; the paired labelers consume its saved top-three lists. Permanent
artifacts and resume commands are documented in the
[main architecture document](resolver-architecture.md#durable-run-and-resumption).

## Fixed inputs and timing

The corpus slice is the first 128 references from the deterministic seed-20260926
shuffle of all 26,221 references. It contains 6,131 candidate pairs and 1,533,529
input tokens. Pair length: minimum 51, median 145, p95 583, maximum 3,102. One
reference has no candidates. Complete original comments and the original top-50
Tantivy results are used; no translation/popularity was introduced.

The precision check uses all 250 existing iteration proposals: 11,933 pairs,
2,957,823 tokens, maximum length 1,367. These labels are the same developmental
work-ID judgments used throughout the spike.

Timings exclude model load and warmup, include model execution and output
collection, and use pretokenized identical inputs. Corpus tokenization took 0.24s
and labeled-set tokenization 0.46s. vLLM load/compile/cache initialization was
approximately 43–45s. The prefix cache was explicitly cleared after warmup and
between datasets. Thus cached benchmark replay does not create the gain. Prefix
reuse is intrinsic to comparing 50 titles against the same marked comment;
approximately 88% of the random slice's tokens are repeated shared prefixes.

The separate embedding service stayed running throughout, occupying about 3.1 GiB.
All benchmark model processes exited afterward; embedding remained healthy.

## Results

| Runtime / precision | Pair cap or sequence cap | Token budget | Random 128 seconds | Pairs/sec |
|---|---:|---:|---:|---:|
| Transformers BF16, original per-reference batches | 32 | 16,384 | 81.53 | 75.2 |
| Transformers BF16, cross-reference length buckets | 32 | 16,384 | 81.04 | 75.7 |
| Transformers BF16, cross-reference length buckets | 64 | 32,768 | 84.63 | 72.4 |
| Transformers BF16, cross-reference length buckets | 128 | 65,536 | 86.57 | 70.8 |
| Transformers BF16, cross-reference length buckets | 256 | 131,072 | 90.54 | 67.7 |
| vLLM BF16, prefix caching | 256 | 8,192 | 6.15 | 997.5 |
| vLLM BF16, prefix caching | 256 | 16,384 | 6.14 | 998.2 |
| vLLM FP8, prefix caching | 256 | 8,192 | 3.78 | 1,623.3 |
| **vLLM FP8, prefix caching** | **256** | **16,384** | **3.78** | **1,620.2** |
| vLLM FP8, prefix caching | 256 | 32,768 | 3.75 | 1,636.2 |

The vLLM budgets are effectively tied on throughput. Choose 16k within that plateau,
not because hundredths of a second establish an optimum. The 256-sequence vLLM
cap was held fixed, not exhaustively swept. Transformers GPU traces show sustained
heavy utilization; a live reading reached 100% and roughly 572W. Larger batches
triggered allocator pressure and did not improve throughput.

This comparison measures the native runtime plus prefix caching together; it does
not isolate how much of the gain belongs to kernel implementation versus caching.

## Precision and ranking behavior

| Configuration | Correct top 1 /201 | Correct within top three /201 |
|---|---:|---:|
| Original Transformers BF16 | 162 | 172 |
| vLLM BF16, 16k | 162 | 172 |
| vLLM BF16, 8k | 162 | 174 |
| vLLM FP8, 8k | 160 | 172 |
| **vLLM FP8, 16k** | **161** | **172** |
| vLLM FP8, 32k | 159 | 172 |

At 16k, FP8 and vLLM BF16 preserve exactly the same 172 answerable top-three
shortlists: zero labeled target losses or gains. Individual catalog IDs and their
order do change. FP8 matches the old runner's exact top ID on 213/249 nonempty
proposals and exact top-three ID set on 179/249. The mean absolute raw-score shift
is .297, p99 .999. Native BF16 also changes some close rankings (240/249 same top
ID); exact ordering is not invariant to execution backend and batch shape.

The previous Luna/Gemma final-selection results were measured on BF16 shortlists.
The full paired labeling run will measure final selection on the new FP8 outputs;
unchanged shortlist answerability does not substitute for that measurement.

TorchAO 0.18.0 row-wise dynamic FP8 with Torch 2.14.0+cu130 failed the finite-logit
assertion during warmup. It was rejected without usable output or fallback. This
failure is retained separately and is not the vLLM FP8 result above.

## Permanent artifacts and reproduction

Both melchior and the local checkout hold:
`data/research/books-resolver-throughput-v1/` (ignored research data, not /tmp).
The remote repository root is `/home/ritsuko/projects/data/search-hn`; local root
is `/Users/ritsuko/projects/data/search-hn`.

- `corpus128.json`, `gold.json`: complete fixed texts, candidate IDs, group offsets,
  model token IDs and labels where available.
- `*-bf16-*.json`, `*-vllm-*.json`: every pair score, configuration and timing.
- `*-gpu.csv`: original-runner GPU utilization/power/memory samples.
- `comparison.jsonl`: score deltas, exact ranking agreement and labeled outcomes.
- `torchao-fp8-failure.json`: failed backend/configuration and assertion.
- `manifest.json`: artifact hashes and selected runtime contract.

Tracked scripts in `packages/search-research/tools/resolver_rollout/`:
`bench_prepare.py`, `bench_run.py`, `bench_vllm.py`, `bench_compare.py`.
Use UV in the host environment for preparation, original-runner benchmarks and
comparison. For native execution, use the pinned container and UV's system-Python
selection, as in `run.sh`; pass `bench_vllm.py --prefix --precision fp8 --tokens
16384` instead of `rerank.py`. Model cache is read-only and full inputs are retained.
The optional `--primed` scheduling variant exists in the harness but was not run
and is not part of the selected configuration.

## Full-run launch check

The resumed worker saved 2,560 references in 100 seconds after model setup,
including first-window warmup. Both Luna and DeepSeek passed their initial eight
calls and their full arms are running. At the first portable checkpoint there were
135 Luna and 12 DeepSeek responses, with zero recorded failures. The permanent run
is `data/research/books-resolver-2025-v1/` on melchior; the same relative local path
contains the early integrity-checked checkpoint and configuration copies. Full-run
completion and calibration results remain pending; consult current stage status.

## API worker recovery and native switch

The reranking job completed all 26,221 references. The first API worker stopped
its entire queue on an output-limit response; it has been replaced with per-item
retries, a rolling queue, retained attempt history and explicit incomplete status.
Luna has now completed every reference. By user instruction, the DeepSeek final
labeling run uses the native `deepseek-flash` endpoint (V4.1 Flash, thinking enabled,
low effort), with separate result identity `deepseek-native`. Earlier OpenRouter
V4 results remain under `deepseek` and are excluded from the final paired summary.
Both current workers use concurrency 128. See the main doc for the streaming
latency measurements, which exposed the slow third-party generation rate.
