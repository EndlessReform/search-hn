# Perplexity BF16 serving gate: stock vLLM

The user authorized stopping the other embedding services and testing vLLM, with
memory provisioned only for this embedding model and approximately batch 64.

**Stock vLLM 0.28.0 successfully serves the pinned Perplexity 0.6B checkpoint in
BF16. No vLLM fork, edited model implementation or custom webserver was needed.**
The architecture override selects native Qwen3 with bidirectional attention;
mean pooling runs with activation/normalization disabled. The existing client
applies Perplexity's published tanh/round/clamp int8 output transform, then cosine
normalization when scoring. Generic vLLM int8 encoding is not substituted.

## Memory and service configuration

- Official image digest:
  `sha256:61fc8a896b0a4fbbbdc063bc4b0dbc25ce98e02b5050c24aeb7830ac02039b14`.
- Model revision: `2c4d510dd4a732063c31a0f70193e35067b51fd8`.
- Pooling runner, native vLLM model implementation, BF16, Flash Attention.
- `max_num_seqs=64`, `max_num_batched_tokens=8192`, `max_model_len=2048`.
- `kv_cache_memory_bytes=0`, prefix caching and chunked prefill disabled.
- Eager execution: no CUDA graphs or whole-model compilation memory pool.
- `gpu_memory_utilization=0.35` is explicitly set, but is **not a hard memory cap**.
  Actual memory stays far below that fraction. Encoder-only layers have no KV cache
  specification; vLLM skips KV allocation for them.
- Endpoint is VM loopback `127.0.0.1:8080`, forwarded to laptop port 58080.
- Only the BF16 vLLM server remains running. The TEI Perplexity/Qwen containers,
  Nemotron process, and temporary FP32 vLLM control are stopped.

Batch size is a scheduling ceiling, not a promise that 64 maximum-length texts
execute together. The 64 longest corpus inputs contain 8,453 tokens in total;
vLLM safely splits that request at the 8,192-token budget. The longest individual
corpus input is 351 tokens. The user-requested workload is title plus URL, not
64 simultaneous 32K-token documents.

Reproduction: [compose file](../compose.pplx-vllm.yaml) and
[launch script](../tools/launch_pplx_vllm.sh). The compose file describes the same
service created with Docker run; do not start a second container on the same port.

## Compatibility and bounded pilot

A separate FP32/Flex Attention control loaded the original weights through native
vLLM. Its 196 query vectors matched TEI at minimum cosine .999911; 320 documents
(256 seeded random plus the 64 longest) matched at minimum .999712. Coordinate
changes were at most one int8 unit, validating the architecture, mask, pooling and
client transformation before switching precision.

BF16/Flash Attention query agreement: mean cosine .999748, minimum .999336.
Document agreement: mean .999626, minimum .997485. These establish close numerical
agreement, not identical rankings; full static retrieval is checked separately.

Two repetitions of the same seeded 256-document sample, after shape warmup:

| Client batch | BF16 stories/s, repetitions |
|---|---:|
| 1 | 38.9 / 38.7 |
| 8 | 134.2 / 152.0 |
| 16 | 194.3 / 218.8 |
| 32 | 243.6 / 244.3 |
| 64 | 259.7 / 267.9 |

The 64-longest-input stress request completed in 0.634 seconds. BF16 weights
loaded in 1.84 seconds using 1.12 GiB; server startup including imports took about
39 seconds. Idle GPU usage after startup was 1,385 MiB, rising to 1,735 MiB after
batch/stress tests. The 500ms memory sampler observed a 1,735 MiB BF16 peak during
those tests (brief sub-sample spikes are not ruled out). FP32 vLLM peaked at
3,169 MiB. This is measured allocation, rather than an assumed KV-cache percentage.

## Full frozen retrieval gate: passed

The independent BF16 cache completed all **64,638 documents in 225.10 seconds
(3m45s), 287.15 stories/s**, followed by all 196 queries in 0.51 seconds. This is
approximately 3.8 times the previous TEI FP32 full-backfill rate. Batch 64 was used
throughout; sampled GPU memory remained at or below **1,735 MiB**, including the
full backfill. These timings include HTTP transport and durable local shard writes.

| Method / precision | Targets at 8 / 196 | Targets at 20 / 196 | nDCG@20 |
|---|---:|---:|---:|
| Dense TEI FP32 | 136 | 153 | .600216 |
| Dense vLLM BF16 | 135 | 153 | .601478 |
| Hybrid TEI FP32 | 144 | 157 | .609427 |
| Hybrid vLLM BF16 | 144 | 157 | .609863 |

Dense loses one target at cutoff 8; hybrid gains one and loses one. Neither changes
which targets are found at cutoff 20. Archived TE3 results were reproduced by the
scorer as a control. There is no material aggregate quality regression on this
frozen set, although outputs and rankings are not bit-identical. This supports
adopting Pplx BF16 for parameter tuning. Previous agentic results remain explicitly
FP32 results; this gate did not rerun paid agent sessions.

The native int8 transform currently lives in the benchmark client tools. Wiring
that small transform and this endpoint into the reusable agent provider remains
part of adopting the recipe; the server itself is stock vLLM.

Upstream references: [pinned Qwen3 implementation](https://github.com/vllm-project/vllm/blob/v0.28.0/vllm/model_executor/models/qwen3.py),
[pooling server options](https://docs.vllm.ai/en/latest/models/pooling_models/embed/),
[Perplexity output quantizer](https://huggingface.co/perplexity-ai/pplx-embed-v1-0.6b/blob/main/st_quantize.py).

## BF16 dimensions and next tuning step (2026-09-06)

Completed the cached BF16 sweep, with prefix slicing followed by L2 normalization.
Both native anchors reproduced exactly. No new inference or paid calls occurred.
Results are in `bf16-full/dimension-sweep/`, separate from native gate outputs.

| Dimensions | Dense hits@8 | Dense hits@20 | Dense nDCG@20 | Hybrid hits@20 | Hybrid nDCG@20 |
|---|---:|---:|---:|---:|---:|
| 256 | 116 | 130 | .4922 | 141 | .5560 |
| 512 | 124 | 145 | .5656 | 153 | .5906 |
| 768 | 133 | 153 | .5947 | 156 | .6008 |
| 1024 | 135 | 153 | .6015 | 157 | .6099 |

Retain 1024 as the default. 768 saves 25% of vector coordinates but does not
reduce model inference work, and slightly worsens ranking quality here.

The [hybrid weight sweep is now complete](21-pplx-hybrid-weight-sweep.md), with
.125 and .25 the useful candidates; no E2E run followed. Original experiment plan:
at 1024 dimensions, fix dense
weight 1, RRF constant 60, and candidate depth 100 per leg; sweep lexical weights
0, .125, .25, .5 (existing baseline), 1, 2, 4, plus a pure lexical control.
Weight zero must exclude lexical-only candidates. Report recall and nDCG at 8 and
20, paired gains/losses, and results by query style/cohort. Lexical currently means
the frozen title-only BM25 index; adding URL fields is a distinct experiment.

Use this already-inspected set as development evidence. Favor a stable range of
weights over a single tiny aggregate win; group both query styles for a target
story together in any resampling or split. Only test candidate depth/RRF constant
if this first sweep gives a reason. Run agentic validation for the selected recipe
against dense after static tuning, rather than one paid E2E run per grid point.

## Artifacts and closure

All new data are under `data/pplx-vllm-gate-20260905/`; the old FP32 cache and
completed E2E artifacts remain intact. The BF16 full cache has its own recipe
manifest and batch-64 shards. No paid driver calls are needed for this gate.

**Cleanup remains OPEN.** Add the vLLM image (28.79 GB uncompressed Docker image
size; shared-layer accounting may differ), both trial containers, the VM's
`/opt/searchhn-embeddings/vllm` caches and the new local BF16 artifacts to the
verified-Garage/archive/reap checklist. Retain the selected live serving files and
any data needed for tomorrow's tuning; do not delete unique results before archive.
