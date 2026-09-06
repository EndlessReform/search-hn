# Nemotron 1B final static bench — 2026-09-05

The user authorized one final static embedding comparison before fresh E2E
sessions next turn. Reuse all 64,638 frozen stories and the original 196 questions,
unchanged score/date gates, exact cosine, and title-only BM25/RRF settings. Native
Nemotron is 2048 dimensions; cached prefix sweeps provide equal-width comparisons
with the earlier models. No agent trajectories are generated in this round.

**Cleanup remains OPEN.** Archive and checksum-verify the final experiment in
Garage, then reap temporary laptop/VM artifacts using the mandatory checklist
in [the main design doc](00-design.md). Include the new Nemotron UV environment,
weights and server process; do not forget these because they are outside Docker.

## Serving recipe and official precision options

- Model: [`nvidia/Nemotron-3-Embed-1B-BF16`](https://huggingface.co/nvidia/Nemotron-3-Embed-1B-BF16),
  pinned revision `c0c9fea93ea424587517f2c59e20db9f1d6bf615`, approximately 1.14B
  parameters, native 2048 dimensions. License: OpenMDW-1.1 per the model card.
- The pinned TEI build used for the other models does not implement this
  Ministral3 architecture. Run NVIDIA's documented Transformers/SDPA alternative
  in `/opt/searchhn-embeddings/nemotron`, using UV: torch 2.10.0+cu128,
  Transformers 5.16.1, Sentence Transformers 6.0.1, BF16 on the same RTX 3060.
  The research HTTP wrapper is [nemotron_server.py](../tools/nemotron_server.py).
- Bidirectional attention (`is_causal=false`), attention-mask-aware mean pooling,
  left padding. Exact client prefixes: `query: ` and `passage: `, each applied
  once. Cache unnormalized float32 outputs; shorten then L2-normalize both sides
  for exact cosine. BF16 compute does not imply a BF16 on-disk cache.
- No truncation. Prefix-aware corpus count: 2,528,094 tokens, max 378 per story;
  queries: 4,431 tokens, max 34. The research endpoint rejects inputs beyond
  2048 tokens; this is a benchmark limit, not the model's context capacity.
- Official SentenceTransformer CPU FP32/SDPA versus the BF16 endpoint, over two
  queries and three documents: minimum cosine **0.99996662**. Singleton/batch
  agreement and the longest-input smoke test also pass. The official configuration's
  `apply_yarn_scaling` compatibility warning was retained, not edited away.

NVIDIA also publishes an official
[`Nemotron-3-Embed-1B-NVFP4`](https://huggingface.co/nvidia/Nemotron-3-Embed-1B-NVFP4)
checkpoint (~1.03 GB weights), intended for vLLM. Its card lists Ampere, Ada,
Hopper and Blackwell compatibility, with a Blackwell serving example. It recommends
vLLM 0.25.0; 0.22.1 is also validated, while 0.23.x/0.24.x have known issues.
**NVFP4 has not been run here:** advertised compatibility is not a measured
3060 speedup. The BF16 card separately documents runtime `fp8_per_tensor` in
vLLM on Hopper/Ada, validated on H100. BF16 gives this round a reference-quality
baseline without another quantization variable. Cards are saved with the artifacts.

## Throughput measurement

The seeded 256-story pilot uses two repeats per batch size, after warmup, over
the same sampled stories as prior models. Initial estimates were over 15 minutes.
Profiling exposed redundant FastAPI recursive conversion before JSON serialization;
returning the already validated matrix through `JSONResponse` removes that work.
The first pilot is preserved under `pilot-before-json-response/`.

The final pilot selects **batch 8**, approximately 73 stories/s and a 14m 42s
projection. Batch 1 gives 40/s; 16/32/64 give approximately 69–70/s. Extra padding
outweighs the benefit of larger batches for these short inputs. Full backfill uses
one request at a time, including HTTP transfer, validation and durable local writes.
These are measured serving recipes: differences from the other models also include
Transformers/SDPA versus TEI and 2048 versus 1024 output floats.

First download plus model load to GPU took **22.41s**, measured inside the server
after Python imports. Cached model load took **1.53s**. Neither includes environment
installation or all process startup work. The new UV environment occupies about
7.1 GiB and model cache 2.2 GiB on the VM; shared UV cache is about 848 MiB.
The native document vector payload alone is 505.0 MiB on the laptop.

**Completed: 906.12s (15m 6s), 71.34 stories/s.** The original 196 queries took
another 2.07s in batches of eight. The journal verifies 8,080 document requests,
25 query requests, zero failures/retries, complete ordered coverage, and exact
token totals. At halfway (32,248 stories), elapsed time was 7m 31s and projected
total 15m 5s. The slight threshold overrun was reported; this pass stayed on the
3060. Peak sampled memory across the pilot/backfill was **3,327 MiB**, temperature
66°C. The complete local run is about 562 MiB including diagnostic vectors.

Server headers account for 436.62s of document tokenization, inference, pooling
and copying results to CPU. Summed HTTP request time is 894.01s; the full durable
pass is 906.12s. Response serialization, transport and client handling are therefore
material here. These measurements do not establish an optimized GPU throughput
ceiling. Warm singleton queries over the private SSH tunnel, all 196 questions
after one excluded warmup, measured **22.8ms median / 27.2ms p95** in the final
diagnostic pass (an earlier pass measured 23.4/29.8ms).

## Combined quality comparison

Each @20 cell is **target hits out of 196 / NDCG@20**. Dimensions are explicit:
the 1024 row compares widths; 2048 is Nemotron's native output. Cached shortening
does not save the already-paid embedding inference cost.

| Model | Dimensions | Dense @20 | Hybrid @20 | Full document pass |
| --- | ---: | ---: | ---: | ---: |
| Nemotron 1B BF16 | 2048 | 153 / **.6154** | 159 / .6169 | 15m 6s |
| Nemotron 1B BF16 | 1024 | 153 / .5976 | 157 / .6102 | Same cached pass |
| Perplexity 0.6B | 1024 | 153 / .6002 | 157 / .6094 | 14m 5s |
| Qwen 0.6B | 1024 | 149 / .5815 | 156 / **.6206** | 6m 36s |
| Jina small-retrieval | 1024 | 148 / .5678 | 156 / .5899 | 6m 43s |
| TE3-large | 1024 | 151 / .5663 | 157 / .6057 | Cached control |
| TE3-large | 1536 | 154 / .5873 | 160 / .6166 | Cached control |

Native Nemotron @8 is **135 dense / 142 hybrid**. Perplexity has 136/144,
Qwen 137/142, Jina 133/137 and historical TE3-1536 134/142. Nemotron does not lead
top-eight coverage, despite its strongest dense NDCG@20. Its @20 style split is
84 entity + 69 paraphrase for dense, 84 + 75 for hybrid, out of 98 each.

| Nemotron dimensions | Dense hits / NDCG@20 | Hybrid hits / NDCG@20 |
| ---: | ---: | ---: |
| 256 | 126 / .4799 | 139 / .5459 |
| 512 | 142 / .5499 | 150 / .5915 |
| 768 | 151 / .5684 | 155 / .6086 |
| 1024 | 153 / .5976 | 157 / .6102 |
| 2048 | 153 / .6154 | 159 / .6169 |

The complete five-model sweep is `combined-dimensions.csv`. All native Nemotron
and TE3 anchors reproduced exactly before sweep publication; the archived TE3
per-case metrics and frozen corpus/query hashes also matched. Thirteen focused
tests pass; Ruff and whitespace checks pass.

## Expanded numerical check

The five-input smoke test did not characterize every BF16 query. An additional
all-196 singleton-versus-batch screen found minimum cosine .999807, with 11 queries
below the initial .9999 threshold. **That screen failed; it is not counted as a
passing test.** We therefore encoded all questions with the official
SentenceTransformer CPU FP32 implementation and checked retrieval sensitivity.

Against FP32, minimum cosine is .999892 for the cached BF16 batches and .999889
for BF16 singleton queries; medians are .999968 for both. Two queries per path
fall just below .9999. This supports small numerical variation rather than a
different prefix, pooling policy or model. Retain the measured BF16 recipe and
report the variation instead of claiming exact batch invariance.

| Query encoding against the same BF16 documents | Dense hits @8 / @20 | Hybrid hits @8 / @20 | Dense / hybrid NDCG@20 |
| --- | ---: | ---: | ---: |
| BF16 batch 8, primary benchmark | 135 / 153 | 142 / 159 | .6154 / .6169 |
| BF16 singleton, agent-like serving | 134 / 153 | 141 / 159 | .6154 / .6161 |
| Official FP32 queries only | 134 / 153 | 141 / 159 | .6153 / .6180 |

The @20 hit counts survive both checks; each singleton/FP32 path loses one @8
hit per method. This is a query-only sensitivity check, not a full FP32 corpus
ablation. Original batched vectors and primary scores are preserved. Evidence:
`numerical-agreement.json`, `numerical-sensitivity-summary.csv`, per-case ranks,
official FP32 query vectors and singleton timing/vector receipts.

## Recommendation for regrouping

Nemotron is a credible third finalist, **not a decisive replacement**. At 1024
dimensions it effectively ties Perplexity on @20 coverage and NDCG; native 2048
improves dense ordering and adds two hybrid hits. Qwen remains the throughput
choice at approximately 2.3 times this measured end-to-end rate, with the best
native hybrid NDCG here. Perplexity retains the best sovereign hybrid @8 coverage
and much smaller native int8 cache. Jina remains out.

For next turn, compare Nemotron-2048, Qwen-1024 and Perplexity-1024 in the agreed
E2E setup, keeping the driver, questions, gates and tool settings fixed. Treat
these static results as known-item evidence from 98 targets with two related
question styles, not statistical proof or a general relevance benchmark. Do not
expand to more models or quantization runs before seeing whether the quality
differences affect agent success and turns. No E2E sessions were run this turn.

## Reproduction and evidence

Artifacts: `data/sovereign-embeddings-20260905/nemotron-2048/`. Preserve manifests,
ordered shards, request and GPU journals, initial/final pilots, official reference
check, server logs, runtime lockfile and source snapshot with the result tables.
The provider is intentionally research-only; production and archived OAI paths
are unchanged.

The private Nemotron endpoint remains running on VM loopback port 8080, reached
through local SSH port 58080. Earlier TEI experiment containers are stopped. On
the already provisioned VM, the server can be relaunched when that port is free:

```sh
cd /opt/searchhn-embeddings/nemotron
HF_HUB_CACHE=/opt/searchhn-embeddings/hf-cache HF_HOME=/opt/searchhn-embeddings/hf-home UV_CACHE_DIR=/opt/searchhn-embeddings/uv-cache /opt/searchhn-embeddings/bin/uv run --locked uvicorn nemotron_server:app --host 127.0.0.1 --port 8080 --no-access-log
```

The runtime `pyproject.toml` and `uv.lock` are copied into the artifact root;
`source/` separately preserves the laptop project's dependency files. No system
packages, sudo operations or permanent service registration were required.

```sh
uv run --locked --package search-research python -m search_research.sovereign_run pilot --model nemotron
uv run --locked --package search-research python -m search_research.sovereign_run embed --model nemotron --batch-size 8
OPENBLAS_NUM_THREADS=4 VECLIB_MAXIMUM_THREADS=4 uv run --locked --package search-research python -m search_research.sovereign_score --root data/sovereign-embeddings-20260905/nemotron-2048
OPENBLAS_NUM_THREADS=4 VECLIB_MAXIMUM_THREADS=4 uv run --locked --package search-research python -m search_research.sovereign_score --root data/sovereign-embeddings-20260905/nemotron-2048 --dimension-sweep
```
