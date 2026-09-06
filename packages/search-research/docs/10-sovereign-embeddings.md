# Sovereign embedding bake-off

Status: design and execution notes, 2026-09-05. Perplexity and Qwen native runs
and cached dimension sweeps are complete, using the unchanged initial questions.
Jina small-retrieval is also complete; further agent/omni experiments remain
follow-up. Jina did not earn a place over Perplexity/Qwen for this text workload;
see the [combined comparison](13-jina-first-bench.md).
The user subsequently selected [Nemotron 1B as the final static candidate](15-nemotron-first-bench.md),
using official BF16 through Transformers/SDPA because the pinned TEI build lacks
its architecture. That run and cached sweep are complete: 15m 6s full backfill,
153 dense / 159 hybrid hits at @20, with best dense NDCG but mixed advantages
against Qwen and Perplexity. Fresh E2E sessions remain next turn; the report
records the expanded BF16 numerical sensitivity check and official quant options.

**OPEN END-OF-BAKEOFF OBLIGATION:** archive and verify results in Garage, then
reap laptop/VM intermediate caches. Follow the explicit checklist at the top of
[the main design doc](00-design.md). Local flash is temporary working storage.

Completed: [Perplexity native-dimension first bench](11-pplx-first-bench.md).
The full document pass took 14m 5s on the 3060; quality was close to the matched
TE3 control, with stronger entity and weaker paraphrase @20 coverage.
Qwen's recipe and results are tracked in [its first-bench report](12-qwen-first-bench.md).

## Decision and sequence

Compare `perplexity-ai/pplx-embed-v1-0.6b`, then
`Qwen/Qwen3-Embedding-0.6B`, then `jinaai/jina-embeddings-v5-omni-small`.
Use TEI where it correctly implements the model. TE3-large remains the cached
research control. Local reproduction and independence from embedding API policy
or model retirement are requirements; the agent's driver LLM may still use OpenAI.
Do not substitute another hosted embedding API when local serving fails.

The [existing design](00-design.md), [static baseline](04-embeddings.md),
[engine experiment](05-engine-bakeoff.md), [fresh trajectories](07-semantic-luna.md)
and [miss audit](09-miss-audit.md) establish the starting point. Dense and hybrid
both reached 182/196 final target exposures in fresh Luna runs. Hybrid's static
gain did not establish a final exposure advantage. Keep both as candidates;
exclude reranking, article crawling, comment indexing and new query rewriting.

Current execution: a pinned Perplexity TEI instance and a small internal research
provider interface, with the archived OpenAI path unchanged. Measure a batch-size
knee and full-document ETA first. The user's threshold is **15 minutes for the
document backfill**: report if it looks longer so the 5090 can be considered;
otherwise continue, giving a midway update. Report one-time download/load timing
separately. No full agent loops or dimension sweep in this first bench.

## Frozen comparison inputs

The user selected the **second Luna run as the next baseline**. Both the primary
static screen and finalist agent runs reuse `data/luna-semantic-20260904/`:
64,638 nondead/nondeleted stories, score **>=25**, dates 2024-09-04 through
2026-09-04 inclusive; 196 questions, two styles for each of 98 targets.
The stored `input` is exactly null-filled title + newline + null-filled URL.
Reuse those bytes, without decoding URLs or adding bodies, scores or comments.
Only question text enters the query encoder; target IDs and audit answers do not.

Verified locally with SHA256 and DuckDB on 2026-09-05:

| Artifact | SHA256 |
| --- | --- |
| `data/luna-semantic-20260904/corpus.parquet` | `5a7b46f7ba78e1a1978b2aca947954168c284a4e0bd205ad310c5a9af6ed2dfd` |
| `data/te3-large-baseline-20260904/questions.parquet` | `737382c55bee6050050bfa712757cab865c45abe38b45477db1ac53aea067e4a` |
| `data/fts-baseline-20260904/plain-results/eval.jsonl` | `36164fd9b350626299242e3359ceac4b488ef48e1beaa3aed3ec662cb8aefc19` |

These files are working copies of the [Garage release](01-artifacts.md).
Do not query today's DB to reconstruct the frozen corpus. Assert target coverage,
row order, unique IDs, question identity and input hashes before any run.
Existing token counts are TE3 tokenizer counts; recompute lengths per local model.

The earlier >=10/105,081-story experiment remains historical evidence, not the
primary comparison. The selected snapshot includes 13 title/URL strings changed
since that earlier export. Use its existing TE3-1536 `vectors.npy` and the matched
question vectors; reuse other vectors only for identical input bytes under the
same recipe. Keep the second run's PG title-only BM25 settings throughout.

## Model recipes and serving quirks

All three advertise 1024-dimensional output and roughly 32K context. Long context
is not a requirement for these short inputs. Pin actual tokenizer/config limits.

| Candidate | Query / document handling | Pooling and output | Initial runtime |
| --- | --- | --- | --- |
| Perplexity 0.6B | Plain text on both sides; no instruction | Bidirectional attention, mean pooling, native int8-valued output | TEI v1.9.3, FP32 |
| Qwen 0.6B | Instruction on queries only; documents plain | Last non-padding token, cosine | Same TEI version, FP16 |
| Jina omni-small | Retrieval adapter; `Query: ` / `Document: ` | Last token, normalized output | Deferred; official Transformers path if TEI unsupported |
| Jina text-small-retrieval | Premerged retrieval adapter; `Query: ` / `Document: ` | Last token, cosine | Same TEI version, FP16; current Jina bakeoff candidate |

Perplexity: MIT license. Use the independent embedding model, not the contextual
chunk model. Its [card](https://huggingface.co/perplexity-ai/pplx-embed-v1-0.6b)
explicitly provides TEI deployment examples. In pinned TEI v1.9.3,
[model dispatch](https://github.com/huggingface/text-embeddings-inference/blob/v1.9.3/backends/candle/src/lib.rs)
requires FP32 for this architecture on CUDA and does not use the Flash Attention
path. This is a real serving asymmetry against Qwen, not a reason to assume equal
throughput from equal parameter counts. The
[TEI implementation](https://github.com/huggingface/text-embeddings-inference/blob/v1.9.3/backends/candle/src/models/pplx1.rs)
applies `round(127 * tanh(mean_pool))`, matching the published
[quantizer](https://huggingface.co/perplexity-ai/pplx-embed-v1-0.6b/blob/main/st_quantize.py).
Request `/embed` with `normalize:false`; retain that native output and derive
float32 cosine vectors locally. Integer-valued JSON floats are not proof of int8
storage. Do not quantize twice or treat unnormalized dot products as cosine.
Binary output is a later compression experiment, outside the primary matrix.

**Precision clarification:** FP32-only describes the pinned TEI implementation,
not an established model requirement. Its CUDA dispatch explicitly leaves BF16
Flash Attention as a TODO. Perplexity's official Transformers `modeling.py`
advertises both SDPA and Flash Attention support. A BF16 Transformers/SDPA pilot
is therefore a reasonable next efficiency check, with output-quantizer and
retrieval validation before relying on it. It has not been measured here.
Qwen's measured efficiency lead must not be interpreted as proof that Perplexity
inherently requires twice the weight memory or cannot achieve comparable speed.

Qwen: Apache-2.0 license. Freeze its published default query prompt exactly:

```text
Instruct: Given a web search query, retrieve relevant passages that answer the query
Query:
```

Append the query immediately after the colon, with no added newline or space,
as in its [saved configuration](https://huggingface.co/Qwen/Qwen3-Embedding-0.6B/blob/main/config_sentence_transformers.json).
Apply it once, never to documents. The
[card](https://huggingface.co/Qwen/Qwen3-Embedding-0.6B) recommends instructions,
supports MRL from 32 to 1024, and demonstrates padding-aware last-token pooling.
Its GPU/CPU Docker examples appear to swap image tags; use TEI's GPU packaging
instead of copying those commands literally. Transformers reference requires
>=4.51.0; pin the tested version. A no-instruction query-only ablation at 1024
dimensions may measure prompt sensitivity, separately from the main ranking.
Do not tune an HN-specific instruction on these labels during model selection.

Jina follow-up (2026-09-05): the user explicitly requested evaluating the
text-first path and accepts the NC license for this personal project. Recommended
next candidate: **`jinaai/jina-embeddings-v5-text-small-retrieval`**, the official
premerged retrieval adapter, rather than the combined multi-adapter repository.
The user subsequently authorized this run; execution and results are tracked in
[the Jina first-bench report](13-jina-first-bench.md).

The [omni paper](https://arxiv.org/abs/2605.08384) says the text backbone remains
frozen and produces the same text embeddings; training changes the modality
connectors. The [omni retrieval card](https://huggingface.co/jinaai/jina-embeddings-v5-omni-small-retrieval)
explicitly supports querying an index built with the matching text-small-retrieval
model without reindexing. Preserve the small family, retrieval task, dimension,
prefixes and normalization; nano or a different task adapter is a different space.
Before adding omni later, pin compatible revisions and validate cross-backend
text-vector agreement. This compatibility is documented, not yet locally tested.

The [text-small-retrieval card](https://huggingface.co/jinaai/jina-embeddings-v5-text-small-retrieval)
provides TEI 1.9 CPU/GPU commands, with FP16 and last-token pooling for GPU.
Its config uses ordinary `qwen3`/`Qwen3Model`; its SentenceTransformer modules are
standard Transformer, Pooling and Normalize. Our pinned
[TEI 1.9.3 backend](https://github.com/huggingface/text-embeddings-inference/blob/v1.9.3/backends/candle/src/models/qwen3.rs)
handles both old `rope_theta` and this config's nested `rope_parameters.rope_theta`.
Thus small-retrieval has documented and source-supported TEI compatibility;
the next run must still pass the same reference-vector smoke check as Qwen.

Use **`Query: `** for questions and **`Document: `** for corpus texts, including
the trailing spaces, exactly once. Unlike Qwen, Jina requires the document prefix
too. No Qwen-style Instruct prefix. Native width 1024; use the same cached MRL
sweep and cosine recipe. Keep literal title/URL strings as text.

The other text size, [nano](https://huggingface.co/jinaai/jina-embeddings-v5-text-nano),
uses EuroBERT and 768 dimensions. Its merged config is `model_type=eurobert`,
absent from the pinned TEI Candle dispatch, so it is not a drop-in for our current
image. Use a separately validated Transformers/other backend if testing nano.

Omni-small itself is ~1.74B with the extra towers and custom
`jina_embeddings_v5_omni` wrapper, still absent from TEI's dispatch. It can later
run as a separate multimodal service using the card's Transformers or vLLM path;
there is no need to move text ingestion out of TEI. Selective `modality="text"`,
`"vision"` or `"audio"` loading is also documented for Transformers. All Python
environments/installations use UV.

## Quality matrix and controls

1. Encode documents and queries once at native 1024 dimensions per local recipe.
   Sweep **256, 512, 768, 1024** using prefixes, converting to float32 and
   L2-normalizing both sides after shortening. Fail on nonfinite/zero vectors.
   These are dimension ablations, not four GPU backfills. Lower dimensions reduce
   index/network costs; do not claim proportional encoder compute savings.
2. Compare each dimension using exact dense cosine and the second run's PG hybrid:
   title-only English pg_textsearch BM25, top 100 per branch, RRF k=60, dense
   weight 1 and lexical weight .5. Reuse its lexical implementation and index
   configuration; no DuckDB lexical substitution or fusion-weight sweep.
   Cache lexical lists and keep BM25-only as a model-independent control.
3. Reuse cached TE3 vectors shortened to the same four dimensions and retain the
   selected 1536-dimensional control. Compare equal storage as well as native
   dimensions. On this >=25 snapshot, TE3-1536 dense/hybrid Recall@8 are 68.4/72.4%,
   Recall@20 78.6/81.6%, and NDCG@8 .560/.592. Reproduce these static results before
   adding candidates. Do not use 92.9% agent exposure as a static target.
4. Report Recall and single-anchor NDCG @1/5/8/10/20, target ranks, top-20 IDs,
   either-variant Recall@8 and entity/paraphrase and recent/older strata.
   Use ascending story ID for exact score ties, including the cutoff boundary;
   Perplexity quantization makes tie handling worth checking explicitly.
5. Report paired gains/losses and bootstrap differences over the 98 **stories**,
   keeping each question pair together (fixed seed, 10,000 resamples, 95% interval).
   Small changes are descriptive, not independent 196-case significance claims.
   Preserve all labels, including the audited ambiguous and weak-title cases.

Primary decision metrics are Recall@20 and NDCG@20, matching the tool's 20-result
default; keep @8 for early ranking and historical continuity, and @10 for the
agent's frequently requested shorter lists. Present the quality/latency/storage frontier, not an
invented composite score. Provisional shortlist rule for review: retain recipes
within two percentage points of matched TE3-1536 Recall@20, plus any candidate
with a compelling measured resource tradeoff. This is a triage heuristic, not a
statistical noninferiority claim or a user-approved production threshold.
The set is development/regression data; an untouched natural-query set is still
needed for acceptance. Do not pick winners solely from the 18 audited misses.

## When to pay for fresh Luna trajectories

Use a funnel instead of running every model/dimension combination through Luna:

1. **Static screen:** run all 196 original question strings once per recipe and
   score all listed cutoffs. Screen quality alongside measured serving performance.
   Retain one or two distinct finalists; do not crown a winner on a tiny static
   difference or run all four dimensions through the agent.
2. **Frozen-query replay, if useful:** retrieve against the actual query strings,
   filters, requested limits and pages from the second Luna run. Deduplicate
   identical encoder inputs and cache them. Compare TE3 and finalists on exactly
   these searches without new LLM calls. This tests realistic short/rewritten
   queries, but not how Luna would react to changed results; the query distribution
   was generated using TE3. Replay is diagnostic, not an agent success estimate.
3. **Finalist loops:** run the full 196-question set with `gpt-5.6-luna` for the
   shortlisted model/dimension/retrieval recipes. Preserve the second run's prompt,
   settings, 10-turn budget, tool interface, result limits, pagination and filters.
   Start with a small fixed smoke cohort for wiring failures, then complete the
   frozen set. Retain archived TE3 as context; if the harness/interface or recovery
   policy changes materially, run a fresh matched TE3 control as well.

Fresh loops are needed before adopting a changed retrieval recipe: result content
can change subsequent queries, stopping, comment reads and final answers. They
are also needed when changing the driver LLM, agent instructions, tool descriptions,
pagination/result budgets or filter behavior. Static recall cannot measure those
feedback effects, nor answer correctness or token cost.

Do not rerun Luna for every dimension, batch-size adjustment, equivalent serving
backend or serialization change. Validate vector/ranking parity for changes that
are meant to preserve behavior; use static/replay checks for exploratory ranking
changes, followed by the finalist loop before adoption. Measurable numerical/rank
drift invalidates an equivalence claim. Dense versus hybrid needs another loop
only for the local-model finalist if both remain credible after screening; the
prior TE3 tie does not prove all embedding models will tie.

## Correctness before throughput

Before bulk encoding, compare TEI against the official reference at the **same
model revision and compute precision**, using a fixed smoke set independent of
target answers: short/long strings, mixed-length batches, Unicode, URL punctuation,
duplicate inputs, and benign security-topic search text. Confirm prompt bytes,
tokenization, special tokens, attention mode, pooling and quantization ordering.
Check output order/count, dimension, finite values, norms and singleton-vs-batch
agreement. Record vector cosine agreement, maximum deviation and pairwise rank
agreement; set precision-specific tolerances before examining retrieval scores.
Do not demand bitwise equality across kernels or proceed through unexplained drift.

Initially inspect token-length distributions, then select a serving limit covering
all frozen inputs including model wrappers. Start with a 512-token limit only if
the inventory proves it sufficient; otherwise raise it. Disable automatic
truncation for scored requests and fail explicitly on overlength inputs. Record
any exceptional treatment separately; no quiet dropped rows or shortened URLs.

Keep weight/compute precision, output quantization and index storage dtype as
three separate fields. Perplexity FP32/native-int8 and Qwen FP16/float output are
the supported deployment comparison. If Perplexity is too slow, an official
alternative backend/precision becomes a separately identified treatment after
reference checks, not a silent downgrade or an immediate sovereignty waiver.

## Performance on the actual VM

Run one resident model at a time. Separate download time, cold load/warmup,
steady-state embedding and local retrieval time. Do not compare old OpenAI
batched matrix timings with new single-query end-to-end timings.

- Backfill: replay a fixed representative 10,000-document sample, covering token
  length strata. Warm up, then measure three runs of client batches 1/8/32/64,
  concurrency 1/2/4 within memory limits. Record actual dynamic batch/token limits,
  docs/s, model-specific tokens/s, wall time, peak GPU/RAM, OOM/errors and retries.
  Start TEI conservatively at 2048 max batch tokens and client batch 8; grow only
  after successful warmup. Extrapolate 64,638/R seconds at observed R docs/s,
  then report measured full-backfill time when available.
- Interactive: cycle all 196 queries five times after warmup, with client caches
  disabled; measure p50/p95/p99 request latency at concurrency 1/2/4. Report raw
  samples and network-inclusive latency separately from inference timings when
  available. Caching is a separately labeled workload.
- Contention: repeat interactive traffic at one offered query/second while the
  background backfill runs, then a five/second stress condition. Report achieved
  rate, queue time, query tails and remaining backfill throughput. Do not assume
  TEI dynamic batching guarantees query priority; constrain document work at the
  driver if necessary and record the policy.
- Storage: compare float32 matrices at all dimensions (about 63/126/189/252 MiB
  for this corpus). Report native-int8 disk savings separately from the float32
  search working set and actual DB/index footprint. No ANN in the quality matrix.

For this first bench, the user set a **15-minute** document-backfill threshold
(~71.8 docs/s for 64,638 stories), above which to discuss the available 5090.
Longer-term proposed query targets remain p95 <=250 ms idle and <=500 ms under
the 1-QPS mixed load, with no lost inputs. Those latency targets are unapproved
service expectations, not grounds to discard sovereignty without discussion.

## Access verification and launch prerequisites

Only `ssh maya@magi06-inference` was accessed. Network sandbox DNS initially
failed; the authorized external-network retry worked. No melchior access.

| Check on 2026-09-05 | Observation |
| --- | --- |
| SSH / identity | Noninteractive login works, maya UID/GID 1000, docker group |
| Resources | 8 CPUs, ~23 GiB RAM (~22 available), 221 GiB free on /opt filesystem |
| GPU | RTX 3060, 12,288 MiB, idle, compute capability 8.6 |
| Docker | Engine 29.8.0; NVIDIA runtime registered; no running containers |
| Diagnostic after operator update | TEI CUDA initialization, allocation, memory write/readback pass using normal library path |
| Downloads | Hugging Face HTTPS 200; actual TEI image pull succeeded from GHCR |
| Directory after operator update | /opt/searchhn-embeddings exists, maya-owned and writable |
| User Python tooling | uv not found on PATH; TEI container does not require host Python |

**Current status: CUDA preflight passes after the operator's driver update.**
Driver/KMD 610.57.04 reports CUDA UMD 13.3. Inside the pinned TEI image, `cuInit`,
device enumeration, allocation, GPU memory write and device-to-host readback all
succeed. The router executable also returns `text-embeddings-router 1.9.3`.
Model loading and actual inference have since passed the first-bench smoke checks.

TEI supplies CUDA user-space runtime libraries; a host CUDA development toolkit
is unnecessary for a container launch. The container toolkit ordinarily injects
NVIDIA driver libraries/devices. TEI also bundles `cuda-compat-12-9`, including
an alternative user-mode CUDA driver, so absence of the VM's `libcuda.so.1` alone
does not prove TEI cannot run. See [NVIDIA prerequisites](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html)
and [compute versus utility capabilities](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/docker-specialized.html).
Historical isolated probes before the driver update, with no server or weights:

- Official `nvidia/cuda:12.0.0-base-ubuntu22.04`: driver `dlopen` fails;
  `cudaGetDeviceCount` and `cudaMalloc(4)` both return error 35. This generic error
  does not by itself prove driver 550 is too old for CUDA 12.0; the driver library
  is missing in that container.
- Actual `ghcr.io/huggingface/text-embeddings-inference:86-1.9.3`: the NVIDIA
  prestart hook rejects `cuda>=12.9` before the entrypoint or diagnostic executes.
  Therefore the image's bundled compatibility library was not exercised.

Commands, probe source and exact output are preserved in
[the preflight evidence](evidence/20260905-cuda-preflight.md).
The operator chose a full driver update instead of the initially proposed single
library installation. No further system changes are indicated by the successful
preflight. Keep project model cache, reference UV environment, configuration and
logs in the now-writable `/opt/searchhn-embeddings`.

**Required launch adjustment for this image/driver pair:** use Docker
`--entrypoint /usr/local/bin/text-embeddings-router`, retaining the image's normal
library path. The stock shell entrypoint looks for `CUDA Version` in nvidia-smi;
driver 610 now prints `CUDA UMD Version`. The unmatched value defaults to zero,
so the script incorrectly prepends `/usr/local/cuda/compat`. With that selection,
the diagnostic returns `cuInit=803`; without it, CUDA operations pass. This was
tested through the actual entrypoint with a temporary probe mounted in place of
the router. Direct launch skips that faulty library-selection script, while the
NVIDIA container prestart compatibility checks remain enabled. No image rebuild
or host toolkit installation is needed for the verified preflight configuration.

The inspected and pulled Ampere-86 image is TEI `86-1.9.3`, pinned to
`sha256:a7d82dfef16c3bf1a95e93f5b226f358312512dbb0d585b48c3cf886f9d470a9`.
It remains in the VM's Docker image cache for follow-up, with no running container.
Pin any replacement image/build and model/tokenizer revisions and file hashes.
The [v1.9.3 CUDA Dockerfile](https://github.com/huggingface/text-embeddings-inference/blob/v1.9.3/Dockerfile-cuda)
builds with CUDA 12.9.1 and installs `cuda-compat-12-9` in its runtime stage.
[Forward compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/forward-compatibility.html)
has hardware restrictions; its presence does not guarantee support on this GeForce.
[CUDA minor-version compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/minor-version-compatibility.html)
is a separate mechanism. Preserve the historical prestart failure and subsequent
successful checks; no NVIDIA compatibility-check bypass was used.

Bind the server to VM loopback and use an SSH tunnel from the laptop; no public
endpoint. Download only needed model/runtime assets. Homepage/registry reachability
does not yet verify weight CDN downloads. After
caching, verify model startup and inference without external-network access.

## Harness handoff and evidence

Next implementation should provide explicit query/document encoding operations
and a validated immutable recipe. Decouple the existing OpenAI-specific tokenizer,
batching, dimensionality and rate/cost journal from reusable frozen-input loading,
sharding and evaluation. Keep the old cached TE3 path reproducible. Validate server
model identity/recipe before use; never silently fall back to OpenAI or mix spaces.

Write each recipe to a new `data/sovereign-embeddings-20260905/<recipe>/` directory.
Retain full-dimension document/query shards, per-dimension ranks/neighbors/strata,
quality and performance tables, raw timing/error journals and reference checks.
Record corpus/eval hashes, source revision, model and tokenizer revisions, image
digest, library versions, prefixes, pooling, truncation, compute/output/storage
precision, dimensions, normalization, batch policy and hardware/driver inventory.
Resume only matching manifests with intact shards; mismatches fail explicitly.
Publish a new Garage release after review; never overwrite the archived baseline.

After regrouping on the first native-dimension Perplexity bench, finalists can
receive fresh Luna trajectories as specified in the funnel above.
Hold driver model/settings, system prompt, tools, pagination, filters
and budgets constant across embedding candidates; any payload cleanup gets its
own interface revision and matched TE3 rerun. Report exposure, first-hit turns,
tokens/cost, citations and answer quality separately, retaining infrastructure
failures. This later phase is outside the current first-bench scope.
