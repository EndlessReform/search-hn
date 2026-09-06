# Final bakeoff manifest and proposed E2E sequence

**Latest status:** the [E2E comparison](19-sovereign-e2e.md) is complete. The
[Pplx BF16 gate](20-pplx-vllm-gate.md) also passed: stock vLLM 0.28.0, batch 64,
1,735 MiB sampled peak, 3m45s full backfill, essentially unchanged static quality.
Pplx BF16 is ready for tuning; the previous E2E Pplx arm used FP32. The planning
sections below preserve the earlier sequence and hypotheses.

Prepared 2026-09-05. Static screening is complete. This document fixes the proposed
comparison scope before continuation; it does not launch paid trajectories.
Perplexity's reduced-precision preflight remains open. The executable run manifest
must record its resolved serving recipe, exact source/prompt hashes and spend guard
before any E2E sessions start.
The subsequent [Luna throughput analysis](17-luna-throughput-plan.md) adds a routing
and pacing preflight: aim for 20–30 minutes only after demonstrating 1.3–2M TPM
capacity. The old 500k TPM account limit implies a roughly 59-minute input-only
floor for these three arms. The subsequent [authorized OpenRouter probe](18-openrouter-capacity-probe.md)
confirmed 2.78M input TPM at concurrency 8 with no errors. Recommend OpenRouter →
OpenAI standard/default, concurrency 8, with an initial 15–20-minute E2E planning
target subject to the wiring smoke. No E2E sessions have yet run.

## What “full dataset” means

Every search runs against the full **64,638-story corpus**. An E2E evaluation
contains **196 questions over 98 target stories**, with two related question styles
per target. There is no plan for 64,638 agent sessions. Keep all 196 questions for
each finalist: with only 98 targets, reducing this set further makes small model
differences harder to interpret and can exclude long-tail failure cases.

## Candidate manifest

| Candidate | Dimensions | Current validated compute/backend | Output cache | Proposed first E2E arm |
| --- | ---: | --- | --- | --- |
| Qwen3-Embedding-0.6B | 1024 | FP16 / TEI 1.9.3 | float32 | Dense, 196 questions |
| pplx-embed-v1-0.6b | 1024 | FP32 / TEI 1.9.3 E2E; BF16 / vLLM 0.28.0 static gate passed | native int8 | Dense, 196 questions |
| Nemotron-3-Embed-1B-BF16 | 2048 | BF16 / Transformers SDPA | float32 | Dense, 196 questions |
| TE3-large | 1536 | Archived second Luna control | float32 | Historical dense/hybrid context |
| Jina small-retrieval | 1024 | Completed; not retained | float32 | None |

Use pinned model revisions, tokenizers, role prefixes, pooling and output transforms
from each existing `data/sovereign-embeddings-20260905/<recipe>/manifest.json`.
The [Nemotron report](15-nemotron-first-bench.md) contains the complete static table,
efficiency comparison and numerical sensitivity. Keep Perplexity because its
quality is close, native storage is compact, and its efficiency ceiling remains
unresolved by the TEI-only FP32 run. Qwen is the measured efficiency contender;
Nemotron is the native dense-ranking contender. No further model hunt or dimension
sweep is included in E2E.

### Perplexity preflight plan (completed; see report 20)

Updated after serving-stack review: prefer a **stock vLLM serving pilot** before
considering a custom Transformers webserver. Upstream vLLM's Qwen3 implementation
supports `is_causal=false` bidirectional attention, its pooling runner supports
mean pooling with output activation/normalization disabled, and its existing
OpenAI-compatible embeddings endpoint provides the HTTP service. Perplexity's
`PPLXQwen3Model` architecture is not registered by name; a Qwen3 architecture
override is a plausible compatibility path, **not yet runtime-validated**.
Pin a release containing these capabilities and verify weight loading/tokenization.

Preserve Perplexity's output transform: unnormalized mean-pooled vectors go through
its tanh/round/clamp int8 quantizer before any cosine normalization. Generic vLLM
int8 output encoding is not assumed equivalent. This small deterministic transform
can live in the existing embedding client, without a custom webserver or vLLM fork.
First compare vLLM FP32 against the frozen TEI/reference outputs to isolate backend
compatibility, then test BF16 quality, VRAM and throughput. The official Transformers
implementation remains the correctness reference. No BF16 serving success is assumed.

Sources: [vLLM Qwen3 attention implementation](https://github.com/vllm-project/vllm/blob/main/vllm/model_executor/models/qwen3.py),
[embedding server and pooling options](https://docs.vllm.ai/en/latest/models/pooling_models/embed/),
[Perplexity quantizer](https://huggingface.co/perplexity-ai/pplx-embed-v1-0.6b/blob/main/st_quantize.py).
TEI's CUDA implementation still explicitly restricts this model to FP32; rebuilding
its current main branch alone does not supply BF16 support.

If adopting BF16, give it a distinct manifest and document/query cache; validate
the full static scores before freezing that E2E arm. Do not silently pair BF16
queries with old FP32 documents or overwrite the old control. If BF16 is not
selected, the existing validated FP32 recipe remains usable. No BF16 performance
or quality result is assumed here.

## Frozen inputs and behavior

- Corpus: `data/luna-semantic-20260904/corpus.parquet`, SHA256
  `5a7b46f7ba78e1a1978b2aca947954168c284a4e0bd205ad310c5a9af6ed2dfd`.
  Nondead/nondeleted stories, score >=25, 2024-09-04 through 2026-09-04 inclusive.
  Embed the unchanged null-filled title + newline + URL; retain result metadata.
- Questions: `data/te3-large-baseline-20260904/questions.parquet`, SHA256
  `737382c55bee6050050bfa712757cab865c45abe38b45477db1ac53aea067e4a`.
  No regenerated paraphrases, selected-miss subset or answer leakage.
- Driver: `gpt-5.6-luna`, same recorded Responses API settings and system prompt
  as the second Luna run; max 10 turns and 4096 output tokens per request.
  Verify actual prompt, tool schema and model-settings hashes before dispatch.
- Preserve the second run's 20-result default, explicit 1–20 limits, pages 1–3,
  score/date/domain filters, sorting semantics, comment tool and session caches.
  **@8 is a diagnostic cutoff, not the E2E default result count.**
- Exact cosine with deterministic story-ID tie breaks, prefix then L2 normalization.
  No ANN, reranker, prompt rewrite, body/comment embedding or fusion-weight tuning.
- Begin at concurrency 2 with shared rate-limit feedback and the established
  recovery policy, then ramp to the concurrency validated by the throughput pilot
  (initial target 8). Staying at 2 would impose about a 40-minute model-call floor.
  Save infrastructure failures separately; retain real turn-budget
  exhaustions as outcomes. No silent retries until a successful trajectory appears.
- Record a fresh explicit spend ceiling and current metering configuration before
  launching. The old $4.80 cap covered the previous experiment; it is not an
  automatic authorization or cost estimate for the new matrix.

Comments still come from the live mirror under the existing behavior. Log returned
tool payloads; this is not a perfectly frozen comment-content benchmark. Interleave
matched target blocks across treatments where practical so serving time/order does
not systematically favor one model. Do not change the model-visible interface merely
to enable provider swapping. Archive source/environment hashes and actual outputs.

## Execution sequence and stopping scope

1. Complete Perplexity preflight and provider integration. Verify archived TE3
   static rankings and all role/recipe invariants. Select a fixed eight-question
   wiring smoke cohort spanning style and age before observing new E2E outcomes.
   Successful smoke sessions count toward the 196 only if the final manifest is
   unchanged; preserve and rerun affected sessions if the harness changes.
2. Run **three dense arms × 196 questions = 588 sessions**, one trajectory per
   question/arm. This isolates the provider change and preserves dense as the
   control selected in the [second Luna report](07-semantic-luna.md). No repeated
   seeds per case and no E2E dimension sweep initially.
3. Regroup using paired success, evidence and resource results. If dense is sufficient,
   stop. If hybrid remains an adoption candidate, run **196 hybrid sessions for the
   selected provider only**, comparing against its existing dense arm. Hybrid keeps
   title-only English BM25, top 100 per branch, RRF k=60, dense weight 1 and lexical
   weight .5. Total then becomes 784 sessions, rather than the 1,176-session full
   three-provider × two-retrieval-method matrix.
4. Keep archived TE3 as historical context, not a randomized contemporaneous arm.
   A material prompt/interface/recovery change requires a fresh matched TE3 control
   under the earlier protocol. Also run one if claiming current parity with OAI is
   essential; it adds 196 sessions for one method. The initial local-provider
   comparison does not require that claim.

Frozen-query replay is optional diagnostic work, not a prerequisite after this
static screen and not an estimate of E2E success. It cannot show how changed
results alter Luna's next query, comment reads, stopping or final answer.

The earlier 392 dense/hybrid sessions recorded $3.5258 in conservative model
charges/reservations, including infrastructure recovery. This is historical context,
not current pricing or a forecast. Their wall time mixed changing rate-limit pacing,
so do not extrapolate an execution-duration promise from it.

## Decision measurements

Primary: final target exposure across all 196 questions, plus target evidence/citation
in the final answer. Exposure alone is not answer correctness; keep the two distinct.
Report turns and tokens to first exposure, final turns, input/output tokens and actual
metered cost, search-list counts, turn exhaustions and infrastructure errors. Separate
embedding service time, model time and rate-limit waiting when reporting latency.

Use paired per-question comparisons, entity/paraphrase and recent/older strata,
and paired win/loss examples. Bootstrap differences over **98 target-story clusters**,
keeping both question variants together, with fixed seed and 10,000 resamples.
Do not claim narrow differences establish universal superiority. This repeatedly
inspected set is development/regression data, not an untouched acceptance set.

If quality is indistinguishable at this resolution, favor lower serving cost and
operational complexity. Do not manufacture a composite quality/latency score or
silently reinterpret absent statistical significance as proven equivalence.

## Required closure

**Cleanup remains OPEN.** After the final decision, inventory and publish immutable
results to Garage, verify restored hashes, and reap the scoped laptop/VM intermediate
artifacts. Include all precision variants, model weights, Docker experiments and
the non-Docker Nemotron UV environment. Follow the mandatory checklist in
[the main design doc](00-design.md); record reclaimed space and tell the user when
cleanup is actually complete.

## Execution authorization and credit guard (2026-09-05)

The user authorized the full three-dense-arm run over OpenRouter, with **16 shared
concurrent conversations**. The earlier capacity replay is complete; it is not an
E2E observation. Current account credit is approximately **$10**, so this new run
uses an **$8 cumulative run ceiling**, leaving $2 headroom. Planning estimate is
$5–7 for 588 sessions, conditional on the new traces resembling prior token use.

Use the already validated precision recipes: Pplx FP32/native int8, Qwen FP16,
Nemotron BF16. The Pplx BF16 experiment is deferred. Three private VM services
fit concurrently at 7,224 MiB/12,288 MiB idle allocated GPU memory. This shared-GPU
E2E run is not an isolated serving-throughput benchmark.

Execution lives in `data/sovereign-e2e-20260905/`. Its immutable manifest includes
source, input hashes, model routing, date, turn/output limits, and embedding recipes.
Cached document matrices occupy separate exact-scan PostgreSQL tables named
`sovereign_vectors_{pplx,qwen,nemotron}`; archived TE3 tables remain intact. Add these
three tables and the two `searchhn-e2e-*` containers to the **OPEN cleanup inventory**.

The preselected smoke is the two lowest story IDs in each age cohort, both prompt
styles, all three arms: 24 sessions. If the scientific manifest remains unchanged,
they count toward the final 588. A full invocation skips terminal sessions and
archives interrupted/infrastructure attempts, preserving their traces and costs.
A genuine ten-turn exhaustion stays in the quality denominator and is not retried.

The durable ledger reserves a conservative text-byte upper bound plus the full
4,096 output tokens before every model request. It replaces that reservation with
OpenRouter's streamed reported cost when available, otherwise explicit token-price
estimation. Failed requests with unknown billing retain their reservation. SDK
retries are disabled so there are no hidden, unreserved attempts. All model calls
stream through the existing journal path. A budget or credit/access failure pauses
new requests; other failures are recorded per session and do not cancel siblings.
In-flight reserved responses drain; unfinished conversations restart from the seed
on a later invocation (the partial attempt remains archived).

Run/resume command (same root and scientific manifest):

```sh
uv run --locked --package search-research python -m search_research.sovereign_rollouts --concurrency 16 --budget-usd 8
```

After a recharge, explicitly raise `--budget-usd` to the desired **total cumulative
run allowance**, not merely the newly purchased amount. Never delete/reset the
ledger to resume. A process lock prevents two drivers from sharing the same budget.

Preflight result: all **588 cached first-query top-20 lists reproduced exactly in
order** through the new repository/PG tables. Twenty-one relevant tests passed.
The 24-session smoke completed with zero tool errors in **18.54 seconds**, using
80 streamed model requests with **$0.11203476 reported cost** and zero unresolved
reservations. Those 24 observations are retained in the full run. Full dispatch
began afterward with the same scientific/source manifest and concurrency 16.
