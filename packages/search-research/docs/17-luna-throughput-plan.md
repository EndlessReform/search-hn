# Luna E2E throughput estimate — 2026-09-05

The user asked to estimate completion time before proceeding and raised OpenRouter
as an alternative route. This is an analysis of saved traces and public provider
metadata; no inference requests, route migration or E2E runs were performed.
**Follow-up:** the user subsequently authorized the
[OpenRouter capacity probe](18-openrouter-capacity-probe.md), which confirmed ample
capacity. The estimates below preserve the pre-probe analysis; use that report's
measured operating point for continuation.

## Saved evidence and the earlier delay

All **392 terminal trajectories** survive under
`data/luna-semantic-20260904/{dense,hybrid}/trajectories/gpt-5.6-luna/`, including
model inputs/outputs, tool results, timestamps and token usage. There are 386 normal
completions and six real turn-budget exhaustions. Separate infrastructure attempts
remain preserved. These final traces contain **1,538 successful model responses**.
Per-request totals were recomputed and exactly match `efficiency.parquet`.

| Previous treatment | Sessions | Model calls | Input tokens | Output tokens | Cached input |
| --- | ---: | ---: | ---: | ---: | ---: |
| Dense | 196 | 774 | 9,779,251 | 71,955 | 6,436,989 (65.8%) |
| Hybrid | 196 | 764 | 9,290,598 | 69,333 | 5,931,152 (63.8%) |

The surviving final traces span approximately **71m 9s** from first model request
to last response. That span includes pauses and recovery scheduling; it is not
pure API service time. Successful model-input to model-output intervals are much
shorter: dense mean **2.09s**, median **1.77s**, p95 **4.03s**; hybrid mean 2.04s,
median 1.77s, p95 3.63s. These are full response intervals, not TTFT measurements.
In the current recovered driver, `model_input` is logged after limiter waiting.

The [earlier report](07-semantic-luna.md) records an initial overly conservative
estimated-token limiter, its replacement with header-driven pacing, and 44 genuine
token-rate-limit failures subsequently recovered. Saved response headers show
**500 RPM / 500,000 TPM**. These are observed historical account limits, not a
live quota check. Even ideal delivery of the previous 19.1M input tokens would
need roughly 38 minutes at that token capacity. Faster generation alone would
not solve that bottleneck.

The context tail is material: dense session input totals have median 20,876,
p95 256,306 and maximum 547,451 tokens across successive turns. Across both arms,
the largest individual request is 125,176 input tokens (p95 approximately 43,052).
Project from full-denominator totals rather than medians or only easy smoke cases.

## Capacity for the proposed three dense arms

Assume the new recipes behave like the old dense arm. For **588 sessions**, that
projects **2,322 model calls, 29,337,753 input tokens and 215,865 output tokens**.
This includes repeatedly submitted conversation history; it is not 29M unique
source tokens. New retrieval can change turns and payload size, so it is a planning
baseline, not a guaranteed workload.

| Finish all 588 sessions in | Input TPM required | Requests/minute | Capacity target with 30% margin¹ |
| ---: | ---: | ---: | ---: |
| 60 minutes | 489k | 39 | 640k TPM |
| 30 minutes | 978k | 77 | 1.28M TPM |
| 20 minutes | 1.47M | 116 | 1.92M TPM |
| 15 minutes | 1.96M | 155 | 2.56M TPM |

¹ Applies 1.3× to observed input + output work. This is operational headroom, not
a confidence interval or a replica of any provider's exact token-admission formula.
Large requests, retries, warmup, quota sharing and completion tails can add time.

At the historically observed 500k TPM, **input tokens alone imply 58.7 minutes**.
Allow approximately 75–90 minutes as an initial planning envelope until a new
capacity pilot establishes better pacing; this envelope is not measured runtime.
One dense arm alone has a 19.6-minute input-only floor at that capacity.

An approximate lower bound is the maximum of input tokens / effective input TPM,
calls / effective RPM, and summed successful call duration / concurrent requests,
with tools, scheduling and tail effects added. At the observed 2.09s mean response
time, concurrency 2 needs about **40.3 minutes for model responses alone** across
three dense arms. It cannot deliver a 20–30-minute run even with unlimited TPM.
Start at two for wiring, then ramp through four to **eight concurrent sessions**,
considering 12 only if actual latency/tool waiting leaves capacity unused.

**Recommended target: 20–30 minutes for the three-arm batch**, conditional on
demonstrating **1.3–2.0M TPM and roughly 80–120 successful RPM** on this workload,
with stable tail latency. Provision roughly 100–150 RPM of headroom as well.
That target covers E2E execution, not an additional document embedding backfill.

OpenAI documents that cached input still counts against TPM; don't subtract the
roughly 65% cached fraction from this capacity calculation. Caching helps billing
and prompt processing, but does not remove that quota constraint.
[Official caching documentation](https://developers.openai.com/api/docs/guides/prompt-caching).
The current public Luna table lists Tier 1 at 500k TPM and Tier 2 at 2M TPM, but
the user's current entitlement must be checked rather than inferred from the table.
[Luna limits](https://developers.openai.com/api/docs/models/gpt-5.6-luna).

## Does OpenRouter solve this?

Possibly, if its chosen route supplies more aggregate token capacity than the
current direct account. It is not automatically a faster model or a capacity
guarantee. Public endpoint metadata currently lists OpenAI, Azure and Amazon
Bedrock routes for `openai/gpt-5.6-luna`, including an OpenAI fast/priority route.
The returned latency/throughput fields are null and there is no useful per-account
TPM allocation in that response. Preserve the downloaded metadata as a point-in-time
observation, not a promise of availability.
[Public model endpoints](https://openrouter.ai/api/v1/models/openai/gpt-5.6-luna/endpoints).

OpenRouter's paid routes have no platform-level request cap of the free-model
kind, but upstream providers can still return rate-limit/capacity errors. Successful
responses do not expose `X-RateLimit-*` headers, so our direct-OpenAI header-driven
pacer needs an explicit route-aware policy; missing headers do not mean infinite
capacity. [OpenRouter limits](https://openrouter.ai/docs/api/reference/limits).

OpenRouter's throughput sorting concerns endpoint token-generation performance;
that is not the same as this experiment's aggregate input TPM. Keep Luna fixed,
verify Responses API tool/streaming/settings behavior, record actual provider and
service tier, and use the same routing policy across all three arms. Do not silently
fall back to another model. Standard routing excludes flex/priority unless requested;
`:nitro` can admit priority endpoints, while `:floor` can admit slower flex capacity.
Don't use flex to meet an interactive completion deadline.
[Routing](https://openrouter.ai/docs/guides/routing/provider-selection),
[service tiers](https://openrouter.ai/docs/guides/features/service-tiers),
[Responses API](https://openrouter.ai/docs/api/reference/responses/overview).

## Preflight before launching the full matrix

1. Verify the current direct account limit from actual response headers during the
   approved wiring smoke. If it is still 500k TPM, concurrency tuning alone cannot
   meet the proposed deadline. Correct the old limiter behavior regardless of route.
2. If choosing OpenRouter, first run a bounded, paid capacity probe after continuation:
   a fixed representative set of recorded model requests including short first turns,
   medium context and long-context requests. Preserve exact model/settings and exclude
   external tool side effects. Replayed requests test serving capacity, not E2E quality;
   do not substitute those results for agent trajectories.
3. Ramp concurrency 2 → 4 → 8, honoring error/backoff feedback. Measure successful
   input TPM, RPM, full-response latency, cache hit rate and retry/wait time over
   sustained windows. Include enough long requests to avoid a false fast result from
   tiny prompts. Select the route based on demonstrated capacity, with explicit spend
   and duration caps and without changing the 4096-output-token treatment setting.
4. Freeze the selected route/controller in every E2E arm, then run the existing
   eight-question wiring smoke and update ETA from real sessions before launching
   the remainder. A provider switch is a new serving condition relative to historical
   TE3; maintain the earlier rule for a fresh control if claiming contemporaneous
   OAI parity or if model-visible behavior changes.

No provider has been switched and no paid capacity probe has run in this analysis.
The user can choose a slower hour-scale run without new routing work, or a bounded
OpenRouter/direct-capacity test aimed at the 20–30-minute target.

## Reproduction

`data/luna-throughput-review-20260905/` contains the DuckDB SQL, slim per-request
and per-session Parquet files, aggregate CSVs, capacity scenarios, public OpenRouter
metadata and SHA256 inventory of all 392 terminal traces. Original logs were not
modified. Run `duckdb -bail -csv < data/luna-throughput-review-20260905/analyze.sql`.
Include this evidence in the end-of-bakeoff Garage archive; cleanup remains OPEN.
