# OpenRouter Luna capacity probe — 2026-09-05

**The measured OpenRouter route comfortably exceeds the old direct-account
500k TPM limit. No hard rate-limit knee was reached within this bounded probe.**
Recommend concurrency **8** for the E2E batch, based on a separate confirmation
pass at 2.78M input TPM, 231 RPM and 3.40s p95 full-response latency.

The user explicitly authorized this paid capacity experiment and confirmed the
synthetic questions and public HN posts/URLs were fair game. No actual agent loops
or tool side effects were executed. This measures model serving with realistic
saved contexts, not new-agent success or complete E2E runtime.

## Recipe and bounds

- `openai/gpt-5.6-luna`, OpenRouter streaming Responses API, `store:false`, complete
  saved model-input history and system instructions, current matching tool schemas,
  4096 max output tokens. Preserve unspecified generation settings as unspecified.
- Restrict to OpenAI, disable provider fallback, standard/default service tier;
  no flex or paid priority tier. Generation receipts sampled at concurrency
  1/8/16/24/32 confirm **OpenAI**, resolved model `gpt-5.6-luna-20260709`, and no BYOK.
- Use all **1,538 successful request inputs** from the earlier 392 terminal traces.
  Balance short/long contexts in shuffled 32-request blocks. Each original request
  is replayed once across the ramp and confirmation, plus two repeated smoke calls.
  The unique replay's **19,069,849 input tokens exactly match the archived total**.
- Ramp 2 → 4 → 8 → 16 → 24 → 32 with 35-second dispatch windows plus drain;
  1,200-request ramp cap. Stop new dispatch on the first error, leave in-flight
  work to drain, and never retry automatically. Stop the ramp on <10% throughput
  improvement together with >50% p95 growth.
- Overall $10 maximum; reserve unknown failed-request costs conservatively.
  After the $0.0048 smoke, the ramp has a $9.75 ledger ceiling. Its actual charge
  is $2.4049, after which confirmation is separately capped at $2, keeping total
  possible spend below $10. All requests completed successfully.

## Results

All rates below use complete stage elapsed time, including draining final requests.
“Input TPM” is successful input-token work divided by elapsed time; it is not a
provider quota header or a contractual sustained capacity allocation.

| Concurrency | Requests | Elapsed | Input TPM | RPM | Median response | p95 response | Errors |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 39 | 36.46s | 0.685M | 64 | 1.64s | 3.00s | 0 |
| 4 | 78 | 36.81s | 1.545M | 127 | 1.69s | 3.04s | 0 |
| 8 | 141 | 37.27s | 3.093M | 227 | 1.92s | 3.50s | 0 |
| 16 | 289 | 37.66s | 5.798M | 460 | 1.80s | 3.42s | 0 |
| 24 | 447 | 41.25s | 7.978M | 650 | 1.72s | 3.35s | 0 |
| 32 | 206 | 17.58s | 8.859M | 703 | 1.68s | 3.82s | 0 |
| **8, confirmation** | **338** | **87.78s** | **2.778M** | **231** | **1.72s** | **3.40s** | **0** |

Concurrency 32 hit the request cap early; its short window and larger relative
drain overhead make it unsuitable for declaring a saturation knee. Throughput
gains appear smaller at the upper end, but there were no 429s or errors and no
large latency increase. **Concurrency 8 is a chosen operating point, not a measured
hard limit.** Higher capacity remains available within what this probe observed.

The test includes individual prompts up to **125,176 input tokens**. Cache hits
increase across the ramp, from 22% at concurrency 2 to 55% at 32, as related
conversation prefixes become available. This and modest differences in token mix
mean the ramp is not a controlled fixed-cache latency ablation. The selected
concurrency already clears the target with the colder 8-concurrency ramp (28%
cached input); the earlier real trajectories had approximately 65% cached input.

**Total: 1,540 successful requests, zero failures/retries, $2.9064 reported cost.**
The OpenRouter usage costs were summed, and sampled generation receipts confirm
actual upstream provider and costs. No unknown failed-request charges remain.

## Implication for the next E2E run

The proposed three dense arms project 29.34M input tokens and 2,322 model calls.
At the confirmed 8-concurrency rate, input work implies about **10.6 minutes** and
request count about **10.1 minutes** of equivalent replay service. A reasonable
initial E2E planning target is **15–20 minutes**, allowing for tool execution,
dependent turns, scheduling and tails. This is an extrapolation, not a measured
E2E runtime; update it after the wiring smoke and first completed target block.

Use OpenRouter → OpenAI standard/default with a shared concurrency target of eight.
Keep the selected route, model settings and controller identical across embedding
arms. Replace assumptions about direct-OpenAI success headers with explicit route-aware
accounting and backoff. Retain meaningful mid-stream failure handling. No reason
was found to purchase priority capacity or change the driver model for this batch.

The provider adapter and model-visible tool/settings parity still need the agreed
wiring smoke. Perplexity BF16 preflight is also outstanding. Do not label replayed
model responses as successful new agent sessions or carry them into E2E scores.

## Evidence and reproduction

Probe source: [openrouter_capacity.py](../tools/openrouter_capacity.py). It passed
Ruff and its real streaming/tool-schema smoke; no fake network tests were added.
Artifacts under `data/openrouter-capacity-20260905/`: per-request receipts, complete
model responses, stage summaries, route/generation evidence, exact primary-run
source snapshots, manifests and final audit. Original traces were not modified.

```sh
uv run --locked --package search-research python packages/search-research/tools/openrouter_capacity.py --root data/openrouter-capacity-NEW/ramp --dollars 9.75
uv run --locked --package search-research python packages/search-research/tools/openrouter_capacity.py --root data/openrouter-capacity-NEW/confirm-8 --concurrency 8 --skip 1200 --max-requests 338 --seconds 90 --dollars 2
```

These reproduction commands are paid requests; do not rerun them automatically.
The second command assumes a first pass that consumed exactly 1,200 source requests.
Archive this evidence with the bakeoff in Garage; the existing scoped cleanup
obligation remains OPEN. No long-lived load generator remains running.
