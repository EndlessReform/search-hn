# Sovereign finalists: Luna end-to-end comparison

Run root: `data/sovereign-e2e-20260905/`.
Protocol and reproduction command: [final manifest](16-final-bakeoff-manifest.md).

Three dense arms, native dimensions: Pplx 1024 FP32/native int8, Qwen 1024 FP16,
Nemotron 2048 BF16. Luna is routed through OpenRouter exclusively to OpenAI's
standard tier; sampled billing receipts resolve `openai/gpt-5.6-luna-20260709`.
The system prompt, tool interface, exact retrieval, filters, vote floor, frozen
64,638-story corpus, 196 synthetic questions and ten-turn ceiling are unchanged.
Archived TE3 is historical context, not a fourth contemporaneous treatment.

All 588 cached first-query top-20 rankings matched exactly through the new query
repository. The fixed 24-session smoke passed and counts toward the 588 sessions.
The main run shares 16 conversation slots across arms. Three embedding servers
share the 3060, so these latencies do not replace isolated serving benchmarks.

## Results

**All 588 sessions reached a scientific terminal state: 564 completed answers and
24 ten-turn exhaustions. Zero infrastructure failures.** Smoke execution took
18.54 seconds and the remaining full invocation 324.81 seconds: **5m 43s combined**,
excluding setup, validation and the gap between invocations. The 2,399 streamed
model requests cost **$3.34013118**, entirely reconciled to reported OpenRouter
billing; no unknown reservations remain. This leaves approximately $6.66 of the
user's stated $10 starting credit, assuming no other account spending.

| Dense provider | Target exposed /196 | Target evidence cited /196 | Turn exhaustions | Mean model turns | Driver cost |
|---|---:|---:|---:|---:|---:|
| Nemotron 2048 | **184 (93.9%)** | 168 (85.7%) | 7 | 4.03 | $1.1247 |
| Perplexity 1024 | 181 (92.3%) | 168 (85.7%) | 6 | 4.08 | $1.1130 |
| Qwen 1024 | 178 (90.8%) | 167 (85.2%) | 11 | 4.13 | $1.1024 |
| TE3-large 1536 (historical baseline) | 182 (92.9%) | **172 (87.8%)** | 3 | 3.95 | Not directly comparable¹ |

¹ The [archived TE3 dense run](07-semantic-luna.md) used the same frozen corpus,
196 questions, Luna driver family and search protocol, but ran earlier through
the direct OpenAI API with different concurrency/rate-limit recovery. Its ledger
combines dense and hybrid costs plus unresolved failed-attempt reservations, so
it does not provide a comparable per-arm billed cost. No new baseline calls were
made. Relative to that historical run, target exposure changes by **+2 cases for
Nemotron, −1 for Perplexity and −4 for Qwen**; evidence citations change by
**−4, −4 and −5**, respectively. The local finalists are close to TE3 on these
measures; this historical comparison does not isolate an embedding-only effect.

### Routing clarification after baseline review

There is **no observed Azure routing in this experiment**. The frozen request
settings restrict providers to `only: ["openai"]`, disable fallback, and require
the default service tier. Saved E2E receipts (one per local arm) all report
`provider_name: OpenAI`, `service_tier: default`, and the OpenRouter model label
`openai/gpt-5.6-luna-20260709`. OpenRouter documents OpenAI and Azure as separate
[provider routes](https://openrouter.ai/docs/guides/routing/provider-selection).
These are sampled receipt checks, not an independent audit of every upstream call.

A read-only retrieval of an original TE3-run response
(`resp_059102a44eefcb53006a9b5e812c3887d18f279a73b6409070`) returned model
`gpt-5.6-luna`, default tier, medium/standard reasoning, temperature 1 and top_p
0.98. The direct endpoint exposes an alias, so a differing checkpoint is unproven;
the same underlying model is a reasonable working assumption. Provider routing
should not be presented as evidence of a quality defect. There is no Azure arm
or provider variation here with which to estimate an Azure/quality correlation.

One concrete metadata discrepancy merits checking before a tighter comparison:
the two saved OpenRouter capacity-smoke responses report temperature 1 and medium
reasoning too, but top_p 1 and reasoning summary `detailed`, versus top_p 0.98 and
summary null in the retrieved direct response. Both callers left these settings
unspecified. The gateway may normalize response metadata; this does not establish
different actual upstream sampling, nor explain the observed outcome differences.
The E2E journals did not retain these raw response-level fields. No new inference
was performed for this review.

Mean conversation latency was 8.92–8.94 seconds for every arm, median 6.42–6.49
seconds. Model-call median was 1.74–1.76 seconds and p95 3.35–3.64 seconds.
Total input/output usage was 31,035,031 / 221,599 tokens. GPU electricity is not
included in these API costs. Repository timing captures database retrieval, not
separately instrumented query-embedding service time; use the earlier isolated
serving measurements for embedding throughput/VRAM comparisons.

Nemotron leads exposure, but its advantage over Perplexity is only three cases:
five Nemotron-only wins versus two Perplexity-only wins. The paired 95% cluster
bootstrap interval for Nemotron minus Perplexity is **−1.02 to +4.08 percentage
points**. Against Qwen it wins seven versus one, a six-case lead; that interval is
**+0.51 to +6.12 points**. These are exploratory, unadjusted intervals from one
stochastic agent run on a repeatedly inspected development set. Citation differences
are tiny and all their paired intervals cross zero. This does not establish a broad
or universal model ranking.

Examples retained in the trace explorers: Nemotron found the anchor for the retro
raycasting entity question (`48459294-entity`) when both others missed; Perplexity
found the rlama paraphrase anchor (`43296918-paraphrase`) when both others missed;
Qwen found the forward-generation contradiction anchor (`48389360-paraphrase`)
when both others missed. These are exposure observations, not manual correctness
judgments of the final answers.

For regrouping: retain **Nemotron and Perplexity** as the quality finalists. Qwen's
substantially faster prior backfill and lower VRAM remain a real operational benefit.
The current run is sufficient to regroup before deciding whether a selected-provider
hybrid follow-up is useful.

## Budget and failure handling

The user reported $10 remaining credit. The new cumulative run guard is $8,
including smoke and any failed attempts. Every model request reserves a conservative
upper cost first. Streamed OpenRouter billing replaces the reservation on completion;
unknown failed charges stay reserved. No SDK retries hide extra attempts. Credit or
budget exhaustion stops dispatch; each conversation streams to an independent,
fsynced journal. Infrastructure failures are resumable and stay outside scientific
quality denominators; genuine ten-turn exhaustions remain included.

A process lock prevents competing drivers. Resume retains completed cases and all
prior spending; raising the budget after recharge means raising the *cumulative*
allowance, not resetting the ledger. A synthetic test verified that one failed
conversation does not cancel its sibling.

The completed-run resume check skipped all 588 terminal cases, dispatched zero
new model calls and retained the exact $3.34013118 ledger total. The final protocol
audit verified all frozen prompts/source identities and all 2,399 streamed billing
records. Twenty-two focused tests passed. No recharge was required.

## Artifacts and interpretation

`manifest.json`, `source.tar.gz`, `cached-parity.json`, per-arm server metadata,
`provider-receipts.json`, `budget.jsonl`, `outcomes.jsonl`, and per-session traces
preserve provenance. The offline reports add per-case metrics, paired comparisons,
first-exposure efficiency, model-call timing/cost, and an HTML trace explorer.

Exposure requires the target in a tool result consumed by a later model request;
a returned-but-unconsumed result does not count. Evidence citation means citing
the anchor story or one of its observed comments. This measures anchor retrieval
and evidence use, not independently judged answer quality. Other relevant stories
remain unjudged. Two question styles from each story stay together in the paired
bootstrap: 98 clusters, 10,000 resamples, seed 20260905. This is an extensively
inspected development set, not a held-out acceptance set.

**Cleanup remains OPEN.** Keep artifacts and serving recipes for the decision and
any selected-provider hybrid follow-up. Before closing the whole bakeoff, publish
a verified Garage release and reap scoped laptop caches, these three vector tables,
the two E2E TEI containers, model weights and the Nemotron UV environment. Follow
[the mandatory checklist](00-design.md#required-before-closing-the-bakeoff-archive-and-reap-local-artifacts).
