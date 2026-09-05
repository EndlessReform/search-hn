# Retrieval ROI: untuned FTS versus dense and hybrid

**Table 1. Matched-question Luna evaluation (196 questions per treatment).**
Values are means per question unless stated otherwise. Higher exposure/citation
is better; lower resource use is better. Parentheses show change versus FTS.

| Treatment | Target in context ↑ | Gain vs FTS | Model turns ↓ | Search lists ↓ | Input tokens ↓ | Output tokens ↓ | Evidence cited ↑ |
|:--|--:|--:|--:|--:|--:|--:|--:|
| Original untuned FTS | 150/196 (76.5%) | — | 4.93 | 7.94 | 51,244 | 474 | 64.3% |
| Dense | **182/196 (92.9%)** | **+16.3 pp** | 3.95 (−19.9%) | **4.60 (−42.1%)** | 49,894 (−2.6%) | 367 (−22.6%) | **87.8%** |
| Hybrid | **182/196 (92.9%)** | **+16.3 pp** | **3.90 (−20.9%)** | 4.69 (−41.0%) | **47,401 (−7.5%)** | **354 (−25.4%)** | 86.7% |

**Topline:** both new treatments recover 32 additional questions while using
about 20% fewer model turns and 41–42% fewer search lists. Input-token savings
are smaller: 2.6% dense, 7.5% hybrid. These are observed research results, not
statistical significance claims or a full production cost calculation.

**Table 2. Question-style breakdown (98 questions per cell).**

| Question style | Treatment | Target in context ↑ | Gain vs FTS | Model turns ↓ | Search lists ↓ | Input tokens ↓ | Output tokens ↓ |
|:--|:--|--:|--:|--:|--:|--:|--:|
| Entity | Original untuned FTS | 79/98 (80.6%) | — | 4.40 | 6.55 | 42,356 | 412 |
| Entity | Dense | **94/98 (95.9%)** | **+15.3 pp** | 3.89 | 4.29 | 47,669 | 339 |
| Entity | Hybrid | **94/98 (95.9%)** | **+15.3 pp** | **3.68** | **4.07** | **39,944** | **304** |
| Paraphrase | Original untuned FTS | 71/98 (72.4%) | — | 5.46 | 9.34 | 60,132 | 537 |
| Paraphrase | Dense | **88/98 (89.8%)** | **+17.3 pp** | **4.01** | **4.91** | **52,120** | **395** |
| Paraphrase | Hybrid | **88/98 (89.8%)** | **+17.3 pp** | 4.11 | 5.31 | 54,858 | 403 |

### Interpretation and accounting

1. Same 196 frozen questions and unchanged system prompt. Original FTS is the
   archived, untuned run—not reconstructed with today's defaults. Dense/hybrid
   also change corpus scope, default result count and pagination versus FTS.
   This is an end-to-end package comparison, not a retrieval-only ablation.
2. All 196 cases stay in each denominator. FTS retains 13 API failures and 26
   turn-limit failures; dense/hybrid each retain three turn-limit failures after
   their 44 rate-limited attempts were archived and rerun. This asymmetric
   infrastructure recovery biases a causal comparison; it also makes FTS's
   recorded resource use artificially low on prematurely aborted cases.
3. A model turn is a successful model response. Search lists include alternatives
   within a batched call and later pages; they are not independent agent turns.
   Tokens sum recorded successful responses, including repeated/cached input.
   Archived retries and unknown failed-request charges are not included here.
4. Exposure means the target entered a successful model context, not that the
   final answer was correct. Citation is separate. One anchor is labeled per
   question; two variants per story are correlated. No confidence intervals here.
5. Token savings are not equivalent to dollar savings: cache mix, embeddings,
   index build/backfill, serving hardware and retrieval CPU are outside this table.
   Wall time is omitted because rate-limit pacing changed during the experiment.
   See [paired efficiency](07-semantic-luna.md#efficiency-not-just-final-recall)
   for matched-success first-exposure and stopping analysis.

### Reproduction

Restore Garage release `research-20260904-v2` using [the artifact instructions](01-artifacts.md).
The tables summarize its original and fresh `metrics.parquet` files. No new model
calls or DB queries are required. Run from the repository root:

```sh
duckdb -markdown <<'SQL'
WITH runs AS (
  SELECT 'Original FTS' AS treatment, *
  FROM read_parquet('data/fts-baseline-20260904/plain-results/metrics.parquet')
  WHERE model = 'gpt-5.6-luna'
  UNION ALL BY NAME
  SELECT 'Dense' AS treatment, *
  FROM read_parquet('data/luna-semantic-20260904/dense/metrics.parquet')
  UNION ALL BY NAME
  SELECT 'Hybrid' AS treatment, *
  FROM read_parquet('data/luna-semantic-20260904/hybrid/metrics.parquet')
)
SELECT treatment, coalesce(style, 'All') AS split, count(*) AS n,
       sum(exposed::INT) AS hits, 100 * avg(exposed::INT) AS exposure_pct,
       avg(model_requests) AS turns, avg(query_count) AS search_lists,
       avg(input_tokens) AS input_tokens, avg(output_tokens) AS output_tokens,
       100 * avg(cited_evidence::INT) AS cited_pct
FROM runs
GROUP BY GROUPING SETS ((treatment), (treatment, style))
ORDER BY split, treatment;
SQL
```
