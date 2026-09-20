# Managed book extraction bakeoff

DeepSeek V4.1 Flash low gives the stronger book gate on the reused audit; Luna
medium is faster. Both sustained concurrency 64 on the same 4,096 fresh
comments with zero errors or 429s. All outputs validated against the extraction
schema. These are annotation proposals, not automatically verified labels.

## Throughput and cost

Measured 2026-09-20 using whole comments, the unchanged Gemma P2 prompt, and
`{has_any_book, books: [{title, author}]}`. DeepSeek uses native JSON mode plus
Pydantic validation. Luna uses strict JSON schema via OpenRouter pinned to
OpenAI, with no fallbacks. One label here means one comment-level record,
including negatives, not one book or one human-approved annotation.

| Configuration | Comments/s | USD / 1k records | USD / 108,194 passes | Minutes / all passes |
|---|---:|---:|---:|---:|
| DeepSeek low, off-peak | 37.03 | 0.1764 | 19.09 | 48.7 |
| DeepSeek low, peak price scenario | 37.03 | 0.3529 | 38.18 | 48.7 |
| Luna medium | 42.87 | 0.2013 | 21.78 | 42.1 |

Times extrapolate sampled wall time. Peak-hour speed was not measured.
Prior local Gemma 26B direct output reached 54.6/s and GLiNER 543.5/s, with
weaker gate precision. No full-filter extraction job was launched.

Luna's 512-comment concurrency sweep measured 9.78/s at 16, 23.29/s at 32,
and 25.30/s at 64. Longer confirmations measured 18.38/s at 32 on 2,048 fresh
comments and 42.87/s at 64 on 4,096. Short runs are sensitive to slow-request
drain; these packets differ. **64 is the highest tested sustainable concurrency,
not a demonstrated saturation knee.** Neither provider was tested above 64.

Luna at 64 consumed 1.84M input tokens/min and generated 122,899 output
tokens/min. DeepSeek consumed 1.50M input tokens/min and generated 521,201 output
tokens/min. Reasoning was 52.8% and 93.4% of their respective generated tokens.
Tokens are model-specific units. Median / p95 request latency was 1.07 / 2.90
seconds for Luna and 1.18 / 3.50 seconds for DeepSeek.

## Reasoning effort and quality

DeepSeek explicitly uses `reasoning_effort: low` with thinking enabled; Luna's
OpenRouter request uses `reasoning: {effort: medium}`. Both low and default
effort were tested. DeepSeek high used a 32,768-token ceiling after one smoke
response exhausted 8,192 tokens. Other main runs used an 8,192-token ceiling.

All four configurations ran the same 320 audit comments; one previously
excluded ambiguous item leaves 319 scored. Gold labels and aliases are unchanged.
The random stratum has 192 comments, including 17 positives. The other 127
deliberately book-heavy comments contain 163 title references.

| Configuration | Random TP / FP / FN | Gate precision / recall | Book-heavy title references found | Valid emitted titles |
|---|---:|---:|---:|---:|
| DeepSeek low | 16 / 1 / 1 | 94.1% / 94.1% | 158/163 | 157/158 |
| DeepSeek high | 16 / 0 / 1 | 100% / 94.1% | 158/163 | 157/159 |
| Luna low | 14 / 5 / 3 | 73.7% / 82.4% | 157/163 | 156/163 |
| Luna medium | 15 / 3 / 2 | 83.3% / 88.2% | 157/163 | 156/160 |

DeepSeek low preserves measured recall and title recovery with one extra random
false positive. Luna low worsens gate precision and recall, so medium is retained.
DeepSeek high's zero random false positives comes from only 175 negatives; it
also adds a title error in the book-heavy stratum and does not dominate overall.
This is a reused development audit, not a fresh held-out benchmark.

Title metrics do not score authors. Manual checks still find missing contextual
author links: Sebald for The Rings of Saturn (43617144), and Doctorow for
Walkaway (43838776), in both selected configurations. DeepSeek puts “by David
L. Parnas” inside Software Fundamentals and leaves author null (43282576);
Luna medium separates them correctly. DeepSeek low recovers all 16 titles in
the screenshot reading list (46392391) and all 19 in the longer science-fiction
list (46396803).

## Caching and accounting

DeepSeek's matched run used 2,772,600 input tokens, including 1,834,752 cache
hits (66.2%), and 960,896 output tokens. Calculated off-peak charge: $0.722719.
With zero cache reads, the same tokens cost $0.2423 per 1k comments or $26.21
for all passes. Fresh comments can share a cached prompt prefix.

Luna used 2,928,735 input tokens, zero cache reads, 76,279 cache writes, and
195,721 output tokens. Its API-reported $0.824426 charge matches reconstruction,
including cache-write charges. GPT-5.6 caching requires 1,024 visible prefix
tokens; the shared prompt alone is shorter. A separate 512-comment replay
produced 15,181 cache-read tokens and cost $0.096879. Replays are excluded from
the fresh-comment throughput projections.

DeepSeek's separate 512-comment replay hit cache on 242,343/344,422 input
tokens (70.4%) and cost $0.088912. One response used its entire 8,192-token
allowance on reasoning and returned no extraction; 511 validated. Low effort
does not impose a hard thinking-token cap. No automatic retry was made.

Prices per million tokens: DeepSeek off-peak input miss / hit / output is
$0.15 / $0.003 / $0.60, doubled Monday–Friday 01:00–04:00 and 06:00–10:00 UTC.
Luna input / cache read / cache write / output is $0.20 / $0.02 / $0.25 / $1.20.
DeepSeek costs are calculated from usage; Luna costs are API-reported. Funding
fees and taxes are outside these estimates.

A supplementary DeepSeek high fresh-512 run had 511 valid responses and one
transport ReadError, with no automatic retry. Its known charge was $0.121767;
the failed request's charge is unknown. Its 12.57 valid comments/s is not used
as a clean high-effort production forecast. Main matched runs had zero errors.

Sources: [DeepSeek pricing](https://api-docs.deepseek.com/quick_start/pricing/),
[thinking controls](https://api-docs.deepseek.com/guides/thinking_mode/),
[concurrency limits](https://api-docs.deepseek.com/quick_start/rate_limit/),
[Luna](https://developers.openai.com/api/docs/models/gpt-5.6-luna),
[prompt caching](https://developers.openai.com/api/docs/guides/prompt-caching),
[OpenRouter prices](https://openrouter.ai/api/v1/models).

## Receipts and runtime

Runner: `packages/search-research/tools/comment_book_api.py`. Artifacts under
`data/probes/books-api-bakeoff-v1/` retain exact requests, input hashes, raw
responses, usage, errors, latency and summaries. Main receipts are
`deepseek-low-sustain-c64/` and `luna-sustain-c64/`; four `*-audit/` directories
retain scored cases. Inputs exclude evaluation IDs and use complete comments
from the 2025 quick-filter pass list.

The Gemma 26B container was stopped and the embedding container restarted on
melchior. GLiNER remains unloaded until requested by the playground.

Across all smoke, quality, throughput and replay runs, 16,145 requests incurred
$3.150294 in known charges, plus the unknown charge for one transport failure.
Embedding health returned successfully after restoration. Accounting and effort
request checks passed locally.
