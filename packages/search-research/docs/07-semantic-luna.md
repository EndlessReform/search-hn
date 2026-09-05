# Fresh Luna trajectories: dense vs hybrid, September 4, 2026

## Fixed experiment

The same 98 stories / 196 original plain questions are reused; no paraphrases are
regenerated. Luna is `gpt-5.6-luna`, Responses API, max 10 turns and 4096 output
tokens per request, with the original system instructions and model settings.
The pilot's recorded system prompt matches the archived baseline byte-for-byte.
Tool schema changes are limited to the requested pagination/sort controls and
20-result default. The original keyword-oriented descriptions and empty-result
guidance are deliberately retained for this comparison.

Treatments:

- **Original FTS:** archived initial Luna trajectories, simple conjunctive PG
  search, score-first ordering, eight-result default. No retrospective retuning.
- **Dense:** exact cosine in scratch pgvector, TE3-large at 1536 dimensions.
- **Hybrid:** the same dense list plus title-only English pg_textsearch BM25,
  top 100 candidates per branch, weighted RRF with k=60 and lexical weight .5.
  No ANN, reranker, or new weight sweep.

The live mirror was exported read-only for 2024-09-04 through 2026-09-04 inclusive,
nondead/nondeleted stories with score >=25. It contains **64,638 stories**, including
all 98 targets. The snapshot stores full story-result metadata in Parquet. Only
13 title+URL strings differed from the earlier embedding corpus; those were
embedded for ~$0.000053. Identical text reuses original TE3-large vectors, shortened
and normalized to 1536 dimensions. The snapshot's SHA256 and SQL are recorded in
`snapshot.json`. Comments still come from the live mirror, as in the original run.

This is the requested end-to-end package comparison, not an isolated algorithm
ablation: corpus scope, floor, default result count, pagination and retrieval all
differ from the archived original. Dense vs hybrid holds these changes constant.

## Tool/session behavior

- `fetch_stories(query, limit=20, page=1, sort="relevance", ...)` accepts 1–20
  results per query and pages 1–3. Each query in a batch has its own `next_page`.
  Repeat the query, filters, sort and limit when following that page number.
- Semantic retrieval has an immutable 25-vote corpus floor. A larger `min_score`
  is optional; lower values cannot restore documents outside this snapshot.
  Date/domain filters remain optional and are applied before candidate selection.
- Score/date sorting is opt-in. For textual semantic search it reorders the
  retrieved candidate union; filter-only search sorts all matching snapshot rows.
- One semantic repository belongs to one conversation. It caches query embeddings
  across pages and filter changes, and complete ordered candidate lists across
  identical requests. A lock coalesces concurrent requests. New TUI conversations,
  new headless conversations and runtime disposal clear these caches.
- The old system prompt still suggests min_score 50+ for evergreen topics. We
  honor Luna's explicit choices rather than secretly ignoring them.

## Run / report

The driver and all artifacts live on the laptop. Local inference models remain
parked; Luna and query embeddings use OpenAI. Scratch PG is the existing disposable
container, with new `semantic_stories` and `semantic_vectors` tables and
`semantic_title_bm25`; older bakeoff tables/results are preserved.

```sh
docker start searchhn-pg-bakeoff-20260904
uv run --project packages/search-research python -m search_research.semantic_snapshot
uv run --project packages/search-research python -m search_research.semantic_report --static
uv run --project packages/search-research python -m search_research.semantic_rollouts --concurrency 4
uv run --project packages/search-research python -m search_research.semantic_report
```

Run from the repository root, with `OPENAI_API_KEY` available and pgpass configured.
The driver reuses terminal outcomes on resume. Infrastructure-aborted attempts
(the corrected byte-based context guard, manual interruption) are preserved under
`infrastructure-attempts/` and rerun; real model/tool-budget errors remain outcomes.
Do not run two drivers concurrently against this output directory.

Four trajectories share a 240-RPM limiter. Token exhaustion is handled using actual
API rate-limit response headers, with shared pauses and SDK Retry-After/backoff;
the account headers confirm 500 RPM / 500k TPM. No context truncation is added.
Every request reserves a conservative upper input-byte cost plus max output;
completed usage replaces that reservation using cached/uncached input prices.
Older completed budget entries still use conservative uncached pricing. Unknown
interrupted charges retain their reservations.
The model spend ceiling is $4.80, leaving $0.20 for embeddings. Budget and trajectory
journals are fsynced after each event. The initial driver incorrectly over-throttled
estimated token counts, adding most of the elapsed time despite ~104ms searches
and fast API responses. It was replaced with header-driven pacing. Valid completed
cases were preserved; interrupted cases were archived and rerun. Raw end-to-end
latencies mix pacing versions and must not be interpreted as engine latency.

Artifacts: `data/luna-semantic-20260904/`, with `dense/` and `hybrid/` trajectory
directories, per-session retrieval/cache statistics and the standard explorers.
`comparison-all.csv` includes all terminal cases; `comparison-paired.csv` compares
only cases terminal in all three treatments while a run is incomplete.
`query_pass@K` means target appears in one of the first K **consumed search lists**,
including pages and batch alternatives. `exposed` also includes fetched target
comments. Citations are measured separately. Unconsumed final tool output is not a
success. There is one labeled anchor per question, not comprehensive relevance
judgment of every returned story.

## Refreshed static baseline (196 original questions, no trajectories)

| Backend | Recall@8 | Recall@20 | NDCG@8 |
|---|---:|---:|---:|
| Dense | 68.4% | 78.6% | .560 |
| Hybrid | 72.4% | 81.6% | .592 |

Question embeddings were reused; this static check made no new API requests.
The fresh trajectory comparisons are produced separately by `semantic_report`.

## Use the same implementation interactively or headlessly

Both entry points accept `--retrieval dense|hybrid` (default remains `fts`):

```sh
uv run --project packages/search-agent search-agent \
  --model gpt-5.6-luna --base-url https://api.openai.com/v1 \
  --retrieval hybrid \
  --database-url postgresql://postgres@127.0.0.1:55432/search_bakeoff \
  --comments-database-url postgresql://readonly_hn_agent@searchhn-pg:5432/searchhn_test
```

Add `--headless --prompt '...' --output path/to/trace.jsonl` for a single headless
conversation. The experimental driver provides the shared budget/rate limiter;
the general interactive CLI does not inherit that experiment-wide $5 cap.

Official prices/limits: [Luna](https://developers.openai.com/api/docs/models/gpt-5.6-luna),
[TE3-large](https://developers.openai.com/api/docs/models/text-embedding-3-large).
