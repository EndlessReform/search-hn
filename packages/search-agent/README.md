# Search Agent

Agent for searching and querying Hacker News data from PostgreSQL.

## Environment variables

Set the database connection before running:

- `DATABASE_URL`: PostgreSQL connection string for your HN mirror DB (see `infra/`)

The TUI defaults to the project's local model server. These optional variables
make the model provider portable without requiring command-line flags:

- `OPENAI_BASE_URL`: OpenAI-compatible API endpoint
- `OPENAI_MODEL`: model name served by that endpoint
- `OPENAI_API_KEY`: required for first-party OpenAI; optional for local servers
- `SEARCH_AGENT_WEB_CALL_LIMIT`: consecutive webpage-tool budget, from 3 to 5
  (default 4; the final allowed response warns the model to move to comments)

## Model providers and presets

The TUI reads model configuration from
`~/.config/search-agent/config.toml`; pass `--config PATH` to use another file.
See [`config.example.toml`](config.example.toml) for a complete local/OpenAI
example. Provider URLs and initial model names belong in TOML, while API key
values remain in the environment or `.env`.

Type `/model` or `/m` without arguments to open the provider/model picker. The
model dropdown is focused first; Tab moves between it, the provider dropdown,
and a free-form model ID field. For non-OpenAI providers the picker requests
the standard `GET /models` endpoint as a lightweight health check and augments
the dropdown with returned IDs. A timeout or invalid response produces a
warning but does not disable configured choices or free-form entry.

Preset names provide a quick path and may change both provider and model. With
the example config, `/model gemma` selects the local Gemma model and `/model
luna` selects OpenAI's `gpt-5.6-luna`. OpenAI is built in at
`https://api.openai.com/v1`, uses only `OPENAI_API_KEY`, and cannot be
redirected by TOML.

## Development (uv workspace)

From repo root:

```bash
# Sync workspace dependencies
uv sync

# Run the interactive TUI
uv run search-agent
```

Press `Ctrl+B` to toggle keyboard focus between the transcript and prompt bar.
While the prompt bar is focused, Up and Down recall user messages submitted
during the current application run. This history survives `/new` conversation
resets, and returning past the newest entry restores the unfinished draft.
When the agent proposes opening a URL found only inside an HN comment, the TUI
pauses first: `A` approves that exact call, `R` rejects it, and any other text
rejects it while passing the text back as corrective guidance. Submission URLs
do not require this checkpoint.

At startup, webpage extraction prefers working `node` and `npx` executables on
`PATH`, then tries the already-installed default Node version managed by `fnm`.
The pinned Defuddle warm-up runs once per process, and the TUI reports the
selected local runtime or a clear unavailable warning. There is currently no
hosted fallback.

You can also run as a Python module:

```bash
uv run python -m search_agent
```

Use explicit provider settings when the local defaults are not available:

```bash
uv run search-agent \
  --base-url https://api.openai.com/v1 \
  --model gpt-5-mini
```

`OPENAI_API_KEY` must be set for that first-party OpenAI example.

## Headless operation

The TUI and headless CLI share `SearchRuntime`: agent instructions/tools, isolated
provider clients, DB lifecycle, per-turn state and tool-failure policy. Textual
is imported only for interactive runs. Presentation observes SDK events; it does
not own provider configuration or execution rules.

Provider selection is scoped to each runtime, including approval resumes and
budget-rejection summaries. Switching `/model` changes the active HTTP client as
well as the model name. The headless runner resolves the same TOML presets and
provider overrides, but keeps webpage tools disabled; the prompt only describes
webpage tools when that capability is enabled.

```bash
uv run search-agent --headless --model qwen-3.6-27b \
  --prompt "Find HN discussions about database index design" \
  --output /persistent/path/trajectory.jsonl
```

Use `--max-turns`, `--max-tokens`, `--timeout`, and `--api responses|chat` to bound
or configure a run. JSONL captures complete model inputs/outputs and tool results,
flushing and fsyncing every event. The final answer is also printed as JSON.
For dataset generation, rollouts, metrics and an offline explorer, see
[`search-research`](../search-research/README.md).

### Diagnose webpage extraction without a model

The webpage tool's production service has a thin standalone entrypoint. It
authorizes only the URL supplied on the command line, then applies the same
publisher policy, network safety checks, bounded fetch, Defuddle extraction,
cache, and preview contract used by the TUI:

```bash
uv run search-agent-web https://example.com/article --story-id 12345
```

To exercise cached inspection without a model, request one subsequent chunk
and/or a literal search in the same diagnostic process:

```bash
uv run search-agent-web https://example.com/article \
  --read-next \
  --find "release date"
```

The optional story ID is included in structured failure guidance. This command
is a diagnostic escape hatch, not a general authorization mechanism available
to the model.

## HTTP API

Run the FastAPI wrapper separately from the TUI:

```bash
uv run fastapi dev packages/search-agent/src/search_agent/app.py
```

## API endpoints

- `GET /search` - Search stories
- `GET /healthz` - Health check endpoint

## Tests

```bash
uv run pytest packages/search-agent/tests
```

## Production hybrid search (Textual default)

The existing Textual agent now defaults to `--retrieval production`, querying the
live `public.story_search` index directly. It needs the embedding proxy as well as
PostgreSQL. Production search covers eligible **25+ point stories**, including
filter-only/date browsing; lowering `min_score` cannot recover stories outside that
corpus. Comments continue to use the existing mirror queries.

Set these in your shell or `.env` (pgpass can supply the database password):

```bash
export DATABASE_URL='postgresql://readonly_hn_agent@searchhn-pg:5432/searchhn_test'
export EMBEDDING_BASE_URL='https://magi06-inference.tail7a3eb.ts.net/embeddings/v1'
uv run search-agent
```

`--embedding-base-url` overrides the embedding endpoint. This URL is independent of
`--base-url`, which selects the conversational model server. The runtime contains
no deployment hostname default. The reader needs SELECT on `story_search`, `items`
and the existing comment tables, plus execution of `story_search_eligible`; the
current production reader was verified, but fresh-role provisioning is still
separate work.

The shared Python backend also serves the headless CLI and FastAPI wrapper. Axum
integration is deferred for this slice. `SEARCH_RETRIEVAL=fts` or `--retrieval fts`
selects the old FTS backend; the historical `dense` / `hybrid` options still refer
to frozen research tables and retain their original recipe.

Retrieval uses `pplx-embed-v1-0.6b`, 1024 final signed integer coordinates stored
losslessly in `halfvec`, and cosine distance. Queries go to the proxy as interactive
work with no query prefix or client-side transformation. The table's recipe comment
must match both the response body and header. Unfiltered vector retrieval uses
HNSW with `ef_search=1000`. Each branch contributes up to 100 stories; title BM25
uses the existing `story_search_title_bm25` index.

The fusion score is `1/(60 + dense_rank) + 0.125/(60 + bm25_rank)`, with zero for
an absent branch. Ranks start at one. For example, the compiler query below returned
story **2661452** at rank 1 in both branches: `1/61 + 0.125/61 = 0.018442623`.
This RRF value orders results; it is not a probability or the story's HN vote score.
Tool results expose `dense_rank`, `bm25_rank`, `rrf`, and `retrieval_mode` for inspection.

Try these in the TUI:

- “Find discussions about how to build a programming language compiler.”
- “Find stories with at least 500 points about solar panels and home battery storage.”
- “Find GitHub database projects, then show the next page.”
- “Find indoor hydroponics discussions from 2024 onward.”

The agent starts with one sentence-like topic query, relevance sorting and no
filters unless the request requires them. It no longer adds a score threshold
for evergreen topics or a mandatory keyword/anchor query. Date, domain and score
constraints remain available when needed; alternate phrasings follow inspection
of the first results. Tool descriptions and empty-result guidance follow the
same policy.

Date/domain/score filters apply **before** both branch limits. Filtered dense queries
use exact cosine over the filtered population to avoid ANN starvation; broad
filters can take seconds. Score/date sorts reorder the candidate union. Filter-only
browsing ranks at most 200 eligible stories without an embedding request.

Rankings are cached for five minutes, at most 128 query/filter/sort combinations per
repository. Pages reuse IDs, while current source fields and eligibility are read
again. Deleted or demoted stories leave empty positions rather than shifting later
pages. An expired page asks the agent to restart at page 1. Conversation reset
clears the cache. Inference/contract failure returns a cached `keyword-only` BM25
list, explicitly labelled in tool output (and `X-Search-Retrieval` on the FastAPI
response); database errors remain errors.

Read-only validation with sample queries, filters, disjoint pages, simulated
inference outage, and an exact comparison to the approved RRF SQL:

```bash
uv run --package search-agent python tools/search/validate-agent.py \
  --output docs/search-validation/2026-09-07/agent-hybrid-final.json
```

See [dated evidence](../../docs/search-validation/2026-09-07/README.md) for measured
latencies and limits. This does not deploy or restart any service.
