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
