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

## Development (uv workspace)

From repo root:

```bash
# Sync workspace dependencies
uv sync

# Run the interactive TUI
uv run search-agent
```

Press `Ctrl+B` to toggle keyboard focus between the transcript and prompt bar.
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
