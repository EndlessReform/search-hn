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

## Development (uv workspace)

From repo root:

```bash
# Sync workspace dependencies
uv sync

# Run the interactive TUI
uv run search-agent
```

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
