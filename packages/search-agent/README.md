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

## Headless operation

The TUI and headless CLI share `SearchRuntime`: agent instructions/tools, isolated
provider clients, DB lifecycle, per-turn state and tool-failure policy. Textual
is imported only for interactive runs. Presentation observes SDK events; it does
not own provider configuration or execution rules.

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
