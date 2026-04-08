# Search Agent

Agent for searching and querying Hacker News data from PostgreSQL.

## Environment variables

Set these before running:

- `DATABASE_URL`: PostgreSQL connection string for your HN mirror DB (see `infra/`)

## Development (uv workspace)

From repo root:

```bash
# Sync workspace dependencies
uv sync

# Run API server
uv run search-agent serve --reload
```

You can also run as a Python module:

```bash
uv run python -m search-agent serve --reload
```

## API endpoints

- `GET /search` - Search stories and comments
- `GET /healthz` - Health check endpoint

