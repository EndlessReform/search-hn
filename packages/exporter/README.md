# HN Exporter

FastAPI + CLI application to mirror Hacker News data from PostgreSQL into Hugging Face datasets.

## Environment variables

Set these before running:

- `DATABASE_URL`: PostgreSQL connection string for your HN mirror DB (see `infra/`)
- `HF_TOKEN` (or `HUGGINGFACE_TOKEN`): Hugging Face API token with write access

## Development (uv workspace)

From repo root:

```bash
# Sync workspace dependencies
uv sync

# Run API server
uv run exporter serve --reload

# Run one mirror job (no upload)
uv run exporter mirror --dry-run
```

You can also run as a Python module:

```bash
uv run python -m exporter serve --reload
```

## API endpoints

- `POST /jobs` with JSON body `{"dry_run": true|false}`
- `GET /jobs/status`
- `GET /metrics`

## Docker

Build from repository root:

```bash
docker build -f packages/exporter/Dockerfile -t hn-exporter ./packages/exporter
```

Run:

```bash
docker run -p 8000:8000 \
  -e DATABASE_URL=your_db_url \
  -e HF_TOKEN=your_hf_token \
  hn-exporter
```
