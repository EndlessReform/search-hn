# HN Mirror Project

Fast API application to mirror Hacker News data to Hugging Face datasets.

## Environment Variables

Set these before running:

- `DATABASE_URL`: PostgreSQL connection string for your HN mirror DB (see `/infra` for details)
- `HF_TOKEN`: Hugging Face API token with write access
  - Get it from: https://huggingface.co/settings/tokens

## Build and Run

```bash
# Build Docker image
docker build -t hn-mirror .

# Run container
docker run -p 8000:8000 \
  -e DATABASE_URL=your_db_url \
  -e HF_TOKEN=your_hf_token \
  hn-mirror
```

## API Endpoints and Usage

Replace `localhost:8000` with your actual host and port if different.

### Start Mirroring Job

```bash
# Regular job
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"dry_run": false}'

# Dry run (doesn't push to HF)
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"dry_run": true}'
```

### Check Job Status

```bash
curl http://localhost:8000/jobs/status
```

### Get Prometheus Metrics

```bash
curl http://localhost:8000/metrics
```

## Dependencies

- Python 3.12
- PostgreSQL
- Docker

## Development

We recommend using [uv](https://github.com/astral-sh/uv).

```bash
# Install dev dependencies
uv pip install -r requirements.in

# Generate requirements.txt
uv pip compile requirements.in > requirements.txt

# Run locally
uvicorn main:app --reload
```
