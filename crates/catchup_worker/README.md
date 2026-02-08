Catchup worker for `search-hn` to sync Hacker News posts and comments from the upstream Firebase.

## Usage

### CLI

#### Arguments

- `-n, --no_catchup`  
  Disable catchup on previous data.

- `-r, --realtime`  
  Listen for HN updates and persist them to the database.

- `--catchup_start <ID>`  
  Start catchup from this post ID.

- `--catchup_amt <N>`  
  Max number of records to catch up. Mostly for debugging.

#### Example Usage

```bash
# Disable catchup and enable realtime updates
./catchup_worker --no_catchup --realtime

# Start catchup from ID 1000 with a maximum of 500 records
./catchup_worker --catchup_start 1000 --catchup_amt 500
```

### Catchup-Only Entry Point

For a fast development loop (no realtime listener), use:

```bash
cargo run -p catchup_worker --bin catchup_only -- \
  --start-id 1 \
  --limit 100000 \
  --workers 16 \
  --segment-width 1000 \
  --batch-size 500
```

Useful knobs:
- `--database-url` (or `DATABASE_URL` env var)
- `--hn-api-url` (defaults to the real HN endpoint)
- `--start-id`
- `--limit`
- `--workers`
- `--segment-width`
- `--queue-capacity`
- `--batch-size`
- `--retry-attempts`
- `--retry-initial-ms`
- `--retry-max-ms`
- `--retry-jitter-ms`
- `--log-level` (unless `RUST_LOG` is already set)

### Configuration

In the environment (preferably in a `.env` file), set the following parameters:

```env
DATABASE_URL=postgresql://user:password@host:port/hn_database
# Optional: defaults to current endpoint
# HN_API_URL="https://hacker-news.firebaseio.com/v0"
```

## Local setup

```bash
cargo build
# If setting up the DB for the first time
cargo install diesel_cli --no-default-features --features postgres
```

If you need to set up the database from scratch, create database `hn_database`, then run migrations:

```bash
diesel migration run
```

## API Documentation

Generate local Rust API docs (including the `segment_manager` docstrings) with:

```bash
# from repo root
cargo doc -p catchup_worker --no-deps
```

Open the docs in a browser:

```bash
# from repo root
cargo doc -p catchup_worker --no-deps --open
```

If you are on a headless machine, open `crates/target/doc/catchup_worker_lib/index.html` manually.
