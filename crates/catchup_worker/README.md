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

Debug replay example (reprocess IDs `1..100000` even if already done):

```bash
cargo run -p catchup_worker --bin catchup_only -- \
  --start-id 1 \
  --end-id 100000 \
  --ignore-highest
```

Useful knobs:
- `--database-url` (or `DATABASE_URL` env var)
- `--hn-api-url` (defaults to the real HN endpoint)
- `--start-id`
- `--end-id`
- `--limit`
- `--ignore-highest` (debug replay mode; requires `--end-id` or `--limit`)
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
DATABASE_URL=postgresql://user@host:port/hn_database
# Optional: defaults to current endpoint
# HN_API_URL="https://hacker-news.firebaseio.com/v0"
```

For production, do not hardcode passwords in `DATABASE_URL`; prefer `~/.pgpass`.

## Local setup

```bash
# from repo root
cargo build
```

## Diesel CLI setup (one-time)

`diesel` is a separate CLI tool. It is not provided just because the Rust crate depends on
`diesel` as a library.

Install it:

```bash
# from repo root (or anywhere)
cargo install diesel_cli --no-default-features --features postgres
```

If `diesel: command not found`, add Cargo's bin dir to PATH (zsh):

```bash
echo 'export PATH="$HOME/.cargo/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
hash -r
diesel --version
```

If install fails due missing postgres headers/tooling, install platform deps first, then rerun:

```bash
# Debian/Ubuntu
sudo apt-get install -y libpq-dev pkg-config

# macOS (Homebrew)
brew install postgresql
```

## Database migrations (manual, per environment)

`catchup_worker` does not run migrations at startup. Apply migrations as a separate deploy step
with an admin/migration role, then run the app with a restricted service role.

### 1) Configure secret handling (no passwords in command history)

Recommended: `~/.pgpass`

```bash
touch ~/.pgpass
chmod 600 ~/.pgpass
```

Add one line per environment:

```text
test-db:5432:hn_database:migrator:YOUR_SECRET_PASSWORD
```

Then use a password-free URL:

```bash
export DATABASE_URL='postgresql://migrator@test-db:5432/hn_database'
```

Alternative (one-off): prompt without echo and keep password out shell history:

```bash
read -s 'PGPASSWORD?DB password: '; echo
export PGPASSWORD
```

### 2) Run migrations for the target environment

`diesel` resolves DB config in this order:
1. `--database-url` flag
2. `DATABASE_URL` in the current shell environment
3. `.env` file in the working directory where you run `diesel`

From repo root:

```bash
diesel migration run --migration-dir crates/catchup_worker/migrations
```

Or from `crates/catchup_worker/`:

```bash
diesel migration run
```

### 3) Start app with service-user credentials

After migrations are applied, start `catchup_worker` or `catchup_only` with your service-user
`DATABASE_URL` (read/write only, no schema changes required).

If you used `PGPASSWORD` for migration, clear it:

```bash
unset PGPASSWORD
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

## Appendix: Structured Logging Schema

`catchup_worker` now emits structured logs via `tracing` and supports newline-delimited JSON.

### Format and controls

- `LOG_FORMAT=json` (default) emits JSON logs.
- `LOG_FORMAT=text` emits human-readable text logs.
- `RUST_LOG` controls filtering (for example: `RUST_LOG=info`, `RUST_LOG=catchup_worker_lib=debug`).
- `APP_ENV` (or fallback `ENVIRONMENT`) is included as the environment field.

### Common envelope fields

Every structured event should include this baseline envelope
(directly on the event or in the `current_span` object):

- `timestamp`: event timestamp (from `tracing_subscriber`)
- `level`: log level
- `target`: Rust module target
- `event`: machine-friendly event name
- `service`: service name (`catchup_worker`)
- `environment`: deployment environment (`dev`, `test`, `prod`, etc.)
- `mode`: runtime mode (`service` or `catchup_only`)
- `run_id`: unique per-process run identifier
- `message`: human-readable operator text

### Catchup event taxonomy

Core catchup events emitted by the orchestrator/service include:

- `catchup_worker_settings`
- `catchup_planning_target`
- `catchup_preparation_complete`
- `catchup_segments_claimed`
- `segment_retry_wait`
- `segment_dead_letter`
- `catchup_summary`
- `catchup_fatal_failures_observed`

### Catchup Prometheus metrics

New catchup-flow metrics exposed on `/metrics`:

- `catchup_segments_claimed_total`
- `catchup_segments_completed_total`
- `catchup_segments_retry_wait_total`
- `catchup_segments_dead_letter_total`
- `catchup_terminal_missing_items_total`
- `catchup_durable_items_total`
- `catchup_frontier_id`
- `catchup_target_max_id`
- `catchup_pending_segments`

Quick PromQL starters:

- Throughput (IDs/sec): `rate(catchup_durable_items_total[5m])`
- Retry ratio: `rate(catchup_segments_retry_wait_total[5m]) / clamp_min(rate(catchup_segments_claimed_total[5m]), 1)`
- Dead-letter ratio: `rate(catchup_segments_dead_letter_total[5m]) / clamp_min(rate(catchup_segments_claimed_total[5m]), 1)`

### Recommended event-specific fields

For planning/scheduling events:

- `frontier_id`
- `planning_start_id`
- `target_max_id`
- `created_segments`
- `claimed_segments`

For segment lifecycle/failure events:

- `segment_id`
- `worker_idx`
- `item_id`
- `attempts`
- `error_message`
- `unresolved_count` (when available)

For terminal run summaries:

- `completed_segments`
- `retry_wait_segments`
- `dead_letter_segments`
- `had_fatal_failures`

### Volume policy

To keep log volume bounded for large runs:

- Do not emit per-item success logs in steady state.
- Emit per-item logs for retries/fatal paths only.
- Use segment-level and run-summary events for normal progress tracking.
