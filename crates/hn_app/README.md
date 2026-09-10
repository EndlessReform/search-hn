# hn_app

Read-oriented Axum application for Search HN.

Current scope:
- Health endpoint
- JSON thread reconstruction endpoint backed by `hn_core`
- HTMX-rendered homepage and story/comment pages

## Local Testing

From repo root:

```bash
cd crates
DATABASE_URL='postgresql://USER@HOST:5432/searchhn_test' cargo run --locked -p hn_app -- --port 3001
```

Notes:
- `DATABASE_URL` is required.
- `--port` is optional; default is `3001`.
- The async Rust driver does not automatically read `.pgpass`; supply credentials through the existing service `DATABASE_URL` environment. Do not put credentials in command history or docs.
- For a trust-authenticated connection, the read-only mirror URL is:

  ```bash
  DATABASE_URL='postgresql://readonly_hn_agent@searchhn-pg:5432/searchhn_test' \
    cargo run --locked -p hn_app -- --port 3001
  ```

Then open:

- `http://127.0.0.1:3001/` for the HTMX homepage
- `http://127.0.0.1:3001/item?id=3173993` for a story page
- `http://127.0.0.1:3001/health` for a cheap process check

Before shipping an app-only change, run:

```bash
cargo test --locked -p hn_app
```

Use `--release` for a closer production smoke test:

```bash
DATABASE_URL='postgresql://readonly_hn_agent@searchhn-pg:5432/searchhn_test' \
  cargo run --locked -p hn_app --release -- --port 3001
```

## Debian 13 Build

Build the Debian 13 (trixie) `hn_app` binary from the repo root with:

```bash
infra/build/build-hn-app-debian13.sh
```

The default output is:

```text
dist/debian13/hn_app
```

Common options:

```bash
infra/build/build-hn-app-debian13.sh \
  --out-dir ./dist/debian13 \
  --jobs 32
```

The script builds inside a Debian 13 container so the resulting binary matches
the target glibc/libpq/libssl ABI. It also passes the current git commit through
`SOURCE_COMMIT_HASH`, which keeps version metadata useful after deployment.

For routine releases, use the shared release wizard and
`infra/ansible/app-install.yml`; see the [deployment guide](../../infra/ansible/README.md#app-installation).
The manual build helper remains useful for development. `hn_app --version`
reports the workspace version plus the release commit (`local` without an injected
`SOURCE_COMMIT_HASH`).

## Endpoints

### Health

```bash
curl -sS http://127.0.0.1:3001/health
```

Returns:

```text
ok
```

### Story Thread JSON

```bash
curl -sS http://127.0.0.1:3001/api/stories/3173993/tree | jq .
```

This endpoint reconstructs one story thread as a nested tree using shared logic in
`hn_core::db::story_tree`.

Status codes:
- `200`: story exists and is a story
- `404`: item missing or item is not a story (for example, passing a comment id)
- `503`: transient DB/backend failure
- `500`: permanent backend failure

Example of a non-story ID:

```bash
curl -sS -i http://127.0.0.1:3001/api/stories/3174158/tree
```

## Logging

`RUST_LOG` is supported via `tracing-subscriber` env filter.

Example:

```bash
RUST_LOG=hn_app=debug cargo run -p hn_app -- --port 3001
```

## Search (local implementation, September 7)

`/search` renders a plain HN-style GET form and story results. `/api/search`
uses the same Rust retrieval service and returns JSON. Configure
`EMBEDDING_BASE_URL` with the proxy's `/embeddings/v1` base URL in the app's
service environment. No default deployment hostname is embedded in runtime code.
If unset, search runs keyword-only; inference errors or recipe mismatches also
produce a labelled keyword-only result set. The database reader needs access to
`story_search` and `items`; this change does not provision roles or deploy services.

Parameters: `q` (required for JSON, 1–2048 UTF-8 bytes), `sort`
(`relevance`, `score`, `date`; default relevance), `page` (1–10; default 1), and
`session` (returned snapshot ID, required after page 1). Unknown/expired sessions
return 410; changed query/sort with a session returns 400. Start a new search by
omitting `session`. Pages contain up to 20 stories; `has_more` describes remaining
positions, not a full-corpus match count. Snapshots expire after five minutes,
are capped at 128 per process, and disappear on restart. They are not durable or
portable between server processes. Live deletion/demotion can leave short pages.

The first slice searches eligible 25+ point stories, using title BM25 and
1024-dimensional title/URL vectors, 100 candidates per branch and RRF weights
1 / 0.125 with k=60. Points/newest sort the retrieved candidate set. Newest uses
full story timestamps. Filters, query batching, agent cutover and the legacy FTS
selector are follow-up work; this API is not yet a complete agent replacement.

Validation against a running local preview:

```bash
uv run tools/search/validate-app.py --base-url http://127.0.0.1:3081
```

Add `--fallback-url` pointing at a second preview with deliberately unavailable
inference to exercise keyword fallback and empty results. See the
[dated checks](../../docs/search-validation/2026-09-07/app-search.md).
