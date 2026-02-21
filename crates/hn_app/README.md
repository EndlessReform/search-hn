# hn_app

Read-oriented Axum application for Search HN.

Current scope:
- Health endpoint
- JSON thread reconstruction endpoint backed by `hn_core`

Planned next:
- HTMX-rendered story page/homepage

## Run

From repo root:

```bash
cd crates
DATABASE_URL='postgresql://USER@HOST:5432/searchhn_test' cargo run -p hn_app --release -- --port 3001
```

Notes:
- `DATABASE_URL` is required.
- `--port` is optional; default is `3001`.
- `.pgpass` works fine for password auth.

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
