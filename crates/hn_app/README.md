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
- `.pgpass` works fine for password auth.
- Use the local readonly mirror when you only need to exercise the UI against data:

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

After copying the binary to the host, restart the app unit:

```bash
scp dist/debian13/hn_app user@lxc-host:/tmp/hn_app
ssh user@lxc-host 'sudo install -m 0755 /tmp/hn_app /usr/local/bin/hn_app && sudo systemctl restart hn-app.service'
```

Check the deployment with:

```bash
ssh user@lxc-host 'sudo systemctl status hn-app.service --no-pager'
ssh user@lxc-host 'journalctl -u hn-app.service -n 100 --no-pager'
curl -sS http://lxc-host:3001/health
```

If you changed `infra/systemd/hn-app.service`, copy the unit and run
`sudo systemctl daemon-reload` before restarting.

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
