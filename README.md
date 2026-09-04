# SearchHN

SearchHN is a local Hacker News mirror with:

- `hn_app`: read-oriented Axum web UI/API with an HTMX homepage and story pages.
- `catchup_worker`: ingest/catchup services for keeping the mirror current.
- `hn_core`: shared database models, migrations, and story-tree retrieval logic.
- `search-agent`: Python TUI and HTTP wrapper for agent-assisted HN research.

## Search Agent

The search agent lives in the uv workspace and queries the PostgreSQL mirror.
See [its README](packages/search-agent/README.md) for provider configuration,
TUI/API startup commands, and tests.

The OpenAI Agents SDK documentation used during development is intentionally
not vendored. The bootstrap script checks out the Git tag matching the
`openai-agents` version in `uv.lock` and records its source revision:

```bash
scripts/init-openai-agent-sdk-docs.sh
```

## Quick Start: Web App

Run the HTMX web UI locally from the repo root:

```bash
cd crates
DATABASE_URL='postgresql://readonly_hn_agent@searchhn-pg:5432/searchhn_test' \
  cargo run --locked -p hn_app -- --port 3001
```

Then open:

- `http://127.0.0.1:3001/` for the homepage
- `http://127.0.0.1:3001/item?id=3173993` for a story page
- `http://127.0.0.1:3001/health` for the health check

Run the app tests:

```bash
cd crates
cargo test --locked -p hn_app
```

Use release mode for a production-ish smoke test:

```bash
cd crates
DATABASE_URL='postgresql://readonly_hn_agent@searchhn-pg:5432/searchhn_test' \
  cargo run --locked -p hn_app --release -- --port 3001
```

## Debian 13 Build And Deploy

Build a Debian 13 (trixie-compatible) `hn_app` binary from the repo root:

```bash
infra/build/build-hn-app-debian13.sh
```

The default output is:

```text
dist/debian13/hn_app
```

Deploy the binary and restart the app service:

```bash
scp dist/debian13/hn_app user@lxc-host:/tmp/hn_app
ssh user@lxc-host 'sudo install -m 0755 /tmp/hn_app /usr/local/bin/hn_app && sudo systemctl restart hn-app.service'
```

The systemd unit to restart for the web UI/API is:

```bash
sudo systemctl restart hn-app.service
```

Check it after restart:

```bash
sudo systemctl status hn-app.service --no-pager
journalctl -u hn-app.service -n 100 --no-pager
curl -sS http://127.0.0.1:3001/health
```

If you changed the unit file itself, copy `infra/systemd/hn-app.service` to
`/etc/systemd/system/` and run `sudo systemctl daemon-reload` before restarting.

## Catchup Worker Build

Build Debian 13-compatible ingest binaries:

```bash
infra/build/build-catchup-only-debian13.sh
```

Default outputs:

```text
dist/debian13/catchup_worker
dist/debian13/catchup_only
dist/debian13/backfill-story-id
```

Main systemd units:

- `catchup-worker-updater.service`: long-running updater
- `catchup-worker-catchup.service`: manual/one-shot catchup
- `catchup-worker-catchup.timer`: optional scheduled catchup sweep

## Useful Docs

- [hn_app README](crates/hn_app/README.md): app endpoints, local testing, Debian build notes.
- [catchup_worker README](crates/catchup_worker/README.md): ingest service operation and deployment.
- [Debian build README](infra/build/README.md): build scripts and smoke tests.
- [systemd README](infra/systemd/README.md): unit install, env files, and service commands.
- [Architecture notes](docs/ARCHITECTURE.md): system overview and module map.
