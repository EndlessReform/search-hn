# Systemd services

The updater has one canonical [unit template](../ansible/templates/updater.service.j2).
Use [Ansible install/rollback](../ansible/README.md); do not copy a second updater
unit by hand. It runs as the existing `catchup:catchup`, reads
`/opt/search-hn/current/worker.toml`, and checks database access with `ExecStartPre`.

Change the protected source [TOML](../ansible/worker.example.toml) and run install
again. Ansible retains the previous configuration with its binaries for rollback.
Set `enabled = false` under `[embedding]` to disable inference while ingestion
continues. No embedding environment file or systemd drop-in is needed. Initial
historical population must precede embedding-enabled startup.

## Separate services

These units remain outside the updater installation playbook:

- `hn-app.service`: read API, using `/usr/local/bin/hn_app` and
  `/etc/search-hn/hn-app.env` (`DATABASE_URL`, optional `RUST_LOG`).
- `catchup-worker-catchup.service` and optional timer: legacy one-shot Firebase
  catchup using `/usr/local/bin/catchup_worker` and `/etc/search-hn/catchup-worker.env`.
  This is not embedding backfill and is not needed for updater recovery. Its
  metrics port is 3002, separate from updater 3000 and read API 3001.
- `backfill-story-id.service`: one-off comment lineage repair, using
  `/usr/local/bin/backfill-story-id` and the legacy worker environment file.

These maintenance commands still use CLI/environment configuration; they do not
accept updater TOML. Existing installations are not removed or enabled by the
updater playbooks. Retired full-crawl and shakedown presets remain in Git history.

The historical updater unit under `infra/ansible/tests/` is a v0.2.0 rollback
fixture, not an installation template. Adoption snapshots the actual installed
legacy unit. Retain the snapshots for rollback. The existing
`/etc/search-hn/catchup-worker.env` supplies DATABASE_URL to both versions and is
left in place during install and rollback; it is not copied into new snapshots.

## Inspect the updater

```bash
sudo systemctl status catchup-worker-updater.service
sudo systemctl cat catchup-worker-updater.service
sudo journalctl -u catchup-worker-updater.service -f
```

Install/rollback need sudo for root-owned files and systemd. The application and
its checks run as `catchup`. Neither operation provisions PostgreSQL, migrates,
or starts historical population.
