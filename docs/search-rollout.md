# First hybrid-index rollout

Run from the repository root on the Mac unless a block says otherwise. This is a
supervised one-off procedure, not a script to run unattended. PostgreSQL stays on
17.11. Target extensions: pgvector **0.8.6**, pg_textsearch **1.4.0**.

## Before booking the window

The migration, preflight and scratch fixture now target 0.8.6; its focused
integration tests passed. Include this change in the normal release build below.
The already-published `v0.2.1-canary.1` still contains the old check.

Finish the agreed final rehearsal and configuration before production:
- Package installation/loading, preload-failure recovery, Diesel migration on the
  full restored snapshot, old-worker writes, and all eight focused package-backed
  integration tests passed. See [evidence](search-validation.md#packaged-debian13-and-restored-database-rehearsal--2026-09-06-local).
  OrbStack's systemd PID tracking failed, so restart/recovery used direct
  `pg_ctlcluster` control there. Production's normal systemd restart path remains
  unverified; do not treat the test-only bypass as a production change.
- Build the updated worker release and complete its normal install/rollback and
  disabled → one-off backfill → enabled activation rehearsal. This package/migration
  check did not publish or deploy a new worker release.
- Confirm the real inventory, worker TOML and inference endpoint. Decide whether
  Garage upload is required before maintenance; its destination/profile is still
  unset. A full local backup/restore has already passed; see [evidence](../tools/backup/RESTORE.md).

The user coordinates other homelab clients affected by the shared DB restart.
Index compaction and PostgreSQL major upgrades are outside this window.

## 1. Build/publish and prepare files — Mac, before downtime

The wizard builds and publishes native worker binaries; there is no worker
container image to SCP to the LXC.

```bash
./scripts/release --dry-run
./scripts/release

# Use the existing worker SSH account; this is the only unknown SSH destination.
WORKER_SSH='EXISTING_USER@WORKER_HOST'
WORKER_CONFIG="$PWD/infra/ansible/worker.local.toml"
test -e "$WORKER_CONFIG" || cp infra/ansible/worker.example.toml "$WORKER_CONFIG"
test -e infra/ansible/hosts.yml || cp infra/ansible/hosts.example.yml infra/ansible/hosts.yml
$EDITOR "$WORKER_CONFIG" infra/ansible/hosts.yml
```

Set inventory to the release selected by the wizard and the absolute TOML path.
In TOML: real DB credentials and embedding endpoint, `startup_rescan_days = 7`,
and **`enabled = false` under `[embedding]`**. Keep the endpoint configured even
while disabled; the explicit backfill command will use it later.

Download the packages **directly on the DB host before downtime**, over HTTPS
from the official project hosts. No Mac download/SCP step, manual checksum step,
or additional PostgreSQL APT repository. The host has curl; install the missing
`unzip` utility for upstream's ZIP-packaged `.deb`.

```bash
ssh root@searchhn-pg
apt-get --no-install-recommends install unzip
mkdir -p /tmp/searchhn-extensions
cd /tmp/searchhn-extensions
curl -fL https://apt.postgresql.org/pub/repos/apt/pool/main/p/pgvector/postgresql-17-pgvector_0.8.6-1.pgdg13+1_amd64.deb -o pgvector.deb
curl -fL https://github.com/timescale/pg_textsearch/releases/download/v1.4.0/pg-textsearch-v1.4.0-pg17-amd64.zip -o pg-textsearch.zip
unzip -p pg-textsearch.zip pg-textsearch-postgresql-17_1.4.0-1_amd64.deb > pg-textsearch.deb
apt-get --simulate --no-install-recommends install /tmp/searchhn-extensions/pgvector.deb /tmp/searchhn-extensions/pg-textsearch.deb
exit
```

Expect only the two extension packages, with no PostgreSQL replacement, unrelated
upgrades or removals. Resolve any different plan before proceeding.

## 2. Fresh backup — Mac, database still online

```bash
./scripts/backup --directory backups --pg-restore /opt/homebrew/opt/libpq@17/bin/pg_restore
# If Garage is required, add: --garage EXISTING_REMOTE:BUCKET/PREFIX
```

Require successful completion and retain the printed backup directory. This covers
all eight application tables and schema dependencies, including source data and
migration bookkeeping. Nothing is staged on the DB LXC. The restore procedure has
already passed on a full snapshot; a new archive is not automatically marked
restore-tested. See [backup procedure](../tools/backup/README.md) for repeating it.
The dump captures backup-start state; writes after that are not in the archive.

## 3. Pause writers, install extensions, restart PostgreSQL

After the user has coordinated other clients, pause this updater and any separately
running maintenance writers identified for this rollout. Keep them paused through
migration. Do not stop unrelated units by wildcard.

```bash
ssh "$WORKER_SSH" 'sudo systemctl stop catchup-worker-updater.service'
ssh root@searchhn-pg
```

The following block runs **as root on the database LXC**. `sudo` is not installed
there; use `runuser` for PostgreSQL commands. Save only the small configuration file
locally, never a database dump. The inspected preload list was empty: if it has
changed, preserve its entries instead of replacing them with the single name below.

```bash
runuser -u postgres -- psql -X -At -c 'SHOW shared_preload_libraries;'
df -h /var/lib/postgresql
mkdir -p /root/searchhn-pre-hybrid
test -e /root/searchhn-pre-hybrid/postgresql.conf || cp -p /etc/postgresql/17/main/postgresql.conf /root/searchhn-pre-hybrid/postgresql.conf
apt-get --no-install-recommends install /tmp/searchhn-extensions/pgvector.deb /tmp/searchhn-extensions/pg-textsearch.deb
pg_conftool 17 main set shared_preload_libraries pg_textsearch
pg_ctlcluster 17 main restart
```

This restarts **PG17/main**, not the whole LXC. Package installation and config
commands must succeed before the restart. Do not continue after an error.

## 4. Check PostgreSQL — same root terminal

```bash
pg_lsclusters
runuser -u postgres -- psql -X -v ON_ERROR_STOP=1 -d searchhn_test -c 'SELECT version(); SHOW shared_preload_libraries; SELECT 1;'
runuser -u postgres -- psql -X -d searchhn_test -c "SELECT name, version FROM pg_available_extension_versions WHERE (name, version) IN (('vector','0.8.6'),('pg_textsearch','1.4.0'));"
exit
```

Expect the cluster online, PG17.11, preload containing pg_textsearch, and both exact
extension versions available. Other clients can perform their agreed recovery
checks now. Our updater remains paused until migration and installation finish.

## 5. Apply the migration — Mac

Use the existing PostgreSQL `admin` role with `.pgpass`; no password in the command.
The migration belongs to the exact source used for the published release: do not
switch to unrelated migration changes between building and this step.

```bash
DATABASE_URL='postgres://admin@searchhn-pg:5432/searchhn_test' diesel migration list --migration-dir crates/hn_core/migrations
# Expect only 20260906000012 pending. Stop if anything else is pending.
DATABASE_URL='postgres://admin@searchhn-pg:5432/searchhn_test' diesel migration run --migration-dir crates/hn_core/migrations
```

This is additive: it creates search objects and an `items` trigger without changing
existing columns, deleting source data, or replacing old FTS. Old-worker writes
must pass the prerequisite rehearsal because the trigger now executes on them.
The existing extra ledger entry `20260904000012` stays untouched. A lock timeout is
a failed migration to investigate/retry, not permission to edit the ledger.

## 6. Install the application WITHOUT embeddings — Mac

```bash
ansible-playbook -i infra/ansible/hosts.yml infra/ansible/install.yml
ssh "$WORKER_SSH" 'sudo systemctl is-active catchup-worker-updater.service; curl --fail http://127.0.0.1:3000/health'
ssh "$WORKER_SSH" 'sudo journalctl -u catchup-worker-updater.service --since "5 minutes ago" --no-pager -n 60'
```

Install performs read-only preflight, captures legacy rollback files, activates the
new unit and verifies health/version. **It already restarts the updater with the
disabled TOML**; a second restart to disable embedding is unnecessary. Require
normal ingestion without permission/trigger errors. Disabled-mode health alone
is not proof that embeddings work.

## 7. One-off full historical backfill — worker host

```bash
ssh "$WORKER_SSH"
sudo systemd-run --unit=searchhn-initial-embedding --collect --wait --pipe \
  --property=User=catchup --property=Group=catchup \
  --property=WorkingDirectory=/var/lib/search-hn \
  /opt/search-hn/current/bin/catchup_worker embedding-backfill \
  --config /opt/search-hn/current/worker.toml
exit
```

This separate invocation reads local history, seeds search rows, and embeds them.
`[embedding] enabled=false` disables the **updater loop**, not this explicitly
requested backfill. It uses the configured endpoint. Run from a persistent terminal
for the potentially long job; inspect its journal with
`sudo journalctl -u searchhn-initial-embedding`. This transient unit logs/supervises one command; it is not an enabled recurring job.

Require `embedding_backfill_finished` and successful exit. Exit 1 means failure;
exit 3 means deferred pending work remains. Inspect and rerun the same command as
needed; completed vectors are preserved. No manual source-ID selection is needed.
`--seed-only` is an alternative if deliberately leaving inference to the updater,
not a requirement to prevent duplicate requests. Keep the DB online throughout.

## 8. Enable updater embeddings and verify — Mac

```bash
$EDITOR "$WORKER_CONFIG"
# Change only [embedding] enabled to true; preserve the seven-day replay setting.
ansible-playbook -i infra/ansible/hosts.yml infra/ansible/install.yml
ssh "$WORKER_SSH" 'sudo journalctl -u catchup-worker-updater.service --since "5 minutes ago" --no-pager -n 80'
ssh root@searchhn-pg "runuser -u postgres -- psql -X -d searchhn_test -c 'SELECT count(*) AS search_rows, count(*) FILTER (WHERE embedding IS NULL) AS pending FROM story_search;'"
ssh root@searchhn-pg 'df -h /var/lib/postgresql'
```

Ansible creates a new configuration snapshot and restarts. Require nonempty search
history, normal ingestion, and successful embedding activity when work is due;
pending work should drain rather than steadily accumulate. A health 200 alone is
not embedding acceptance. Review deferred input errors. Resume any separately
paused maintenance writers deliberately. Existing FTS remains the user-facing
backend; query-backend cutover is a later phase.

## Recovery: choose the failure that actually occurred

- **PostgreSQL cannot start after preload change:** on the DB host as root, restore
  the saved configuration and restart. Then check its log and clients. Do not
  restore database data for a configuration-load failure.

  ```bash
  cp -p /root/searchhn-pre-hybrid/postgresql.conf /etc/postgresql/17/main/postgresql.conf
  pg_ctlcluster 17 main restart
  tail -n 80 /var/log/postgresql/postgresql-17-main.log
  ```

- **Application activation fails:** Ansible attempts to restore the previous
  deployment and reports failure. After the first successful install, explicit
  rollback is `ansible-playbook -i infra/ansible/hosts.yml infra/ansible/rollback.yml`.
  After enabling embeddings, that rolls back to the new binary with embeddings
  disabled, not all the way to v0.2.0. The legacy snapshot is retained separately.
- **Embedding trouble after activation:** set `enabled=false` in the source TOML
  and run install again. Stop the transient backfill if it is still running.
  This preserves ordinary ingestion and all existing source/search data.
- **Trigger errors in ordinary ingestion:** disabling embeddings does not disable
  the trigger, and an old binary does not remove it. Keep the updater paused and
  inspect the error before deciding on schema rollback. Do not blindly run Diesel
  revert against the live ledger's extra migration. Database restore is a separate
  deliberate recovery operation using [the verified backup](../tools/backup/README.md).

Retain the backup and previous deployments through acceptance. No database/index
compaction, automatic retention deletion, or PostgreSQL major upgrade is included.
