# Deploy hybrid indexing phases 3–4

> **Obsolete — do not execute these steps.** This draft contains superseded and
> inconsistent procedures. Decisions and open proposals are tracked in
> [deployment-decisions.md](deployment-decisions.md); replacement tooling is pending.

Run the numbered steps in order. Keep a Mac terminal, a database-host terminal and
an updater-host terminal open; the labels below say where each block runs.
Only the two SSH destinations in step 1 need administrator-supplied values.
Commands below assume the checked-in systemd layout; confirm it in step 2.

Verified database facts on 2026-09-06: `searchhn-pg:5432`, database `searchhn_test`,
Debian 13 x86_64, PostgreSQL 17.11, source-table owner `admin`, service database role
`catchup_worker`. Only plpgsql is currently installed; `story_search` is absent.
The ledger includes `20260904000012`, an additional migration outside this checkout.
`diesel migration run` from the Mac applies **only** `20260906000012` (the one
checked-in file missing from the ledger) and ignores the extra ledger row. There
is no hand-written ledger insert. Ownership and grants live in the migration
itself (`crates/hn_core/migrations/2026-09-06-000012_add_story_search/up.sql`);
the scratch test harness creates the same `admin` / `catchup_worker` roles so
tests exercise that path.

## 1. Mac — build the release binary

Fill the SSH destination once. The updater address is not recorded in the repo.
The database migration runs from this Mac via diesel in step 5, after extensions
are installed — nothing is copied to the database host up front.

```bash
cd /Users/ritsuko/projects/data/search-hn
export SEARCH_UPDATER_SSH='YOUR_ADMIN_USER@YOUR_UPDATER_HOST'

# The build script defaults to --platform linux/amd64 (the deploy target), so no
# DOCKER_DEFAULT_PLATFORM is needed on an ARM Mac. --platform overrides it.
infra/build/build-catchup-only-debian13.sh \
  --image-tag search-hn/catchup-builder:debian13-amd64 \
  --out-dir /Users/ritsuko/projects/data/search-hn/dist/hybrid-amd64

file dist/hybrid-amd64/catchup_worker
# Must say ELF ... x86-64, not ARM/aarch64 or Mach-O.
git rev-parse HEAD

scp dist/hybrid-amd64/catchup_worker "${SEARCH_UPDATER_SSH}:/tmp/catchup_worker.hybrid-new"
```

You deploy the exported **binary**, not the builder container. The existing unit
runs `/usr/local/bin/catchup_worker updater`. This deploy needs no hn_app rebuild
and no inference-service restart.

Open the database and updater SSH sessions in separate terminals. The binary is
staged in step 2 on the updater host before anything stops; the migration itself
stays in this checkout and is applied with diesel in step 5.

## 2. Updater host — confirm layout and retain rollback files

```bash
sudo systemctl cat catchup-worker-updater.service
cat /etc/os-release
uname -m
```

Expect Debian 13, `x86_64`, `User=catchup`,
`EnvironmentFile=/etc/search-hn/catchup-worker.env`, and the executable
`/usr/local/bin/catchup_worker updater`. There should be no existing
`--embedding-base-url` argument in ExecStart. If the deployed unit differs, adapt
its paths/user before continuing; do not overwrite a custom unit blindly.
If `/var/lib/search-hn/.env` exists, ensure it does not set `EMBEDDING_BASE_URL`:
the binary also loads dotenv, which could otherwise re-enable an unset variable.

Rollback snapshots live only in `/var/lib/search-hn/pre-hybrid-20260906` and the
`/tmp` staging file. Both are reaped in step 9 once the rollout is healthy — they
are not permanent. If this block refuses to run because a backup already exists,
it prints the existing files; inspect them (a previous attempt) rather than
overwriting the last known-good copy.

```bash
sudo bash <<'ROOT'
set -euo pipefail
echo "== snapshot current binary, env and effective unit =="
install -d -m 0700 /var/lib/search-hn/pre-hybrid-20260906
if [[ -e /var/lib/search-hn/pre-hybrid-20260906/catchup_worker ]]; then
  echo "REFUSING to overwrite existing rollback backup:"
  ls -la /var/lib/search-hn/pre-hybrid-20260906
  echo "Inspect it; 'rm -rf /var/lib/search-hn/pre-hybrid-20260906' only once you are sure it is stale."
  exit 1
fi
cp -a /usr/local/bin/catchup_worker /var/lib/search-hn/pre-hybrid-20260906/
cp -a /etc/search-hn/catchup-worker.env /var/lib/search-hn/pre-hybrid-20260906/
systemctl cat catchup-worker-updater.service \
  > /var/lib/search-hn/pre-hybrid-20260906/updater-unit.txt
echo "== refuse if embedding is already half-enabled =="
if [[ -e /etc/systemd/system/catchup-worker-updater.service.d/30-hybrid-embedding.conf ]]; then
  echo "Drop-in 30-hybrid-embedding.conf already exists; inspect it before continuing."
  exit 1
fi
echo "== stage and smoke-test the replacement while the old service still runs =="
install -m 0755 /tmp/catchup_worker.hybrid-new /usr/local/bin/catchup_worker.hybrid-new
/usr/local/bin/catchup_worker.hybrid-new --version
/usr/local/bin/catchup_worker.hybrid-new embedding-backfill --help
echo "Staged. Rollback copies remain in /var/lib/search-hn/pre-hybrid-20260906 until step 9 reaps them."
ROOT
```

A missing-library/architecture error here must be fixed before pausing the
running updater.

## 3. Database host — backup decision and extension installation

**Use an existing recent, usable backup if you have one.** A fresh full dump is not
a requirement just for this additive migration. If no recovery point exists, take
one before changing PostgreSQL's loaded extensions. A stopped-database snapshot
is another option; a plain copy of the running data directory is not a backup.
[PostgreSQL backup guidance](https://www.postgresql.org/docs/17/backup-file.html)

Save lightweight configuration/schema records regardless:

```bash
sudo bash <<'ROOT'
set -euo pipefail
test "$(uname -m)" = x86_64
pg_lsclusters
install -d -o postgres -g postgres -m 0700 /var/backups/postgresql/pre-hybrid-20260906
test ! -e /var/backups/postgresql/pre-hybrid-20260906/schema.sql
runuser -u postgres -- /usr/lib/postgresql/17/bin/pg_dump -p 5432 \
  --schema-only --file=/var/backups/postgresql/pre-hybrid-20260906/schema.sql searchhn_test
runuser -u postgres -- /usr/lib/postgresql/17/bin/pg_dumpall -p 5432 \
  --globals-only --file=/var/backups/postgresql/pre-hybrid-20260906/globals.sql
runuser -u postgres -- /usr/lib/postgresql/17/bin/psql -X -p 5432 -At -d postgres \
  -c 'SHOW shared_preload_libraries' > /var/backups/postgresql/pre-hybrid-20260906/preload.txt
runuser -u postgres -- /usr/lib/postgresql/17/bin/psql -X -p 5432 -d searchhn_test \
  -c 'SELECT version();'
df -h
ROOT
```

Schema/globals files alone do **not** back up the item data. If a full backup is
needed, this separate block creates it while PostgreSQL remains online. Ensure
the destination has room or use your normal backup storage instead:

```bash
sudo -u postgres /usr/lib/postgresql/17/bin/pg_dump -p 5432 \
  --format=custom --file=/var/backups/postgresql/pre-hybrid-20260906/searchhn_test.dump searchhn_test
sudo -u postgres /usr/lib/postgresql/17/bin/pg_restore \
  --list /var/backups/postgresql/pre-hybrid-20260906/searchhn_test.dump > /dev/null
```

The second command checks archive readability, not a full restore. Your regular
backup procedure should retain its restore path.

Build the exact extensions against this host's PG17 headers:

```bash
sudo bash <<'ROOT'
set -euo pipefail
apt-get update
apt-get install -y --no-install-recommends \
  build-essential git ca-certificates postgresql-server-dev-17
/usr/lib/postgresql/17/bin/pg_config --version
SEARCH_BUILD_DIR=$(mktemp -d /var/tmp/search-hn-extensions.XXXXXX)
git clone --depth 1 --branch v0.8.2 https://github.com/pgvector/pgvector.git "$SEARCH_BUILD_DIR/vector"
make -C "$SEARCH_BUILD_DIR/vector" -j2 PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config OPTFLAGS=''
make -C "$SEARCH_BUILD_DIR/vector" PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config install
git clone --depth 1 --branch v1.4.0 https://github.com/timescale/pg_textsearch.git "$SEARCH_BUILD_DIR/textsearch"
test "$(git -C "$SEARCH_BUILD_DIR/textsearch" rev-parse HEAD)" = 7a932505b537d50ad8d2d068a053cd1dd7b646ea
make -C "$SEARCH_BUILD_DIR/textsearch" -j2 PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config
make -C "$SEARCH_BUILD_DIR/textsearch" PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config install
runuser -u postgres -- /usr/lib/postgresql/17/bin/psql -X -p 5432 -d searchhn_test -v ON_ERROR_STOP=1 \
  -c "SELECT name,version FROM pg_available_extension_versions WHERE (name,version) IN (('vector','0.8.2'),('pg_textsearch','1.4.0'));"
ROOT
```

Expect both rows. These are the sources tested on both architectures. pg_textsearch
requires preload and a PostgreSQL restart; the next step preserves existing preload
entries. [Pinned extension instructions](https://github.com/timescale/pg_textsearch/blob/v1.4.0/README.md)

## 4. Updater host — pause writers

This saves which managed writers/timers were active, then stops them. Also stop
any separately launched ingest/admin-write jobs for this short maintenance window.
The read API can stay running, but requests may fail briefly during the DB restart.

```bash
sudo bash <<'ROOT'
set -euo pipefail
systemctl list-units --type=timer --state=active --no-legend --plain 'catchup-worker-*.timer' \
  | awk '{print $1}' > /var/lib/search-hn/pre-hybrid-20260906/active-timers.txt
while IFS= read -r unit; do systemctl stop "$unit"; done \
  < /var/lib/search-hn/pre-hybrid-20260906/active-timers.txt
systemctl list-units --type=service --state=running --no-legend --plain \
  'catchup-worker-*.service' 'backfill-story-id.service' \
  | awk '{print $1}' > /var/lib/search-hn/pre-hybrid-20260906/active-writers.txt
while IFS= read -r unit; do systemctl stop "$unit"; done \
  < /var/lib/search-hn/pre-hybrid-20260906/active-writers.txt
systemctl stop catchup-worker-updater.service
ROOT
```

## 5. Database host — preload, restart, then migrate atomically

```bash
sudo bash <<'ROOT'
set -euo pipefail
SEARCH_CLUSTER=$(pg_lsclusters --no-header | awk '$1 == "17" && $3 == "5432" {print $2}')
test -n "$SEARCH_CLUSTER"
test "$(printf '%s\n' "$SEARCH_CLUSTER" | wc -l)" -eq 1
runuser -u postgres -- /usr/lib/postgresql/17/bin/psql -X -p 5432 -d postgres -v ON_ERROR_STOP=1 <<'SQL'
SELECT format('ALTER SYSTEM SET shared_preload_libraries = %L',
    concat_ws(', ', nullif(current_setting('shared_preload_libraries'), ''), 'pg_textsearch'))
WHERE NOT ('pg_textsearch' = ANY(string_to_array(replace(current_setting('shared_preload_libraries'), ' ', ''), ',')))
\gexec
SQL
pg_ctlcluster 17 "$SEARCH_CLUSTER" restart
runuser -u postgres -- /usr/lib/postgresql/17/bin/psql -X -p 5432 -d postgres -v ON_ERROR_STOP=1 \
  -c 'SHOW shared_preload_libraries;'

# One transaction includes the canonical migration, ownership/grants and ledger entry.
# Failure rolls all of these back; no unrelated pending migration is applied.
runuser -u postgres -- /usr/lib/postgresql/17/bin/psql -X -p 5432 -d searchhn_test \
  -v ON_ERROR_STOP=1 --single-transaction \
  -c 'SET LOCAL search_path = public;' \
  -f /opt/search-hn/hybrid-20260906/up.sql \
  -f /opt/search-hn/hybrid-20260906/grants-and-ledger.sql

runuser -u postgres -- /usr/lib/postgresql/17/bin/psql -X -p 5432 -d searchhn_test -v ON_ERROR_STOP=1 <<'SQL'
SELECT extname,extversion FROM pg_extension ORDER BY extname;
SELECT version FROM __diesel_schema_migrations ORDER BY version DESC LIMIT 3;
SELECT count(*) AS search_rows FROM story_search;
SELECT bool_and(has_table_privilege('catchup_worker','story_search',privilege)) AS service_access
FROM unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE']) AS p(privilege);
SQL
ROOT
```

Expect pgvector 0.8.2, pg_textsearch 1.4.0, ledger entry `20260906000012`, an empty
search table, and service access true. If the migration hits its five-second lock
timeout, inspect the blocking session and retry the migration block; do not delete
ledger entries or raise the timeout blindly. If already applied, do not rerun it.

## 6. Updater host — install and start with embeddings disabled

```bash
sudo bash <<'ROOT'
set -euo pipefail
install -d -m 0755 /etc/systemd/system/catchup-worker-updater.service.d
cat > /etc/systemd/system/catchup-worker-updater.service.d/30-hybrid-embedding.conf <<'UNIT'
[Service]
UnsetEnvironment=EMBEDDING_BASE_URL
UNIT
mv /usr/local/bin/catchup_worker.hybrid-new /usr/local/bin/catchup_worker
systemctl daemon-reload
systemctl start catchup-worker-updater.service
systemctl is-active catchup-worker-updater.service
journalctl -u catchup-worker-updater.service --since '-2 minutes' --no-pager -n 60
ROOT
```

Observe ordinary ingestion for a few minutes. Existing health/monitoring should
remain healthy; there should be no permission, missing-function or trigger errors.
New eligible stories can now become pending, but there is no embedding consumer yet.

## 7. Updater host — bounded embedding canary

This loads credentials with systemd's EnvironmentFile parser, exactly as the
updater does. No password needs to be pasted, printed or passed as a CLI argument.
The updater continues ingesting with its embedding loop disabled.

```bash
sudo systemd-run --unit=search-hn-embedding-canary --collect --wait --pipe \
  --property=User=catchup --property=Group=catchup \
  --property=WorkingDirectory=/var/lib/search-hn \
  --property=EnvironmentFile=/etc/search-hn/catchup-worker.env \
  /usr/local/bin/catchup_worker embedding-backfill \
  --start-id 41000000 --end-id 41001000 --source-chunk-size 100 \
  --embedding-batch-size 4 \
  --embedding-base-url https://magi06-inference.tail7a3eb.ts.net/embeddings/v1
```

Expect successful embedding batches and exit 0. Exit 3 means deferred inputs
remain; inspect the logged story/error and rerun later. Exit 1 is a failure to fix
before broadening the rollout. Run the same command a second time: completed
embeddings should remain intact and should not be requested again.

On the **database host**, verify the range is nonempty and completed:

```bash
sudo -u postgres /usr/lib/postgresql/17/bin/psql -X -p 5432 -d searchhn_test -v ON_ERROR_STOP=1 <<'SQL'
SELECT count(*) AS admitted,
       count(*) FILTER (WHERE embedding IS NOT NULL) AS embedded,
       count(*) FILTER (WHERE embedding IS NULL) AS pending
FROM story_search WHERE story_id BETWEEN 41000000 AND 41001000;
SQL
```

Expect admitted > 0 and pending = 0. A zero-row result is not a successful canary;
choose another bounded source range if the source contents have changed.

## 8. Updater host — enable the loop, then admit history

Only proceed after the canary and ordinary ingestion look healthy.

```bash
sudo bash <<'ROOT'
set -euo pipefail
cat > /etc/search-hn/hybrid-embeddings.env <<'ENV'
EMBEDDING_BASE_URL=https://magi06-inference.tail7a3eb.ts.net/embeddings/v1
ENV
chmod 0644 /etc/search-hn/hybrid-embeddings.env
cat > /etc/systemd/system/catchup-worker-updater.service.d/30-hybrid-embedding.conf <<'UNIT'
[Service]
EnvironmentFile=/etc/search-hn/hybrid-embeddings.env
UNIT
systemctl daemon-reload
systemctl restart catchup-worker-updater.service
systemctl is-active catchup-worker-updater.service
ROOT
```

This second EnvironmentFile overrides just the embedding URL without editing the
existing credentials file. The earlier UnsetEnvironment directive is gone because
the same drop-in file was replaced. Observe embedding progress in the journal:

```bash
sudo journalctl -u catchup-worker-updater.service --since '-5 minutes' --no-pager -n 100
```

Then start all-history admission as a separate, logged systemd job. **Use
`--seed-only` now: the updater is the single embedding consumer.**

```bash
sudo systemd-run --unit=search-hn-embedding-seed --collect \
  --property=User=catchup --property=Group=catchup \
  --property=WorkingDirectory=/var/lib/search-hn \
  --property=EnvironmentFile=/etc/search-hn/catchup-worker.env \
  /usr/local/bin/catchup_worker embedding-backfill --seed-only --source-chunk-size 1000

sudo journalctl -u search-hn-embedding-seed.service --since '-5 minutes' --no-pager -n 40
```

The command returns immediately; check for `embedding_backfill_finished` in its
journal. The updater continues embedding after admission finishes. You can stop
the seeding job and rerun the same command safely; it preserves finished vectors.

Restore previously active catchup timers after the rollout is healthy:

```bash
sudo bash <<'ROOT'
set -euo pipefail
while IFS= read -r unit; do systemctl start "$unit"; done \
  < /var/lib/search-hn/pre-hybrid-20260906/active-timers.txt
cat /var/lib/search-hn/pre-hybrid-20260906/active-writers.txt
ROOT
```

If that last list included another one-shot recrawl or lineage-repair job, decide
when to resume it; this procedure does not automatically replay those large jobs.

## 9. Database host — progress and disk space

```bash
sudo -u postgres /usr/lib/postgresql/17/bin/psql -X -p 5432 -d searchhn_test -v ON_ERROR_STOP=1 <<'SQL'
SELECT count(*) AS pending,
       count(*) FILTER (WHERE retry_after <= now()) AS due
FROM story_search WHERE embedding IS NULL;
SELECT pg_size_pretty(pg_total_relation_size('story_search')) AS search_size;
SQL
df -h
```

Check these periodically along with normal ingestion health and existing inference
monitoring. Allow space for both index growth and WAL. Admission finishing is not
embedding completion: pending must reach zero or its failed inputs be explicitly
accounted for. Existing FTS remains the user-facing search backend; phases 5–6 are
not part of these commands.

## 10. Rollback if needed

**Embedding trouble only:** on the updater host, stop the seed job if still active,
then replace the drop-in with the disabled form and restart. Source ingestion and
the search table remain available.

```bash
sudo bash <<'ROOT'
set -euo pipefail
if systemctl is-active --quiet search-hn-embedding-seed.service; then
  systemctl stop search-hn-embedding-seed.service
fi
cat > /etc/systemd/system/catchup-worker-updater.service.d/30-hybrid-embedding.conf <<'UNIT'
[Service]
UnsetEnvironment=EMBEDDING_BASE_URL
UNIT
systemctl daemon-reload
systemctl restart catchup-worker-updater.service
ROOT
```

**Trigger/schema trouble:** first run this on the updater host. It stops current
writers without overwriting the original active-unit records or restarting the
updater:

```bash
sudo bash <<'ROOT'
set -euo pipefail
systemctl list-units --type=timer --state=active --no-legend --plain 'catchup-worker-*.timer' \
  | awk '{print $1}' | while IFS= read -r unit; do systemctl stop "$unit"; done
systemctl list-units --type=service --state=running --no-legend --plain \
  'catchup-worker-*.service' 'backfill-story-id.service' \
  | awk '{print $1}' | while IFS= read -r unit; do systemctl stop "$unit"; done
systemctl stop catchup-worker-updater.service
if systemctl is-active --quiet search-hn-embedding-seed.service; then
  systemctl stop search-hn-embedding-seed.service
fi
ROOT
```

Then run on the database host. This discards the derived index/embeddings, never
`items` or old FTS:

```bash
sudo -u postgres /usr/lib/postgresql/17/bin/psql -X -p 5432 -d searchhn_test \
  -v ON_ERROR_STOP=1 --single-transaction \
  -c 'SET LOCAL search_path = public;' \
  -f /opt/search-hn/hybrid-20260906/down.sql \
  -c "DELETE FROM public.__diesel_schema_migrations WHERE version='20260906000012';"
```

On the updater host, disable embeddings and restore the saved binary with a staged
rename:

```bash
sudo bash <<'ROOT'
set -euo pipefail
systemctl stop catchup-worker-updater.service
cat > /etc/systemd/system/catchup-worker-updater.service.d/30-hybrid-embedding.conf <<'UNIT'
[Service]
UnsetEnvironment=EMBEDDING_BASE_URL
UNIT
install -m 0755 /var/lib/search-hn/pre-hybrid-20260906/catchup_worker /usr/local/bin/catchup_worker.rollback
mv /usr/local/bin/catchup_worker.rollback /usr/local/bin/catchup_worker
systemctl daemon-reload
systemctl start catchup-worker-updater.service
systemctl is-active catchup-worker-updater.service
ROOT
```

Use the timer-restoration block in step 8 to resume previously active timers once
ordinary ingestion is healthy again.

The extensions can stay installed for ordinary feature rollback. If PostgreSQL
itself fails to start immediately after the preload change, a **DB administrator**
must edit the active cluster's `postgresql.auto.conf` offline, restoring
`shared_preload_libraries` to the value saved in
`/var/backups/postgresql/pre-hybrid-20260906/preload.txt`, then start that cluster.
Do not run `ALTER SYSTEM` against a different cluster to try to repair a stopped one.
If it is running and you deliberately want to restore the old preload list, the
administrator can apply that saved value via ALTER SYSTEM and restart the same
PG17 cluster. Full database restore is not the routine rollback for this feature.
