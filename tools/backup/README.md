# Database backup and restore

Run from a receiving machine with uv, SSH, and a pg_restore client at least as new
as the source pg_dump. Dumps stream over SSH; nothing is staged on the DB LXC.

```bash
./scripts/backup --directory backups
# To also upload using an existing rclone Garage remote:
./scripts/backup --directory backups --garage REMOTE:BUCKET/PREFIX
```

On this Mac, pass `--pg-restore /opt/homebrew/opt/libpq@17/bin/pg_restore` because
the default PATH currently selects PostgreSQL14.

A backup contains the whole `searchhn_test` database: all table data, schema,
functions, indexes, sequences, ownership and grants. It excludes other databases,
cluster roles/passwords, PostgreSQL configuration and extension binary packages.
Keep role provisioning and extension installation separate from database restore.

The command writes an isolated `.partial` directory, checks pg_dump succeeded,
lists the archive, computes SHA256, then renames the directory. Failed runs are
left visibly incomplete. `manifest.json` initially says `restore_verified: false`.
When Garage is requested, rclone copies the directory and compares remote bytes
using `check --download`; failures return nonzero and retain the local backup.
Retry a failed upload with ordinary `rclone copy` and `rclone check --download`
against the existing backup directory; there is no need to dump the source again.
No automatic retention/deletion or scheduling is introduced.

## Scope for the initial hybrid migration

Migration `20260906000012` changes:

- `items`: attaches the source synchronization trigger and grants access; source
  rows are not rewritten by the migration. The updater later refreshes rows.
- `__diesel_schema_migrations`: migration bookkeeping and read access.
- `story_search`: new table, indexes, functions and grants. It is absent before
  rollout, so there is no pre-change search data to back up.
- `public`: schema access, plus database-level extension objects.

The full dump also includes `kids`, `users`, `ingest_segments`, `ingest_exceptions`,
`ingest_dlq_items`, and `updater_state`, including dependencies needed for a complete
restore. Backing up only selected tables would omit standalone functions and other
schema dependencies. It is not worth maintaining a fragile partial-backup recipe.

## Restore rehearsal

Use a **new** database on `searchhn-deploy-test@orb`. Copy the archive to that host,
verify its SHA256 there, create a fresh database from template0, and run:

```bash
# On the disposable test host, with an explicitly created empty target database:
sudo -u postgres pg_restore --exit-on-error --no-owner --no-privileges \
  --dbname=searchhn_restore_YYYYMMDD /tmp/RESTORE_DIRECTORY/database.dump
```

`--no-owner --no-privileges` avoids importing production access policy into the
fixture. This proves schema/data/index recovery, not production login restoration.
Never use `--clean` against an existing database. Successful restoration includes
index and constraint creation. Verify restored table counts, migration ledger,
source triggers and foreign keys, then record the result and archive identity. `verify.sql` contains the read-only
checks used for this rehearsal.
A live source changes during backup: later source counts are not an exact reference
for the dump's consistent snapshot. Do not falsely claim a byte-for-byte comparison
with a later production query.

See `RESTORE.md` for the actual run's evidence once completed. The dump is a
snapshot at backup start; it does not recover subsequent writes and is not PITR.
