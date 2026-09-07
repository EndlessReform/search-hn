# Backup and restore evidence — 2026-09-06 (local)

Backup: `backups/searchhn_test-20260907T005706Z/`, ignored by Git, on the Mac.
Source: `searchhn-pg`, database `searchhn_test`, PostgreSQL17.11.
Archive: **9,533,619,587 bytes** (9.53 GB), custom format, compression level 1.
SHA256: `dc46739adfb7dd1717128ef329fc1a3a79553e321cc36fa3a9cd2b95f6fb081d`.

The source pg_dump completed successfully and streamed directly to the Mac.
No dump files were written on the PostgreSQL LXC. The initial archive-list step
encountered the Mac's default PG14 client; the same completed dump was inspected
successfully with `/opt/homebrew/opt/libpq@17/bin/pg_restore`, then hashed/published.
The backup command now checks client major versions before dumping; rejection of
PG14 against source PG17 was exercised successfully without starting another dump.

Restored on the disposable Debian13/amd64 host `searchhn-deploy-test`, PostgreSQL17.11,
into a **new** `searchhn_restore_20260907` database created from template0.
The copied archive's SHA256 matched. `pg_restore --exit-on-error --jobs=4
--no-owner --no-privileges` completed with exit code 0. Existing fixture databases and
production were not modified. The restored database is retained for subsequent
migration/package rehearsal.

| Table | Restored rows |
| --- | ---: |
| items | 49,543,995 |
| kids | 43,234,856 |
| ingest_segments | 10,584 |
| ingest_exceptions | 763 |
| ingest_dlq_items | 513 |
| updater_state | 1 |
| users | 0 |
| __diesel_schema_migrations | 14 |

Verification (`verify.sql`) passed: no invalid/unready public indexes; all 21
reported constraints validated, including the ingest-exceptions foreign key;
existing full-text search returned results. The migration ledger preserves the
production-only `20260904000012` entry. `story_search` is absent, as expected before
the hybrid migration. These counts describe the restored snapshot, not a comparison
against the subsequently changing live source.

The archive includes ownership/ACLs; the test restore deliberately omitted applying
them. This verifies data/schema recovery, not production credential/role recovery.
Full dump logs, archive contents, checksum, manifest and verification output are
retained alongside the local archive. **Garage upload has not run**: bucket/prefix
and credential profile are still pending. The optional upload path is not claimed
as tested. No retention or backup deletion was performed.
