# Phases 3–4 validation — 2026-09-06

This implementation pass changed no production schema, trigger, source row or
search backend. Its only live-database operation was a bounded read-only source
export, preceded by EXPLAIN. Two small documents also went through the existing
shared embedding proxy; no inference service was restarted or reconfigured.

## Database and source fixture

- ARM64 PostgreSQL **17.11** in a separate OrbStack Compose project, with
  pgvector **0.8.2** and pg_textsearch **1.4.0** preloaded.
- The same pinned extension sources also built and loaded successfully in an
  isolated **x86_64 PostgreSQL 17.11** image. Its smoke container was removed.
  This verifies architecture/build compatibility, not installation on the live host.
- Source IDs `[41000000, 41100000)`: **100,000 rows**, **937 initially eligible
  stories**. Source text and ordinary comments were included; rare edge cases were
  constructed in the deterministic integration tests.
- EXPLAIN selected `items_pkey` with both ID bounds in its index condition. The
  COPY connection forced read-only transactions and a 30-second statement timeout.
  The binary export is temporary and is not committed as a dataset or backup.

## Correctness checks

The normal Rust regression run passed **76 tests**, with the load test and live
proxy smoke deliberately ignored by that command and run separately. Coverage
includes the existing ingestion tests as well as:

- Admission/demotion, title changes, sparse tombstones, HN dead flags, physical
  deletion, self-post URL normalization, unchanged replay, metadata-only updates,
  transaction rollback and migration down/reapply with existing FTS preserved.
- Historical seeding in chunks, preservation of completed embeddings and retry
  times, two executions of the actual backfill command, and real BM25/vector lookups.
- An HTTP barrier followed by edit/delete/demotion; stale results are discarded.
  Another test observes a PostgreSQL lock wait, commits the source edit, and checks
  that completion rechecks the newly committed source rather than its old snapshot.
- Cancellation during HTTP and resumed durable work; bad-document splitting;
  outage, timeout, overload, recipe mismatch, zero vectors and duplicate response
  indices. Global failures issue one request rather than recursively splitting.
- The actual updater process ingesting three Firebase fixture rows during a proxy
  outage, retaining two pending stories, then finishing them after process restart.

The independent live proxy smoke passed for **two documents**, including recipe
and vector validation and writes into a fresh scratch halfvec table. It makes no
claim about repeated-inference bit identity or combined retrieval quality.

`cargo doc --locked -p hn_core -p catchup_worker --no-deps` passed. Clippy completed;
strict `-D warnings` encountered existing warnings in unrelated story-tree, ingest,
listener and lineage-backfill code. Those are not silently described as a clean
strict lint run. No new-code warnings remain after correcting the shared test
module import.

## Mixed-load comparison

The final report is [the recorded JSON](search-validation/stress.json). Each half
uses a fresh database, imports the same source fixture, seeds historical stories
concurrently, and replays 50-row batches through the actual `PgBatchPersister` at
five batches/second for 150 seconds. The second half enables the real trigger.
Embeddings are deterministic varied signed integer vectors from a local HTTP fake;
both real indexes remain enabled. The workload includes ordinary comment/replay
writes, metadata changes, title edits and score threshold crossings.

The final run began after x86_64 compilation and other regression checks finished.
An earlier exploratory run overlapped compilation and is not used for the final
timing comparison. The only later code cleanup was a shared test import, an unused
result-field name and Rustdoc; none changes the measured queries or worker behavior.

| Measurement | Trigger off | Trigger on |
|---|---:|---:|
| Source writes | 37,550 | 37,550 |
| 50-row ingest batch p50 | 10.23 ms | 10.43 ms |
| 50-row ingest batch p95 | 14.83 ms | 15.31 ms |
| 50-row ingest batch p99 | 19.05 ms | 17.28 ms |
| Embedding completions | 937 | 1,048 |
| Pending at end | 0 | 0 |
| Maximum sampled pending | 0 | 1 |
| Database deadlocks | 0 | 0 |
| Maximum sampled lock waiters (30 samples) | 0 | 0 |
| Derived table + indexes | 6,012,928 bytes | 6,651,904 bytes |

Both halves sustained the requested **250 source rows/second**. Trigger-enabled
p95 was about **0.48 ms higher per 50-row batch** in this run. The lower enabled
p99 is ordinary run variability, not evidence that adding a trigger improves tail
latency. The enabled run also passed a full source/derived admission and text
consistency check after the concurrent workload. Completions include re-embedding
edited stories; they are not counts of distinct final indexed stories.

One `docker stats` snapshot per half showed **138.2 MiB / 3.88% CPU** with the
trigger off and **146.9 MiB / 5.60% CPU** with it on. These are whole scratch
PostgreSQL container samples, not peak memory, isolated trigger CPU cost or a
production sizing estimate. Cumulative container I/O counters were not attributed
to these individual runs.

## Limits and rollout decision

This fixture stresses source ingestion alongside index maintenance and embedding
completion. With only 937 initially eligible stories, it does **not** measure
full-history HNSW behavior, full-index memory/storage, filtered ANN quality or
production backfill throughput. Lock waits are sampled, so a zero sampled count
does not rule out brief waits. Fake HTTP removes GPU throughput as a bottleneck.
The one off/on sequence is a practical canary rehearsal, not a statistical claim
about a universal percentage overhead.

Production still needs extension packaging/installation for its actual host,
preload/restart, service-role grants where roles differ, bounded migration-lock
acquisition, and a small source-range canary with normal ingestion observed.
Full-history backfill and phases 5–6 remain outstanding. Keep the updater embedding
loop opt-in and the existing FTS path available throughout that rollout.

Commands and operating details are in [the search guide](search.md).

## Packaged Debian13 and restored-database rehearsal — 2026-09-06 local

Installed directly from upstream on OrbStack `searchhn-deploy-test` (Debian13.6,
amd64): `postgresql-17-pgvector=0.8.6-1.pgdg13+1` and
`pg-textsearch-postgresql-17=1.4.0-1`. Only these two extension packages were added
(plus the missing unzip utility); PostgreSQL stayed `17.11-0+deb13u1`. No PGDG APT
repository or TimescaleDB package was added. Production was not accessed.

The packages loaded successfully with pg_textsearch preloaded. A deliberately
missing preload library prevented PostgreSQL startup; restoring the saved empty
preload configuration brought it back, then enabling pg_textsearch succeeded.
The fixture's previous `conf.d/search.conf` override initially masked changes to
postgresql.conf; it was moved to `search.conf.pre-package` before the real fault
case. The final active setting is pg_textsearch in postgresql.conf.

**Platform limit:** this host's systemd failed to track PostgreSQL's PID, reporting
`Failed to check if main PID ... exists or is a zombie: Inappropriate ioctl for device`.
The server accepted connections while the unit remained activating. The stuck job
was stopped; subsequent lifecycle checks used `pg_ctlcluster --skip-systemctl-redirect`.
Thus package loading, PostgreSQL restart and configuration recovery passed, but the
normal systemd-mediated restart command is not certified by this rehearsal.
PostgreSQL is left running directly; its systemd unit is inactive. The existing
synthetic updater service was resumed and reports Healthy.

Against full restored database `searchhn_restore_20260907`:

- Diesel listed only `20260906000012` pending and applied it successfully.
- vector0.8.6 and pg_textsearch1.4.0 were enabled; all four search indexes were valid.
  The new table was initially empty, with the source trigger installed.
- The actual v0.2.0 binary (`511a6e0c77f6`) completed a bounded Firebase catchup
  against the local mock as existing restricted role catchup_worker. Story1 was
  updated to fixture text and its pending search row was created by the trigger.
  Since the restore omitted ACLs, baseline ingestion DML/sequence grants were
  supplied in the fixture; the migration itself supplied the new search grants.
- Diesel reverted the hybrid migration and reapplied it successfully. Existing
  FTS still returned results, and the extra `20260904000012` ledger entry remained.
- The current application `check --config` passed connectivity, migrations,
  ingestion/search privileges, extensions and recipe as catchup_worker. It emitted
  the expected empty-search warning after reapplication.

The eight focused lifecycle/worker integration tests also passed against these
**packaged libraries**, each in its own temporary database. The optional real
inference smoke remained skipped. Logs are retained under ignored
`infra/ansible/test-output/debian-package-tests.log` and `legacy-package-check.log`.
No full-history population or newly published worker deployment was performed.
The restored database now contains the migration and synthetic test changes;
the original backup archive remains unchanged.
