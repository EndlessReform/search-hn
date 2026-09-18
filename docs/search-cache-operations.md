# PostgreSQL search cache: diagnosis and remediation

**This procedure restarts PG17/main and disconnects all clients of the shared
cluster. It allocates 4 GiB to PostgreSQL's buffer cache and explicitly warms
approximately 2.8 GB of search data. Coordinate the outage first.** No re-embedding
or index rebuild is involved. Prewarming adds the standard `pg_prewarm` extension.

Current deployment state is in [search status](search-status.md). The September 7
configuration and warming were applied and checked; migration bookkeeping was
subsequently verified. This is a reusable procedure, not a request to repeat them.
For an ordinary restart, repeat only warming (section 4) and verification (section 5).
Measured results and limitations are in [the dated evidence](search-validation/2026-09-07/README.md).

## Why this change

A fresh hybrid query about solar panels took 423 ms in PostgreSQL, of which
403 ms was database read waiting. HNSW accounted for 402 ms total and 390 ms of
read waiting. Embedding HTTP completed separately before SQL. Both BM25 and HNSW
indexes were used; no JIT compilation or temporary-file spill occurred.

The host reported 16 GiB RAM, roughly 15 GiB of reclaimable filesystem cache,
no current memory pressure, and no observed OOM events. PostgreSQL had only
128 MB of `shared_buffers`. HNSW was 1,325 MB, BM25 59 MB, and the search table plus
indexes approximately 2.8 GB. Repeated warm hybrid SQL took roughly 16–30 ms.
These are individual diagnostic measurements, not an SLA or a benchmark.

`shared_buffers` is PostgreSQL's shared cache of database pages. Linux separately
caches file contents. A PostgreSQL miss may be served by Linux or require storage
access; measured read waits do not distinguish those layers. The small PostgreSQL
cache cannot hold this search working set alongside ingestion. Use existing RAM
before increasing the host allocation: 4 GB is 25% of the reported 16 GiB and gives
the search data room while leaving memory for Linux and other database work.

`effective_cache_size` is a planner estimate, not another memory allocation.
`track_io_timing` exposes read waits; the measured timing-loop overhead on this host
was approximately 22 ns. Keep search accuracy settings unchanged during validation.

References: [PostgreSQL 17 memory settings](https://www.postgresql.org/docs/17/runtime-config-resource.html),
[pg_prewarm](https://www.postgresql.org/docs/17/pgprewarm.html).

## 1. Install the utility through Diesel — Mac, repository root

Migration `20260907000013` owns installation; it deliberately does not warm data.
The installed `pg_prewarm.control` is not marked trusted: first installation needs
a PostgreSQL superuser, while production's `admin` is not one. Provision using the
**same migration file**, then let Diesel apply its idempotent SQL and record the
ledger normally. Do not grant superuser to the application or edit the ledger.

```bash
ssh root@searchhn-pg \
  'runuser -u postgres -- psql -X -1 -v ON_ERROR_STOP=1 -d searchhn_test' \
  < crates/hn_core/migrations/2026-09-07-000013_add_pg_prewarm/up.sql

DATABASE_URL='postgres://admin@searchhn-pg:5432/searchhn_test' \
  diesel migration list --migration-dir crates/hn_core/migrations
# On the September 7 pre-pg_prewarm baseline, only 20260907000013 was pending.
# The current host has it applied; expect no pending migrations for this source.
# Stop and reconcile any other pending migrations before the following command.
DATABASE_URL='postgres://admin@searchhn-pg:5432/searchhn_test' \
  diesel migration run --migration-dir crates/hn_core/migrations
```

This step is online and adds only extension objects. It must precede deployment
of a worker build whose migration preflight includes this new migration.

## 2. Pause updater — Mac

Coordinate other clients of this shared cluster, then:

```bash
ssh root@magi06-searchhn-worker \
  'systemctl stop catchup-worker-updater.service'
```

## 3. Configure and restart — root on searchhn-pg

The inspected cluster uses `/etc/postgresql/17/main/postgresql.conf`, which includes
`conf.d`. That directory was empty before the initial change; the tuning file now exists.
On reuse, inspect any existing tuning
first; do not overwrite a newer allocation without reconciling it with host RAM.
The following values assume the same 16 GiB host and search working set.

```bash
set -e

cat > /etc/postgresql/17/main/conf.d/90-search-cache.conf <<'CONF'
# Search working set approximately 2.8 GB; host reports 16 GiB RAM.
shared_buffers = '4GB'
# Planner estimate of PostgreSQL + OS cache; does not allocate RAM.
effective_cache_size = '12GB'
track_io_timing = on
CONF

chown postgres:postgres /etc/postgresql/17/main/conf.d/90-search-cache.conf
chmod 644 /etc/postgresql/17/main/conf.d/90-search-cache.conf

# Parse configuration before restarting.
runuser -u postgres -- /usr/lib/postgresql/17/bin/postgres \
  -D /var/lib/postgresql/17/main \
  -c config_file=/etc/postgresql/17/main/postgresql.conf \
  -C shared_buffers

pg_ctlcluster 17 main restart

runuser -u postgres -- psql -X -v ON_ERROR_STOP=1 -d searchhn_test <<'SQL'
SELECT name, setting, unit, pending_restart
FROM pg_settings
WHERE name IN ('shared_buffers', 'effective_cache_size',
               'track_io_timing', 'shared_preload_libraries')
ORDER BY name;
SQL
```

Expect `shared_buffers=524288` in 8 kB units, `effective_cache_size=1572864` in
8 kB units, timing `on`, and no pending restart. The existing `pg_textsearch`
preload must remain present. Stop and investigate any failed command.

## 4. Warm search data — Mac, repository root

```bash
ssh root@searchhn-pg \
  'runuser -u postgres -- psql -X -v ON_ERROR_STOP=1 -d searchhn_test' \
  < tools/search/prewarm.sql
```

This reads existing data into memory; it does not rebuild indexes or rewrite
stories/vectors. It temporarily generates read activity. Warming is not pinning:
other workloads can evict pages. After future PostgreSQL restarts, repeat this
block when predictable first-query latency matters. No automatic warming is
installed by this procedure.

## 5. Resume and verify — Mac, repository root

```bash
ssh root@magi06-searchhn-worker \
  'systemctl start catchup-worker-updater.service &&
   curl -fsS http://127.0.0.1:3000/health'

bash tools/search/verify-cache.sh
```

The [verification script](../tools/search/verify-cache.sh) needs Bash, curl, jq,
psql, tailnet connectivity, and the existing pgpass entry. It obtains four query
embeddings sequentially, then executes each full hybrid query three times using
`EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON)`. It checks the response recipe
against the table comment and reports embedding HTTP separately from SQL time.
It requires no existing `/tmp` files and cleans its scratch files on exit.

Defaults: `PGHOST=searchhn-pg`, `PGDATABASE=searchhn_test`,
`PGUSER=readonly_hn_agent`, and
`EMBEDDING_BASE_URL=https://magi06-inference.tail7a3eb.ts.net/embeddings/v1`.
Override these through environment variables when appropriate. The reader needs
schema access and SELECT on `story_search`, already granted during this rollout.
Do not put credentials into the script.

The [SQL](../tools/search/hybrid-query.sql) uses 100 candidates per branch,
HNSW `ef_search=1000` (set by the harness), and reciprocal-rank fusion with k=60,
dense weight 1 and lexical weight 0.125. Negative BM25 distances are matches.
It displays 10 results when run without EXPLAIN. No database data is mutated.

Acceptance: first executions after warming should avoid the previously measured
390–400 ms HNSW read waits. Compare all four queries, not just repeated identical
queries. The historical 16–30 ms warm times are a reference, not a guarantee.
`null` I/O times mean instrumentation unavailable, not zero waiting. Check updater
logs for successful embedding activity and pending work draining as well as health.

If reads remain expensive immediately after warming, inspect eviction, competing
workloads and actual memory constraints before adding RAM. If reads are cheap but
SQL remains slow, inspect the remaining plan work. Do not reduce `ef_search` as an
unmeasured latency fix; that changes retrieval recall.

## Rollback and backup scope

For a failed start after this change, run as root on `searchhn-pg`:

```bash
mv /etc/postgresql/17/main/conf.d/90-search-cache.conf \
   /etc/postgresql/17/main/conf.d/90-search-cache.conf.disabled
pg_ctlcluster 17 main restart
```

This restores the previous settings for the initially inspected configuration.
Then resume the updater with the command above. If a tuning file existed before
this procedure, restore its previous content instead. `pg_prewarm` may remain
installed; its presence alone does not run warming jobs.
The migration's down SQL also retains the extension for other consumers, matching
the existing search migration's extension policy. Neither reverting the migration
nor rolling back configuration undoes cache reads.

The [backup scope](../tools/backup/README.md) excludes PostgreSQL configuration and
extension binaries. The [verified archive](../tools/backup/RESTORE.md),
`backups/searchhn_test-20260907T005706Z/`, predates `story_search`; its full restore
into disposable `searchhn_restore_20260907` passed, but it cannot recover the newly
backfilled search data. Garage upload was not completed in that evidence.
This procedure changes configuration, adds the utility extension's schema objects,
and reads existing relations. It changes no source trigger, table data, search
index definition or existing extension version. Use configuration rollback for a
configuration failure, not a database restore. Future full archives include the
new table and utility extension definitions; configuration remains separately managed.

