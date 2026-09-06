# Isolated search PostgreSQL

This PG17.11 scratch image builds pgvector **0.8.2** and pg_textsearch **1.4.0**
(commit `7a932505b537d50ad8d2d068a053cd1dd7b646ea`). It preloads pg_textsearch and
publishes only loopback port 55437. It is not a production database deployment.
The source build works on ARM64 and can be checked on x86_64 with
`docker build --platform linux/amd64 -t searchhn-postgres-test:17.11-amd64 .`.

From the repository root:

```sh
docker compose -p searchhn-hybrid-test -f deploy/search-postgres/compose.yaml up -d --build --wait
export TEST_SEARCH_DATABASE_URL=postgres://postgres:scratch-only@127.0.0.1:55437/searchhn_scratch
cd crates
cargo test --locked -p hn_core -p catchup_worker --lib --tests
```

Process and search integration tests create a fresh database per test, apply the
canonical migrations, and drop only that database afterward. They require the
explicit local `TEST_SEARCH_DATABASE_URL`; they never fall back to `DATABASE_URL`.
The old process tests now share this harness instead of starting whichever native
Postgres happens to be on PATH. SQLite tests remain useful for their existing scope;
there is no imitation of BM25, halfvec or triggers in SQLite.

The Compose database uses a disposable Docker volume. After testing, remove only
this project's containers and volumes:

```sh
docker compose -p searchhn-hybrid-test -f deploy/search-postgres/compose.yaml down -v
```

## Production prerequisites, not performed by this setup

Install these extension versions for the actual PostgreSQL 17 x86_64 installation.
Build with that installation's `pg_config` and matching server development headers;
the scratch image's libraries are not a universal package for another host OS.
Preserve existing `shared_preload_libraries` entries when adding `pg_textsearch` and
schedule the required PostgreSQL restart. Verify extension availability before
applying the migration. Nothing in this directory connects to production.
The migration rejects already-installed versions that differ from the verified
pair. If migration and ingestion use different database roles, grant the ingestion
role SELECT/INSERT/UPDATE/DELETE on `story_search` in the rollout transaction before
committing the trigger; also grant the embedding role SELECT on source/derived data
and UPDATE on source (for row locks) and derived data. The default function EXECUTE
privileges must be retained or explicitly granted. Test those actual service roles
in the production canary; the scratch tests use their database owner.

The additive migration creates an empty `story_search` table and its indexes,
then attaches the source trigger. It does not scan or rewrite `items`, populate
history, or replace FTS. Trigger creation still needs a table lock; the migration
uses a five-second lock timeout so it fails instead of waiting indefinitely behind
ingestion. Schedule and retry that operation deliberately. A failed transactional
migration rolls back its changes. The down migration removes the derived search
objects but leaves extensions installed for other consumers.

See [the search guide](../../docs/search.md) for admission, worker/backfill behavior,
integration tests and rollout steps.
