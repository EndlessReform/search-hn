# Hybrid search indexing

Phases 3–4 implement derived search storage and document embedding. They are tested
on isolated PostgreSQL; production extension installation, migration and historical
backfill are separate rollout steps. The existing FTS search remains unchanged.
The Axum hybrid endpoint and web/agent cutover are phases 5–6.

## Dataflow and admission

`items` remains authoritative. An AFTER trigger synchronizes one `story_search` row
for each story with score >=25, a nonblank title, positive Unix timestamp with a
valid generated calendar day, and neither HN `dead` nor `deleted`. NULL HN flags
mean false. Self-posts use an empty URL. The document is exactly `title + "\n" + url`.

`story_search` owns copied title/URL, nullable `halfvec(1024)` embedding and
`retry_after`. NULL embedding means unfinished work. Indexes are English title BM25
(k1=1.2, b=0.75), cosine HNSW (m=16, ef_construction=128), and a partial due-work
index on `(retry_after, story_id)`. Query-time ef_search belongs to the later API.

The trigger calls `sync_story_search(items)`. Backfill locks a short chunk of current
source rows and calls the same function. Do not call it with previously cached
source records: the caller must hold the source row lock before reading its input.
Replay of unchanged fields, eligible score changes and comment-count changes do
not replace the derived tuple or clear its embedding. A title/URL edit clears the
vector and resets its retry time. Demotion or HN deletion removes the derived row;
a sparse tombstone is recognized using OLD.type. Physical source deletion cascades.
Comments and comment-lineage repairs do no search synchronization.

The table comment holds the supported embedding recipe. The client reads it when
starting a loop and checks both response body and header against it. It also checks
model, count, response indices, dimensions, signed integer range and nonzero norm.
Only validated coordinates can pass through the Rust storage writer. They are
already transformed by the proxy; there is no normalization or second quantization.
Recipe mismatch pauses the loop through its ordinary error/restart delay and logs
the mismatch. Changing models requires an explicit index rebuild and service restart;
editing the comment alone is not a model migration.

## Updater operation

Production deployments use a single TOML file (see
[worker.example.toml](../infra/ansible/worker.example.toml)):

```sh
./catchup_worker check --config /etc/search-hn/worker.toml
./catchup_worker updater --config /etc/search-hn/worker.toml
```

The check uses application credentials and read-only, time-bounded PostgreSQL
queries. It does not migrate, fetch Firebase, or request embeddings. TOML rejects
unknown fields and supplies an explicit `[embedding] enabled` setting; configuration
mode does not load `.env` or fall back to endpoint/database environment variables.
Legacy CLI/environment invocations below remain supported for existing deployments.

Startup behavior: if embedding is enabled but
`story_search` is empty, warn that initial population is required and skip the
embedding loop for that process. Keep source ingestion/replay running. Complete
population and restart to enable embedding; do not silently activate it when a
later source write creates the first search row. The skipped loop makes no writes;
ordinary source-trigger synchronization remains active. An empty-table check is
not a certificate that a nonempty table has been fully backfilled.

Without an embedding URL, ingestion runs as before. Enable the loop explicitly:

```sh
export EMBEDDING_BASE_URL=https://magi06-inference.tail7a3eb.ts.net/embeddings/v1
./catchup_worker updater --embedding-batch-size 4
```

`--embedding-base-url` overrides the environment variable. Both updater and
`embedding-backfill` accept:

| Option | Default | Meaning |
|---|---:|---|
| `--embedding-batch-size` | 4 | Inputs per request, 1–8 |
| `--embedding-poll-seconds` | 5 | Idle poll interval |
| `--embedding-retry-seconds` | 60 | General failure delay and supervisor restart delay |
| `--embedding-invalid-retry-seconds` | 3600 | Delay for an isolated invalid document |
| `--embedding-timeout-seconds` | 40 | End-to-end client request timeout |

The loop sends one bulk request at a time. It releases the database connection
before HTTP, then briefly locks the source before updating a still-pending derived
row with the same title and URL. It never inserts on completion. The same guard
protects retry scheduling so a failure for old text cannot delay newly edited text.
If ingestion updates a locked source first, completion rechecks the committed row.
Both paths lock source before derived data to avoid inverted lock order.

Input errors split a batch until the bad document is isolated; good documents
finish and the failing story ID is logged and delayed. Byte-limit failures are
detected before HTTP; token limits come from the proxy. Availability, timeout,
malformed response and recipe errors do not split: they delay unfinished members
and pause the loop globally. Numeric `Retry-After` seconds extend the pause for
overload. Unexpected task exits/panics are supervised and restarted without
cancelling ingestion. Shutdown cancels the child; an unfinished request remains
pending and can be retried after restart. No durable queue or lease table is needed.

The updater owns its background embedding loop. A separate `embedding-backfill`
invocation can also embed; overlapping requests are acceptable at this workload.
Conditional completion preserves the first saved embedding and rejects obsolete
results. `--seed-only` is an optional way to populate without inference, not a
correctness requirement or an exclusive-worker protocol.

## Historical backfill

The first rollout order is **migration → one-off historical search population →
embedding-enabled updater with seven-day Firebase startup replay**. Historical
population includes eligible stories older than that replay window. Later releases
do not need another all-history population run.

`catchup_worker updater` runs Firebase ingestion/replay and the embedding loop in
one process. `catchup_worker embedding-backfill` is a separate invocation of that
same executable: it reads PostgreSQL, not Firebase. It does not apply migrations
or repair comment lineage. It walks existing source IDs with keyset pagination
and preserves finished embeddings and retry delays on reruns.

Current CLI examples (assuming database and endpoint configuration is supplied):

```sh
# After migration, populate all historical search rows before updater startup.
# This variant leaves inference to the updater; omit --seed-only to also embed.
./catchup_worker embedding-backfill --seed-only

# After successful population, start ingestion/replay and embedding together.
./catchup_worker updater --startup-rescan-days 7 \
  --embedding-base-url "$EMBEDDING_BASE_URL"
```

With TOML, the one-off population command is
`catchup_worker embedding-backfill --config /etc/search-hn/worker.toml --seed-only`.
It reuses database/embedding settings; explicit diagnostic range flags remain available.

Seven days is explicit here: the current startup default is three days, and the
separate stale-stream recovery default is two. This rollout does not change replay
anchoring or introduce another replay mechanism. Firebase reports creation time,
not item modification time, so having a local row does not prove it is fresh.
Replay re-fetches source data; crossing score >=25 admits a story, falling below
25 or becoming ineligible removes it, and title/URL edits invalidate embeddings.
Unchanged text retains its embedding. Initial population has already created
eligible search rows, so unchanged replay need not repair an empty search table.

Explicit `--start-id`/`--end-id` ranges remain useful for tests and diagnosis, not
as routine deployment inputs. No persistent backfill cursor or automatic
all-history scan on updater startup is required.

ID bounds are inclusive. `--source-chunk-size` defaults to 1000 (maximum 10000).
Each chunk selects only stories and locks them for its local synchronization statement. Neither
command keeps a source lock during inference. A restart rewalks the requested
source range safely; there is no extra checkpoint table. Range flags constrain the
command, not a separately running updater loop, which consumes all due work.

Embedding mode exits 0 if its range has no pending work, 3 if only deferred work
remains, and 1 on failure. Seed-only exits 0 after successful admission even though
embeddings remain pending. This is deliberately a finite command, not an infinite
wait for a permanently invalid document. Rerun later or leave the updater enabled.

## Logs and troubleshooting

Existing tracing/Loki collection receives `embedding_batch_finished`,
`embedding_invalid_document`, `embedding_batch_failed`, `embedding_loop_exited`,
`embedding_progress`, and backfill seed/finish events. Progress includes pending
and due counts about once per minute; errors include story IDs for isolated inputs.

```sql
SELECT count(*) AS pending,
       count(*) FILTER (WHERE retry_after <= now()) AS due
FROM story_search WHERE embedding IS NULL;

SELECT story_id, retry_after FROM story_search
WHERE embedding IS NULL ORDER BY retry_after, story_id LIMIT 20;
```

An absent table/comment or unavailable extension is a migration/deployment issue.
A recipe mismatch requires checking deployment compatibility, not disabling the
check. Repeated invalid inputs require inspecting the logged source story. Text is
never silently shortened. An endpoint outage should leave ingestion healthy and
pending work intact. There is no automatic fallback to raw vLLM.

## Focused integration tests

Use the [scratch PG17 setup](../deploy/search-postgres/README.md). The normal Rust
suite tests lifecycle/rollback, concurrent source locking, stale completion,
backfill reruns, HTTP input isolation and global failure handling, cancellation and
supervisor recovery. Existing real-updater/Firebase process tests also run on the
same extension-capable server, in separate disposable databases.

Two explicit checks are excluded from the normal suite:

```sh
# From crates/, with TEST_SEARCH_DATABASE_URL set to the local scratch server.
TEST_EMBEDDING_BASE_URL=https://magi06-inference.tail7a3eb.ts.net/embeddings/v1 \
  cargo test --locked -p catchup_worker --test search_worker live_proxy_smoke -- --ignored

TEST_SEARCH_SLICE=/tmp/searchhn-hybrid-slice.bin \
TEST_SEARCH_STRESS_REPORT=/tmp/searchhn-stress.json \
  cargo test --locked -p catchup_worker --test search_stress -- --ignored --nocapture
```

The smoke uses two inputs through the real proxy and writes only to scratch PG.
The load test imports an explicitly supplied binary COPY slice, runs two fresh
databases with trigger off/on for 150 seconds each, and replays 50-row batches at
five batches/second through `PgBatchPersister`. It concurrently seeds history and
embeds with deterministic varied 1024-coordinate fake vectors. It includes title
edits and threshold crossings, then checks source/search consistency. The report
records batch latency, progress, sampled lock waits, deadlocks and derived size.
`TEST_SEARCH_STRESS_SECONDS` can shorten each phase for development; use the default
for the recorded comparison. This is not a full-history HNSW or retrieval benchmark.

To reproduce the input, obtain a bounded, read-only export after inspecting its
plan. The tested range is `41000000 <= id < 41100000`; retain source columns in this
exact order for binary COPY:

```sql
COPY (SELECT id, deleted, type, "by", time, text, dead, parent, poll, url,
             score, title, parts, descendants
      FROM items WHERE id >= 41000000 AND id < 41100000 ORDER BY id)
TO STDOUT WITH (FORMAT binary);
```

The fixture is a temporary export, not a committed backup or a presumed-complete
research dataset. See [validation evidence](search-validation.md) for measured
results and limits.
