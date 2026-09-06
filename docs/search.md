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

Run **one embedding consumer per deployment**. This first version intentionally
does not coordinate competing consumers. When the updater loop is enabled, use
`embedding-backfill --seed-only`; it admits historical work for that loop. Otherwise
the command itself can embed. Do not run its embedding mode concurrently with the
updater's embedding loop. The proxy's bulk admission limit is not a worker lock.

## Historical backfill

Commands do not apply migrations, call Firebase, or repair comment lineage. They
walk existing source IDs with keyset pagination and preserve finished embeddings
and retry delays on reruns. Start with an explicit small ID range:

```sh
# An updater embedding loop is already active: only admit the chosen source range.
./catchup_worker embedding-backfill --start-id 41000000 --end-id 41001000 --seed-only

# Alternatively, with no updater embedding loop: seed and embed this range.
./catchup_worker embedding-backfill --start-id 41000000 --end-id 41001000 \
  --embedding-base-url "$EMBEDDING_BASE_URL"

# After the canary is reviewed, admit all history for the existing updater loop.
./catchup_worker embedding-backfill --seed-only
```

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
