# Search operational tools

Start with [current status](../../docs/search-status.md) and
[cache operations](../../docs/search-cache-operations.md). These are one-off
operational diagnostics, not the application's search API.

- `prewarm.sql`: repeatable main table/index and TOAST/index warming after PostgreSQL
  restart. Requires the pg_prewarm migration. Generates read I/O; does not pin pages.
- `verify-cache.sh`: four sequential interactive embedding requests and three
  read-only full hybrid EXPLAIN executions per query. Prints HTTP and SQL timings
  separately; uses a fresh scratch directory and needs no old `/tmp` files.
- `hybrid-query.sql`: shared query prototype used by the profiler. Bind psql
  `query_vector` and `query_text`; set `hnsw.ef_search=1000` in the transaction.
  The harness validates the recipe before using this SQL. It is not a standalone
  complete client and provides neither filters nor pagination.

From repository root:

```bash
bash tools/search/verify-cache.sh
```

Requires Bash, curl, jq, psql, tailnet access and pgpass. Defaults are
`PGHOST=searchhn-pg`, `PGDATABASE=searchhn_test`, `PGUSER=readonly_hn_agent` and
`EMBEDDING_BASE_URL=https://magi06-inference.tail7a3eb.ts.net/embeddings/v1`.
Override through environment variables; never embed credentials. Missing I/O timing
appears as `null`, not zero. The script performs no deployment or schema changes.

Keep small dated results under `docs/search-validation/YYYY-MM-DD/`. For important
profiles preserve full EXPLAIN JSON with query, settings, cache state and provenance.
The current script prints summaries and removes scratch plans; capture additional
full plans explicitly when investigating a regression. Do not treat repeated-query
warm timings as evidence of performance for an unseen query or concurrent load.

- `validate-agent.py`: read-only checks of the Textual agent's direct production
  backend, including proxy contract, approved SQL/RRF agreement, filters, three
  pages and a simulated inference outage. Requires `DATABASE_URL` and
  `EMBEDDING_BASE_URL`; run with `uv run --package search-agent python`.
  `--output PATH` records results/timings; optional `--plans PATH` records complete
  EXPLAIN JSON without executing the searches again. Output contains public HN
  results and query vectors in plans, never connection credentials. See the
  [agent README](../../packages/search-agent/README.md) for examples.
