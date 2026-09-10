# Search: current state and next slice

Last checked: **2026-09-07**. This is the entry point for operational state and open
work; dated evidence is a snapshot, not permission to change production. Recheck
live state before a later deployment. Branch at handoff: `fts-improvement-spike`.

## What is running

| Surface | Last verified state |
| --- | --- |
| Database | `searchhn-pg`, `searchhn_test` **is production despite its name**, PostgreSQL 17.11 |
| Search extensions | vector 0.8.6, pg_textsearch 1.4.0; pg_textsearch preloaded; pg_prewarm 1.2 |
| Migration ledger | `20260906000012` and `20260907000013` applied; extra historical `20260904000012` retained |
| Worker | `magi06-searchhn-worker`, `catchup-worker-updater.service`, v0.3.1 / `841aa2a8d599` |
| Active deployment | `/opt/search-hn/deployments/v0.3.1-dafb9e633c1f`, via `/opt/search-hn/current` |
| Configuration | deployed `worker.toml`: embeddings enabled, seven-day startup replay; local source `infra/ansible/worker.local.toml`, inventory `infra/ansible/hosts.yml` (ignored) |
| Credentials | existing `/etc/search-hn/catchup-worker.env`; do not read/print/copy into documentation |
| Embedding API | `https://magi06-inference.tail7a3eb.ts.net/embeddings/v1`; contract at `/embeddings/usage.md` on the same host |
| Search reader | `readonly_hn_agent` has SELECT on `story_search` and `items`; live vector/BM25 queries passed |
| Cache | 4 GB shared_buffers, 12 GB effective_cache_size estimate, track_io_timing on; host reports 16 GiB RAM |

Historical embedding backfill completed **01:36:47 Central, September 7**, pending
and due both zero. Updater was restarted afterward to activate its embedding loop.
The later cache-maintenance restart also recovered successfully. Last measured
count: 509,204 embedded, zero pending; ordinary ingestion had zero realtime failures.
Counts change with ingestion. **Do not repeat historical backfill for the application slice.**

A process started against an empty search table skips embeddings for its lifetime,
even if TOML says enabled. After backfill, restart that process; healthy ingestion
alone does not prove embeddings run. This is a startup guard, not a recurring step.

## What is implemented versus still planned

Storage, triggers, historical population, continuous embedding, reader access, and
one-off hybrid SQL are live. **The existing Textual agent now defaults locally to
production hybrid retrieval**, using a shared Python repository over `story_search`.
The user selected direct database access for the agent slice. An initial Axum
search endpoint and Rust HTML search page are now implemented and verified locally,
but not deployed; filters and agent HTTP cutover remain deferred. See
[app validation](search-validation/2026-09-07/app-search.md). The local UI now includes experimental freshness/votes sliders;
[four-reviewer tuning results](search-validation/2026-09-09/tuning-review/README.md)
retain the neutral 0/0 default and identify remaining topic/date-quality work. Headless and FastAPI share the Python
repository. No service deployment/restart or schema change was performed.
See [agent usage](../packages/search-agent/README.md#production-hybrid-search-textual-default)
and [dated application evidence](search-validation/2026-09-07/README.md#textual-agent-direct-hybrid-integration).
Legacy `--retrieval fts` remains selectable. Historical research `dense`/`hybrid`
backends retain their original frozen-table implementation.

The branch now includes merged main/PR #18 (`6333e07`), restoring the provider/model
picker, presets, prompt history and webpage approval workflow alongside hybrid
retrieval. Provider changes use the selected per-run client. The hybrid changes
remain uncommitted locally; see [merge validation](search-validation/2026-09-07/agent-main-sync.md).

Next shared-server application slice should start with:

1. [Production design, phases 5–6](../packages/search-research/docs/production-design.md):
   extend the initial Axum query handler toward complete agent parity. Put the embedding endpoint
   in application configuration; do not copy the smoke tool's deployment default
   into runtime code. Avoid independently implementing hybrid fusion in each UI.
2. [Hybrid SQL prototype](../tools/search/hybrid-query.sql): 100 candidates/branch,
   RRF k=60, vector weight 1, BM25 weight 0.125; ef_search=1000 in its harness.
   Validate recipe, filters, pagination and quality before backend cutover. The
   smoke query has no filters/pagination and is not a production handler. The
   Python agent now implements filters, short-lived ID pagination, live source
   eligibility checks and labelled BM25 fallback. Explicit filters use exact
   cosine to avoid ANN starvation; measured filtered latency remains seconds.
3. [Measured query evidence](search-validation/2026-09-07/README.md): after warming,
   four queries 32–38 ms SQL, a fresh query 69 ms including 4 ms reads. Embedding HTTP
   is separate. These do not establish an end-to-end or sustained-workload SLA.

## Open work, deliberately separate

- **Deployment fixes:** the active incomplete-staging guard is now fixed and its
  five regression cases pass. The release builder includes the app, and a separate
  app installer passed a synthetic install/idempotence/rollback rehearsal. See
  [September 9 evidence](search-validation/2026-09-09/release-app.md). These changes
  are local, not published/deployed. Earlier uncommitted Ansible files fix root/runuser execution,
  source-config checks and partial-stage retries. Preserve them; review the active
  deployment edge case and rerun focused rehearsal before calling them certified.
  See [worktree audit](worktree-audit-2026-09-07.md) for the earlier validation scope.
- **Batch-size divergence:** [audit](embedding-batch-audit.md) records production
  default 4 / maximum 8 versus research 64. Worker/shared client/proxy caps remain.
  No tuning decision or deployment is implied by the audit. Do not re-embed history.
- **Backup:** the verified full archive predates the new search table. Take a new
  full backup and test a fresh disposable restore if recovery of completed vectors
  is required; Garage destination/upload and retention remain unconfirmed. Preserve
  the old verified archive. See [backup evidence](../tools/backup/RESTORE.md).
- **Warming:** manual after PG restart; the script now includes TOAST and its index.
  No automatic warming installed. Inspect sustained eviction only if observed.
- **Reader grant durability:** the live SELECT grant was applied manually; migration
  12 grants the worker only. Define the application's reader role/provisioning in
  the application slice instead of assuming every fresh database has that grant.

## Where to look

- [Cache configuration, warming and rollback](search-cache-operations.md)
- [Worker install/rollback](../infra/ansible/README.md) and [release publishing](../tools/release/README.md)
- [First rollout](search-rollout.md): historical initial-install procedure, not today's checklist
- [Search implementation](search.md): source/derived data and worker semantics
- [Deployment decisions](deployment-decisions.md): dated rationale, with superseded proposals retained
- [Verification tools](../tools/search/README.md), [evidence](search-validation/2026-09-07/README.md),
  and [worktree audit / cleanup recommendations](worktree-audit-2026-09-07.md)
