# Project status and next steps

Audited September 17, 2026. **Recommendation: merge `fts-improvement-spike`;
finish the app deployment separately.** No unresolved merge blocker was found
in the recorded history, focused code review, or checks below. This is a practical
merge-readiness audit, not an exhaustive review of every research artifact.

## Where we actually stopped

- `main` and GitHub `main`: `2511d23673918944a2ed1461ca7639a46248c662` (PR #18).
- `fts-improvement-spike`: `1b8cba725084a212dc26008f323f7199cac292ae`
  ("Search app," September 9), 33 commits ahead and zero behind main.
  Main was already merged into it in `6333e07`; there is no outstanding main
  reconciliation. GitHub has no corresponding branch, PR, or issue.
- **The code is preserved remotely:** published release
  [v0.4.0](https://github.com/EndlessReform/search-hn/releases/tag/v0.4.0)
  points to `d2e62af1b073a9280e00b3a37a62568c49eec417`, whose parent is the
  spike tip. The release commit changes only Cargo version/lockfile entries.
  Its manifest reports a passing Debian amd64 release build and identifies both
  the worker and `hn_app` binaries. Published September 9 Central time.
- **The app upgrade was attempted and rolled back.** A later status update,
  saved in the September 15 stash, says the v0.4.0 hybrid check failed because
  `EMBEDDING_BASE_URL` was missing from the app environment. Requests to inference
  as `catchup` succeeded. Configuration and retry were left pending.
- September 15's checkout back to main stashed that update. This explains why
  the branch's status page still says the app release was not published/deployed.
  That wording is stale: publication succeeded; activation failed and rolled back.

The missing endpoint is an **app deployment prerequisite**, not an indexing bug
or merge blocker. The installer rejected keyword-only operation as designed.
The audit did not re-inspect the current app environment or change production.

## The three requested pieces

| Piece | Implemented and deployed state |
| --- | --- |
| Story index | `story_search`: title + URL embeddings for eligible stories with score >=25; 1024-coordinate halfvec storage, HNSW cosine index and title BM25. Source triggers admit, invalidate and remove derived rows. |
| Model service | Pinned `pplx-embed-v1-0.6b` through vLLM, Rust embedding proxy and Caddy, on `magi06-inference`. |
| Updater | Supervised embedding loop in `catchup_worker updater`, plus rerunnable `embedding-backfill`. Historical population and loop activation were completed September 7. |

Fresh read-only checks in this audit session: **510,233 embedded, zero pending,
zero due**; vector 0.8.6, pg_textsearch 1.4.0 and pg_prewarm 1.2 installed;
the inference `/embeddings/readyz` endpoint returned `ready`. Counts are a snapshot.
The worker process itself was not inspected. Do not rerun the historical backfill
as part of merging or deploying the app.

## Next actions, in order

1. **Land the branch.** Publish `fts-improvement-spike`, open a PR to main and
   merge after review. No need to wait for better search ranking, a new backfill,
   or a fresh-host deployment framework. Merging does not deploy the services.
2. **Recover the small stashed follow-up deliberately.** Stash commit
   `a4e597e` (September 15; currently `stash@{0}`) contains four files:
   `.githooks/pre-commit`, `AGENTS.md`, `docs/search-status.md`, and
   `infra/ansible/README.md`. It includes terminology preferences and the app
   configuration fix instructions. Inspect/apply it on the spike branch; don't
   blindly pop all stashes onto main. Keep this project status page as the
   concise entry point and reconcile the old status page's stale claims.
3. **Complete app activation as a separate deployment task.** First verify that
   the existing app environment still lacks the setting. Preserve its database
   credentials and permissions while configuring:

   ```dotenv
   EMBEDDING_BASE_URL=https://magi06-inference.tail7a3eb.ts.net/embeddings/v1
   ```

   The stashed instructions identify `/etc/search-hn/hn-app.env`. Verify the
   real inventory selects the app host, then retry the existing app installer
   with `release_version=v0.4.0`. Its checks should confirm binary identity,
   health and hybrid retrieval. No new release is needed just to fix this setting.
4. **Later:** improve entity/date relevance and Rust/Python search parity;
   make reader-role provisioning repeatable; refresh and restore-test a full
   backup containing the populated search table. Keep neutral boost defaults.
   These are follow-ups, not gates on recording the working implementation in main.

## What was checked

Tests ran against an exported copy of the exact spike commit, leaving the current
checkout on main and leaving every stash intact.

| Check | Result |
| --- | --- |
| Rust workspace library and binary tests, `cargo test --locked --offline --workspace --lib --bins` | **89 passed** across worker, proxy, app and core. |
| Agent tests with locked UV environment | **130 passed, 2 skipped** (optional live DB tests). |
| Release tooling tests with locked UV environment | **8 passed**. |
| Local Ansible staging-guard regression | **All five cases passed**, including rejecting an active candidate missing either manifest or checksums. |
| GitHub PRs/issues, release manifest and ancestry | No recorded merge hold; v0.4.0 build and source identity confirmed. |
| Focused review | Search trigger/conditional completion, worker failure handling, app retrieval, release packaging, staging protection and app rollback inspected. |

Initial test setup problems were environmental: the sandbox prohibited mock
listener ports, and the first Python command omitted the release package's
dependencies. Correctly provisioned, permitted runs passed. No production writes,
deployments, model changes, or new historical embedding work were performed.

The isolated PostgreSQL lifecycle suite and full worker rollback rehearsal were
not rerun; dated branch records report those earlier runs. The broader worker
credential/rollback rehearsal remains a gate before a future worker deployment,
not before this merge. The previously recorded active-incomplete-staging bug is
fixed in the branch, and its regression was rerun here. Whitespace-only findings
were not treated as merge blockers.

## Recovering the older records

These files live on the spike branch (and v0.4.0), so they are absent from main
until merged. Read them with `git show fts-improvement-spike:<path>`:

- `docs/search-status.md`: implementation state; some publication and
  "uncommitted" language predates the September 9 commit and release.
- `docs/search-validation/2026-09-09/release-app.md`: app installer and guard tests.
- `docs/worktree-audit-2026-09-07.md`: earlier findings, partly superseded.
- `docs/search-validation/2026-09-09/tuning-review/README.md`: ranking follow-ups.
- `infra/ansible/README.md`: deployment procedure; the September 15 stash adds
  the missing endpoint setup instructions.

The stashed status update links to `app-deployment-diagnosis.md`, but that file
is absent from the stash and committed branch. Do not make it a prerequisite:
the short diagnosis survives in the stashed status/Ansible diffs and is summarized
above. Use `git show a4e597e --` or `git diff a4e597e^1 a4e597e --` to inspect
that saved work. Do not delete the older safety stashes, backups or real inventory.
