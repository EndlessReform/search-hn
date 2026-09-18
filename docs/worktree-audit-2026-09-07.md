# Worktree and documentation audit — September 7, 2026

**The deployment is ahead of the committed repository documentation.** At audit
start the branch was `fts-improvement-spike`, HEAD `a44c788`. Three Ansible fixes
were modified but uncommitted, and the batch audit, cache guide, pg_prewarm
migration and operational tools were untracked. They must be included in reviewed
commits before another checkout can reconstruct this work. No commits or artifact
deletions were performed by this audit.

The audit read every initially modified source file, the new migration/tools/docs,
README and operational entry points, backup/restore evidence, and the overnight
handoff. It inventoried matching local `/tmp/searchhn-*` and `/tmp/search-hn-*`
artifacts by name/size; it did not indiscriminately read old configuration files,
logs, caches, or bulk data. Older research scratch files are recommendations for
owner review, not claims that their contents were audited or are safe to publish.
A live read-only ledger check confirmed `20260907000013` is now applied.

## Findings and disposition

| Finding | Consequence | Disposition |
| --- | --- | --- |
| Deployment/proposal docs still say extensions/backfill are pending | A successor could rerun initial maintenance or misunderstand application readiness | Added current status; historical banners on rollout, decisions and research design; updated implementation guide |
| Cache guide said changes unverified and migration pending at the top while later text said otherwise | No reliable answer to “what is applied?” | Separated dated results into evidence; current status confirms both migrations |
| Useful full EXPLAIN plans existed only in `/tmp` | The I/O diagnosis would become unverifiable after cleanup | Preserved five exact JSON plans and checksums under dated evidence |
| Example Ansible inventory selected old canary with incompatible TOML credentials | Copying the example could select the wrong contract | Example now names v0.3.1 and explains the old canary incompatibility |
| Root execution fixes and partial-stage retry changes lacked a final full rehearsal | Syntax passing does not establish rollback/idempotence correctness | Preserve code, record validation gap, review/test separately before certifying |
| Live read-only search grant is not in migration 12 | A fresh application deployment can lack reader access | Recorded as application-role provisioning work; no assumption that live grants define new environments |
| Verified backup predates search table; Garage incomplete in evidence | Existing archive cannot recover completed vectors | Keep archive, plan a new full backup plus fresh disposable restore; do not relabel exports as backups |

### Code review item: active incomplete staging

In `infra/ansible/tasks/stage.yml`, `staging_complete` requires both manifest and
installed checksums. However, the active-deployment refusal runs only when the
checksum file is missing. If the active directory has checksums but its manifest
is absent, `staging_complete` is false and unarchive can run into the active path.
This contradicts the new protection's intended scope. Check the active target for
**every incomplete state**, then rehearse both missing-manifest and missing-checksum
cases alongside inactive partial-stage retry. This is a static finding; no live
corruption was observed or induced. Source behavior was not changed in this audit.

The earlier deployment rehearsal validated an older credential contract. Its
addendum explicitly says the full rehearsal was not repeated after credential
separation. September 7 production success establishes the exercised normal path,
not every failure/rollback path for the modified playbooks. Install and rollback
syntax checks passed again during this audit; full rehearsal remains outstanding.

## Permanent homes and proposed commit groups

Keep the existing flat docs layout; add an entry point rather than moving every
historical document and breaking references.

| Group | Files/home | What belongs here |
| --- | --- | --- |
| Worker deployment fixes | `infra/ansible/install.yml`, `tasks/activate.yml`, `tasks/stage.yml`; accompanying README/example and rehearsal evidence | Root/runuser checks, early TOML validation, incomplete staging; address review item before certification |
| Cache operations | migration `2026-09-07-000013_add_pg_prewarm`, `tools/search/`, `docs/search-cache-operations.md` | Durable extension install, repeatable warming including TOAST, query diagnostic source, rollback |
| Production evidence and navigation | `docs/search-status.md`, `docs/search-validation/2026-09-07/`, README/AGENTS links and stale-document notices | Last verified state, raw profiles, dates/limitations and routes to canonical procedures |
| Unresolved batching audit | `docs/embedding-batch-audit.md`, linked from current status | Preserve findings separately from any future tuning implementation; no implied authorization for batch 64 |

These are proposed review/commit boundaries, not commits already made. Add all new
files explicitly: `git diff --stat` alone omits untracked files. The audit report
itself belongs with navigation/evidence. Existing deployment and research histories
remain in Git; avoid a giant consolidated narrative that repeats them all.

Ownership rules now linked from README and AGENTS:

- `docs/search-status.md`: **current** observed state and open work; update after changes.
- `docs/search.md`: implemented dataflow and contracts; avoid deployment timelines.
- `infra/ansible/README.md`: routine worker install/rollback, not DB maintenance.
- `docs/search-cache-operations.md`: reusable DB cache commands; not cumulative logs.
- `docs/search-validation/YYYY-MM-DD/`: immutable small evidence with provenance.
- `docs/deployment-decisions.md` and research design: dated rationale/proposals.
- `tools/search/`: reusable scripts/SQL; no dependency on a previous session's `/tmp`.
- `tools/backup/`: recovery procedure and evidence; archive bytes remain outside Git.

## Temporary artifact disposition

**No wildcard cleanup was run.** After these changes are committed and retained
copies verified, the following session files have replacements and can be removed:

| Ephemeral material | Durable replacement / decision |
| --- | --- |
| `/tmp/search-hn-THROWAWAY-overnight-handoff-2026-09-06.md` | Relevant deployment, DNS, batch and backlog facts folded into status/evidence; do not publish it verbatim as a runbook |
| `/tmp/searchhn-hybrid-{smoke,query,repeat,explain}.sql`, `/tmp/searchhn-verify-cache.sh` | Replaced by `tools/search/`; old scripts contain hard-coded scratch dependencies and should not be promoted |
| `/tmp/searchhn-explain-{1,2}.json`, `searchhn-io-plan.json`, `searchhn-fresh-cache-plan.json`, `searchhn-ocean-plan.json` | Exact copies/checksums retained under dated evidence; originals can be cut |
| `/tmp/searchhn-smoke-*.json` | Query response scratch, not indexing data; relevant vectors are already in retained plans. Cut after evidence retention; future tools fetch/validate their own responses |
| `/tmp/searchhn-hybrid-repeat-{first,second}.txt` | Repetitive console output; key before/after numbers and limitations retained, no need for a new permanent log collection |
| `/tmp/searchhn-runbook-check/` | Generated snippets from an older runbook; regenerate checks from current docs rather than retain stale copies |
| `/tmp/searchhn-hybrid-slice.bin` (40 MB) | Temporary benchmark fixture, explicitly documented in `docs/search.md`; not a backup. Retain only if rerunning that experiment, otherwise regenerate from documented bounded export |
| `/tmp/searchhn-stress*.json` and `.log` | Compare against tracked `docs/search-validation/stress.json` and its methodology before pruning; don't replace baseline results blindly |
| `/tmp/searchhn-*-build.log`, test/rehearsal/package logs | Keep only logs supporting claims not already preserved in release assets, ignored rehearsal output or backup evidence. Read/redact before publishing; filenames alone cannot establish they are safe |
| `/tmp/searchhn-check.local.toml`, `searchhn-package-check.toml` | Local operational/test configuration; keep out of Git and inspect privately before deletion. Never use them as credential documentation |
| Older model/source downloads, MTEB/OpenRouter trees, PDFs/screenshots, caches | Not needed by the new operational tools. Owner review before deletion if unique research evidence; do not copy wholesale into docs |

**Retain:** `backups/searchhn_test-20260907T005706Z/`, checksum/restore logs and
manifest; ignored real inventory/TOML; deployment snapshots on the worker; release
artifacts in GitHub; original rehearsal evidence. These are not interchangeable
with scratch query responses. No backup, deployment snapshot, local config, or
older research artifact was deleted.

## Deployment-guide limitations remaining

The initial rollout is now explicitly historical, with known deviations recorded:
root SSH versus sudo; extension-superuser provisioning; enabled/empty-table startup;
and a newer migration set. It is **not certified as a fresh-machine bootstrap**.
Before another first install, rehearse the exact release and migration set on a new
disposable database/host. Do not silently reuse its old single-pending-migration
assertion. Routine deployments use the current Ansible guide instead.

The cache procedure is current, including TOAST. No automatic warming or new RAM
allocation was installed. The application slice still needs endpoint configuration,
canonical hybrid HTTP handling, filters/pagination/evaluation and reader-role setup;
one-off SQL is evidence of the data path, not completion of those features.

## Checks performed in this audit

- Live migration ledger: migration 13 confirmed, earlier extra entry retained.
- Five retained plan files parse as EXPLAIN JSON; checksums recorded.
- Ansible install/rollback syntax checks passed using the example inventory.
- `bash -n` for `tools/search/verify-cache.sh`; whitespace/link checks recorded at closeout.
- No full deployment rehearsal, mutation of production, or new performance benchmark.

Closeout checks passed: all local file-link targets in changed/new Markdown exist;
all five retained EXPLAIN checksums match; shell syntax and `git diff --check` pass.
Broken root-relative links in the research design were corrected as part of the
navigation audit. Anchor semantics and external URLs were not exhaustively checked.
