# Deployment decisions and proposal log

Updated: 2026-09-06. This is the running record of deployment discussions, not an
executable runbook. Record subsequent decisions here and retain superseded choices
below so rejected proposals do not silently return. Release slice implemented;
TOML, Ansible install/rollback, and the embedding startup guard are implemented
and have passed disposable-host rehearsal. Production activation remains separate.

## Agreed constraints and decisions

### 1. Releases: GitHub Releases, semver, Ansible

- Implemented release entry point: `./scripts/release`, with `--dry-run` for a
  read-only preview. See [wizard usage](../tools/release/README.md).
  The wizard does not deploy or modify production;
  [Ansible](../infra/ansible/README.md) owns installation and rollback.
- Live source `511a6e0c77f6aae29a2cffe039fea9b825b63f20` is retrospectively tagged
  and published as `v0.2.0`, with a rebuilt Debian 13 amd64 artifact and evidence.
- GitHub Releases is the authoritative home for versioned release artifacts.
- Use semantic versions. Ansible deploys an explicitly selected release version,
  verifies its artifact checksum, and can redeploy a selected previous version.
- Build locally using the existing builder; upload artifacts to the corresponding
  GitHub Release. GitHub Actions is not required.
- Keep release identity and artifacts visible in GitHub. Do not introduce a custom
  artifact service, an S3 release catalog, or a second release API.
- No Docker-in-LXC work and no Zot dependency.
- Release creation must be guided: one interactive command discovers existing
  releases/tags, presents major/minor/patch with the resulting versions, allows
  release-note editing, and builds/publishes the selected version. No manual
  version arithmetic or separate tag/push/upload steps in the operator workflow.
- Optional LLM-drafted notes may prepopulate the editor; publication still follows
  review. Exact-version/tag conflicts and interrupted attempts must be handled
  explicitly instead of silently overwriting an existing release.
- Implemented tags use `vMAJOR.MINOR.PATCH` with optional `-canary.N`/`-pre.N`.
  Archives contain binaries and migrations; manifests/checksums/logs accompany
  them. Install/rollback live here in `infra/ansible/` with ignored host inventory.

### 2. Application configuration: TOML

- Implemented `--config PATH` for updater, check and embedding-backfill. See
  [the example](../infra/ansible/worker.example.toml). Legacy CLI operation remains
  available for historical deployments; TOML mode does not mix in `.env` settings.
- Move application settings to a TOML configuration file instead of accumulating
  environment flags. Include an explicit embedding enabled/disabled setting and
  the endpoint in that file.
- No external feature-flag service. Disabling embedding leaves ingestion running.
- Schema and deployed file layout are implemented in the example and playbooks.
  The real inventory and protected source TOML still need operator-supplied host
  settings/credentials. No new secrets service or credentials in release artifacts.

### 3. PostgreSQL provisioning: proposal awaiting final agreement

- The PostgreSQL instance serves other databases. Any preload change/restart must
  be handled as shared-host maintenance, not hidden in an application rollout.
- Proposed: homelab Ansible owns extension installation/version selection, server
  configuration, coordinated restarts, and backup scheduling. This repository
  owns application migrations, grants, and compatibility tests.
- Prefer existing packages/upstream binaries. Verify compatibility with the actual
  Debian 13 / PostgreSQL 17 installation before proposing a source build.
- Making extension files available on the host does not mean enabling extensions
  automatically in every database.

### 4. Concurrent embedding requests: retain the existing design

- Duplicate requests are acceptable at this workload. Do not add exclusive worker
  ownership, leases, locks spanning inference, or force backfill to seed-only just
  to avoid duplicate inference.
- Code review: both consumers finish through `story_search::finish`. It locks the
  source row, checks current eligibility and matching source/search title and URL,
  and updates only a row whose embedding is still NULL. The first successful
  completion wins; a later duplicate cannot overwrite it. Failed requests use the
  same guard and cannot erase a completed embedding. Deleted/ineligible/changed
  documents cannot receive an obsolete result.
- This conclusion assumes the existing shared embedding recipe/model contract.
  It is a review of the current code, not a new test execution. Duplicate failures
  may adjust the retry time of still-pending work; duplicate requests cost inference
  and brief database contention, not a new correctness problem requiring redesign.
- Historical search population is a one-off application operation before starting
  the embedding-enabled updater. Ansible does not own a recurring backfill job.
  Later deployments need neither a historical population run nor hand-picked IDs.
- `catchup_worker updater` runs Firebase ingestion/replay AND, when configured,
  the embedding loop concurrently in one process. Replay re-fetches existing HN
  items: their presence locally does not prove freshness. HN exposes creation time,
  not item modification time, and documents no replayable changes-since cursor.
- `catchup_worker embedding-backfill` is a separate invocation of the SAME binary.
  It walks local `items`, synchronizes eligible historical rows into `story_search`,
  then embeds pending rows unless `--seed-only`. It NEVER fetches Firebase and
  therefore cannot establish whether the local source data is fresh.
- `catchup_worker catchup` is another subcommand for one-shot Firebase ingestion;
  the package also has a `catchup_only` executable. Do not confuse either with
  `embedding-backfill` or with the unrelated `backfill-story-id` helper.
- Initial search population is settled by the rollout order below. The one-off
  scan covers eligible local stories across all history, including those outside
  the recent Firebase replay window. No automatic historical scanner or new
  persistent cursor is needed.
- Explicit ranges remain diagnostic/test tools, not routine rollout inputs.

### First hybrid rollout: agreed order

1. Prepare extensions and apply the migration, creating the search table/indexes
   and source synchronization trigger. Shared-instance maintenance and the remote
   backup are separate prerequisites, as described elsewhere in this log.
2. Run `catchup_worker embedding-backfill` once to populate search rows from
   existing eligible `items`, before starting the embedding-enabled updater. This
   is another invocation of the same executable, not an Ansible-owned service.
   The existing command can also embed; `--seed-only` populates rows without
   inference. Population must finish; vectors may remain pending for the updater.
3. Start `catchup_worker updater` with embeddings enabled and the agreed seven-day
   startup replay window. It re-fetches recent source items from Firebase while
   its embedding loop processes pending search rows.
4. The trigger admits stories crossing to score >=25, removes those falling below
   25 or otherwise becoming ineligible, and clears embeddings for changed title/URL.
   Unchanged text retains its embedding. Conditional completion rejects results
   made obsolete by a concurrent source update.

"Clobber the last seven days" means re-fetch source data and invalidate affected
embeddings, not re-embed every unchanged story. Firebase creation time/local row
presence cannot establish freshness; preserve existing forced replay behavior.
The checked-in startup default is currently three days (stale-stream replay is
separately two); seven days must be set explicitly in the eventual configuration.
The example TOML sets seven days; the CLI default and replay anchoring are unchanged.

Once initial population is complete, subsequent releases use the existing updater
and replay behavior. They do not repeat all-history search population. The prior
claim of a missing startup scanner overlooked the planned one-off population.

### Embedding startup guard (implemented)

- Before starting an enabled embedding loop, check whether `story_search` is empty.
  If empty, log a warning directing the operator to complete the one-off historical
  search population, and do not start the embedding loop for this process.
- Continue normal `items` ingestion, Firebase replay, and ingestion health checks.
  The skipped embedding loop performs no writes and does not auto-populate history.
- Do not automatically enable the loop if source ingestion later creates a search
  row; after population, restart the updater to check again.
- Interpretation of "untouched": no embedding-loop writes. Existing source-trigger
  synchronization remains active so normal ingestion still maintains derived rows.
- This is an empty-table guard, not proof that an all-history scan completed. A
  nonempty table can be partially populated. Do not claim otherwise or introduce
  a new completion-marker/checkpoint design without discussing it.

### 5. Backups: transfer over the network, never stage on the DB LXC

- Do not assume spare space on the PostgreSQL LXC. Do not write a rollout dump
  there, even temporarily, and do not stage it there before uploading.
- Run a PostgreSQL client on a separate backup machine and stream `pg_dump` output
  to storage there, or stream directly to an existing remote backup destination.
- Exact destination is OPEN: a designated other machine/disk or Garage. Available
  capacity and an existing backup workflow must be checked before choosing.
- Proposed simplest implementation: run `pg_dump` on the receiving host against
  `searchhn_test`; write a partial archive there and mark it complete only on
  success. A failed or incomplete transfer must not count as a usable backup.
- Proposed retention: keep the pre-change backup through acceptance and until a
  replacement backup is verified. Exact duration remains undecided. Do not delete
  it immediately after a successful restart.
- Validate recovery with a disposable restore. Do not restore cluster-wide roles
  or another database as part of ordinary application rollback.
- Streaming avoids local dump storage but still consumes database read I/O; choose
  timing/concurrency with the other instance users in mind.

## Deployment boundaries (implemented)

- Application rollback selects an earlier release/configuration and preserves the
  additive search schema/data. Schema removal is separate deliberate maintenance.
- Restricted service-role writes passed rehearsal, alongside read-only preflight.
- Published-artifact install and both rollback paths passed rehearsal.
- Keep operator commands small; do not replace the old runbook with a giant shell
  script or expand the service API to orchestrate deployments.

## Superseded proposals / corrections

| Earlier proposal | Disposition |
| --- | --- |
| SSH copies or Garage/S3 as the authoritative release home | Rejected: use semver GitHub Releases and Ansible. |
| More environment variables for application configuration | Rejected: use TOML. |
| Enforce one embedding consumer or make backfill seed-only | Rejected: duplicate work is acceptable; preserve conditional completion. |
| Store a 40 GB-class backup on the PostgreSQL LXC | Rejected: no capacity assumption or local staging; transfer over the network. |
| Build extension packages ourselves by default | Withdrawn: inspect existing distribution/upstream artifacts first. |
| Execute the existing lengthy rollout document | Withdrawn: it contains obsolete and inconsistent procedures. |
| Manual semver assignment, tagging, pushing, and release upload | Replaced by one guided interactive release command. |
| Ansible owns backfill or asks for routine source IDs | Rejected: one-off application search population precedes updater startup; later releases need no historical backfill. |
| Add automatic historical scanning and a durable cursor to updater startup | Withdrawn: existing one-off search population plus Firebase replay covers the agreed rollout. |

## Discussion history

- 2026-09-06, initial audit: identified missing deployable release workflow,
  configuration ambiguity, superuser-only runtime tests, and a runbook mixing
  shared PostgreSQL maintenance with app deployment and recovery.
- 2026-09-06, user corrections: selected GitHub Releases with semver and Ansible;
  selected TOML; reaffirmed duplicate inference as an accepted design choice;
  required backups to cross the network without using DB-LXC staging space.

- 2026-09-06, release UX: require guided major/minor/patch selection and editable
  release notes rather than manual tag/version/upload commands.
- 2026-09-06, backfill clarification: distinguish Firebase freshness replay from
  local search population and embedding. Agreed order is migration, one-off
  historical search population, then embedding-enabled updater with seven-day
  Firebase replay. Withdraw automatic scanner/cursor proposals. Superseded command
  drafts removed; their decisions remain recorded above rather than retaining
  contradictory executable-looking instructions.
- 2026-09-06, startup guard: empty search table must warn and skip the embedding
  loop while source ingestion continues. Implemented and rehearsed.

## Current operator documentation

- [Release wizard](../tools/release/README.md): semver selection, build and publication.
- [Install and rollback](../infra/ansible/README.md): inventory, staging, activation,
  privileges and recovery. No migrations or population inside these playbooks.
- [Worker configuration](../infra/ansible/worker.example.toml): updater, preflight
  and one-off embedding population share TOML.
- [Search operations](search.md#historical-backfill): population and replay behavior.
- [Systemd services](../infra/systemd/README.md): canonical updater template and
  separate maintenance services.
- [Rehearsal evidence](../infra/ansible/tests/VALIDATION.md): passed cases and
  OrbStack's filesystem-sandbox limitation. Production is still v0.2.0.

## Cleanup and slice 2 completion

- 2026-09-06: confirmed slice 2 means finishing TOML/systemd cleanup, not PostgreSQL
  provisioning. TOML and startup guard were already implemented with Ansible.
- Removed the obsolete rollout draft rather than retaining rejected executable
  commands. Git history retains it; the decisions and corrections remain above.
- Removed the duplicate CLI-based updater unit. Ansible's template is the sole
  updater definition; the v0.2.0 fixture remains solely for rollback rehearsal.
- Removed old full-crawl/shakedown service presets. Retained independent catchup,
  lineage repair and API units, clearly documented as outside updater deployment.
- Moved the optional catchup unit's metrics port from 3001 to 3002 to avoid the API
  port. This changes the checked-in example only; no installed units were changed.
- PostgreSQL provisioning and the remote backup destination remain open as above.
- Validation: `systemd-analyze verify` accepted the changed catchup unit on the
  disposable Debian host without installing or starting it. Removed-file references
  and whitespace checks passed. Application code and the rehearsed updater template
  are unchanged.
