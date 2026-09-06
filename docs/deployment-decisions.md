# Deployment decisions and proposal log

Updated: 2026-09-06. This is the running record of deployment discussions, not an
executable runbook. Record subsequent decisions here and retain superseded choices
below so rejected proposals do not silently return. Implementation is still pending.

## Agreed constraints and decisions

### 1. Releases: GitHub Releases, semver, Ansible

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
- Still to specify: tag convention, archive contents, and the existing Ansible
  repository/playbook that will consume the release. These are implementation
  details, not a reason to introduce another release system.

### 2. Application configuration: TOML

- Move application settings to a TOML configuration file instead of accumulating
  environment flags. Include an explicit embedding enabled/disabled setting and
  the endpoint in that file.
- No external feature-flag service. Disabling embedding leaves ingestion running.
- Still to specify: file location, exact fields, and how existing secrets are
  supplied. Do not invent an additional secrets system or copy credentials into
  release artifacts. Avoid multiple competing configuration sources and implicit
  production `.env` discovery.

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
This documentation change does not alter either default or replay anchoring.

Once initial population is complete, subsequent releases use the existing updater
and replay behavior. They do not repeat all-history search population. The prior
claim of a missing startup scanner overlooked the planned one-off population.

### Embedding startup guard (required; not implemented yet)

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

## Other proposals still pending

- Application rollback selects an earlier release/configuration and preserves the
  additive search schema/data. Schema removal is separate deliberate maintenance.
- Test actual restricted service-role access, not just migration execution as a
  scratch superuser.
- Rehearse deployment and rollback with the published artifact and Ansible path.
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
  loop while source ingestion continues. Guard implementation is still pending.

## Operator interface still to implement

The guided release command and Ansible deployment are still proposals. Their
names, TOML schema, and initial-population invocation with service credentials
must be fitted to existing tooling. Do not infer that those interfaces exist yet.
The initial deployment must allow migration and one-off population to finish
before starting the embedding-enabled updater. No new scheduler or backfill API
is required. See [search.md](search.md#historical-backfill) for current binary usage.
