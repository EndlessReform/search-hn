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

### 3. PostgreSQL maintenance: manual commands, rehearsed first

- The PostgreSQL instance serves other databases. Any preload change/restart must
  be handled as shared-host maintenance, not hidden in an application rollout.
- Use ordinary package-manager commands and a coordinated PostgreSQL restart.
  No extension-install playbook or migration wrapper: use the existing Diesel CLI.
  Automate the recurring backup only; keep installation/recovery commands short
  and rehearse them on the disposable host before maintenance.
- Prefer existing packages/upstream binaries. Verify compatibility with the actual
  Debian 13 / PostgreSQL 17 installation before proposing a source build.
- Making extension files available on the host does not mean enabling extensions
  automatically in every database.

#### Installation research — checked 2026-09-06; proposal, not executed

**Keep PostgreSQL 17; install packaged extensions rather than compiling on the
shared host.** A live read-only query reports `PostgreSQL 17.11 (Debian
17.11-0+deb13u1)` on x86_64. PostgreSQL's [version policy](https://www.postgresql.org/support/versioning/)
currently lists 17.11 as the latest 17.x, supported until November 8, 2029.
No server upgrade is required for these extensions. A major upgrade would add a
separate cluster migration and compatibility exercise for every database.

- **pgvector:** upstream documents installing `postgresql-17-pgvector` through
  the [PostgreSQL APT repository](https://github.com/pgvector/pgvector#apt), which
  [supports Debian 13](https://www.postgresql.org/download/linux/debian/).
  Debian's own [trixie package](https://packages.debian.org/trixie/postgresql-17-pgvector)
  is 0.8.0; it does not meet this application's 0.8.6 target. PGDG publishes
  [0.8.6 for PG17/Debian13/amd64](https://apt.postgresql.org/pub/repos/apt/pool/main/p/pgvector/).
  Use the tested 0.8.6: the [upstream changelog](https://github.com/pgvector/pgvector/blob/master/CHANGELOG.md)
  records HNSW vacuum corruption fixes in 0.8.3 and further vacuum/insert fixes in
  0.8.4. Migration, preflight and scratch fixture now target 0.8.6; the next
  normal release build will include the updated checks.
- **BM25 (`pg_textsearch`, not PostgreSQL's built-in FTS):** the exact
  [1.4.0 release](https://github.com/timescale/pg_textsearch/releases/tag/v1.4.0)
  includes `pg-textsearch-v1.4.0-pg17-amd64.zip`. Downloaded and inspected it: it
  contains `pg-textsearch-postgresql-17_1.4.0-1_amd64.deb`, with PG17 library and
  extension SQL files in the standard Debian paths. Install that local `.deb`
  through APT/Ansible after checking dependencies. It accepts plain `postgresql-17`;
  TimescaleDB is not required. Its maintainer scripts only print instructions;
  they do not restart PostgreSQL or enable the extension.
- The inspected ZIP's SHA256 matches GitHub's asset digest:
  `93dbb144b09675ce5294d2a8655ed6b7f53a79cb7ebee1b7c8c3c148561a0383`.
  This confirms the inspected artifact, not a completed compatibility rehearsal.
- The pinned [pg_textsearch instructions](https://github.com/timescale/pg_textsearch/blob/v1.4.0/README.md#installation)
  support PG17/18 and require adding `pg_textsearch` to existing
  `shared_preload_libraries`, then restarting the shared PostgreSQL instance.
  Enable extensions per database through this repo's migration after that.
  pgvector's documented installation does not require a preload change.

Read-only root inspection subsequently confirmed Debian 13.6, PG17.11 from
Debian security, libc6 2.41, no held/pinned packages, and Debian/Tailscale APT
origins (no PGDG). The preload list is empty; neither extension is available on
the server. A simulated install of the current Debian pgvector candidate selects
only 0.8.0-1 and changes no other packages. No APT refresh or installation was run.

Downloaded PGDG's `postgresql-17-pgvector_0.8.6-1.pgdg13+1_amd64.deb` locally and
inspected its metadata: it requires PostgreSQL17 and libc6 >=2.38, and conflicts
with a `postgresql-17-jit-llvm` provider older than 19. The installed PG17 package
and libc meet these requirements; actual installation of this package still needs
rehearsal. Do not change PostgreSQL package origin merely to acquire an extension.

Quick 0.8.6 compatibility test completed on a separate PG17.11 linux/amd64 Docker
instance with pg_textsearch1.4.0: **8 integration tests passed**, one optional live
inference test skipped. Tests cover migration down/reapply, eligibility and edits,
index queries, preserved embeddings on backfill reruns, concurrent source locking,
stale results, and endpoint failure recovery. The test used a source-built 0.8.6
library and temporarily changed the exact-version checks; those edits were restored.
It does not certify the downloaded Debian package, production throughput, or
reproduce every upstream vacuum bug. The earlier test restored temporary edits; the migration and preflight have now
been updated permanently to 0.8.6. Published releases remain unchanged.

The reason to avoid upstream 0.8.0 is the parallel HNSW build overflow fixed in
0.8.2 and HNSW vacuum corruption/errors fixed in 0.8.3/0.8.4, not missing search
features. Distribution backports could change that assessment; none were verified
for Debian's 0.8.0-1 during this inspection.

Backups can stream while PostgreSQL is online. Package downloads and preparation
can also precede the maintenance window. The required instance-wide interruption
is the coordinated preload restart, not the entire backup/population duration.
The remote backup destination and homelab repository handoff remain to be agreed.
No packages, PostgreSQL settings, extension pins or production data were changed
as part of this research.

### 4. Populate search history once, then let the updater maintain it

**Implementation is complete. What remains is the first production population
and verification, after section 3 prepares PostgreSQL.** Later application
releases do not repeat this initial population.

#### What we still need to do

1. After the extensions and migration are ready, stage the candidate binary and
   TOML with Ansible without activating it.
2. Run `catchup_worker embedding-backfill --config PATH` as the existing `catchup`
   user. It scans local `items` across all history and populates eligible rows in
   `story_search`. It can also generate embeddings; `--seed-only` skips inference.
   Finish the population before starting the embedding-enabled updater. Vectors
   may still be pending at that point.
3. Activate `catchup_worker updater` using the TOML's seven-day startup replay.
   It downloads recent items again from Firebase while generating pending embeddings.
4. Verify real inference succeeds, pending work progresses, and normal ingestion
   remains healthy. Production population time and real inference throughput have
   not yet been measured by the disposable deployment rehearsal.

Ansible stages and activates the application. The one-off population is an
application command, not a recurring Ansible job or a new systemd service.
Routine rollout requires no hand-picked story IDs.

#### Which command does what

Both commands are in the same `catchup_worker` binary, but run separately:

- `embedding-backfill` reads the PostgreSQL mirror. It does not contact Firebase
  and cannot tell whether an existing source story is still accurate.
- `updater` downloads from Firebase and maintains `items`. When enabled, its
  embedding loop runs alongside ingestion in the same process. Firebase does not
  provide item modification timestamps, so having a story locally is not enough
  to skip downloading it during the recent replay window.

When a source story changes, the existing database trigger admits stories newly
at score 25 or above, removes those that become ineligible, and clears the vector
when title or URL changes. Unchanged text retains its vector. Replaying seven days
means refreshing source data and the affected embeddings, not re-embedding every
unchanged story. The example TOML sets seven days; the CLI default remains three
and stale-stream recovery remains a separate two-day window.

#### Behavior already implemented and checked

If embeddings are enabled but `story_search` is empty at startup, the updater
warns and skips its embedding loop for that process. Ordinary ingestion and its
source trigger continue. Populate history, then restart to enable embedding; the
loop does not silently start when ingestion creates the first search row. This
checks emptiness, not whether a partially populated table contains all history.

Duplicate inference is acceptable. Both consumers save through
`story_search::finish`, which checks current eligibility and matching title/URL
while locking the source row, and only fills an embedding that is still NULL.
A duplicate cannot overwrite a completed embedding or save a result made obsolete
by a source edit. Failed duplicates can change a still-pending retry time. There
is no need for a new owner, lease, scheduler, or historical scan on updater startup.
`--seed-only` is an operational choice, not a correctness requirement.

The disposable rehearsal exercised source edits, crossing the score threshold in
both directions, and the empty-table guard. See [recorded results](../infra/ansible/tests/VALIDATION.md).
The separate Firebase `catchup` command and comment-lineage `backfill-story-id`
helper are unrelated to this one-off search population.

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

- [First-rollout runbook](search-rollout.md): exact operator sequence and outstanding
  prerequisites. Rewritten after backup/restore and version research; supersedes
  the deleted early draft.

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

- Backup/restore work: see [backup scope and procedure](../tools/backup/README.md).
  The backup covers the entire application database, including every table affected
  by the migration/updater and their schema dependencies. No other databases are
  included and no dump is staged on the PostgreSQL LXC. Garage destination remains
  pending operator selection.
- Full backup/restore completed: [evidence](../tools/backup/RESTORE.md). All eight
  tables restored into a new database on the disposable host; 49.5 million items
  and 43.2 million kids rows, valid indexes/constraints, and working existing FTS.
  The archive remains on the Mac; Garage upload has not run.

- Final rollout order clarified: package/release rehearsal, build, fresh backup,
  pause writers, install extensions and restart PG17/main, DB health, Diesel
  migration, new updater with embeddings disabled, one-off backfill, then enable
  embeddings through a second TOML deployment. No redundant disabled restart.
  The next release includes the 0.8.6 checks; package testing belongs to the
  agreed final rehearsal, not a separate patch-version approval gate.

- Communication rule added to AGENTS.md: lead with consequential changes and
  departures from approved scope. Extension downloads now go directly to the DB
  host over official HTTPS; removed the Mac/SCP detour and manual package checksum
  commands. Backup integrity checks and the existing app-release workflow remain
  as implemented.
- Re-ran the focused integration suite after making the 0.8.6 checks permanent:
  eight passed, zero failed; the optional live-inference smoke remained skipped.
  Runbook shell syntax passed; production was not modified.
