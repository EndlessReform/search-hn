# Install / rollback rehearsal — 2026-09-06

Test target: disposable OrbStack `searchhn-deploy-test`, Debian 13 amd64,
PostgreSQL **17.11 (Debian 17.11-0+deb13u1)**. No production activation, migration,
backfill, or configuration change was performed.

Artifacts:

- Baseline: published `v0.2.0`, commit `511a6e0c77f6aae29a2cffe039fea9b825b63f20`,
  with the systemd unit from that historical tag and its environment-based config.
- Candidate: [v0.2.1-canary.1](https://github.com/EndlessReform/search-hn/releases/tag/v0.2.1-canary.1),
  commit `1dd00bd4e710d3c44169de23c55dc7cb566e9e0d`. The release builder passed
  47 worker library tests and two TOML binary tests, then verified executable
  identity and downloaded release checksums. Ansible consumed that GitHub archive.
- Controller playbooks include the inventory-precedence/platform-drop-in fixes
  made during rehearsal; those do not change the candidate executable.

The fixtures use only local HTTP endpoints and `searchhn_rehearsal`. The actual
application runs as Linux `catchup`, connecting as PostgreSQL `catchup_worker`
(`rolsuper=false`). Canonical migrations were applied with Diesel using a separate
fixture administrator. Existing pinned scratch extension artifacts were reused;
this does not implement production PostgreSQL extension provisioning.

| Case | Observed result |
| --- | --- |
| Wrong password | Specific authentication failure at preflight; original PID unchanged. |
| Missing `story_search` | Specific missing-relation failure at preflight; original PID unchanged. |
| Revoke UPDATE on `story_search` | Specific missing-privilege failure at preflight; original PID unchanged. |
| Occupied candidate metrics port | Preflight passed; real systemd startup failed; previous binary/config/unit restored; old version healthy; install returned failure. |
| Normal installation | Correct version/commit running as Linux `catchup`; healthy ingestion. |
| Actual source and embedding writes | Initial embedding saved, title edit re-embedded, score 24 removed the row, score 25 re-admitted and embedded it. |
| Same deployment installed again | Service PID and previous-deployment record unchanged. |
| Empty table at startup | Embedding stayed disabled while source fetching and ingestion health continued. Later seed-only population did not start inference; explicit restart did. |
| Rollback to v0.2.0 | Original unit/environment executable restored, old version healthy, search data retained. |
| TOML configuration change and rollback | Changed configuration activated in a new directory; rollback ran the previous binary's preflight and restored its exact TOML/target. |
| Stage/preflight only (`--skip-tags activate`) | Candidate verified without changing the active service PID. |

Full logs are retained locally under ignored `infra/ansible/test-output/`:
`bad-password.log`, `missing-table.log`, `missing-privilege.log`,
`activation-failure.log`, `successful-install.log`, `idempotent-install.log`,
`legacy-rollback.log`, `modern-baseline.log`, `config-change.log`, `toml-rollback.log`,
`stage-only.log`.
The fixture is left running the candidate with the restored original test TOML,
for inspection. Its traffic is entirely synthetic.

## Issues caught and corrected

- An initial schema probe duplicated column names incorrectly. Probes now come
  from the same Diesel schema used by ingestion instead of a second column list.
- Today's legacy unit contains arguments absent from v0.2.0. The fixture now uses
  the unit from the actual historical tag, matching rollback's real requirement.
- Play-level defaults overrode inventory settings. Defaults are now resolved
  explicitly without overriding host values.
- OrbStack inserts `/run/systemd/system/catchup-worker-updater.service.d/zzz-lxc-service.conf`.
  Its presence initially stopped adoption, as unknown drop-ins should. The test
  inventory now explicitly preserves this reviewed platform override; production
  defaults still refuse unknown overrides.
- The intentionally occupied-port test needed readiness polling and unique
  transient unit names. These changes are confined to fixture orchestration.

## Practical limit

OrbStack's platform override disables ProtectHome, ProtectSystem, PrivateTmp and
PrivateDevices. Effective settings were inspected. This rehearsal certifies the
actual systemd lifecycle, Unix user, application/database checks, and rollback;
it **does not certify those disabled filesystem sandbox settings** on production's
kernel. The checked-in unit retains the existing production hardening directives.
Read-only preflight on the real host remains a step before production activation.
The final production metric read still reported `version="0.2.0",commit="511a6e0c77f6"`.

## Credential separation correction — 2026-09-06

The rehearsal above tested the earlier credential-bearing TOML contract. Current
source requires DATABASE_URL in the process environment and rejects it in TOML.
Install/rollback preflight and the documented backfill invocation now load the
existing worker EnvironmentFile through systemd, as the updater does. Rollback
leaves that file in place instead of restoring a credential snapshot.

Verification of this correction:

- All three worker binary configuration tests passed, including rejection of a
  TOML database_url and missing/blank environment credentials.
- Rebuilt macOS worker preflight passed against disposable
  `searchhn_restore_20260907` as restricted `catchup_worker`, using environment
  credentials: migrations, ingestion privileges, search schema/extensions/recipe.
- Missing DATABASE_URL failed configuration before connecting.
- On `searchhn-deploy-test@orb`, a transient systemd service running as `catchup`
  loaded the existing EnvironmentFile and verified DATABASE_URL was present without
  printing it. Production was not accessed.
- Install and rollback Ansible syntax checks passed. The rehearsal harness now
  tests an incorrect password in the fixture EnvironmentFile and restores it in
  a finally block, rather than putting the password in TOML.

The complete release install/rollback rehearsal has **not** been rerun for this
correction; it needs a rebuilt Linux release. The published canary above still
uses the earlier TOML contract and must not be paired with the new example.
