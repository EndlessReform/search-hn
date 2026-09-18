# Install and rollback

These playbooks consume an **already-published GitHub Release**. They do not build,
migrate PostgreSQL, install extensions, or start historical backfill. The controller
needs Ansible, authenticated `gh`, GitHub access, and root SSH or SSH with sudo
access to the worker. The target also needs `runuser` for service-account checks.
The worker must already have Linux user/group `catchup`, the existing updater unit,
and its runtime libraries. No PostgreSQL administrator credentials are used.

```bash
cp infra/ansible/hosts.example.yml infra/ansible/hosts.yml
# Fill in the existing SSH account, target, explicit release and local TOML path.
$EDITOR infra/ansible/hosts.yml

ansible-playbook -i infra/ansible/hosts.yml infra/ansible/install.yml
ansible-playbook -i infra/ansible/hosts.yml infra/ansible/rollback.yml
```

For first rollout or a production preflight without activation:

```bash
ansible-playbook -i infra/ansible/hosts.yml infra/ansible/install.yml --skip-tags activate
```

This stages/checks the candidate and prints its directory; it leaves the active
unit and process alone. Embedding-enabled preflight needs the search migration
already applied. The historical first-rollout
[runbook](../../docs/search-rollout.md) describes: migrate, install with embeddings disabled,
run the one-off backfill, then install the enabled TOML. Staging without activation
remains available when you specifically need it. Ansible does not schedule backfill.

That initial rollout is complete on the current host; see
[current status](../../docs/search-status.md). Routine releases do not repeat
historical population. If an enabled updater started against an empty table,
backfill completion requires restarting that process to activate its loop.
PostgreSQL restart/cache warming is a separate
[operational procedure](../../docs/search-cache-operations.md).

`hosts.yml`, `hosts.test.yml`, `*.local.toml` and rehearsal output are ignored. Keep
DATABASE_URL in the existing protected `/etc/search-hn/catchup-worker.env` on the
worker. TOML contains operational settings and rejects `database_url`. Use `-K` if sudo requires a password.

Install downloads/checksums the exact release on the controller, stages an immutable
deployment on the worker, and runs its `check --config ...` as `catchup` through
`systemd-run`, loading that same EnvironmentFile. Failed
preflight leaves the running service untouched. The root-owned TOML is mode 0640,
group `catchup`; its contents are suppressed in Ansible output. Binary/config/unit
snapshots live under `/opt/search-hn/deployments/`. Configuration or unit changes
produce another deployment directory. Existing deployment file corruption is rejected.

Activation saves the previous deployment, switches `current`, installs the unit,
and restarts the existing service. ExecStartPre repeats the read-only check inside
the systemd sandbox. Ansible verifies ingestion health and actual version/commit
metrics. Failure restores the previous binary/config/unit and reports the install
as failed. An unchanged deployment is checked without restarting or replacing its
previous pointer. The configured metrics endpoint must match `worker_health_url`
(default `http://127.0.0.1:3000`, requested from the worker itself).

First adoption recognizes the existing `0.2.0+511a6e0c77f6` installation, snapshots
its original executable and unit, and refuses unaccounted unit
drop-ins. Explicitly reviewed platform overrides can be listed in
`worker_preserved_dropins`; they remain in place during install and rollback.
Credentials remain in the existing environment file and are neither snapshotted
nor reverted by rollback. Rollback to the original release restores its binary/unit; it does not invoke a nonexistent
TOML/check command. Later rollback targets get their own preflight before switching.
A continuing database outage can prevent recovery: the playbook reports failure,
not a successful rollback merely because systemd accepted a restart.

The preflight checks connectivity, required migrations, ingestion columns using
the same Diesel schema as the application, role privileges, and enabled search's
extensions/recipe. It performs no source writes or inference. It cannot certify
all trigger write paths; the rehearsal verifies real writes with restricted credentials.

See `tests/rehearse.py` for fault cases. It targets only `searchhn-deploy-test@orb`
and `searchhn_rehearsal` through localhost port 55439. It runs the actual playbooks,
not a duplicate deployment implementation. `tests/bootstrap.yml` is test-only and
must never be included by production install/rollback.

OrbStack generates `zzz-lxc-service.conf`, disabling ProtectHome, ProtectSystem,
PrivateDevices and PrivateTmp. The test inventory explicitly preserves that
platform override. Rehearsal therefore verifies systemd lifecycle/User=catchup,
but does not certify those disabled mount-sandbox settings on production's kernel.

## App installation

The same release archive now contains `bin/hn_app`. App and worker installs remain
separate commands using the same `release_version`:

```bash
ansible-playbook -i infra/ansible/hosts.yml infra/ansible/app-install.yml -e release_version=vX.Y.Z
```

Run these commands from the repository root. Add the existing app host under
`searchhn_apps` in your actual `hosts.yml`; updating the example does not update
an existing inventory. If the app runs on the existing worker host, reuse its
inventory alias under `all.children`:

```yaml
    searchhn_apps:
      hosts:
        worker: {}
```

This inherits the worker host’s SSH settings without duplicating them. Verify
selection before deploying:

```bash
ansible-playbook -i infra/ansible/hosts.yml infra/ansible/app-install.yml --list-hosts
```
This upgrades an existing Debian 13 amd64 installation; it does not provision a
new host. It retains `hn-app.service`, its existing environment, and the worker.
The unit must execute `/usr/local/bin/hn_app` directly. Inventory can override
`app_binary`, `app_service`, `app_health_url` and `release_repository`.
The existing service environment must contain `DATABASE_URL` with reader access
and `EMBEDDING_BASE_URL` for hybrid search. No credentials are copied by Ansible.

**First upgrade from the old app:** its working homepage does not establish that
inference is configured. Add the following non-secret setting to the existing
`/etc/search-hn/hn-app.env` on the current deployment host, preserving its existing
`DATABASE_URL` and file permissions, before running the installer:

```dotenv
EMBEDDING_BASE_URL=https://magi06-inference.tail7a3eb.ts.net/embeddings/v1
```

That URL is the current deployment's inference service; other deployments must
use their own endpoint. The changed-binary install restarts the app and loads the
setting. Missing configuration yields keyword-only results, which intentionally
fail the installer’s hybrid check and trigger rollback. A working old homepage
or `/health` endpoint does not verify this prerequisite.

The target verifies downloaded checksums and the candidate's version/commit,
backs up the old binary beside its installed path, and atomically replaces it.
Changed binaries restart the app. Checks verify HTTP health, the SHA256 of the
actual running executable through `/proc`, and a hybrid search for `compiler`.
A failed activation restores the saved executable and restarts the app, checks
health, and returns failure. Unchanged binaries undergo verification without a
restart. Timestamped binary backups remain available; there is no automatic
retention policy. To return to another published app release, run this same
target with that version; releases predating app inclusion cannot be used.
This target neither applies migrations nor rolls back schema or credentials.

The isolated installer rehearsal uses a synthetic executable, its own service and
port on `searchhn-deploy-test@orb`, and no database/inference traffic:

```bash
uv run infra/ansible/tests/rehearse-app.py
ansible-playbook -i localhost, infra/ansible/tests/staging-guard.yml
```

See [September 9 evidence](../../docs/search-validation/2026-09-09/release-app.md).
