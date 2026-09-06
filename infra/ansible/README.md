# Install and rollback

These playbooks consume an **already-published GitHub Release**. They do not build,
migrate PostgreSQL, install extensions, or start historical backfill. The controller
needs Ansible, authenticated `gh`, GitHub access, and SSH/sudo access to the worker.
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
unit and process alone. After migrations, run the one-off application population
as `catchup` using that staged binary and TOML (`embedding-backfill --config PATH
--seed-only`). Then run the normal install command to activate. No story IDs are
required. Ansible does not schedule or supervise the population command.

`hosts.yml`, `hosts.test.yml`, `*.local.toml` and rehearsal output are ignored. Keep
real credentials in the ignored TOML or your existing protected configuration
workflow; do not copy them into release assets. Use `-K` if sudo requires a password.

Install downloads/checksums the exact release on the controller, stages an immutable
deployment on the worker, and runs its `check --config ...` as `catchup`. Failed
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
its original executable, environment file and unit, and refuses unaccounted unit
drop-ins. Explicitly reviewed platform overrides can be listed in
`worker_preserved_dropins`; they remain in place during install and rollback.
Rollback to it restores those exact files; it does not invoke a nonexistent
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
