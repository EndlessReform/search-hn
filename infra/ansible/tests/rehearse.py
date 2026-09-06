"""Exercise the actual install/rollback playbooks on the disposable OrbStack host.

Run with UV after bootstrap and canonical Diesel migrations. All faults target
searchhn_rehearsal through an explicit localhost SSH tunnel, never a live database.
Each playbook's full output is retained under ignored test-output/.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[3]
ANSIBLE = ROOT / "infra/ansible"
OUTPUT = ANSIBLE / "test-output"
HOST = "searchhn-deploy-test@orb"
SERVICE = "catchup-worker-updater.service"


def command(*args, env=None, check=True):
    return subprocess.run(args, cwd=ROOT, env=env, text=True, check=check,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def ssh(script):
    return command("ssh", HOST, script).stdout.strip()


def sql(statement):
    env = dict(os.environ, PGPASSWORD="rehearsal-admin-only")
    return command("psql", "-X", "-h", "127.0.0.1", "-p", "55439", "-U", "admin",
                   "-d", "searchhn_rehearsal", "-At", "-v", "ON_ERROR_STOP=1", "-c", statement, env=env).stdout.strip()


def eventually(predicate, message):
    for _ in range(30):
        if predicate():
            return
        time.sleep(1)
    raise AssertionError(message)


def pid():
    return ssh(f"systemctl show {SERVICE} --property=MainPID --value")


def metrics():
    return ssh("curl -fsS http://127.0.0.1:3000/metrics")


def state():
    return json.loads(ssh("curl -fsS http://127.0.0.1:18080/state"))


def play(name, version, config, expected_success=True, label=None):
    result = command("ansible-playbook", "-i", str(ANSIBLE / "hosts.test.yml"),
                     str(ANSIBLE / f"{name}.yml"), "-e", f"release_version={version}",
                     "-e", f"worker_config={config}", check=False)
    log = OUTPUT / f"{label or name}.log"
    log.write_text(result.stdout)
    assert (result.returncode == 0) == expected_success, f"Unexpected playbook outcome; see {log}"
    print(f"PASS {label or name}: rc={result.returncode}", flush=True)
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", required=True)
    args = parser.parse_args()
    OUTPUT.mkdir(exist_ok=True)
    fixture = ANSIBLE / "tests/fixture.toml"
    # Guard the fixture before any SQL mutation or service restart.
    assert ssh("hostname") == "searchhn-deploy-test"
    assert sql("SELECT current_database()") == "searchhn_rehearsal"
    assert sql("SELECT rolsuper FROM pg_roles WHERE rolname='catchup_worker'") == "f"
    assert 'version="0.2.0"' in metrics(), "Start from the healthy historical baseline"
    before = pid()

    bad_password = OUTPUT / "bad-password.toml"
    bad_password.write_text(fixture.read_text().replace("rehearsal-only@", "wrong-password@"))
    log = play("install", args.release, bad_password, False, "bad-password")
    assert "password authentication failed" in log
    assert pid() == before, "Failed preflight restarted the original service"

    sql("ALTER TABLE story_search RENAME TO hidden_story_search")
    try:
        log = play("install", args.release, fixture, False, "missing-table")
        assert 'story_search' in log and 'does not exist' in log
        assert pid() == before
    finally:
        sql("ALTER TABLE hidden_story_search RENAME TO story_search")

    sql("REVOKE UPDATE ON story_search FROM catchup_worker")
    try:
        log = play("install", args.release, fixture, False, "missing-privilege")
        assert "missing UPDATE privilege on story_search" in log
        assert pid() == before
    finally:
        sql("GRANT UPDATE ON story_search TO catchup_worker")

    # A valid config passes preflight, then fails inside the real systemd service
    # because another process already owns its metrics port. No product test hook.
    bad_bind = OUTPUT / "occupied-port.toml"
    bad_bind.write_text(fixture.read_text().replace("127.0.0.1:3000", "127.0.0.1:3001"))
    blocker = f"searchhn-port-blocker-{os.getpid()}"
    ssh(f"sudo systemd-run --collect --unit={blocker} /home/ritsuko/.local/bin/uv run --no-project --offline --python /usr/bin/python3 -m http.server 3001 --bind 127.0.0.1")
    try:
        eventually(lambda: command("ssh", HOST, "curl -fsS http://127.0.0.1:3001/ >/dev/null", check=False).returncode == 0,
                   "Port blocker did not start")
        log = play("install", args.release, bad_bind, False, "activation-failure")
        assert "previous deployment restored" in log
        assert 'version="0.2.0"' in metrics()
    finally:
        ssh(f"sudo systemctl stop {blocker}")

    # One-off historical admission before the normal embedding-enabled install.
    sql("SELECT sync_story_search(i) FROM items i WHERE type='story'")
    play("install", args.release, fixture, label="successful-install")
    assert f'version="{args.release.removeprefix("v")}"' in metrics()
    assert ssh(f"ps -o user= -p {pid()}").strip() == "catchup"
    eventually(lambda: sql("SELECT count(*) FROM story_search WHERE embedding IS NOT NULL") == "1", "Embedding was not saved")
    ssh("curl -fsS -X POST http://127.0.0.1:18080/control -H 'Content-Type: application/json' -d '{\"title\":\"fixture changed\"}'")
    eventually(lambda: sql("SELECT count(*) FROM story_search WHERE title='fixture changed' AND embedding IS NOT NULL") == "1", "Changed source was not re-embedded")
    ssh("curl -fsS -X POST http://127.0.0.1:18080/control -H 'Content-Type: application/json' -d '{\"score\":24}'")
    eventually(lambda: sql("SELECT count(*) FROM story_search") == "0", "Demoted story remained indexed")
    ssh("curl -fsS -X POST http://127.0.0.1:18080/control -H 'Content-Type: application/json' -d '{\"score\":25}'")
    eventually(lambda: sql("SELECT count(*) FROM story_search WHERE embedding IS NOT NULL") == "1", "Promoted story was not embedded")
    print("PASS restricted-role ingestion, edit, demotion and promotion", flush=True)

    before = pid()
    previous = ssh("sudo cat /opt/search-hn/previous.json")
    play("install", args.release, fixture, label="idempotent-install")
    assert pid() == before
    assert ssh("sudo cat /opt/search-hn/previous.json") == previous

    # Empty-table latch: source ingestion must stay healthy; later seeding must not
    # silently start inference until another explicit process restart.
    sql("TRUNCATE story_search")
    ssh(f"sudo systemctl restart {SERVICE}")
    before_calls, before_requests = state()["calls"], state()["requests"]
    ssh("sudo -u catchup /opt/search-hn/current/bin/catchup_worker embedding-backfill --config /opt/search-hn/current/worker.toml --seed-only")
    time.sleep(3)
    assert state()["calls"] == before_calls
    assert state()["requests"] > before_requests
    assert sql("SELECT count(*) FROM story_search WHERE embedding IS NULL") == "1"
    assert ssh("curl -fsS localhost:3000/health") == "Healthy"
    ssh(f"sudo systemctl restart {SERVICE}")
    eventually(lambda: sql("SELECT count(*) FROM story_search WHERE embedding IS NOT NULL") == "1", "Restart did not enable embedding after population")
    print("PASS empty-table embedding guard preserves ingestion and requires restart", flush=True)

    play("rollback", args.release, fixture, label="legacy-rollback")
    assert 'version="0.2.0"' in metrics()
    assert ssh("sudo cat /etc/systemd/system/catchup-worker-updater.service") == (ANSIBLE / "tests/legacy-updater.service").read_text().strip()
    assert sql("SELECT count(*) FROM story_search WHERE embedding IS NOT NULL") == "1"
    print("PASS legacy unit/config rollback preserves search data", flush=True)

    # Also exercise the non-legacy rollback path, including its own preflight.
    play("install", args.release, fixture, label="modern-baseline")
    target = ssh("sudo readlink /opt/search-hn/current")
    changed_config = OUTPUT / "changed-config.toml"
    changed_config.write_text(fixture.read_text().replace("realtime_workers = 2", "realtime_workers = 3"))
    play("install", args.release, changed_config, label="config-change")
    assert ssh("sudo readlink /opt/search-hn/current") != target
    play("rollback", args.release, fixture, label="toml-rollback")
    assert ssh("sudo readlink /opt/search-hn/current") == target
    assert "realtime_workers = 2" in ssh("sudo cat /opt/search-hn/current/worker.toml")
    print("PASS TOML rollback preflight and exact configuration restoration", flush=True)


if __name__ == "__main__":
    main()
