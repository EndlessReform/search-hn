"""Rehearse the actual app installer using synthetic binaries on the disposable host.

Uses a unique service/path/port, not the existing worker, app or database. GitHub
is replaced by a local artifact-copy shim; checksums and extraction still execute.
Run from the repo root with uv run infra/ansible/tests/rehearse-app.py.
"""
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[3]
HOST = "searchhn-deploy-test@orb"
REMOTE = "/tmp/searchhn-app-install-rehearsal"
SERVICE = "searchhn-app-install-rehearsal.service"
OUTPUT = ROOT / "infra/ansible/test-output"


def run(*args, **kwargs):
    return subprocess.run(args, text=True, check=True, capture_output=True, **kwargs).stdout.strip()


def ssh(command):
    return run("ssh", HOST, command)


def main():
    assert ssh("hostname") == "searchhn-deploy-test"
    # Refuse stale fixtures: a previous interrupted run must be inspected first.
    ssh(f"test ! -e {REMOTE}")
    OUTPUT.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="searchhn-app-rehearse-") as temp:
        local = Path(temp)
        try:
            ssh(f"mkdir {REMOTE}")
            run("scp", str(Path(__file__).with_name("app-fixture.c")), f"{HOST}:{REMOTE}/fixture.c")
            for name, version, mode in [("old", "0.0.0", "hybrid"), ("good", "0.0.1", "hybrid"), ("bad", "0.0.2", "keyword-only")]:
                ssh(f"cc -DVERSION='\"{version}\"' -DMODE='\"{mode}\"' {REMOTE}/fixture.c -o {REMOTE}/{name}")
            ssh(f"cp {REMOTE}/old {REMOTE}/hn_app")
            unit = local / SERVICE
            unit.write_text(f"[Unit]\nDescription=Isolated app installer fixture\n[Service]\nExecStart={REMOTE}/hn_app\n")
            run("scp", str(unit), f"{HOST}:{REMOTE}/{SERVICE}")
            ssh(f"sudo cp {REMOTE}/{SERVICE} /etc/systemd/system/{SERVICE} && sudo systemctl daemon-reload && sudo systemctl start {SERVICE}")
            inventory = local / "inventory.json"
            inventory.write_text(json.dumps({"all": {"children": {"searchhn_apps": {"hosts": {"fixture": {"ansible_host": "orb", "ansible_user": "searchhn-deploy-test", "app_binary": f"{REMOTE}/hn_app", "app_service": SERVICE, "app_health_url": "http://127.0.0.1:18883"}}}}}}))
            shim = local / "gh"
            shim.write_text("#!/bin/sh\nset -eu\nwhile [ \"$1\" != --dir ]; do shift; done\ncp \"$APP_TEST_ASSETS/\"* \"$2/\"\n")
            shim.chmod(0o755)
            env = dict(os.environ, PATH=f"{local}:{os.environ['PATH']}", ANSIBLE_LOCAL_TEMP=str(local / "ansible"))

            def install(name, version, success):
                assets = local / name
                assets.mkdir(exist_ok=True)
                binary = local / "hn_app"
                run("scp", f"{HOST}:{REMOTE}/{name}", str(binary))
                archive = assets / f"search-hn-v{version}-linux-amd64.tar.gz"
                with tarfile.open(archive, "w:gz") as tar:
                    tar.add(binary, arcname="bin/hn_app")
                manifest = {"tag": f"v{version}", "version": version, "commit": "fixture", "platform": "linux/amd64", "archive": archive.name}
                (assets / "manifest.json").write_text(json.dumps(manifest))
                (assets / "SHA256SUMS").write_text("".join(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n" for p in [archive, assets / "manifest.json"]))
                result = subprocess.run(["ansible-playbook", "-i", str(inventory), str(ROOT / "infra/ansible/app-install.yml"), "-e", f"release_version=v{version}"], env=dict(env, APP_TEST_ASSETS=str(assets)), text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                label = name + ("-repeat" if (OUTPUT / f"app-{name}.log").exists() else "")
                (OUTPUT / f"app-{label}.log").write_text(result.stdout)
                assert (result.returncode == 0) == success, result.stdout[-6000:]

            install("good", "0.0.1", True)
            pid = ssh(f"systemctl show {SERVICE} --property=MainPID --value")
            install("good", "0.0.1", True)
            assert ssh(f"systemctl show {SERVICE} --property=MainPID --value") == pid
            install("bad", "0.0.2", False)
            assert ssh(f"{REMOTE}/hn_app --version") == "hn_app 0.0.1+fixture"
            assert ssh(f"sha256sum {REMOTE}/hn_app").split()[0] == ssh(f"sha256sum {REMOTE}/good").split()[0]
            print("PASS: install, unchanged PID on repeat, failed hybrid check restores previous executable")
        finally:
            ssh(f"sudo systemctl stop {SERVICE} || true")
            ssh(f"sudo rm -f /etc/systemd/system/{SERVICE} && sudo systemctl daemon-reload && sudo rm -rf {shlex.quote(REMOTE)}")


if __name__ == "__main__":
    main()
