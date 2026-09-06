"""Build immutable source snapshots and record exactly what was validated.

The release version is committed in a detached temporary worktree. The user's
branch and worktree are never bumped or rewritten. The published tag retains the
release commit; subsequent development can continue on the original branch.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile

import tomlkit

BUILDER = "search-hn/catchup-builder:debian13-amd64"
BINS = ("catchup_worker", "catchup_only", "backfill-story-id")


def run(*args: str, cwd: Path, capture: bool = True) -> str:
    """Run argument arrays, never interpolate notes or credentials into shell code."""
    result = subprocess.run(args, cwd=cwd, check=True, text=True,
                            stdout=subprocess.PIPE if capture else None)
    return result.stdout.strip() if capture else ""


def version_commit(root: Path, version: str, source: str) -> str:
    """Commit only the worker's Cargo manifest and lock entry in an isolated tree."""
    with tempfile.TemporaryDirectory(prefix="searchhn-release-") as temp:
        tree = Path(temp) / "source"
        run("git", "worktree", "add", "--detach", str(tree), source, cwd=root)
        try:
            manifest = tree / "crates/catchup_worker/Cargo.toml"
            data = tomlkit.parse(manifest.read_text())
            if data["package"]["version"] == version:
                return source
            data["package"]["version"] = version
            manifest.write_text(tomlkit.dumps(data))
            lock = tree / "crates/Cargo.lock"
            data = tomlkit.parse(lock.read_text())
            packages = [p for p in data["package"] if p["name"] == "catchup_worker"]
            assert len(packages) == 1, "Expected one worker lockfile entry"
            packages[0]["version"] = version
            lock.write_text(tomlkit.dumps(data))
            run("git", "add", "crates/catchup_worker/Cargo.toml", "crates/Cargo.lock", cwd=tree)
            run("git", "commit", "-m", f"release: catchup_worker v{version}", cwd=tree)
            return run("git", "rev-parse", "HEAD", cwd=tree)
        finally:
            run("git", "worktree", "remove", "--force", str(tree), cwd=root)


def package(root: Path, output: Path, version: str, commit: str,
            binaries: Path, validation: dict, builder: str) -> None:
    """Bundle native executables and canonical migrations; checksum every asset."""
    archive = output / f"search-hn-v{version}-linux-amd64.tar.gz"
    with tempfile.TemporaryDirectory(prefix="searchhn-package-") as temp:
        source_tar = Path(temp) / "migrations.tar"
        run("git", "archive", "--format=tar", f"--output={source_tar}", commit,
            "crates/hn_core/migrations", cwd=root)
        with tarfile.open(archive, "w:gz") as bundle:
            for name in BINS:
                bundle.add(binaries / name, arcname=f"bin/{name}")
            with tarfile.open(source_tar) as migrations:
                for member in migrations:
                    member.name = member.name.replace("crates/hn_core/", "", 1)
                    bundle.addfile(member, migrations.extractfile(member) if member.isfile() else None)
    manifest = {"version": version, "tag": f"v{version}", "commit": commit,
                "platform": "linux/amd64", "builder_image_id": builder,
                "archive": archive.name, "archive_sha256": digest(archive),
                "validation": validation}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (output / "SHA256SUMS").write_text("".join(
        f"{digest(path)}  {path.name}\n" for path in sorted(output.iterdir())
        if path.is_file() and path.name != "SHA256SUMS"))


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def build(root: Path, output: Path, version: str, commit: str) -> None:
    """Run unit tests, locked amd64 build, and binary identity check in Debian 13.

    A Docker volume caches compilation, not source. Full stdout/stderr remains
    visible and is retained as a release asset. No production database is contacted.
    """
    run("docker", "build", "--platform", "linux/amd64", "-t", BUILDER,
        "-f", "infra/build/debian13-catchup-only.Dockerfile", ".", cwd=root, capture=False)
    builder = run("docker", "image", "inspect", BUILDER, "--format={{.Id}}", cwd=root)
    with tempfile.TemporaryDirectory(prefix="searchhn-build-") as temp:
        source = Path(temp) / "source"
        bins = Path(temp) / "bin"
        source.mkdir()
        bins.mkdir()
        archive = Path(temp) / "source.tar"
        run("git", "archive", "--format=tar", f"--output={archive}", commit, cwd=root)
        with tarfile.open(archive) as tar:
            tar.extractall(source, filter="data")
        command = ["docker", "run", "--rm", "--platform", "linux/amd64",
                   "-e", f"SOURCE_COMMIT_HASH={commit}", "-e", "CARGO_BUILD_JOBS=8",
                   "-e", "CARGO_TARGET_DIR=/target", "-v", f"{source}:/src:ro",
                   "-v", f"{bins}:/out", "-v", "searchhn-release-target:/target",
                   "-v", "searchhn-release-cargo:/cargo/registry", "-w", "/src/crates",
                   builder, "bash", "-lc",
                   "set -euo pipefail; cargo test --locked -p catchup_worker --lib; "
                   "cargo build --release --locked -p catchup_worker --bins; "
                   "for bin in catchup_worker catchup_only backfill-story-id; do "
                   "cp /target/release/$bin /out/$bin; done; /out/catchup_worker --version"]
        with (output / "build.log").open("w") as log:
            process = subprocess.Popen(command, cwd=root, text=True, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)
            assert process.stdout is not None
            for line in process.stdout:
                print(line, end="", flush=True)
                log.write(line)
            if process.wait() != 0:
                raise RuntimeError(f"Build failed. Log retained at {output / 'build.log'}")
        identity = run("docker", "run", "--rm", "--platform", "linux/amd64",
                       "-v", f"{bins}:/out:ro", builder, "/out/catchup_worker", "--version", cwd=root)
        assert f"{version}+{commit[:12]}" in identity, f"Unexpected binary identity: {identity}"
        package(root, output, version, commit, bins,
                {"unit_tests": "passed", "locked_release_build": "passed",
                 "binary_identity": identity, "integration_tests": "not run by release builder"}, builder)
