"""Check source isolation and detect corrupt artifacts before they reach GitHub."""
import tarfile

import pytest
import tomlkit

from hn_release.build import BINS, package, run, version_commit
from hn_release.github import verify_assets


@pytest.fixture
def repo(tmp_path):
    run("git", "init", cwd=tmp_path)
    run("git", "config", "user.name", "Release Test", cwd=tmp_path)
    run("git", "config", "user.email", "release-test@example.invalid", cwd=tmp_path)
    worker = tmp_path / "crates/catchup_worker"
    worker.mkdir(parents=True)
    (worker / "Cargo.toml").write_text('[package]\nname = "catchup_worker"\nversion = "0.2.0"\n')
    (tmp_path / "crates/Cargo.lock").write_text('version = 4\n[[package]]\nname = "catchup_worker"\nversion = "0.2.0"\n')
    migration = tmp_path / "crates/hn_core/migrations/001"
    migration.mkdir(parents=True)
    (migration / "up.sql").write_text("SELECT 1;\n")
    run("git", "add", ".", cwd=tmp_path)
    run("git", "commit", "-m", "baseline", cwd=tmp_path)
    return tmp_path


def test_release_commit_isolated_from_working_branch(repo):
    before = run("git", "rev-parse", "HEAD", cwd=repo)
    release = version_commit(repo, "0.2.1-canary.1", before)
    assert release != before
    assert run("git", "rev-parse", "HEAD", cwd=repo) == before
    assert not run("git", "status", "--porcelain", cwd=repo)
    manifest = tomlkit.parse(run("git", "show", f"{release}:crates/catchup_worker/Cargo.toml", cwd=repo))
    assert manifest["package"]["version"] == "0.2.1-canary.1"
    lock = tomlkit.parse(run("git", "show", f"{release}:crates/Cargo.lock", cwd=repo))
    assert lock["package"][0]["version"] == "0.2.1-canary.1"


def test_package_includes_migrations_and_detects_tampering(repo, tmp_path):
    output = tmp_path / "output"
    bins = tmp_path / "bins"
    output.mkdir()
    bins.mkdir()
    for name in BINS:
        (bins / name).write_text("test executable")
        (bins / name).chmod(0o755)
    (output / "notes.md").write_text("test notes")
    (output / "build.log").write_text("test log")
    commit = run("git", "rev-parse", "HEAD", cwd=repo)
    package(repo, output, "0.2.0", commit, bins, {"test": "fixture"}, "fixture")
    manifest = verify_assets(output)
    with tarfile.open(output / manifest["archive"]) as archive:
        assert archive.extractfile("migrations/001/up.sql").read() == b"SELECT 1;\n"
        assert archive.getmember("bin/catchup_worker").mode & 0o111
    (output / "notes.md").write_text("unexpected change")
    with pytest.raises(AssertionError, match="Checksum mismatch"):
        verify_assets(output)
