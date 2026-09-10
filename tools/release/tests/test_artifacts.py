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
    crates = tmp_path / "crates"
    crates.mkdir()
    names = ["catchup_worker", "hn_core", "hn_app", "embedding_proxy"]
    (crates / "Cargo.toml").write_text(
        '[workspace]\nmembers = ["catchup_worker", "hn_core", "hn_app", "embedding_proxy"]\n'
        'resolver = "2"\n[workspace.package]\nversion = "0.2.0"\n'
        '[workspace.dependencies]\nhn_core = { path = "hn_core" }\n'
    )
    for name in names:
        member = crates / name
        (member / "src").mkdir(parents=True)
        dependencies = "" if name == "hn_core" else "[dependencies]\nhn_core.workspace = true\n"
        (member / "Cargo.toml").write_text(
            f'[package]\nname = "{name}"\nversion.workspace = true\nedition = "2021"\n{dependencies}'
        )
        (member / "src/lib.rs").write_text("")
    run("cargo", "generate-lockfile", "--offline", cwd=crates)
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
    manifest = tomlkit.parse(run("git", "show", f"{release}:crates/Cargo.toml", cwd=repo))
    assert manifest["workspace"]["package"]["version"] == "0.2.1-canary.1"
    lock = tomlkit.parse(run("git", "show", f"{release}:crates/Cargo.lock", cwd=repo))
    assert len(lock["package"]) == 4
    assert {p["version"] for p in lock["package"]} == {"0.2.1-canary.1"}
    assert version_commit(repo, "0.2.1-canary.1", release) == release


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
        assert archive.getmember("bin/hn_app").mode & 0o111
    (output / "notes.md").write_text("unexpected change")
    with pytest.raises(AssertionError, match="Checksum mismatch"):
        verify_assets(output)


def test_release_refuses_independently_versioned_member(repo):
    manifest = repo / "crates/hn_app/Cargo.toml"
    manifest.write_text(manifest.read_text().replace("version.workspace = true", 'version = "0.2.0"'))
    run("git", "add", ".", cwd=repo)
    run("git", "commit", "-m", "drift", cwd=repo)
    source = run("git", "rev-parse", "HEAD", cwd=repo)
    with pytest.raises(AssertionError, match="hn_app must inherit"):
        version_commit(repo, "0.2.1", source)
    assert run("git", "rev-parse", "HEAD", cwd=repo) == source
