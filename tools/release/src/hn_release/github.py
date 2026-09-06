"""GitHub is the release catalog; local files only stage a single build."""
import json
from pathlib import Path
import tempfile

from .build import digest, run


def catalog(root: Path) -> tuple[str, list[str]]:
    repo = run("gh", "repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner", cwd=root)
    tags = run("gh", "api", f"repos/{repo}/tags", "--paginate", "--jq", ".[].name", cwd=root).splitlines()
    releases = json.loads(run("gh", "release", "list", "--limit", "1000", "--json", "tagName", cwd=root))
    local = run("git", "tag", "--list", cwd=root).splitlines()
    return repo, sorted(set(tags + local + [r["tagName"] for r in releases]))


def verify_assets(directory: Path) -> dict:
    """Reject incomplete/tampered staging directories before publication or resume."""
    lines = (directory / "SHA256SUMS").read_text().splitlines()
    names = set()
    for line in lines:
        checksum, name = line.split("  ", 1)
        if Path(name).name != name:
            raise ValueError("Unsafe asset path in SHA256SUMS")
        assert digest(directory / name) == checksum, f"Checksum mismatch: {name}"
        names.add(name)
    manifest = json.loads((directory / "manifest.json").read_text())
    required = {"manifest.json", "notes.md", "build.log", manifest["archive"]}
    assert required <= names, f"Missing checksummed assets: {required - names}"
    assert digest(directory / manifest["archive"]) == manifest["archive_sha256"]
    return manifest


def publish(root: Path, output: Path) -> str:
    """Create a draft, verify downloaded assets, then publish. Safe to resume drafts.

    Existing published releases are never changed. A failed upload is recoverable
    from this exact directory; existing draft assets must match, never be clobbered.
    """
    manifest = verify_assets(output)
    tag, commit = manifest["tag"], manifest["commit"]
    rows = json.loads(run("gh", "release", "list", "--limit", "1000",
                          "--json", "tagName,isDraft", cwd=root))
    existing = next((r for r in rows if r["tagName"] == tag), None)
    if existing and not existing["isDraft"]:
        raise ValueError(f"{tag} is already published; refusing to modify it")
    refs = run("git", "ls-remote", "--tags", "origin", f"refs/tags/{tag}",
               f"refs/tags/{tag}^{{}}", cwd=root).splitlines()
    if refs:
        resolved = next((line.split()[0] for line in refs if line.endswith("^{}")), refs[0].split()[0])
        assert resolved == commit, f"Remote {tag} points at another commit"
    else:
        local = run("git", "tag", "--list", tag, cwd=root)
        if local:
            assert run("git", "rev-parse", f"{tag}^{{commit}}", cwd=root) == commit
        else:
            run("git", "tag", "-a", tag, commit, "-m", f"Search HN worker {tag}", cwd=root)
        run("git", "push", "origin", f"refs/tags/{tag}", cwd=root, capture=False)
    if not existing:
        run("gh", "release", "create", tag, "--verify-tag", "--draft",
            "--title", f"Search HN worker {tag}", "--notes-file", str(output / "notes.md"), cwd=root)
    asset_names = [line.split("  ", 1)[1] for line in (output / "SHA256SUMS").read_text().splitlines()]
    asset_names.append("SHA256SUMS")
    remote = json.loads(run("gh", "release", "view", tag, "--json", "assets", cwd=root))["assets"]
    uploaded = {a["name"] for a in remote}
    assert uploaded <= set(asset_names), "Draft has unexpected assets; inspect it manually"
    with tempfile.TemporaryDirectory(prefix="searchhn-download-") as temp:
        if uploaded:
            run("gh", "release", "download", tag, "--dir", temp, cwd=root)
            for name in uploaded:
                assert digest(Path(temp) / name) == digest(output / name), f"Draft asset differs: {name}"
        for name in asset_names:
            if name not in uploaded:
                run("gh", "release", "upload", tag, str(output / name), cwd=root, capture=False)
    with tempfile.TemporaryDirectory(prefix="searchhn-verify-") as temp:
        run("gh", "release", "download", tag, "--dir", temp, cwd=root)
        verify_assets(Path(temp))
    prerelease = "-" in manifest["version"]
    run("gh", "release", "edit", tag, "--draft=false",
        f"--prerelease={str(prerelease).lower()}",
        f"--latest={str(not prerelease).lower()}", "--notes-file", str(output / "notes.md"), cwd=root)
    return run("gh", "release", "view", tag, "--json", "url", "--jq", ".url", cwd=root)
