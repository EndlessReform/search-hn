"""Interactive release preparation; flags support reproducible dry runs and tests."""
import argparse
from pathlib import Path
import subprocess
import sys

import questionary
from rich.console import Console
from rich.table import Table

from .build import build, run, version_commit
from .github import catalog, publish, verify_assets
from .versions import choices

console = Console()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Plan only: no git/file/build/release mutations")
    parser.add_argument("--version", help="Select one of the offered versions without the picker")
    parser.add_argument("--notes-file", type=Path, help="Use reviewed notes (including externally LLM-drafted notes)")
    parser.add_argument("--yes", action="store_true", help="Accept build/publication confirmations")
    parser.add_argument("--resume", type=Path, help="Verify a completed build and resume its draft publication")
    args = parser.parse_args()
    root = Path(run("git", "rev-parse", "--show-toplevel", cwd=Path.cwd()))
    repo, tags, stable = catalog(root)
    if args.resume:
        output = args.resume.resolve()
        manifest = verify_assets(output)
        console.print(f"Resume {repo} {manifest['tag']} at {manifest['commit']}")
        if args.dry_run:
            return
        if args.yes or questionary.confirm("Verify remote assets and publish this draft?", default=False).ask():
            console.print(publish(root, output))
        return
    options = choices(tags, baseline=stable)
    console.print(f"[bold]{repo}[/bold] · stable v{stable}")
    if args.version:
        version = args.version.removeprefix("v")
        if version not in options.values():
            raise ValueError(f"Version is reserved or not an offered next version: {version}")
    else:
        label = questionary.select("Choose release:", choices=list(options)).ask()
        if label is None:
            return
        version = options[label]
    source = run("git", "rev-parse", "HEAD", cwd=root)
    dirty = run("git", "status", "--porcelain", "--untracked-files=normal", cwd=root)
    table = Table("Field", "Value")
    for key, value in [("Version", f"v{version}"), ("Source", source),
                       ("Target", "Debian 13 · linux/amd64 · native binaries"),
                       ("Publication", "GitHub prerelease" if "-" in version else "GitHub stable release"),
                       ("Working changes", "Present — commit before real release" if dirty else "None"),
                       ("Validation", "worker unit tests, locked build, binary identity, downloaded checksums")]:
        table.add_row(key, value)
    console.print(table)
    if args.dry_run:
        console.print("Dry run complete. No tag, commit, build, upload, or deployment performed.")
        return
    if dirty:
        raise ValueError("Commit the release source first; wizard builds an exact committed snapshot.")
    output = root / "dist/releases" / f"v{version}"
    if output.exists():
        raise ValueError(f"Staging directory exists: {output}. Use --resume for a completed build; inspect failed builds before removing.")
    if args.notes_file:
        notes = args.notes_file.read_text()
    else:
        if not run("git", "tag", "--list", f"v{stable}", cwd=root):
            run("git", "fetch", "origin", "tag", f"v{stable}", cwd=root)
        history = run("git", "log", "--format=- %s", f"v{stable}..{source}", cwd=root)
        draft = f"# Search HN worker v{version}\n\n{history}\n\nValidation: see attached manifest and build.log.\n"
        notes = questionary.text("Edit release notes (commit subjects are a draft):", default=draft, multiline=True).ask()
        if notes is None:
            return
    if not notes.strip():
        raise ValueError("Release notes cannot be empty")
    console.print(notes, markup=False)
    if not (args.yes or questionary.confirm("Build this release in isolation?", default=False).ask()):
        return
    output.mkdir(parents=True)
    (output / "notes.md").write_text(notes)
    commit = version_commit(root, version, source)
    console.print(f"Release commit: {commit} (your branch remains at {source[:12]})")
    build(root, output, version, commit)
    console.print(f"Build passed. Assets: {output}\nResume with: scripts/release --resume {output}")
    if args.yes or questionary.confirm("Publish verified assets to GitHub?", default=False).ask():
        console.print(publish(root, output))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, AssertionError, subprocess.CalledProcessError) as error:
        console.print(f"Release stopped: {error}", style="red", markup=False)
        sys.exit(1)
