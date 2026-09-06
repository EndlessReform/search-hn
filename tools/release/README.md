# Worker release wizard

From the repository root, run `./scripts/release`. This builds locally with
OrbStack/Docker and publishes native Debian 13 amd64 binaries to GitHub Releases.
It does not deploy, call the live database, or require GitHub Actions.

The picker shows the stable baseline and computed major/minor/patch versions.
Each bump also offers `canary.N` and `pre.N`; existing series can be continued or
promoted. Prereleases do not become GitHub's latest stable release. Release notes
start from commit subjects and are editable in the terminal. An LLM can prepare a
notes file externally and pass it with `--notes-file`; no model service is required.

```bash
./scripts/release --dry-run
./scripts/release
```

The real run requires committed source. It creates a detached release commit with
the worker version changed in Cargo.toml/Cargo.lock, builds that exact snapshot,
and tags it after validation. Your working branch is not switched or rewritten.
The release commit is visible through its tag on GitHub. Build logs, a manifest,
checksums, notes, and the binary/migrations archive are attached to the release.

Validation runs worker library unit tests, a locked Linux release build, a binary
version/commit check, and verification of assets downloaded from GitHub. This is
not a substitute for PostgreSQL integration or deployment rehearsal; the manifest
explicitly records that limitation.

Interrupted publication leaves a draft and local verified assets. Resume with:

```bash
./scripts/release --resume dist/releases/v0.2.1-canary.1
```

Resume refuses changed checksums, mismatched tags/assets, and already published
releases. It uploads missing draft assets without overwriting existing ones. A
failed build has its log retained but cannot be resumed as a successful build:
inspect/remove that staging directory and rerun. Credentials use existing `gh`
authentication and Git SSH configuration. Build cache volumes contain no credentials.

For repeatable automation/tests, `--version`, `--notes-file`, and `--yes` bypass
interactive selections but retain version validation and artifact checks. `--dry-run`
reads Git/GitHub only and performs no release/build/source mutations.

Tests: `PYTHONPATH=tools/release/src uv run --locked --project tools/release pytest tools/release/tests`.
