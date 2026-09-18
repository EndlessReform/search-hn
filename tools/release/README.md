# Search HN release wizard

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
the shared Rust workspace version changed in Cargo.toml/Cargo.lock, builds that exact snapshot,
and tags it after validation. Your working branch is not switched or rewritten.
The release commit is visible through its tag on GitHub. Build logs, a manifest,
checksums, notes, and the binary/migrations archive are attached to the release.

Validation runs worker library unit tests serially (for timing tests under amd64
emulation), app unit tests, a locked Linux release build of both app and worker,
version/commit checks for both executables, and verification of assets downloaded from GitHub. This is
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

## One Rust workspace version

`crates/Cargo.toml` owns `[workspace.package].version`; all four crates inherit it
with `version.workspace = true`. Internal `hn_core` dependencies inherit a shared
path dependency, so they do not carry stale independent version requirements.
The baseline is 0.3.1, matching the existing latest release tag; this change does
not publish a new release or change deployed binaries.

The wizard changes that one workspace value in its detached release snapshot,
checks every member inherits it, and runs `cargo update --workspace --offline`
to update local package lock entries while retaining locked registry dependencies.
The working branch stays untouched by a release, as before; its version identifies
its development baseline, while the tagged snapshot carries the released version.
Cargo must be installed on the release controller for lockfile reconciliation.

The embedding proxy's publisher resolves the inherited version into the temporary
standalone build manifest. Its container remains independently deployable. Python
tool/research package metadata is outside this Cargo version mechanism.

The archive includes `catchup_worker`, `catchup_only`, `backfill-story-id`, and
`hn_app`, plus the canonical migrations. This increases release build/test work;
it does not couple service restarts. Use `infra/ansible/install.yml` for the worker
and `infra/ansible/app-install.yml` for the app, selecting the same release version.
The embedding proxy container still uses its existing publisher.
