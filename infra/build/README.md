# Debian 13 Build Artifacts

This directory contains host-side tooling to produce `catchup_worker` Linux binaries
that are ABI-compatible with Debian 13 (trixie) targets, such as your Debian LXC.

## Why this exists

Building on a newer or different host can produce binaries that require newer glibc
symbols than your deploy target provides. These scripts force compilation inside a
Debian 13 userspace while still using all CPU cores on the build host.

The source tree is mounted read-only in the builder container to keep builds
reproducible and avoid accidental host mutations. Cargo output is redirected to
`/tmp/cargo-target` inside the container so build artifacts remain writable.

The build script resolves `git rev-parse HEAD` on the host and passes that value
as `SOURCE_COMMIT_HASH` into the container build, so built binaries show
`0.1.0+<commit>` instead of `+unknown` for normal git checkouts.

## Files

- `debian13-catchup-only.Dockerfile`
  - Builder image based on `debian:trixie-slim`.
  - Installs toolchain dependencies including `libssl-dev` and `libpq-dev`.
- `build-catchup-only-debian13.sh`
  - Builds the builder image.
  - Compiles all `catchup_worker` package binaries with `cargo build --release --locked --bins`.
  - Exports binaries to a host output directory.
- `test-build-catchup-only-debian13.sh`
  - Smoke test for Dockerfile deps and build-script command wiring.

## Usage

From repo root:

```bash
infra/build/build-catchup-only-debian13.sh
```

Default output path:

```text
dist/debian13/catchup_worker
dist/debian13/catchup_only
dist/debian13/backfill-story-id
```

Common options:

```bash
infra/build/build-catchup-only-debian13.sh \
  --out-dir ./dist/debian13 \
  --jobs 32
```

Dry run (prints commands only):

```bash
infra/build/build-catchup-only-debian13.sh --dry-run
```

Notes:

- The build script does not run `chmod` on the output binary.
- This is intentional for compatibility with mounted output directories; `scp`-based deploys work fine as-is.

## Test

Run the smoke test:

```bash
infra/build/test-build-catchup-only-debian13.sh
```

This smoke test does not run Docker builds; it verifies:

- Dockerfile base image and key packages (`libssl-dev`, `libpq-dev`)
- build script emits the expected locked cargo release command for all binaries
- build script uses writable `CARGO_TARGET_DIR=/tmp/cargo-target`
- build script resolves and injects `SOURCE_COMMIT_HASH`
