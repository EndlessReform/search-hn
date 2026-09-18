# App release wiring and active staging guard — September 9, 2026

Implemented locally, not published or deployed to production. No schema, source
trigger, database grant, extension or backup/restore procedure changed.

- Release builder tests/builds `hn_app` alongside the worker; the existing archive
  includes all four binaries. Both main executables have checked version/commit
  identity. Cargo workspace version remains unchanged at 0.3.1.
- `app-install.yml` upgrades the existing app binary/unit contract independently
  of the worker; it checks the running executable and hybrid search, and restores
  the saved binary after failed activation. It does not provision hosts or secrets.
- Worker staging refuses an active candidate missing either its manifest or its
  checksums. This resolves the corresponding September 7 worktree-audit finding.

## Verification

- `cargo test --manifest-path crates/Cargo.toml --locked -p hn_app`: 13 passed.
- Release pytest suite: 8 passed, including explicit app archive membership and
  executable permissions, checksum tampering, and workspace version inheritance.
- Ansible guard regression: five actual task executions in temporary directories.
  Missing manifest, missing checksums and both missing on an active target are
  refused; a complete active target and an incomplete inactive target are allowed.
- Actual app playbook rehearsed on `searchhn-deploy-test@orb` (Debian 13 amd64)
  with a synthetic native C executable and isolated service/port/path. Normal
  install passed; repeat install preserved PID; candidate returning keyword-only
  failed the hybrid check, restored the prior executable and recovered health.
  Fixture was removed. GitHub download was replaced by a local artifact-copy shim;
  actual checksum verification, extraction, copy, systemd and health tasks ran.
- Logs: ignored `infra/ansible/test-output/app-*.log`; reproducible harnesses in
  `infra/ansible/tests/rehearse-app.py` and `staging-guard.yml`.

A full combined Debian release build and verification of the real production app
service/environment remain part of the next release/deployment step. The synthetic
rehearsal establishes installer behavior, not real database/inference availability.
The broader worker credential/preflight/rollback rehearsal was not repeated.
