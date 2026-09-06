# Release slice verification — 2026-09-06

- WIP checkpoint before release work: `7d9f4eb`.
- Live metric checked before and after: worker `0.2.0`, commit `511a6e0c77f6`.
- Retroactive [v0.2.0 release](https://github.com/EndlessReform/search-hn/releases/tag/v0.2.0)
  points to `511a6e0c77f6aae29a2cffe039fea9b825b63f20`.
  Rebuilt from that exact source in Debian 13 amd64: 46 worker library tests passed;
  locked release build passed; executable reported the expected version/commit.
  Archive includes all three worker binaries and that commit's migrations.
  Uploaded assets were downloaded and checksummed before publication. Release
  notes distinguish the rebuild from a byte-identical copy of the live executable.
- Interactive terminal dry run: arrow-key picker selected `0.2.1-canary.1`, displayed
  the source/target/validation plan, and exited without source/build/release changes.
- Real canary: `v0.2.1-canary.1`, isolated release commit
  `54a5a54d93660423d16c3039e3cdb741af391869`, built from `b00b1b3`.
  47 worker library tests passed, locked amd64 release build passed, executable
  reported `0.2.1-canary.1+54a5a54d93660423d16c3039e3cdb741af391869`.
  All five assets uploaded/downloaded/verified; GitHub marked it a prerelease,
  and `/releases/latest` still returned `v0.2.0`.
- Publication recovery: an attempt to resume the published canary was rejected.
  Then the temporary canary was returned to draft and resumed successfully,
  verifying its already-uploaded assets without overwriting them.
- Removed the temporary release, remote/local tag, and canary staging directory.
  Verified GitHub and remote tags contain only `v0.2.0`. The working branch was
  never version-bumped or switched by the wizard.
- Seven release-tool regression tests pass: semver ordering, prerelease series,
  published-baseline selection, source isolation, archive contents, and corruption
  detection. A final `0.2.1-pre.1` dry run passed after cleanup.

Failures caught during the rehearsal:

- The existing builder lacked `libsqlite3-dev`, needed to link unit tests. Added
  the build dependency; historical application source was not modified.
- The first canary stopped on the existing 100 ms inactivity test under parallel
  amd64-emulated test execution. No canary was published from that attempt. The
  release builder now runs the full library suite serially; the suite passed.
  No tests were skipped or automatically retried. The failed log was retained in
  `/tmp/searchhn-canary-first-failed-build/build.log` for this session.

This verifies release creation and recovery, not deployment. No production
configuration, database migration, service restart, or Ansible change was performed.
PostgreSQL integration tests are not part of this release builder's test claim.
