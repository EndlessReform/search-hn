# Agent main-branch synchronization — September 7, 2026

Fetched GitHub `main` at `2511d23673918944a2ed1461ca7639a46248c662` (PR #18,
merged September 4). Merged it into `fts-improvement-spike` as `6333e07`, whose
parents are the previous branch head `a44c788` and that main commit. Local
`origin/main` had been stale at `8682850` before this fetch.

Resolved overlapping CLI, Textual, prompt, tool-payload and test changes. The
provider/model modal, presets, prompt history, webpage tools and approval handling
are present alongside the branch's shared runtime. Provider switching now supplies
an explicit per-run client: using the incoming SDK-global client setter would have
left this branch's isolated runtime on its old provider. Fresh, approval-resumed
and rejection-summary turns receive the selected run configuration.

Restored the previously uncommitted hybrid work after the merge and resolved its
four overlapping files. It remains uncommitted, as before. Checked all 13 saved
non-agent tracked files and all 25 saved untracked files byte-for-byte against the
safety stash before writing this synchronization note. No unrelated saved work was
lost. The safety stash `pre-main-sync-2026-09-07 hybrid agent and deployment work`
remains available.

The hybrid prompt retains sentence-like unfiltered queries as its default, with
filters used for required constraints. Webpage instructions now appear only when
that capability is enabled. Production retrieval, publisher-policy payload tags,
rank metadata and stable pagination coexist.

Validation: `uv run --no-sync --package search-agent pytest packages/search-agent/tests -q`
reported **130 passed, 2 skipped** (optional live-database tests). This includes
Textual pilot coverage that opens `/model`, switches the active client and checks
the per-run provider, plus prompt history, approvals, web safety, runtime and
hybrid tests. Changed agent files passed Ruff and scoped whitespace checks;
`search-agent --help` exposes both provider configuration and hybrid flags.
`git merge-base --is-ancestor origin/main HEAD` succeeded and no unmerged paths
remain. No database migration, service restart, deployment or push was performed.
