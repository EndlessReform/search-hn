# Artifact releases: Garage is canonical

Endpoint: `http://magi06-storage.tail7a3eb.ts.net:3900` (private Tailscale network).
Bucket: `searchhn-data`. Credentials remain in `packages/search-research/.env`:
`SEARCHHN_EVAL_DATA_BUCKET`, `SEARCHHN_GARAGE_BASE_URL`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`; optional `AWS_DEFAULT_REGION` defaults to `garage`.
Never commit that file. boto3 uses signed, path-style requests.

## Current research handoff

The current five-page whitepaper and Typst source are in
`research-20260906-paper-v2`. This editorial revision uses abstract, introduction,
methods, evaluation, and conclusion; the experiment results are unchanged.
All three files (176,590 bytes) were downloaded and SHA256-verified.
Manifest SHA256: `177b04ad0fbb23bc2c0ca0be9227a7173f3b564939f4bc57440d472c04024ca1`.
Restore this release for the current paper; the earlier closeout preserves the
original six-page presentation.

`research-20260906-v4` is the complete experiment-evidence release: **8,987 files,
871,885,024 logical bytes**, with 8,922 distinct objects downloaded and SHA256
verified on 2026-09-06 before local cleanup. Manifest SHA256:
`404830d1204129aff6866cb52d5061de2be9fd8704138d6660675d4b242fb136`.

The companion `research-20260906-closeout` preserves the final whitepaper/source
snapshot and cleanup receipts. See [closeout](26-research-closeout.md). Source was
uncommitted at publication and is explicitly marked dirty; frozen source files
are included rather than falsely claiming a clean commit reproduces every run.

Companion verification: 163 files, 1,587,840 logical bytes, 155 distinct blobs
downloaded and SHA256-checked. Manifest SHA256:
`ce39eb92f30830cc4dd836b9a59caef694342b99556721c6c6a9c6316af44f4f`.

V4 includes all current rollouts, eval/corpus sets, scores, sweeps, model recipes,
serving/latency evidence, and selected Pplx vectors. Rejected/control embedding
arrays and rebuildable databases/installers were intentionally omitted from the
new release; every exclusion is recorded. Older immutable releases are untouched;
TE3 reference arrays remain in v3 and can be restored separately when needed.

```sh
uv run --locked --package search-research python -m search_research.artifacts restore research-20260906-paper-v2 --destination /absolute/path/to/paper-restore
uv run --locked --package search-research python -m search_research.artifacts restore research-20260906-v4
uv run --locked --package search-research python -m search_research.artifacts restore research-20260906-closeout --destination /absolute/path/to/closeout-restore
```

The source snapshot and paper are under `data/research-closeout-20260906/` after
restore. The original `artifacts publish` command covers only historical roots;
for this closeout, the explicit policy and byte-verifier are in
`tools/research_closeout.py`. Do not use the old publisher expecting sovereign
runs to be included automatically.

## Historical releases


- `research-20260904-v3`: v2 plus Phase 0 review packets, four Luna annotation
  shards, validated per-case judgments and the full audit report. V3 is the historical initial-study handoff; v2 remains the unchanged
  rollout/efficiency baseline.
  1,680 files, 2,157,854,526 logical bytes; clean source commit `a82a219`.
  Manifest SHA256: `407b785146fd703858df0dae33449ffe0c75f667832495011eb7ae585ee699d0`.

- `research-20260904-v1`: completed rollouts, recovered original provenance,
  corpus/vectors and reports; 1,663 files, 2,155,462,497 logical bytes.
  Manifest SHA256: `cef1a39112e14d9c7ef701632ccd87e14658bc79b3aac41e5eb870ebe190f8ce`.
- `research-20260904-v2`: the same frozen experiments plus paired efficiency and
  entity/paraphrase analysis. This is the frozen rollout/efficiency comparison.
  Blobs shared with v1 are reused, not uploaded twice.
  1,667 files, 2,155,477,791 logical bytes; clean source commit `73e93ee`.
  Manifest SHA256: `4a44b19eeab5d067d7f731c2a8bed1fcaea410133cc0a9c60323c408ea81ed12`.

## Storage contract

`releases/<version>/manifest.json` is the canonical, versioned inventory.
`blobs/sha256/<digest>` holds each unique file. The manifest records byte length,
SHA256, original relative path, creation time, source Git revision and dirty flag.
It is published **last**, with a conditional create. A retry of an interrupted
upload reuses existing blobs; an existing release name cannot be overwritten.
This is application-level versioning, not a claim of S3 Object Lock or bucket
versioning. Do not mutate/delete published objects; protect that operationally
with bucket permissions/backups. One bucket alone is not an independent backup.

The initial release covers four named experiment trees: original FTS, TE3 dense/
DuckDB, PG engine/reranker, and fresh Luna. It preserves the frozen eval sets,
generation/review evidence (including the original results tree recovered from
melchior), all local rollout attempts
(including infrastructure failures), captured tool/model context, corpus snapshots,
embedding shards, configs, metrics and offline explorers. The manifest explicitly
lists exclusions: rebuildable DuckDB databases, installers/binaries, logs and other
non-allowlisted formats. It is not a full production DB backup. Historical source
fingerprints remain in journals; the publication commit does not retroactively
claim to be the exact code used for every older attempt.

## Commands

```sh
# Restore into a new checkout's data/ tree; no DB/API access needed.
uv run --locked --package search-research python -m search_research.artifacts restore research-20260904-v3
# Verify local files against the remote manifest without downloading replacements.
uv run --locked --package search-research python -m search_research.artifacts verify research-20260904-v3
# Optional independent restore directory, preserving all relative paths:
uv run --locked --package search-research python -m search_research.artifacts restore research-20260904-v3 --destination /absolute/path/to/restore
# Future release: stop writers, regenerate reports, review inventory, commit code.
uv run --locked --package search-research python -m search_research.artifacts publish research-YYYYMMDD-v2
```

Restore refuses to overwrite differing local files. Interrupted downloads use a
`.download` file, then SHA256 verification before rename. Publication verifies
remote sizes and hash metadata; restore verifies actual downloaded bytes. A new
run must use a fresh output directory or the explicitly documented resume path,
then publish a new release. Keep old releases for comparison; do not edit labels
after viewing retrieval results.

Reports can be regenerated offline from restored traces. New retrieval replay
requires rebuilding the scratch database from the included corpus/embeddings;
see the numbered experiment recipes. Fresh trajectories require model access and
currently live comment access; they are not bitwise deterministic replays.
