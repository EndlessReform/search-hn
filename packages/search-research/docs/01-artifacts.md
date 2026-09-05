# Artifact releases: Garage is canonical

Endpoint: `http://magi06-storage.tail7a3eb.ts.net:3900` (private Tailscale network).
Bucket: `searchhn-data`. Credentials remain in `packages/search-research/.env`:
`SEARCHHN_EVAL_DATA_BUCKET`, `SEARCHHN_GARAGE_BASE_URL`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`; optional `AWS_DEFAULT_REGION` defaults to `garage`.
Never commit that file. boto3 uses signed, path-style requests.

Releases:

- `research-20260904-v1`: completed rollouts, recovered original provenance,
  corpus/vectors and reports; 1,663 files, 2,155,462,497 logical bytes.
  Manifest SHA256: `cef1a39112e14d9c7ef701632ccd87e14658bc79b3aac41e5eb870ebe190f8ce`.
- `research-20260904-v2`: the same frozen experiments plus paired efficiency and
  entity/paraphrase analysis. Use this release for the final conversation handoff.
  Blobs shared with v1 are reused, not uploaded twice.

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
uv run --locked --package search-research python -m search_research.artifacts restore research-20260904-v2
# Verify local files against the remote manifest without downloading replacements.
uv run --locked --package search-research python -m search_research.artifacts verify research-20260904-v2
# Optional independent restore directory, preserving all relative paths:
uv run --locked --package search-research python -m search_research.artifacts restore research-20260904-v2 --destination /absolute/path/to/restore
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
