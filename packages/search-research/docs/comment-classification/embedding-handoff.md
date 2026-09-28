# Remaining-year embedding kickoff on Melchior

Prepared 2026-09-27. This handoff is **embedding only**: export, embed and verify
one yearly slice at a time. Do not run the resolver, NER, quick filter or paid
labelers as part of this kickoff. The preparation session did not launch the job
or start the GPU server.

Both checkouts are aligned on branch `codex/readable-source-titles`; the embedding
changes, research work, search-agent edits and handoff are committed and pushed
to origin. Melchior's previous
state is retained in the stash named `pre-embedding-sync-20260927`; do not apply
that whole stash over the newer checkout. Its previously remote-only `wild_audit.py`
and `wild_audit_cpu.py` scripts are included in the shared checkout. Superseded `docs/comment-embeddings.md`
and `docs/comment-explorer.md` remain recoverable in the stash; use the current
`comment-classification/` pages instead.

The 20 focused embedding, storage/resume and explorer compatibility tests passed
on both hosts. The documented loop passed `bash -n`, the remote CLI imports, and
the read-only PostgreSQL connection succeeds. GPU health must be checked after
starting the stopped server during kickoff.

## Scope and resource cost

- Repository: `/home/ritsuko/projects/data/search-hn` on `melchior`.
- Remaining years: **2007–2023, then 2026**. Use `data/comment-YEAR` for each.
- Already complete: 2024 has 3,117,812 comments / 3,117,906 vectors; 2025 has
  3,266,889 comments / 3,266,991 vectors. Both use storage format 2 and have
  `completed_rows == total_rows`. Prior full verification is recorded in
  [Corpus](corpus.md) and [Current pipeline](pipeline-current.md).
- Approximately 35M remaining comments: roughly **6–7 hours embedding**, plus
  export and verification, based on 2024/2025 throughput. Older years can differ.
  The older 91 GiB estimate for slices outside 2025 predates compact storage and
  includes 2024; treat it as planning headroom, not a measured new-format total.
  Melchior currently has 1.6 TB free. Keep the 5090 available for embedding;
  coordinate any competing NER/rerank job before starting.
- The 2026 slice is a **frozen partial-year snapshot**. Resume never appends new
  comments, even after the year ends. A later catch-up needs a separately designed
  date-range workflow or a new output directory; do not delete this snapshot.
- PostgreSQL access is read-only. Outputs are local research artifacts, not a
  production database migration or backup. There are no paid API calls here.

## Preflight in the kickoff session

Read this page and `corpus.md`. Both working trees were clean at handoff. Check
`git status` before pulling in case subsequent work has started; preserve any
new edits and do not overwrite `data/`.

```sh
cd /home/ritsuko/projects/data/search-hn
git log -1 --oneline
git status --short
df -h .
nvidia-smi
docker ps --format '{{.Names}} {{.Status}} {{.Ports}}'
systemctl --user list-units --all '*comment*' '*resolver*' --no-pager
loginctl show-user ritsuko -p Linger
```

At handoff, no Docker container or embedding/resolver writer was running. The
2025 explorer service was still running on CPU; it is not an embedding writer.
Recheck runtime state instead of assuming these observations remain current.

**Lingering was disabled.** At kickoff, enable it so the user manager and detached
job survive the last logout: `loginctl enable-linger ritsuko` (use administrator
approval if the host requires it), then verify `Linger=yes`. This persists beyond
this job and keeps the user's services eligible to run after logout. A transient
job still does not restart itself after reboot; use the resume procedure below.

The existing GPU environment includes optional research packages. Use
`uv run --no-sync --package search-research` here to preserve it; do not run an
unqualified `uv sync` that prunes those packages. The checked-in `uv.lock` matches
both hosts. A fresh embedding-only environment can use the locked package setup,
but rebuilding the existing GPU environment is not part of kickoff.

```sh
/usr/bin/uv run --no-sync --package search-research python \
  packages/search-research/tools/comment_slice.py --help
psql 'host=searchhn-pg dbname=searchhn_test user=readonly_hn_agent connect_timeout=5' \
  -X -v ON_ERROR_STOP=1 -c 'SELECT 1'
```

The read-only account uses existing pgpass. If a new query is expected to exceed
one minute, inspect its EXPLAIN first; the CLI `sql` action prints the year query.

## Start the existing pinned embedding service (kickoff session only)

```sh
docker compose -f data/comment-5090-tuning/compose.yaml up -d vllm
docker compose -f data/comment-5090-tuning/compose.yaml ps
curl --fail http://127.0.0.1:18080/health
curl --fail http://127.0.0.1:18080/v1/models
```

Wait for health success before launching the loop. The Compose file and image
are already present on Melchior. Do not substitute an image or production proxy.
The recipe is vLLM 0.28.0, BF16, mean pooling, model
`perplexity-ai/pplx-embed-v1-0.6b` revision
`2c4d510dd4a732063c31a0f70193e35067b51fd8`; served name `pplx-embed-v1-0.6b`.
Image digest:
`sha256:61fc8a896b0a4fbbbdc063bc4b0dbc25ce98e02b5050c24aeb7830ac02039b14`.
The raw loopback endpoint returns floats; the client applies tanh/round-to-int8.
The output is 1,024-dimensional native int8. Client defaults are batch 128,
concurrency 2, checkpoint 131,072 rows; keep these measured settings.

## Detached sequential run

Create this driver during kickoff. It stops on any failure and verifies each year
before advancing. Reusing the driver resumes completed checkpoints and checks
already-finished years without issuing new embedding calls. Each year retains
its own SQLite snapshot, tokenizer, vector file and append-only run log.

```sh
mkdir -p data/comment-remaining-years
cat > data/comment-remaining-years/run.sh <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
cd /home/ritsuko/projects/data/search-hn
for year in {2007..2023} 2026; do
  root="data/comment-$year"
  mkdir -p "$root"
  printf '%s year=%s start\n' "$(date -Is)" "$year"
  /usr/bin/uv run --no-sync --package search-research python \
    packages/search-research/tools/comment_slice.py run "$root" \
    --slice year --year "$year" --base-url http://127.0.0.1:18080 \
    --batch-size 128 --concurrency 2 --checkpoint-rows 131072 >> "$root/run.log" 2>&1
  /usr/bin/uv run --no-sync --package search-research python \
    packages/search-research/tools/comment_slice.py verify "$root" >> "$root/run.log" 2>&1
  /usr/bin/uv run --no-sync --package search-research python \
    packages/search-research/tools/comment_slice.py status "$root"
  printf '%s year=%s verified\n' "$(date -Is)" "$year"
done
SCRIPT
bash -n data/comment-remaining-years/run.sh
systemd-run --user --unit=searchhn-comment-remaining-years \
  --property=WorkingDirectory=/home/ritsuko/projects/data/search-hn \
  --property=StandardOutput=append:/home/ritsuko/projects/data/search-hn/data/comment-remaining-years/run.log \
  --property=StandardError=append:/home/ritsuko/projects/data/search-hn/data/comment-remaining-years/run.log \
  /usr/bin/bash /home/ritsuko/projects/data/search-hn/data/comment-remaining-years/run.sh
```

No Rust exporter, storage replacement, all-years FAISS index, or additional
parallel yearly jobs are needed. New exports already use compact format 2 and
batched tokenization. Existing formats 1 and 2 remain readable. The year selector
uses each comment's own calendar day, all reply depths, and no story-score gate;
`--score-gt` and `--top-k` do not restrict year slices.

## Monitor, stop, resume and finish

```sh
systemctl --user status searchhn-comment-remaining-years.service --no-pager
systemctl --user show searchhn-comment-remaining-years.service -p Result -p ExecMainStatus
tail -n 30 data/comment-remaining-years/run.log
# Replace YEAR with the year currently reported by the driver.
tail -n 20 data/comment-YEAR/run.log
/usr/bin/uv run --no-sync --package search-research python \
  packages/search-research/tools/comment_slice.py status data/comment-YEAR
```

During preparation, `index.sqlite` may not exist yet; inspect the log instead.
`completed_rows` is the durable boundary, not NPY length or a stale `running`
record. Do not run verification concurrently with its writer.

To stop: `systemctl --user stop searchhn-comment-remaining-years.service`.
Confirm it is inactive before resuming. After fixing an HTTP/service failure,
check health and rerun the same driver with a fresh unit name (for example,
`searchhn-comment-remaining-years-resume1`) and the same output/log paths.
Interrupted preparation restarts its temporary export; interrupted embedding
recomputes only the unfinished checkpoint. The existing directory lock rejects
competing writers. Corrupt committed vectors fail loudly; do not delete or reset
progress to bypass the error. HTTP errors are not automatically retried.

Completion requires a successful driver exit, a `verified` entry for every
requested year, and equal total/completed counts for all 18 slices. `verify`
checks input hashes, committed vector hashes and SQLite integrity; a successful
`run` alone is not the final check. Leave logs with the slices. After all checks,
stop the embedding Compose service to release the GPU if no other session needs
it: `docker compose -f data/comment-5090-tuning/compose.yaml down`.
Do not start downstream resolver or paid labeling stages without their own scope.
