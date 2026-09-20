# Corpus ingest, embeddings, and durable storage

[Workflow overview](README.md) · [Usage](usage.md) · [Search index](explorer.md#scoring-and-joins)

The current completed slice is `data/comment-2025/` on melchior: **3,266,889
comments and 3,266,991 chunks**, embedded in **36.50 minutes**. This page preserves
the earlier sizing work and explains extraction, checkpointing, and recovery.
FAISS is built in memory by the explorer; the durable vector artifact is NPY.

## Measured sizing facts

Measured on melchior's RTX 5090 (32 GB VRAM), using the completed top-three
comments on stories with score >100 slice and batch size 128 / concurrency 2:

| Quantity | Observed value |
| --- | ---: |
| Comments / embedding chunks | 610,947 / 610,968 |
| Total input tokens across chunks | 83,915,604 |
| Embedding run wall time, including vector checkpoints | 516.055 seconds (8.60 minutes) |
| Comments per second | **1,183.88** |
| Input tokens per second | **162,609.87** |
| Input tokens per minute | **9,756,592 (9.76 million)** |
| Full-slice input tokens per comment, including chunking | **137.35** |
| Independent 50,000-comment sample: mean tokens/comment | **137.20** |
| Sample median / p90 / p95 / p99 | 109 / 281 / 344 / 473 tokens |
| Sample maximum | 1,670 tokens |

For rough scaling **at this same length distribution and measured throughput**:
one million comments is approximately **137.35 million input tokens** and
**14.08 minutes of embedding**; ten million is approximately **1.374 billion
tokens** and **2.35 hours**. More generally, estimated tokens = comment count ×
137.35, and embedding minutes = estimated tokens / 9,756,592. Preparation/export,
final verification, and subsequent indexing are separate from this embedding time.
These are input-token counts, including tokenizer special tokens, not padded GPU
batch positions or generated tokens. Longer-input distributions can change speed.

The sample check used Hugging Face **Transformers 5.17.0** `AutoTokenizer` with
`perplexity-ai/pplx-embed-v1-0.6b`, pinned revision
`2c4d510dd4a732063c31a0f70193e35067b51fd8` (resolved tokenizer class
`Qwen2Tokenizer`). It tokenized decoded comment bodies with
`add_special_tokens=True`, `truncation=False`, and `padding=False`. The 50,000 IDs
were sampled uniformly without replacement from sorted frozen comment IDs using
NumPy `default_rng(20260919).choice(..., size=50000, replace=False)`. Their total was
6,859,942 tokens; every sampled count matched the stored per-comment sum of chunk
token counts. This sample contained no comments requiring chunking. The full-slice
mean above uses all saved chunk counts, rather than extrapolating from the sample.

The throughput denominator is the run log's `embedding_complete.seconds`;
numerators come from SQLite `count(*) FROM comments` and `sum(tokens) FROM inputs`.
The frozen slice is representative of **this top-comment selection**, not a random
sample of all HN comments. A wider-corpus token estimate should use its own length
sample rather than treating 137.35 as a universal average.

### All of 2025: measured count and projected backfill size

On 2026-09-19, the mirror contained **3,266,889 eligible comments** whose own
`day` falls in `[2025-01-01, 2026-01-01)`, at every reply depth and without a story
score gate. This exact SQL count excludes dead/deleted and blank HTML bodies;
decoded-empty bodies would additionally be excluded during preparation.

A uniform `ORDER BY random() LIMIT 50000` sample of that population contained
50,000 decoded usable comments, 50,002 chunks, and 3,765,437 model tokens:
**75.31 tokens/comment**, substantially shorter than the top-comment selection.
It used the same pinned model tokenizer, HTML decoding, 2048-token chunking, and
SQLite writer as the backfill. Its SQLite file occupied 79,790,080 bytes. This is
a sizing sample at `data/comment-2025-sizing-sample/`, not a completed yearly
embedding slice.

| Whole-year quantity | Estimate from the sample |
| --- | ---: |
| Input tokens | **246.0 million** |
| Vector chunks | **3.267 million** |
| Native int8 NPY | **3.12 GiB** |
| SQLite text, metadata, and row mappings | **4.86 GiB** |
| Combined persistent output | **7.97 GiB** (8.56 decimal GB) |
| Embedding alone | **about 36 minutes** |
| Preparation + embedding + verification | **roughly 45–60 minutes**, not a full-run measurement |

Two warmed inference trials over 8,192 sampled inputs (614,803 tokens), with the
same batch size 128 and concurrency 2, took 5.403 and 5.388 seconds: **1,516–1,521
chunks/second**, or **6.83–6.85 million tokens/minute**. These include HTTP transfer
and quantization, but not vector persistence. Shorter comments increased comments
per second while reducing tokens per minute; using the top-comment token rate
would therefore underestimate yearly embedding time. The 36-minute projection
uses the observed yearly-sample rate, not the top-comment rate.

The full population count took 14.2 seconds, the sample SQL query 20.5 seconds,
and local sample decoding/tokenization/SQLite writing 4.31 seconds. Linear scaling
of the latter is about 4.7 minutes; allow roughly 5–10 minutes for full preparation
and additional time for verification and longer-run variation. Disk estimates
exclude the existing model cache and optional search caches; an operational
allocation of 10–12 GiB leaves room for SQLite journals and sizing variation.
No full-year backfill was started during that sizing exercise; the subsequent
completed run is recorded at the end of this page.

This is the reusable comment backfill path. It writes one exactly sized int8 NPY
array incrementally, with text, row mappings, and committed progress in SQLite.
It does not write to PostgreSQL or change the production search index. The older
Parquet/per-batch-NPY pilot remains available for its historical artifacts.

## Original top-comment slice and inference host

The first completed full slice is stored on `ritsuko@melchior-1` under
`/home/ritsuko/projects/data/search-hn`. Its output directory is
`data/comment-top3-score100/`, and the transient user service is
`searchhn-comment-top3-20260919.service`. The selector takes the first three usable
top-level replies in HN display order on each live story with **score >100** across
all history. This is not comment-score ranking. Ties use comment ID; missing
display orders sort last. Dead/deleted comments and blank HTML are excluded.
Markup is decoded with paragraph breaks; text is not prefixed with the story title.
Long comments are split into contiguous character ranges of at most 2048 model
tokens, including special tokens, without dropping text. A markup-only body such
as `<i>` is recorded in `exclusions`; the top-comment selector continues to the
next usable reply in display order. The year selector records and excludes it.
Thus top-k counts decoded usable bodies, not merely nonblank HTML strings.

The dedicated raw model endpoint is `http://127.0.0.1:18080`, served by
`comment-embedding-dev-vllm-1`. Its pinned Compose file remains at
`data/comment-5090-tuning/compose.yaml`. This client expects raw pooled floats and
performs the Pplx tanh/round-to-int8 transform itself. Do not use the quantizing
embedding proxy URL. Defaults match the measured dedicated-host settings:

- 128 inputs per HTTP request, two requests in flight, no pause.
- 131,072 vectors per durable checkpoint, with a smaller final checkpoint.
- Server budget: 32,768 tokens and 512 sequences; pinned vLLM/model/BF16 recipe.

The inference batch, checkpoint interval, and NPY file size are independent.
131,072 vectors are 128 MiB and about 104 seconds of inference at the measured
rate. All vectors for this slice live in one file; no later compaction is needed.
The checkpoint interval is an explicit redo/write-frequency choice, not a memory
limit. `--checkpoint-rows` can be changed when resuming.

## Commands

Run these on the inference host from the checkout root, using its locked UV
environment. Preparation needs the read-only PostgreSQL account and its pgpass
entry; embedding and verification need only the frozen local artifacts.

```sh
# Prepare and then embed the original scope; rerunning resumes it.
uv run --locked --package search-research python packages/search-research/tools/comment_slice.py \
  run data/comment-top3-score100 --slice top-comments --score-gt 100 --top-k 3

# Inspect durable progress while the service runs.
uv run --locked --package search-research python packages/search-research/tools/comment_slice.py \
  status data/comment-top3-score100
tail -n 10 data/comment-top3-score100/run.log
systemctl --user status searchhn-comment-top3-20260919.service

# Resume frozen inputs without opening a PostgreSQL connection.
uv run --locked --package search-research python packages/search-research/tools/comment_slice.py \
  embed data/comment-top3-score100 --batch-size 128 --concurrency 2 --checkpoint-rows 131072

# After embedding stops: check vector range hashes, input hashes, and SQLite.
uv run --locked --package search-research python packages/search-research/tools/comment_slice.py \
  verify data/comment-top3-score100
```

One directory lock excludes competing writers and verification. Do not start a
second foreground invocation while its service is active. A transient user service
survives SSH disconnection; it is not a boot-enabled service. After host reboot,
rerun `embed` (or the original `run` command) to resume. HTTP failures propagate
with a traceback and a failed exit; there is no automatic retry or model fallback.
If interrupted, the currently unfinished checkpoint is recomputed. A run record
left as `running` after abrupt termination is historical, not a liveness signal;
consult the actual process/service and the `progress` table.

To launch a new detached run with a distinct unit name:

```sh
mkdir -p data/another-comment-slice
systemd-run --user --unit=searchhn-comments-another \
  --property=WorkingDirectory=/home/ritsuko/projects/data/search-hn \
  --property=StandardOutput=append:/home/ritsuko/projects/data/search-hn/data/another-comment-slice/run.log \
  --property=StandardError=append:/home/ritsuko/projects/data/search-hn/data/another-comment-slice/run.log \
  /usr/bin/uv run --locked --package search-research python packages/search-research/tools/comment_slice.py \
  run data/another-comment-slice --slice top-comments --score-gt 250 --top-k 3
```

## Other slices

Selection and persistence are separate modules. Every different selection gets a
different output directory. Existing `run`/`prepare` invocations reject a changed
selector, recipe, or tokenizer instead of reinterpreting saved inputs.

For all usable comments posted in 2025, including nested replies:

```sh
uv run --locked --package search-research python packages/search-research/tools/comment_slice.py \
  prepare data/comment-2025 --slice year --year 2025
uv run --locked --package search-research python packages/search-research/tools/comment_slice.py \
  embed data/comment-2025
```

These are the commands for the now-completed yearly slice. The year is the comment's own
calendar day, not its story's year. Story score/deletion does not filter this
selector. `--score-gt` and `--top-k` apply only to `top-comments`. The `sql` action
prints the selector query and bound parameters for an EXPLAIN review before a new
large scope. Future selectors should return the same source fields and feed
`append_comments`; they do not need to implement checkpointing or inference.

## Artifacts and reading vectors

`index.sqlite` contains:

- `comments`: original HTML, decoded text, author, story ID, text hash, and source
  metadata (including source dates and, for top comments, title/score/order).
- `inputs`: zero-based `vector_row`, comment ID, chunk number, character offsets,
  token count, exact model input, and input hash. Rows are ordered by story and
  display order for top comments, or comment ID for the year selector.
- `exclusions`: comment IDs, reason, and original source fields for nonblank HTML
  that decodes to no text. These exclusions also appear as structured log events.
- `metadata`: format/recipe, selection, PostgreSQL snapshot identity/time,
  tokenizer hash, complete input digest, and comment count.
- `progress`: total rows and the durable `completed_rows` prefix boundary.
- `checkpoints`: contiguous committed ranges, their vector-byte SHA-256 hashes,
  and commit timestamps. `runs` records scheduling settings and run outcomes.
- `completed_embeddings`: a view of inputs below the durable boundary, with the
  NPY filename. Unfinished slots are deliberately absent from this view.

`vectors.npy` has shape `(total_rows, 1024)` and dtype `int8`. The header and length
are established once; subsequent writes fill slices without rewriting earlier
vectors. Preparation must finish before its exact size is known. Unwritten slots
can read as zero: the SQLite completion boundary, not file length or nonzero
values, determines which rows consumers may use.

```python
# Run with: uv run --locked --package search-research python your_reader.py
import sqlite3
from pathlib import Path
import numpy as np

root = Path("data/comment-top3-score100")
db = sqlite3.connect((root / "index.sqlite").resolve().as_uri() + "?mode=ro", uri=True)
vectors = np.load(root / "vectors.npy", mmap_mode="r", allow_pickle=False)
row = db.execute("SELECT vector_row,comment_id,chunk FROM completed_embeddings LIMIT 1").fetchone()
if row is not None:
    vector_row, comment_id, chunk = row
    vector = vectors[vector_row]  # 1024 signed bytes, tied to this comment/chunk
db.close()
```

`tokenizer.json` is the pinned tokenizer. `writer.lock` excludes writers. During
embedding SQLite can have `-wal`/`-shm` sidecars; do not copy just the main SQLite
file while it is live. For a simple complete copy, stop the writer, close readers,
and copy the whole directory. Log output is diagnostic, not the resume authority.

## Persistence and failure boundaries

Preparation streams a read-only repeatable-read PostgreSQL snapshot in 2048-comment
blocks. It writes `index.partial.sqlite`, then closes/syncs and atomically renames
it to `index.sqlite`, syncing the containing directory. A failed preparation
restarts the temporary export; it never mixes snapshots in a published index.
The source SQLite database stores HTML, decoded text, and exact chunk inputs;
these useful copies consume more space than the vector payload alone.

The writer creates/syncs the NPY file and its directory entry before publishing
vector progress. For each checkpoint it hashes the new range, flushes the mapping,
fsyncs the NPY file, then commits the range hash and progress in one SQLite
transaction with `synchronous=FULL`. The NPY bytes are not stored in SQLite's WAL.

SQLite and NPY are not one cross-file transaction. The ordering is intentional:
an interruption before index commit leaves an unpublished tail that resume can
overwrite; index commit only follows a successful vector sync. Committed rows are
never deliberately overwritten. Resume validates all committed vector hashes and
refuses missing/corrupt committed files. It does not silently rebuild them.

Tests cover abrupt process exit specifically between NPY sync and SQLite commit,
out-of-order HTTP completion, HTTP failure after a checkpoint, repeat runs with no new requests, file corruption,
exclusive writers, and changed selection. These are process-interruption tests,
not a power-cut test of the SSD/filesystem. Final `verify` additionally checks the
complete input digest and SQLite integrity/foreign keys.

Implementation: `comment_slice_export.py` owns selection/freeze;
`comment_selection_rows.py` handles recorded exclusions and replacement replies;
`comment_vector_store.py` owns ordered durable publication;
`comment_slice_embed.py` owns bounded concurrent inference; `comment_index.py`
owns the shared schema. The CLI is `tools/comment_slice.py`.

```sh
uv run --locked --package search-research pytest \
  packages/search-research/tests/test_comment_slice.py \
  packages/search-research/tests/test_embedding_backfill.py -q
```

## Completed 2025 run

`data/comment-2025/` on melchior now contains **3,266,889 usable comments** and
**3,266,991 embedded chunks**. The completion event reported 2,190.289 seconds
(**36.50 minutes**) for embedding. Full `verify` passed with all 3,266,991 rows
committed. The earlier sizing section is the pre-run estimate; the explorer now
serves this completed yearly slice.

