# Explorer, search implementation, and measurements

[Workflow overview](README.md) · [Usage](usage.md) · [Dataset and labeling](dataset.md)

The current workstation serves the completed **2025** slice and supports editable
positive/negative sets, centroid queries, classifier drafts, and rollouts.
See [the workstation](#positive-set-workstation-v02) below
for the new controls, storage, API, and measured full-year costs. The original
610,947-comment baseline measurements remain here for comparison.

## Original phrase baseline and operating costs

The first explorer uses **native signed-int8 FAISS with exact cosine ranking**, as
selected by the project owner. `--dtype f32` retains a directly comparable float32
mode. Both read the existing `index.sqlite` and `vectors.npy`; neither writes a
second vector database or needs PostgreSQL. A local embedding endpoint is required
for new phrases. Cached phrases and pages do not repeat inference.

The application holds an in-memory FAISS index, vector norms, row-to-comment IDs,
and at most four complete phrase rankings. Full comment text stays in SQLite.
Startup checks the completed checkpoint ledger and vector hashes, then builds the
index. There is no training step or saved FAISS artifact to keep synchronized.
Restarting repeats that load and discards the phrase cache.

On melchior, the actual int8 web process loaded 610,968 vectors in 1.44 seconds.
After a query and two pages, Linux reported **0.74 GiB RSS**, **1.85 GiB peak RSS**,
and zero process swap. These are CPU RAM measurements, separate from the already
running GPU embedding service. The host reported 60 GiB usable RAM and 55 GiB
available before these trials. Virtual address space is not resident RAM.

## Run and use

From the repository root:

```sh
uv run --locked --package search-research python \
  packages/search-research/tools/comment_explorer.py \
  --slice-dir data/comment-top3-score100 --dtype int8
```

Open `http://127.0.0.1:18081`. The current remote instance is available on the
private tailnet at <http://100.90.118.117:18081/?q=book+review>. It binds that tailnet
address explicitly; the CLI defaults to loopback. No login is implemented for this
single-user annotation tool. Run a single process; each additional process creates
its own index and cache.

The remote process is a transient user service, not enabled at boot:

```sh
systemctl --user status searchhn-comment-explorer
journalctl --user -u searchhn-comment-explorer
systemctl --user stop searchhn-comment-explorer
```

To start it again from the remote checkout (after the transient unit is gone):

```sh
systemd-run --user --unit=searchhn-comment-explorer \
  --property=WorkingDirectory=/home/ritsuko/projects/data/search-hn \
  /home/ritsuko/.local/bin/uv run --locked --package search-research --extra ner python \
  packages/search-research/tools/comment_explorer.py \
  --slice-dir data/comment-2025 --dtype int8 \
  --host 100.90.118.117 --port 18081 --ner-device cuda
```

Use `--dtype f32` for the alternative. `--base-url` defaults to the pinned raw
vLLM endpoint at `http://127.0.0.1:18080`; `--threads` defaults to 16.
The same command accepts another **completed** slice directory produced by
[the reusable comment pipeline](corpus.md). Incomplete slices or
incompatible recipes fail at startup. Year slices may lack story titles; the UI
shows that absence rather than looking up live data.

The original phrase-only page had a phrase box, minimum cosine cutoff, page size (default 250, maximum
1000), and previous/next links at both ends. Text is rendered in full and escaped;
HN comment and story links provide source context. HTMX is served from the existing
vendored repository asset. Ordinary GET navigation also works without JavaScript.

`GET /api/search?q=book+review&page=1&page_size=250&min_score=-1` returns JSON with
results, score, winning vector row/chunk coordinates, total qualifying comments,
engine, cache status, and timings. `/health` reports loaded vectors and dtype.
The API does not expose arbitrary SQL or write to the slice. Requests are serialized
with a lock, appropriate to this single-user baseline.

## Scoring and joins

A phrase is embedded with the exact saved Pplx recipe: raw pooled model output,
`tanh`, multiply by 127, round ties to even, clip, native signed int8. No query
instruction prefix is added. The float32 search mode casts these same saved integer
coordinates to floats and L2-normalizes them; it does not recover unquantized model
outputs or constitute a comparison of two inference precisions.

The int8 index is `IndexScalarQuantizer(QT_8bit_direct_signed, INNER_PRODUCT)`.
It stores the existing signed coordinates without learned quantization. The FAISS
Python API takes a temporary float32 ingestion/query buffer, but stored coordinate
codes occupy one byte each. Search obtains all dot products, divides by document
and query norms, then sorts by cosine. Taking a small top-k by raw dot product
before this division would produce the wrong cosine candidate set.

For each comment, keep its highest-scoring chunk. Equal scores sort by comment ID,
then vector row. SQLite joins the page's winning `inputs.vector_row` to `comments`
and extracts frozen story/date fields from `source_json`. Existing primary keys
serve this lookup; no new SQL index or schema migration is needed. All 610,947
comments are pageable, rather than only an approximate candidate window. The
minimum score is applied to the best-chunk scores. A score is **not a probability**
of the comment being a book recommendation.

## Backend trials on melchior (2026-09-19)

Trials used the same 610,968 x 1024 native vectors and three phrases: `book review`,
`book recommendation`, `a novel worth reading`. RSS and high-water RSS were read
from `/proc/self/status` in separate processes, after releasing input mmaps and
conversion buffers. Figures are warm local-file trials, not cold-disk guarantees.

| Backend | Resident RAM | Peak RAM | Build/load | Query |
| --- | ---: | ---: | ---: | --- |
| FAISS 1.15.1 float32 flat | 2.45 GiB | 4.86 GiB | 0.96 s | 208–216 ms, all rows plus sort |
| FAISS 1.15.1 direct signed int8 | 0.66 GiB | 1.74 GiB | 0.49 s | 171–175 ms, all rows plus cosine/sort |
| Chroma 1.5.9 local HNSW | 4.40 GiB | 4.98 GiB | 143 s | 5–6 ms, approximate top-250 |

The standalone FAISS trials include writing small ranking outputs in query timing.
An earlier vector-only float32 trial took 44–46 ms for top-250 and 128–129 ms for
all rows before stable sorting. Do not compare that timing directly to the full
application path. In the live int8 web app, a new phrase plus ranking, comment
lookup, and JSON response took 332 ms; the next 250-comment page took 7 ms. The
first query embedded in 19 ms and ranked/deduplicated in 281 ms.

Int8 and float32 top-250 sets matched for all three phrases. Maximum coordinate-
matched cosine score difference was 1.79e-7 across the complete rankings. This is a
numeric search check, not a labeled assessment of phrase classifier quality.

Chroma was tested using `PersistentClient`, explicit embeddings (no embedding
function), cosine distance, M=16, construction ef=128, search ef=512, 16 threads,
internal batch=1024, sync threshold=131072, and ingestion calls of 4000 rows.
Only vector IDs and embeddings were imported. It produced about 2.74 GiB of derived
files at the end of ingestion. Top-250 recall against exact cosine was 96.8%, 96.8%,
and 99.6%; the 20,000-vector preliminary trial had 98.8% for each phrase. These
few queries are insufficient for a general recall claim. The disposable trials
successfully reopened all 610,968 records in a fresh process: first query including
index loading took 15.9 seconds and resident/peak RAM was 3.13 GiB. Thus Chroma's
4.40 GiB figure above is the post-ingestion process, not its fresh-reader footprint.
The disposable trials
remain at `data/comment-search-probe-chroma` and
`data/comment-search-chroma-full-probe` on melchior; the explorer does not use them.
Chroma was installed in an isolated uv environment, not added to this project.
Its [local HNSW implementation](https://github.com/chroma-core/chroma/blob/main/rust/index/src/hnsw.rs)
accepts float32 vectors; its [collection configuration](https://docs.trychroma.com/docs/collections/configure)
does not expose native signed-int8 storage. The FAISS
[scalar quantizer reference](https://faiss.ai/cpp_api/struct/structfaiss_1_1ScalarQuantizer.html)
documents the direct signed-int8 option used here.

DuckDB 1.5.5 loaded all float32 vectors in 12.3 seconds; exact cosine top-250 took
137 ms, top-10,000 took 154 ms. A 20,000-vector HNSW built in 2.35 seconds and its
query plan confirmed `HNSW_INDEX_SCAN`. Those probes did not isolate DuckDB RSS.
DuckDB can [attach SQLite read-only](https://duckdb.org/docs/current/core_extensions/sqlite)
and join frozen metadata. Its [VSS extension](https://duckdb.org/docs/lts/core_extensions/vss)
indexes float32 arrays and documents experimental index persistence. It remains a
reasonable SQL-oriented alternative; no DuckDB database is needed by this version.

## Positive-set workstation (v0.2)

The current UI adds a persistent set library and three query sources: freeform
text, positive mean, and positive mean minus a scaled random-background mean.
Create a set, search for an initial phrase, and use **Add positive** on useful
comments. **Open / edit positives** shows the saved members. Choose a mean query
and **Apply query** to retrieve unseen candidates. Membership edits and parameter
changes mark displayed search results stale; Apply refreshes them. Previous/next
remain usable after label edits: paging preserves the applied membership snapshot
for both centroid construction and positive exclusion. Current label toggles still
reflect the editable set. In mean modes, with hiding enabled (the default), newly selected
positives disappear from the browser immediately, including on revisited pages.
This can shorten a page without shifting its boundaries; Apply rebuilds the
ranking and exclusions. Free-text search always shows positives regardless of the checkbox, which is
disabled in text mode. The editable set view still shows every saved positive.
The page-number input jumps to a chosen page; clicking the total page count jumps
to the lowest-scoring page within the current minimum-score cutoff. Use `-1` to
include the whole corpus. Selected
positives are hidden by default, with an explicit checkbox to include them.

The appearance is a late-1990s lab application: beveled gray controls, navy title
bar, dense panels, and a terminal-style timing readout. The JSON-driven controls
use a small vendored JavaScript file; FastAPI and the existing HTMX fragment/search
endpoints remain. There is no frontend build step or new dependency.

The writable store defaults to `SLICE/annotations.sqlite`; override with
`--annotations-db PATH`. Its parent directory must exist. A store is bound to the
frozen manifest/checkpoint identity and rejects a different corpus. It stores
named sets, positive memberships, revisions, and seeded background sample IDs.
The source `index.sqlite` is still opened read-only. The annotation file is not
covered by PostgreSQL backups: use SQLite backup or stop the application before
copying it. This is still a private single-user tool, without login; mutating
requests require JSON and cross-site browser mutations are rejected.

Baseline sizes are 100, 1,000, and 10,000 unique comments, as nested prefixes of
one persisted sample. A new seed is only applied when requested. Each comment
contributes the normalized mean of its normalized chunks; positive and background
means retain their magnitude until after subtraction. Gamma zero equals plain
mean. The result is a direction, not a probability. Full math and the accepted
workflow are in [the methodology](README.md#2-iterative-human-refinement).

**Centroid scoring differs from phrase scoring:** direct signed-int8 FAISS also
casts query coordinates, so fractional centroid queries use exact streaming NumPy
float accumulation over the int8 NPY file. The engine field reports
`numpy-exact-int8-cosine` for these queries. This keeps the native-int8 index and
avoids a full float32 corpus copy, but the file mapping can add one corpus's worth
of file-backed resident pages. `--dtype f32` continues to use FAISS for both paths.
Both paths share cosine normalization, best-chunk deduplication, stable ties,
cutoffs, and complete pagination. A slider does not launch requests on every tick.

Additional JSON endpoints:

- `GET /api/sets`: corpus identity/name and set counts.
- `POST /api/sets` with `{"name":"Books"}`: create a set.
- `GET /api/sets/1?page=1`: membership IDs and full comments, 50 per page.
- `PATCH /api/sets/1` with `{"name":"Novels"}`: rename.
- `DELETE /api/sets/1`: delete that set and its memberships.
- `PUT` / `DELETE /api/sets/1/members/123`: add/remove a positive.
- `POST /api/experiment`: run or page a text/mean/corrected query.

All mutations require `Content-Type: application/json` (use `{}` for bodyless
operations). Example experiment body:

```json
{"mode":"corrected","set_id":1,"gamma":1.0,"baseline_size":1000,
 "seed":20260919,"hide_positives":true,"page":1,"page_size":50,"min_score":-1}
```

The response includes `query_positive_ids`, the applied membership snapshot;
pass it back when paging and omit it for a fresh Apply. `positive_ids` reports
current membership for the label controls. The response also includes applied
settings, set revision, selected IDs, centroid
norms, timings, cache status, and the existing result shape. Pooling recipe and
baseline identity are preserved in the experiment response. Accepted rollout
labels can be exported separately as JSONL; see [dataset provenance](dataset.md#provenance-and-export).
Run the focused verification with:

```sh
uv run --locked --package search-research pytest \
  packages/search-research/tests/test_comment_explorer.py \
  packages/search-research/tests/test_comment_experiments.py -q
```

### Full-year measurements (2026-09-19)

The 2025 slice contains **3,266,889 comments / 3,266,991 vectors**. Full slice
verification passed before the workstation run. On melchior with 16 FAISS threads:

| Operation | Observed time |
| --- | ---: |
| Verify/load/build at application startup | 6.76 s |
| New text query, including ranking and response | 1.34 s |
| Positive mean, including ranking and response | 1.02 s |
| Corrected mean, first background sample construction | 1.12 s |
| Corrected mean, cached background at 100 / 1k / 10k scale | about 1.02–1.03 s |
| Cached next page, including positive exclusion and SQLite lookup | 37 ms |

These are warm local-file trials with three disposable positive examples, not a
quality assessment or cold-disk guarantee. Plain mean and corrected mean with
zero gamma returned identical results. The three background sizes and gamma 0.5
produced finite sorted scores; consecutive pages did not overlap or include seeds.
The Python process had 6.86 GiB RSS (3.70 GiB anonymous / 3.16 GiB file-backed),
7.49 GiB high-water RSS, and zero swap after these trials.


### Negative examples and notes

Named sets now collect mutually exclusive positive and negative labels. The set
view groups positives first, then negatives, across the existing 50-row pages.
Negative cards have an optional multiline rationale with an explicit Save note
button. Unsaved drafts survive paging and switching sets within the browser;
leaving the page warns about unsaved drafts.

Negatives and notes are stored in the annotation SQLite database's separate
`negatives` table. Existing positive rows need no rewrite. Negatives neither
alter ranking nor get excluded from search; positive mean queries still use only
positives. Converting a positive to a negative changes the next applied mean,
while existing applied paging retains its snapshot. Classifier prompt building and accepted-label export are implemented in the
Classifier and Rollouts views; see [usage](usage.md).

### Inline parent context

Expand parent comment fetches the immediate parent on demand from the frozen
index and inserts collapsible, unranked context as `#<position>+p`. It changes
neither ranking, page boundaries, nor labels. Context disappears on rerender.
Parent IDs live in `comments.source_json`; parent text uses a primary-key lookup.
Top-level comments link to their story. Missing parent text (outside the slice
or excluded) is reported with an HN link; no live database or network fallback
is introduced. The main mirror retains sibling order in `kids.display_order`,
but this one-parent lookup does not need sibling order.

### Classifier workbench

Open `/classifier` via the navbar immediately after Corpus. Choose the attached
example set, describe the category, define taxonomy entries, and optionally
select teaching examples. For each example choose saved, omitted, or custom
rationale. Save + compile displays the prompt, structured output JSON Schema,
and separate exact `o200k_base` token counts. No model requests are made by compilation. Taxonomy descriptions and their
positive/negative polarity are visible in the compiled Markdown; the rollout
validator rejects a boolean that disagrees with the chosen taxonomy entry.

The `classifier_drafts` table is in annotations.sqlite, outside PostgreSQL
backup scope. Before deployment, take a SQLite online backup of annotations.
The tokenizer vocabulary is cached by tiktoken; its first use may download the
official o200k_base vocabulary. Warm this cache before restarting the service.
Neither frozen corpus data nor ranking behavior changes.

### Rollout operations

The /rollouts view uses the project-root .env for OPENROUTER_API_KEY, loaded
without overriding existing environment variables. Credentials never enter
run snapshots or browser responses. The service must run as one process:
one background worker owns dispatch, with configurable request concurrency.
Stopping a run drains in-flight calls and leaves undispatched rows pending.
A process restart marks unknown queued/running attempts interrupted rather than
silently repeating potentially billed work. Requeue those explicitly.

Additional annotation SQLite tables: rollout_pools, rollout_rules,
rollout_picks, rollout_sources, classifier_runs, classifier_attempts and
rollout_reviews. They remain outside the PostgreSQL backup scope. An online
SQLite backup was taken before the rollout deployment. Invalidation archives
pool generations; manual labels and classifier drafts are unaffected.

The authorized 50-row Luna smoke run is retained as run 1: 48 accepted outputs
and two boolean/taxonomy disagreements, $0.012651 reported cost. It used 25
rank-source and 25 random-source candidates, split into 41 train and 9 test.
The smoke exposed an ordering bug: partial dispatch ranked the random sample
before taking its prefix. Subsequent batches preserve original pick order;
the recorded smoke inputs/results were not rewritten or automatically retried.

### GLiNER entity playground

The Corpus browser has a global **Select ontology** control and **Add / edit
label sets** editor. Give a set a name and enter one freeform entity label per
line (for example, `book title`, `author`, `publisher`). Ontologies and their
per-label confidence thresholds live in browser local storage, independently of
positive/negative sets. The initial Books ontology is only a starting suggestion.

Click **Extract entities** on a search result or saved positive/negative comment.
A collapsible result shows highlighted spans with score tooltips, individual
confidence bars, the ontology/threshold snapshot, device, window count, and elapsed
time. Changing the ontology or thresholds affects the next click; existing results
keep their original snapshot. Predictions disappear when cards are rerendered.
Scores are the confidence values returned by GLiNER.

Enable the optional model dependencies when starting the explorer:

```sh
uv run --locked --package search-research --extra ner python \
  packages/search-research/tools/comment_explorer.py \
  --slice-dir data/comment-2025 --ner-device cuda
```

The first extraction downloads and loads
[`gliner-community/gliner_large-v2.5`](https://huggingface.co/gliner-community/gliner_large-v2.5).
A persistent child process retains one model and serializes inference, isolating
PyTorch from the explorer's FAISS native runtime. `--ner-device auto` (the
default) selects CUDA when available, otherwise CPU; the result reports the device
and dtype. CUDA uses BF16 after the [precision and batching check](entity-extraction.md);
CPU uses FP32.
Allow disk space for model weights and, on Linux, PyTorch/CUDA dependencies, plus
GPU memory beside the embedding service. Initial loading is included in the first
request's elapsed time. No corpus-wide job or prediction database is created.

Long comments use overlapping windows constrained by the model word limit and
its tokenizer budget, including label prompts. Each window overlaps by the model's
maximum span width. Duplicate span/label pairs keep the highest score; all offsets
map back to the complete original comment. Nested spans and multiple labels are
allowed by the model decoder; this is decoded NER output, not an exhaustive table
of every possible span. Chunk boundaries can still affect predictions.

`POST /api/comments/123/entities` accepts an ontology snapshot:

```json
{"labels":["book title","author"],"thresholds":{"book title":0.5,"author":0.5}}
```

The endpoint reads the frozen comment, returns text and character-offset spans,
and uses the existing same-origin JSON request boundary.

### Separate title annotator

Open `/annotator` or the **Annotator** link in the navigation bar. This is a
separate review surface with the same Comment Lab styling. The first batch has
1,300 quick-filter passes with existing DeepSeek low predictions and Luna medium
predictions available for comparison; creating this batch required no new calls.

- **Delete** keeps a title and its highlight visible in grey; **Restore** undoes it.
- Select text in the comment and click **Add selected title**. Its exact Unicode
  character offsets and `manual` origin are saved immediately. Existing teacher
  proposals retain `predicted` origin and their original optional author value.
- **Reviewed + next** records completion and advances. Merely opening a comment
  does not count as review. Editing a reviewed comment makes it unreviewed again.
- A reviews and advances with one hand; N/P navigates. Ctrl+Enter or Cmd+Enter
  also reviews and advances. Shortcuts are ignored while editing text. Filters expose
  review status, train/evaluation split, and teacher agreement. Deep links retain
  batch and comment IDs. Reviewed JSONL includes effective titles, original
  predictions, deleted entities and manual provenance.

Both batches are now fully reviewed: batch 1 has 1,300 comments and batch 2 has
300 fresh random quick-filter passes, reserved for evaluation. Batch 2 uses
DeepSeek only; the comparison panel is hidden when no second teacher exists.
Teacher filters include DS-positive and DS-negative rows. Freeform review notes
are saved separately and the Notes filter finds comments to revisit.
See [Trained book extraction](entity-training.md) for split lineage and results.

The batch was sampled from the 4,096 matched fresh-comment API runs. Seed
20260920 selects 300 uniform random evaluation comments first. The remaining
training candidates include all shared positives and title-set disagreements,
then random shared negatives to reach 1,000. The training counts are 469 matching
positives, 115 disagreements and 416 matching negatives. Evaluation counts are
29 matching positives, 13 disagreements and 258 matching negatives. Agreement
uses case-folded, whitespace-normalized title sets, not author agreement.
No exact-text duplicates or train/evaluation text overlaps occur in this batch.

All initial spans use literal, case-insensitive whole-word matching, with every
occurrence retained. Eighteen proposals have no exact span and remain visible
with that warning; none are silently dropped or fuzzily assigned. To correct a
boundary or an unmatched title, delete the proposal and select its source span.
The later occurrence audit found that some repeated literal matches name a
character, an ordinary concept, or a different work. Its corrections live in a
separate training snapshot; it did not rewrite this annotation ledger.
This UI reviews titles; optional teacher author values have not been separately
reviewed. It does not train GLiNER.

Persistence reuses the corpus-bound AnnotationStore connection/transaction
abstraction and the rollout pattern of frozen batches, original predictions and
append-only review events. Extraction has separate `entity_batches`,
`entity_items`, `entity_labels`, and `entity_review_events` tables because the
classifier rollout's labels and acceptance rules are boolean-specific. Revision
checks return HTTP 409 for stale edits. Duplicate imports fail rather than
replacing manual work.

Preparation tool: `tools/prepare_entity_annotation_batch.py`. The import snapshot,
selected comments and original SQLite backup are in
`data/probes/books-annotation-first1300/` on melchior and the local checkout.
The online SQLite backup passed `integrity_check`; it is outside PostgreSQL
backup scope and has not been uploaded to Garage. Neither source PostgreSQL
nor the frozen comment index was modified. Filter membership and full source
text were verified for all 1,300 records.
