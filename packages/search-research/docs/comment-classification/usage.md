# Run the comment-classification workflow

[Methodology and results](README.md) · [Dataset details](dataset.md)

Commands run from the repository root. The completed research slice lives on
melchior at `/home/ritsuko/projects/data/search-hn/data/comment-2025/`. To explore
that slice, skip preparation and embedding and start at step 2.

## 1. Prepare and embed a new slice

Preparation needs the read-only PostgreSQL account and pgpass configuration.
Embedding needs the pinned raw Pplx endpoint, normally `http://127.0.0.1:18080`.
Use a new directory for a different corpus/recipe; do not reinterpret an existing
slice. The commands below reproduce the 2025 selection:

```sh
uv run --locked --package search-research python packages/search-research/tools/comment_slice.py \
  prepare data/comment-2025 --slice year --year 2025
uv run --locked --package search-research python packages/search-research/tools/comment_slice.py \
  embed data/comment-2025 --batch-size 128 --concurrency 2 --checkpoint-rows 131072
uv run --locked --package search-research python packages/search-research/tools/comment_slice.py \
  verify data/comment-2025
```

`status data/comment-2025` reports durable progress. Rerun `embed` after an
interruption to resume from the last committed checkpoint. Only one writer may
operate on the directory. See [Corpus](corpus.md) for endpoint requirements,
selectors, detached jobs, and recovery behavior.

## 2. Open Corpus and collect teaching examples

```sh
uv run --locked --package search-research python packages/search-research/tools/comment_explorer.py \
  --slice-dir data/comment-2025 --dtype int8
```

Open `http://127.0.0.1:18081`. The existing melchior instance is available at
<http://100.90.118.117:18081>. Startup verifies vectors and rebuilds the in-memory
index. The app is a private single-user tool with no login; run one process.
It uses `SLICE/annotations.sqlite` by default, or `--annotations-db PATH`.

1. Create/select a named example set in **Corpus**.
2. Start with a broad text query. Add real positives and nearby negatives; write
   a negative note when it illustrates a useful distinction.
3. Switch to **Positive mean**, or the background-subtracted mean mode, and Apply.
   The search phrase is ignored in mean modes. Try gamma/background sizes if useful.
4. Read and label more results. Changes mark the applied query stale; Apply uses
   the new membership. Paging retains the prior applied snapshot until then.
5. Inspect the set to edit labels/notes. Positive and negative examples are grouped.

Selected positives are hidden by default in mean modes. Free-text search shows
positives regardless of that checkbox. Type a page number to jump, or click the
total page count to go to the final page. Expand parent comment for temporary,
unranked context; it does not automatically become part of a classifier input.

## 3. Build and inspect the classifier prompt

Open **Classifier** and choose the same example set.

1. Describe what qualifies, including important exclusions.
2. Add taxonomy names/descriptions and mark each positive or negative. Those flags
   are shown to the model and checked against its output. If no fine-grained
   taxonomy is wanted, use a simple positive/negative pair.
3. Select optional teaching examples. Accept their saved rationale, omit it, or
   enter custom wording for this draft. Original annotation notes are unchanged.
4. **Save + compile**. Read the Markdown prompt and JSON Schema, including the
   explicit `is_positive` instruction. The preview counts prompt and schema tokens
   separately using `o200k_base`; runtime comment/chat-wrapper tokens are additional.

Compilation makes no vendor call. Save the draft before dispatching a rollout.
The tiktoken vocabulary may need downloading on its first use.

## 4. Construct the candidate dataset

Open **Rollouts** for the same set. Choose the seed/test fraction and create the
pool. This freezes the current plain positive mean, selected IDs, and corpus
identity. Later set edits do not silently rebuild that anchor.

The default rules are ranks 1–1000 plus 150 uniform corpus-random comments.
These are starter settings, not a recommended final class balance. Add both:

- Nearby and deeper rank bands for positives and hard negatives, for example
  1–1500, 5000–5500, or another interval chosen after inspecting the ranking.
- A sufficiently broad random-corpus source so ordinary background comments
  appear in the labeled dataset too. Random comments can still be positives.

Edit an existing range and **Save + sample** to extend it. Rank endpoints are
1-based and inclusive; exclusions can reduce the returned count. Similarity rules
start at or below a chosen score and move downward. Random rules use seeded order.
Repeated sampling is idempotent, and overlap produces one candidate with multiple
source records. Use rank bounds rather than translating UI page numbers.

Deleting/narrowing a rule prunes uncovered, unsent picks. Attempted predictions
remain. **Remove pending outside current rules** cleans up older uncovered picks.
**Invalidate all** requires typing `INVALIDATE ALL`; it archives the pool so a new
anchor/pool can be created, preserving manual examples, drafts, and run history.

## 5. Label a bounded batch and inspect predictions

Set `OPENROUTER_API_KEY` in the project-root `.env` on the service host, or in its
environment. The app loads `.env` without overriding existing environment values;
restart the service if its environment needs refreshing. Do not put keys in docs,
commands, or exported artifacts.

Choose model, batch limit, and concurrency in **Model dispatch**. Defaults are
OpenRouter `openai/gpt-5.6-luna`, limit 50, concurrency 8. The preview shows the
eligible pending count and how many this batch will label before you start it.
There is one paid request per dispatched comment and no automatic retries.

Valid, consistent labels are accepted automatically. In predictions, inspect
positive/negative and train/test totals, filter by labels/taxonomy, and reject or
requeue individual mistakes/errors. Stop drains in-flight requests; undispatched
rows remain pending. A process restart preserves interrupted attempts for explicit
retry rather than silently billing them again.

## 6. Export and train

Use the Rollouts export control, or download accepted rows for the desired set:

```sh
# Replace 1 with the example-set ID; run beside the service or adjust the host.
curl --fail http://127.0.0.1:18081/api/sets/1/rollouts/export \
  -o /tmp/book-labels.jsonl
```

Export includes text, labels, train/test split, rank/similarity, source rules,
model/run provenance, and the frozen sampling anchor. See [provenance details](dataset.md#provenance-and-export).
The UI is not needed to consume JSONL. Historical rule edits remain in the SQLite
ledger; export source specifications reflect the current rule rows.

For the simplest training baseline against the current accepted pool:

```sh
uv run --locked --script packages/search-research/tools/comment_linear_probe.py \
  --slice-dir data/comment-2025 --set-id 1 --output data/probes/NEW-linear-run
```

This tool snapshots accepted labels from the annotation database and writes its
own frozen labels/artifacts; it does not consume the JSONL download above. The
later experiment drivers reuse frozen files under `data/probes/`. Some have fixed
book-experiment paths; inspect them and choose fresh output directories before
running on a new category. Their isolated script environments keep CPU PyTorch
and experiment dependencies separate from the explorer.

For a repeatable real-corpus check of a saved linear/MLP checkpoint:

```sh
uv run --locked --script packages/search-research/tools/comment_probe_wild.py \
  --slice-dir data/comment-2025 \
  --fixture data/probes/books-wild-1000-v1/predictions.jsonl \
  --checkpoint data/probes/books-mixes-v1/random_emphasis.pt \
  --exclude-labels data/probes/books-mixes-v1/labels.jsonl \
  --output data/probes/NEW-fixture-check
```

The scorer checks label overlap and uses checkpoint cutoffs. XGBoost/ensemble
comparisons use their separate drivers; the generic checkpoint scorer does not
load XGBoost. See [Training](training.md) for the selected blend's exact formula,
artifacts, and comparisons. Training/scoring makes no new labeling calls.

## Preserve the work

The frozen slice and writable annotations are different artifacts. PostgreSQL
backups do not include `annotations.sqlite`. Preserve it through SQLite's backup
API or copy it with the application stopped; do not copy just the main database
while a WAL writer is active. Retain the frozen model/label/split artifacts with
any chosen classifier so scores and cutoffs stay tied to the same recipe.
