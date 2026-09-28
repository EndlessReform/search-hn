# Current book pipeline and end-of-day handoff

This page describes the implemented research pipeline as of 2026-09-27.
It supersedes the older resolver design and original label-run handoff for current
operations. Model training history and original measurements remain in their
linked pages. All data paths below are repository-relative unless stated otherwise.

For the inspected component inventory, remaining legacy paths and proposed cleanup,
see the [2026-09-27 pipeline audit](pipeline-audit-2026-09-27.md).

## Current decision and run

The user authorized the full frozen 2025 reference set with Luna and **no shortcut
acceptance gate**. Backtest shortcuts afterward from saved scores and decisions;
do not dispatch additional labeling experiments. No cheaper selector is trained
here. These Luna outputs can supply its pseudolabels if that later becomes useful.

- Host: **melchior**, repository `/home/ritsuko/projects/data/search-hn`.
- Current run: `data/research/books-resolver-2025-heal-v2/`.
- Recovery: all initial and both repair-round model decisions are complete.
  The original repair author join exhausted memory; recovery preserves the initial
  paid results and uses the bounded Tantivy retrieval described below.
- Runner: `packages/search-research/tools/resolver_heal/run_full.sh`.
- Log: `data/research/books-resolver-2025-heal-v2/run.log`.
- Approved scope: 26,221 original references, up to two repair rounds, Luna only.
- API estimate: $8.52, using the previous slice's receipts and 460 added example
  tokens per call. Dispatch stops at $12 recorded total; active calls drain and
  can add a small amount. Local GPU/CPU work is outside that API estimate.
- The eight-call smoke prefix is retained in the run, not paid for twice.
- Recovery timers and invocation logs live alongside the original `run.log`.
  `recovery-manifest-20260927.json` records the changed retrieval implementation;
  the original manifest remains unchanged.

## Algorithm, from comments to work IDs

### 1. Embed the frozen corpus

The completed `data/comment-2025/` slice contains **3,266,889 comments and
3,266,991 chunks**. Decode comment HTML, preserve paragraphs and offsets, split
long comments into contiguous chunks of at most 2,048 model tokens. Do not prepend
parent/story text. Embed with `perplexity-ai/pplx-embed-v1-0.6b`, revision
`2c4d510dd4a732063c31a0f70193e35067b51fd8`, producing native signed-int8,
1,024-dimensional vectors. `vectors.npy`, `index.sqlite`, and `tokenizer.json`
preserve vectors, text, mappings and the frozen recipe. Existing embeddings are
reused for this rollout. See [Corpus](corpus.md) for runtime and storage details.

### 2. Apply the embedding quick filter

Use the frozen 25% centroid / 75% XGBoost blend. For XGBoost, normalize chunk
vectors, average per comment, then normalize the average. For the centroid,
use the best chunk cosine against the frozen 60-positive anchor from pool 1.

```text
zc = (cosine - 0.4018084356464245) / 0.20650860033072777
zx = (logit(clip(p, 1e-7, 1-1e-7)) + 1.6561394556670892) / 4.033332085834178
pass = 0.25 * zc + 0.75 * zx >= -0.48585514643850203
```

`p` comes from `data/probes/books-xgb-sweep-v1/depth4_child1.ubj`, using saved
best iteration 293 (294 trees). Anchor: pool 1's `anchor_json.query` in
`data/comment-2025/annotations.sqlite`. **108,194 comments pass**. No refit or
re-embedding is part of the current rollout. See [Training](training.md#closing-decision-centroid--xgboost-quick-filter-2026-09-20).

### 3. Extract titles with fine-tuned NER

The selected GLiNER reference checkpoint is
`data/probes/books-gliner-training-v1/final-refit/model`, threshold **0.17**.
Its training used reviewed title spans plus older silver labels, nine epochs,
effective batch 16. The sole trained label is `book title`. Preserve source
comment IDs, complete text, offsets and NER scores. Deduplicate titles within a
comment by whitespace-collapsed lowercase title, retaining the first occurrence.

The frozen extraction produced **17,927 comments, 28,026 span occurrences,
26,221 deduplicated references**. The current run consumes those saved references;
it does not redo title NER. The alternative gold-only five-epoch checkpoint at
threshold .06 was evaluated but is not selected here. Reviewed data totals 1,600
comments, including a separate fresh random 300. See [Entity training](entity-training.md)
for splits and quality measurements.

For a new slice, `tools/resolver_heal/titles.py` now produces both raw
`title-proposals.jsonl` and deduplicated `references.json`. Run from the repository
root on melchior, using a new output directory:

```sh
uv run --no-sync --package search-research python \
  packages/search-research/tools/resolver_heal/titles.py \
  --slice data/comment-2025 \
  --passes data/probes/books-gliner-v1/filter_passes.jsonl \
  --output data/research/books-title-ner-NEW
```

The defaults are the fine-tuned checkpoint above, threshold 0.17, whole-year
encoder-token sorting, and FP32 weights with BF16 autocast. Static batches contain
64 windows through 128 tokens, 16 through 384 tokens, and 4 through 1,536 tokens.
Batch shapes can vary. Window boundaries and source text do not change; an input
beyond the final bucket fails explicitly rather than being truncated. Sorting
holds the filtered year's text/windows in RAM (about 2.8 GB total process RSS in
the complete 2025 title run). `--batch-size` overrides all buckets;
`--legacy-order --batch-size 16` reproduces the original block/character schedule.
Overlapping windows retain the maximum
score for identical offsets; title deduplication preserves proposal order, as in
`resolver_rollout/prepare.py`. The command refuses an existing output directory.
`title-manifest.json` records input/model hashes, settings, counts, elapsed time
and peak allocated GPU memory. For a reproduction check, add
`--baseline data/probes/books-resolver-smoke-v1/reference-proposals.jsonl`;
`title-comparison.json` records every span change and shared-span score drift.

The tuned complete 2025 run took **291.7 s**, versus **342 s** for the original
schedule. It produced 26,203 references versus 26,221: 46 span occurrences added,
64 removed, and changed references in 101 of 108,194 comments. Batched BF16
inference is not bit-for-bit equivalent across padding/batch choices. These are
extraction differences, not measured accuracy losses or gains. Frozen resolver
outputs remain unchanged. Measurements live on melchior under
`data/research/ner-tuning-title-full-eager-20260927/`.

### 4. Extract person names as a soft author signal

Separately run off-the-shelf `gliner-community/gliner_large-v2.5` with label
`person`, threshold **0.3**, BF16 on GPU. Process complete comments through windows,
cache spans once per comment in `names.jsonl`. These spans are possible names,
not assertions that each person authored the marked title.

Person NER now sorts the complete pending workload by encoder-token length and
uses static batches of **64 / 16 / 8** at the same 128 / 384 / 1,536 token ceilings.
It restores original window order before merging overlaps and writes each
completed comment immediately, preserving resumability. `--batch-size` overrides
the schedule. All 17,927 frozen 2025 comments took **47.7 s**, including preparation
and durable output writes. Compared with saved batch-1 output, 68 spans were added
and 76 removed across 131 comments (24,241 spans versus 24,249). The exact legacy
loop took 12.51 s on the 1,111-window benchmark sample; token-sorted batch 16 took
3.27 s. Do not mistake that sample speedup for a measured full-year baseline.

For 2024, all **116,090** quick-filter passes yield **116,432** title windows:
median 63 encoder tokens, p95 281, p99 534, maximum 1,088, including label prompts.
Whole-year token sorting makes padding negligible: at batch 64 it adds 0.28%
token positions, compared with 51.7% for character sorting in 2,048-window blocks.
Length analysis is under `data/research/ner-tuning-20260927-2024/` on melchior.
The 32 longest 2024 title windows (876–1,088 tokens) also completed inference
through both models at their selected long-window batch sizes, without truncation
or failed batches; `tail-check.json` records timings and GPU allocations.
Both NER stages and reranking still run in separate, sequential processes.

**Backend conclusion:** keep eager PyTorch as the default. FlashDeBERTa 0.0.7
with the same Torch 2.14.0 / Transformers 5.16.1 took 282.4 s for the complete
title workload and 45.7 s for person NER: only 3.2% and 4.3% faster respectively,
with higher peak tensor allocation (title 8.45 versus 6.10 GiB; person 3.51 versus
2.57 GiB). It remains an explicit `--backend flash` experiment; install it through
`uv run --with flashdeberta==0.0.7 --with torch==2.14.0 --with transformers==5.16.1`
when invoking either NER command. No new dependency is required for the default.
GPU allocator caches/workspaces can approach the full 32 GB card, so the tensor
allocation figures must not be used to budget concurrent model residency.

The person sample measured 339 windows/s with eager batch 16, versus 209 with
ONNX Runtime FP32 and 324 with ONNX Runtime FP16. Compiled PyTorch reached 491
windows/s when warm, but the initial cold experiment took about eleven minutes;
a cached repeat still spent 59 s on its first measured pass versus 2.26 s warm.
No compiled or ONNX runtime is introduced into the yearly runner. Casting title
weights to BF16 also gave no material throughput gain, so their precision stays
unchanged. Reproduce sweeps with `tools/ner_benchmark.py`; artifacts are under
`data/research/ner-tuning-20260927-*` on melchior.

For initial retrieval, all tokens of a detected name must occur within one catalog
author name. Join matching author IDs to work IDs. A matching work receives a
single **+5 BM25-point bonus**, applied before the top-50 cutoff. It is an optional
bonus, not a required author filter. Repeated names and multiple matching authors
do not stack bonuses. This describes the retained initial round's implementation
in `tools/resolver_retry/ner.py` and `retrieve.py`; repairs use stored author
tokens in Tantivy instead of exporting author-to-work joins to Python.

### 5. Search the offline work catalog

The current backend is **embedded Tantivy through Python bindings**, not a deployed
standalone search server. The offline Open Library snapshot is 2026-08-31:
`data/probes/books-resolver-smoke-v1/{works.parquet,authors.parquet,title-bm25}`.
Search title tokens with lowercase tokenization, without stemming or stopword
removal, add the author bonus above, and retain **50 work IDs**. Preserve the title
query, individual candidates, original BM25 values and author bonuses.

**Recovery changes the candidate boundary:** repair searches retrieve a fixed
10,000 title hits, score authors only within that pool, then retain 50. An excluded
title hit cannot be rescued by an author bonus. There is no adaptive expansion.
On 256 sampled queries, two top-50 sets differed from verified expanded retrieval;
this measures candidate differences, not answer accuracy. Index rebuilding also
changes equal-score tie ordering. New runs break score ties by reading-log
count descending, then numeric work ID ascending; exactly 50 candidates remain.

The replacement index is `data/probes/books-catalog-tantivy-v1`, with 41,590,995
documents and stored per-author tokens. Tantivy handles live repair retrieval;
DuckDB is used only for offline index construction and other tabular preparation,
not live author matching. The index occupies 5.08 GB and built in 157.4 seconds.
The complete 5,113-query first repair retrieval took 296 seconds. The original
global partial-author join would emit 422,072,850 name/work rows; it is no longer
on the repair path. Code: `tools/resolver_heal/{build_catalog,catalog,retrieve}.py`.

The earlier proposed persistent Rust search executable has not been implemented.
The HTMX server on port 8767 is a review UI, not the retrieval backend. Live Open
Library metadata can be expanded in that UI; it is inspection data and is not
silently substituted into the frozen resolver inputs.

### Reusing the catalog and retrieval in later slices

The full 2024 slice is exported, embedded and verified on melchior: 3,117,812
comments, 3,117,906 vectors. Export took 152.7 s and local 5090 embedding took
2,105.1 s at batch 128/concurrency 2. Its embedding server is stopped. The full
2024 resolver completed on 2026-09-27 under the user service
`searchhn-resolver-2024.service`, run root `data/research/books-resolver-2024-v1/`.
The full run took **42 min 14 s** (16:00:34–16:42:48 CDT). The filter completed in
42.2 s with 116,090 passes; title NER took 315.3 s and person NER 68.4 s.
Initial retrieval took 141.0 s; initial reranking processed 1,480,258 pairs in
996.0 s (1,486 pairs/s). Luna completed all 31,392 initial cases, 5,680 first
repair cases, and 2,977 final repair cases. Total recorded cost: **$5.60002594**,
below the $8 stop, using concurrency 512 and 16 retrieval workers. There were
40,050 attempts for 40,049 successful decisions: one invalid work ID was rejected
and its automatic retry succeeded. No manual repairs, model changes, or omitted
cases were needed.

Publication is complete: `checkpoint.sqlite` has 31,392 matching reference,
ranking, history and Luna-selection IDs, and passes SQLite `integrity_check`.
26,766 references have at least one selected work; 4,626 have none. All three
rounds' successful receipt IDs exactly match their ready-case IDs.
`gate-backtest.json` contains all eight offline comparisons over the full
31,392-reference denominator. Supervision is finished; no later year was started.
Both 2024 and 2025 now use compact slice format 2:
decoded text once, chunk offsets, binary hashes, and a compatible `inputs` view.
The SQLite files are 1.78 GB and 1.86 GB respectively (decimal); vectors are
unchanged. See `corpus.md` for the conversion command and compatibility details.

The yearly runner now handles initial and repair retrieval through the same
`Catalog.search` implementation. It requires an already embedded slice and a new
run root; baseline reuse is opt-in. Paths must be inside the repository so the
GPU container can read them. For a full 2024 run after preparing its slice:

```sh
bash packages/search-research/tools/resolver_heal/run_full.sh \
  --run-root data/research/books-resolver-2024-v1 \
  --slice data/comment-2024 --budget 8 --concurrency 512
```

The harness explicitly selects eager NER. Its new run manifests freeze the
shared title/person batch schedules from `search_research.ner_batching`; changing
those settings rejects resumption instead of silently mixing configurations.
The stage commands and harness use the same recipe. Historical manifests remain
readable under their original contract.

Retrieval defaults to 16 CPU worker processes for both initial and repair queries.
Use `--retrieval-workers 1` on the yearly runner (or `--workers 1` on
`retrieve.py`) for serial execution. Workers each load frozen popularity counts
and open their own index handles, increasing RAM and CPU use; index pages are
shared by the OS. Candidate order, scores, provenance and the 50-work cutoff are
unchanged. Result files are written in reference order only after all searches
succeed; timing metrics vary with concurrency. Small repair batches can take
longer because process startup dominates. On melchior, 2,048 saved references
took 104.7 s with one worker and 11.9 s with 16 (8.8x), with exact candidate
and non-timing counter parity. The 22-reference 2024 smoke also produced
byte-identical serial/parallel CLI candidate files.

The runner applies the frozen quick filter, extracts titles and names, retrieves,
reranks, selects through at most two repair rounds, and publishes. It copies the
Luna template into the run root once; `--config` can supply another template.
Omit `--baseline` for a new year. An explicit baseline enables score reuse and
`luna-original` comparison rows. Existing published 2025 outputs are unchanged.
Completed stages and paid receipts are reused on restart. A completed publication
is a no-op after input verification. An interrupted title extraction leaves its
new output directory for inspection; use a new run root to retry that stage.

`manifest.json` records the slice index hash and the hashes of the frozen filter
recipe, anchor, model, reader counts, run-local Luna config, and optional baseline.
Input verification reads the slice index once per invocation; changing inputs
requires a new run root. Budget and concurrency are recorded per invocation.
The budget stops new dispatch at the recorded dollar limit; active calls drain.

The quick filter is also a standalone command:

```sh
uv run --no-sync --package search-research --with xgboost-cpu python \
  packages/search-research/tools/quick_filter.py run \
  --slice data/comment-2024 \
  --recipe data/research/books-quick-filter-v1/recipe.json \
  --output data/research/filter-2024.jsonl
```

The frozen recipe on melchior contains `anchor.json`, `model.ubj`, their hashes,
the best iteration, cutoff, and blend constants. It was created once with
`quick_filter.py freeze --annotations data/comment-2025/annotations.sqlite
--model data/probes/books-xgb-sweep-v1/depth4_child1.ubj
--sample-summary data/probes/books-gliner-v1/sample-summary.json
--output data/research/books-quick-filter-v1` and reproduces all 108,194 original
2025 pass IDs. Runtime inference never opens the annotation database.

The 1,000-comment 2024 smoke run is in
`data/research/books-resolver-2024-smoke-r3/`: 62 passes, 22 references,
30 successful Luna calls across three rounds, $0.004651205 recorded cost, and
baseline-free publication. A completed rerun left scores, checkpoint and receipts
unchanged. This small scan-order sample is a pipeline check, not a year-level
filter-rate or quality estimate.

Both reranker entry points submit windows of 128 references and reuse only
cached case/work scores. The offline 2,048-reference check scored 96,375 pairs in
83.0 seconds (1,161 pairs/s excluding model load, including first-call compilation
and writes). Results and logs are in `data/research/books-rerank-r5-20260927/` on
melchior. Explicit engine cleanup avoids the pinned vLLM image's shutdown abort
seen in the first smoke attempt; that interrupted run resumed without repeat
payments. Full-year 2024 throughput remains to be measured.

The catalog itself is slice-independent and can be reused while the Open Library
snapshot, index schema and tokenizer stay fixed. On melchior:

- Source snapshot: `data/probes/books-resolver-smoke-v1/works.parquet` and
  `authors.parquet`, Open Library 2026-08-31.
- Completed retrieval index: `data/probes/books-catalog-tantivy-v1/`.
- Builder: `tools/resolver_heal/build_catalog.py`; search: `catalog.py`;
  initial/repair adapter: `retrieve.py`. These paths are relative to
  `packages/search-research/`.
- `build.json` marks a completed build. The promoted recovery index has the
  prototype metadata shape (`docs`, `seconds`, `bytes`, `peak_rss_mib`); the current
  builder emits format 1 metadata with source paths, sizes and modification times.
  The recovery manifest separately records the promoted index's source identity.
  File size/mtime metadata is not a content hash or automatic invalidation system.

For a genuinely new catalog generation, select a new output directory and run
from the repository root (example output name; do not rebuild merely for a new
comment slice):

```sh
uv run --no-sync --package search-research --with tantivy python \
  packages/search-research/tools/resolver_heal/build_catalog.py \
  --source data/probes/books-resolver-smoke-v1 \
  --output data/probes/books-catalog-tantivy-v2 --partitions 32
```

The builder joins work author IDs to author metadata in 32 hash partitions inside
DuckDB, with a 6 GB DuckDB memory limit and four threads. Partitioning repeats
Parquet scans. It transfers completed records in batches of 1,000 into a Tantivy
writer with a 512 MiB budget and eight threads. It verifies document count after
commit/merge, writes `build.json`, and renames the `.building` directory. It refuses
to overwrite an existing output. A failed `.building` directory is not a published
index; inspect it or choose a new generation rather than treating it as complete.
The 6 GB setting limits DuckDB, not total process RSS.

Tantivy indexes work ID with the raw tokenizer and title with simple tokenization
plus lowercase, without stemming or stopword removal. Author metadata is stored
JSON bytes containing separate author names and Unicode letter/number token lists;
it is **not** an author FTS index. DuckDB native FTS is not used. At query time,
Tantivy tokenizes the title and suggested/person names. Suggested-author scoring
excludes one-character tokens and uses the best overlap against one author:
`5 * matched_suggested_tokens / suggested_tokens`. With no suggested author, a
complete person-name token set within one author earns +5. Initials-only suggestions
earn zero; they do not enable person-name fallback. Bonuses do not stack and tokens
from different contributors cannot combine into a match.

Each query makes one native title search for at most 10,000 hits. Python decodes
stored metadata one candidate at a time and maintains a top-50 heap; it does not
receive a catalog-wide author/work mapping. A +5 score bound may skip decoding the
remaining already-returned hits. This is not another query or dynamic expansion.
The final sort uses float32 scores then descending work ID. Equal-score inclusion
at the 10,000-hit boundary can still depend on index layout: stable final sorting
does not promise identical candidate sets across index rebuilds.

For an already-prepared repair file in a **new, explicitly selected** run directory:

```sh
RESOLVER_RUN_ROOT=data/research/NEW_SLICE_RUN \
uv run --no-sync --package search-research --with tantivy python \
  packages/search-research/tools/resolver_heal/retrieve.py \
  --round 1 --catalog data/probes/books-catalog-tantivy-v1 --title-pool 10000
```

Replace `NEW_SLICE_RUN` with the intended run directory. This consumes
`round1-queries.json` and writes `round1-cases.json` plus
`round1-retrieval-metrics.json`; it does not prepare references or run Luna.
Metrics include index path, pool size, native search calls, hits returned, decoded
metadata count, timings and whether the global score bound was satisfied. The
frozen 256-query profile averaged 57 ms/query, p90 107 ms, with 4.03 GiB peak RSS
mostly from clean mapped index pages; this is a measurement, not a RAM guarantee.

Reuse boundaries for subsequent slices:

- Reuse the catalog across slices; build a new generation when snapshot, indexed
  metadata, schema or tokenization changes. Record the selected generation and
  query/scoring settings in each run manifest. There is no automatic refresh.
- No exact-title result cache was implemented. Only a raw title-only hit pool is
  eligible for such sharing under an identical index generation and query config.
  Author boosts, contextual reranking and verdicts are not functions of title alone:
  observed “Meditations” and “The Road” mentions referred to different authors.
- Reranker reuse requires the same marked comment/offsets, candidate title/authors,
  repair target/reason, model revision and scoring recipe. Existing scripts key
  cached pairs by case/work ID; they do not automatically enforce all those inputs.
  Use a separate run/cache when inputs change; do not reuse IDs for changed text.
- Paid selector resume checks exact request fingerprints. New slices need a new
  run root, frozen references, provenance and explicit model/budget settings; never
  edit old requests to evade a mismatch. The fixed 2025 baseline/config paths in
  `rerank.py`, `select.py` and `publish.py` also need deliberate wiring before
  treating these scripts as a general new-slice runner. In particular, `ready.py`
  loads reader counts from `/tmp/resolver-reading-log-counts.parquet`;
  preserve/recreate that frozen input before running preparation on another host.

### 6. Rerank, then apply popularity, then take three

Zerank-2 (`zeroentropy/zerank-2-reranker`, revision
`5eae30d5ee3c6b2df2ef6d723bde45172d761c4c`) scores each candidate using the full
comment with the occurrence marked, plus candidate title and authors. Use the
pinned vLLM 0.23 FP8 container, BF16 remaining computation, prefix caching,
32,768 maximum model length, 16,384 batched tokens and 256 sequence cap.
[Throughput](resolver-throughput.md) records the measured runtime configuration.

```text
score = raw_reranker_logit + 0.1 * log2(1 + readinglog_count)
```

Reader counts are joined by work ID from the frozen
`/tmp/resolver-reading-log-counts.parquet` on each host. Missing counts receive zero.
Sort all 50 by this score, retain the top three, then deterministically shuffle
those three for Luna. Luna sees reader counts, titles and authors, but not ranking
scores. There is **no hard substitution of duplicate records**, no equivalence
clustering, and no popularity cutoff. Multiple catalog IDs can still occupy all
three positions. Saved parallel ID/score arrays are not necessarily sorted;
explicitly sort before calculating a top-two gap.

Initial unchanged case/work pairs reuse pinned prior scores. Repair searches use
the explicit requested title, optional author and reason alongside the original
comment. Their scores are saved separately under repair IDs.

### 7. Luna resolves or requests repairs

Use `openai/gpt-6-luna` through OpenRouter, provider pinned to OpenAI, medium
reasoning, strict JSON schema, output allowance 4,096 tokens. The full-run runner
now defaults to concurrency **512**, configurable with `RESOLVER_CONCURRENCY`.
The standalone selector CLI retains its original default of 16; pass
`--concurrency 512` when invoking it directly.
The request includes the full marked comment and the three candidates. Parent
thread context is not supplied. The authoritative prompt/schema live in
`tools/resolver_heal/schema.py`; exact requests are retained in receipts.

Useful-work concurrency measurements processed distinct pending repairs, without
repeating successful calls. Fully occupied completions/second at concurrency
16/32/64/128/256/512 were 5.9/11.8/23.0/45.8/81.4/150.0; all those batches had zero
failed attempts. At 512, mean service time was 3.00 seconds and p95 5.64 seconds.
The final-round 1,024-concurrency batch fell to 52.0/second, mean 14.85 seconds,
p95 26.90 seconds, with 12 failed attempts that all recovered. Eleven had no HTTP
response and one selected an invalid work ID; the original exception logging
omitted transport classes, so their precise cause is unknown. Future receipts
include exception class and representation. No HTTP 429 was observed.

512 is the measured operating choice, not an exact universal knee. The last round
forces a terminal decision and averaged 206 output tokens versus 167 in the
512 batch, so it is not a controlled same-workload comparison. The backlog was
exhausted; do not repeat paid successes just to refine the threshold. The 1,024
test raised its own file-descriptor limit to 4,096; the 512 runner needs no such
change on this host. Batch active time and full invocation timers are both saved.

Across 26,221 initial successes, input tokens had mean 1,468, median 1,390,
p90 1,672, p99 2,737 and maximum 4,212. Output tokens, including reasoning, had
mean 175, median 116, p90 327, p99 958 and maximum 2,823. Reasoning alone had
mean 106 and median 47. Cached tokens were 77.2% of input volume. The 4,096-token
allowance did not truncate a successful initial response.

Each result has `title`, optional `author`, `action`, optional `work_id`, and
`reason`. Actions are `select`, `search`, or `abstain`. Multiple results can repair
a span that merged several titles: retain available selections and search missing
ones separately. An optional suggested author is a separate field, not appended
to the title. World knowledge may expand abbreviations, recover authors or identify
series installments. A supplied work ID is required for selection, except
`special:bible`, which represents biblical books/testaments as the Bible.

For an unspecified series prefer a collection, otherwise its first book; honor
an explicit installment instead. Poems, manuscripts, translations and anonymous
works are valid matches. Prefer a useful match to bibliographic pedantry, but do
not substitute a different work merely because its subject is similar.

The final approved prompt adds exactly **three positive and two negative examples**:
Annabel Lee; Rivers of London -> first novel; Design of Everyday Things -> its
earlier title; fire-starting title -> search rather than another fire-skills book;
Cervantes as a lecture topic -> no book selection. These examples were added after
the 2,500-case evaluation; their yield effect is not yet measured.

### 8. Drain the repair backlog and publish

First repair round: retrieve each requested title, rerank, boost popularity, take
three and ask Luna again. Suggested authors use per-author token overlap as a soft
bonus, `5 * matched_query_token_fraction`; initials are excluded. This permits
partial names such as Don Norman / Donald A. Norman. Without a suggested author,
reuse the person-NER matching signal.

Second repair round: new queries get another search; identical previous queries
reuse that candidate list for a final decision instead of rerunning retrieval.
The last call must select or abstain. Child IDs preserve root and parent linkage;
each original reference may finish with multiple selections and/or abstentions.

Publish a frozen `checkpoint.sqlite` with `refs`, `documents`, `rankings`,
`selections` and `history`. Current choices use `selection.work_ids` (a list), not
the old singular `work_id`. `resolution_items` retains per-target actions and
reasons; `history` retains initial decisions and all repair inputs/receipts.
For this full run, the comparison key `luna-original` means the original 26,221-case
baseline. In the earlier 2,500-case UI it means the preceding author-NER run.

## Durability, inspection and resumption

`receipts.sqlite` is the live authoritative paid-response journal: WAL mode,
FULL synchronous commits, one transaction per response. It retains failed attempts
as well as successes. Resume verifies identical request fingerprints and skips
successful calls. A selector lock blocks concurrent selector workers. One failed
case does not cancel other calls in flight. Each case has at most three attempts;
exhausted cases remain failures, not abstentions. Authorization/payment/rate-limit
responses stop new dispatches while active requests finish. The runner stops at
an incomplete stage; inspect it before resuming rather than launching a fresh run.

`roundN-decisions.jsonl` is exported from the journal after a selector invocation;
it can lag behind an active invocation. `names.jsonl` and `new-scores.jsonl` flush
and sync incremental outputs. Complete ready files and the final checkpoint use
rename-on-completion. A process killed during an external API call can still incur
an unrecorded charge; request fingerprinting cannot recover a response never received.
Do not open a live WAL database with SQLite's immutable option.

Read status on melchior:

```sh
cd /home/ritsuko/projects/data/search-hn
tail -30 data/research/books-resolver-2025-heal-v2/run.log
pgrep -af 'resolver_heal|resolver_retry'
```

The historical 2025 run above is complete. For a new yearly run, resume by
repeating its original explicit `run_full.sh --run-root ... --slice ... --budget
... --concurrency ...` invocation after the previous process has stopped. The
runner no longer has a zero-argument 2025 default. Do not change inputs in place
to bypass a fingerprint mismatch or reset paid attempt counts. Use the new-run
manifest contract described above; historical manifests and publications remain
readable. Full artifacts stay on melchior unless explicitly copied elsewhere.

## Completed results and follow-up inspection

The full 2025 run is complete: **26,221 terminal roots**, including 22,013 with
at least one selection and 4,208 without one. Final target results contain 22,702
selections and 4,330 abstentions; no searches remain pending. There are **9,929
distinct selected catalog work IDs** across 15,097 comments. Catalog duplicates
are not clustered. All 5,113 first-round and 2,791 final-round repairs completed.
Total recorded cost is **$4.933901275**, including failed attempts; recovery added
$1.25416821. Fourteen attempts failed across the whole run and every affected case
subsequently succeeded. Eleven transport failures had no usage receipt, so these
totals are recorded charges, not an independently reconciled provider invoice.

`checkpoint.sqlite` and `gate-backtest.json` are published on melchior. Publication
needed a compatibility fix for legacy comparison documents without reader counts;
their counts now come from the frozen count table. Round 2 reranking saved all
6,850 scores but aborted during container teardown; a cache-only resume completed
cleanly without repeating inference. The old manifest and paid request bodies
were retained. The report includes original output-timestamp estimates and explicit
recovery stage timers; diagnostic pauses are not presented as pipeline runtime.

The preceding `data/research/books-resolver-heal-v1/` run resolved at least one
work for **2,105/2,500 references (84.2%)**, leaving 395 unresolved. Repairs touched
473 roots and recovered 188 previously unselected roots. There were 2,181 selected
targets, 58 split spans, 3,289 recorded attempts and $0.66124 recorded spend.
These are yield counts, not measured correctness or distinct-book counts.

The user reviewed cases 1–280. Four Luna reviewers spotchecked 100 random cases
outside that prefix (seed 260926): 85 passed, 15 flagged before adjudication. Flags
included reviewer mistakes as well as real wrong selections. This was not a final
human-gold accuracy estimate. Packets and individual reviews were originally saved
under `/tmp/resolver-final-review/` on the workstation; their durable copy is
`data/research/books-resolver-heal-v1/final-review/`.

Completion and terminal coverage have been verified. For subsequent inspection:

1. Read [Final Luna verdicts](resolver-labels-handoff.md#final-luna-verdicts-after-repairs-2026-09-27)
   for the checkpoint schema, queries and distinction from initial receipts.
2. Inspect `gate-backtest.json`. The runner computes score/gap shortcuts offline
   against final Luna ID sets. This measures agreement with Luna, not accuracy.
   Current replay uses adjacent work-ID score gaps (including duplicates), with
   gap thresholds 1/2/3/5 and optional minimum boosted score 10. The older
   different-title/author-group raw-score gate is a distinct definition; do not
   present the current replay as a reproduction of that old experiment.
3. Copy the completed checkpoint locally and point the review UI at it. Do not
   overwrite the older checkpoints or imply the currently open UI is the new run.
4. Report yield, spend and shortcut tradeoffs before enabling any gate. No extra
   paid sweeps or cheaper-model training are authorized by this run.

The current local UI remains the completed **2,500-case** run:

```sh
uv run --package search-research python -m search_research.resolver_review_web \
  --checkpoint data/research/books-resolver-heal-v1/checkpoint.sqlite --port 8767
```

It shows source comments inline, collapsed parent context, retrieval inputs,
initial and repair candidate lists, raw/boosted scores, model choices and exact
requests. Case navigation and filtering are available at `http://127.0.0.1:8767/`.
