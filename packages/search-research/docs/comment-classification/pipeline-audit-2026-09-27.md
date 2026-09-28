# Book pipeline: what blocks the 3.3M → 41M comment rollout

Reviewed 2026-09-27 against source, melchior artifacts and the live mirror
(read-only). Nothing was changed or restarted. For what the pipeline does
algorithmically, see [the current pipeline](pipeline-current.md).

## Conclusions

**R1–R5 code changes are implemented. A 1,000-comment 2024 run completed through
publication with no baseline, and resumption preserved its paid receipts and
outputs. The full 2024 proof run is next. No new storage layer, queue, service or
index is needed.**

The mirror has **38.07M eligible comments outside 2025** (2007–2026, counted
2026-09-27 with the same filters as the 2025 slice; 2026 is still growing). The
largest year, 2023, has 4.01M comments, **1.23× the 2025 run** that already
completed on melchior. Year-sized shards therefore stay close to a memory and
disk envelope we have already run. That makes the whole-run JSON files and the
other scaling problems in the code survivable without rewriting them.

### Decisions for you before the backfill

| Decision | Consequence |
|---|---|
| **Luna spend** | Extrapolated from 2025 ($4.93 for 34,125 calls): **about $57** for the other years. `run_full.sh` hardcodes a $12 cap per run; each year needs an explicit cap. |
| **Initial retrieval: popularity breaks ties at the 50 cutoff** | Measured on 1,000 sampled 2025 references. With popularity tie-breaking, 1.1% of Luna's original initial picks lose both the picked record and any record with the same title and authors. Keeping every tied work would cut that to 0.7% but costs +45% rerank pairs. Recommended: popularity, 50 candidates. See [the RCA](#initial-retrieval-why-the-top-50-changed-rca). |
| **5090 occupied for about a day** | Embedding, NER and rerank run one after another on melchior. They can't run at the same time (see VRAM below). |
| **Disk** | About 91 GiB of new comment slices plus up to about 57 GiB of run directories. Melchior has 1.6 TB free. |
| **Filter drift** | The quick filter was trained and checked on 2025 comments only. Compare pass rates per year. Spot-check any year that is far from 2025's 3.3% before paying for Luna on it. |

## Required code changes (implementation checklist)

Paths are relative to `packages/search-research/`. Each item ends with the check
that marks it done. **R** items block 2024. **B** items are needed before the full
backfill. **S** items belong to the later service work, not this handoff.

### R — required before the 2024 run

| # | Change | Files | Done when |
|---|---|---|---|
| R1 | **Complete: title-NER batch command.** Fine-tuned checkpoint `data/probes/books-gliner-training-v1/final-refit/model`, threshold 0.17, full-text windows from `comment_entities.text_windows`, **batch 16 with length sorting** (confirmed in the original `/tmp/searchhn-full-ner.py`). Deduplicate titles per comment by whitespace-collapsed lowercase, keeping the first, as `resolver_rollout/prepare.py` does. Write proposals with offsets and scores. | `tools/resolver_heal/titles.py` | Verified 2026-09-27 on melchior: all 108,194 passes; proposals byte-identical to the saved baseline; 28,026 spans and 26,221 references; zero score drift or threshold flips. Reference IDs, titles, offsets, scores and contexts match the frozen resolver run. Outputs: `data/research/books-title-ner-r1-20260927/`. Runtime 342 s; peak allocated GPU memory 8.6 GiB. |
| R2 | **Complete: initial retrieval through `Catalog.search`.** Add a first-pass mode to `resolver_heal/retrieve.py` that reads references and `names.jsonl` and writes the existing candidate schema (`query_title`, `person_spans`, `retrieved`, `candidates`, `author_bonus`). Then delete `resolver_retry/retrieve.py`, `resolver_heal/authors.py` and the `RESOLVER_AUTHOR_CACHE` variable. Port the per-author scoring test in `tests/test_resolver_heal.py`, which currently imports `authors.py`, to `catalog.author_bonus`. **Change `Catalog.search`'s tie-break** from descending work-ID string to: score, then reading-log count (descending), then numeric work ID (ascending). Load the counts from the same frozen file that `ready.py` uses (moved by R3). Do not add them to the index. | `tools/resolver_heal/{retrieve,catalog}.py`, `run_full.sh`, the tests | The [RCA](#initial-retrieval-why-the-top-50-changed-rca) numbers reproduce on the same 1,000-reference sample: about 1.8% of Luna's initial picks absent by ID, about 1.1% with no same-title-and-authors record, and zero score mismatches on shared works. No supported entry point reaches the global author join. |
| R3 | **Complete: remove the 2025 hardcoding.** `run_full.sh` takes run root, slice directory, budget, concurrency and an optional baseline. `resolver_heal/rerank.py` and `resolver_retry/rerank.py`: the 2025 baseline score preload becomes optional and is off for new years. `publish.py`: the baseline is optional; with no baseline, skip the `luna-original` rows and `copy_baseline_documents`. `select.py` reads the Luna config from the run root, copied from `books-resolver-2025-v1/luna-config.json` when the run starts. Reader counts move from `/tmp/resolver-reading-log-counts.parquet` to a durable path under `data/`, hashed in the manifest. `resolver_rollout/prepare.py` takes slice and proposal paths and drops its 26,221-count assertions. | `tools/resolver_heal/*`, `tools/resolver_retry/rerank.py`, `tools/resolver_rollout/prepare.py` | Verified on 1,000 2024 comments: 62 filter passes, 22 references, 30 successful Luna calls, $0.004651205, baseline-free publication. Completed resumption leaves receipts, scores and checkpoint unchanged. Config is copied once into the run; reader counts are hashed in the manifest. Existing 2025 outputs are unchanged. |
| R4 | **Complete: standalone frozen quick filter.** Move centroid + XGBoost inference out of `comment_entity_samples.py`. Freeze the anchor (currently read from mutable `annotations.sqlite` pool 1) into an artifact file with a hash, alongside the XGBoost model path, best iteration and blend constants. Inputs are a slice directory; the output is passing comment IDs with scores. Leave sampling and assertions in the research script. | `tools/quick_filter.py`; `tools/comment_entity_samples.py` | Verified all 108,194 pass IDs match exactly on all 3,266,889 comments (44.6 s). Frozen recipe, anchor and model: `data/research/books-quick-filter-v1/` on melchior. |
| R5 | **Implemented: restore the benchmarked rerank submission size.** Submit windows of 128 references (about 6,100 pairs per `llm.classify` call) instead of 512 pairs. | `tools/resolver_heal/rerank.py`, `tools/resolver_retry/rerank.py` | Offline validation on 2,048 saved 2025 references: 96,375 pairs in 83.0 s, **1,161 pairs/s** excluding model load but including first-call compilation and output writes. The GPU process shuts down cleanly with explicit cleanup. Confirm throughput on the full 2024 run; the 22-reference smoke batch is too small for this target. |

Case IDs are `comment_id:start:end`, so years can't collide with each other or
with 2025. Use a new run root for each year and whenever inputs change.

### B — before the full backfill (after 2024 passes)

| # | Change | Why | Done when |
|---|---|---|---|
| B1 | **Implemented before 2024: parallel retrieval.** 16 spawned processes over `Catalog.search`, preserving output order; configurable with `--retrieval-workers` on the yearly runner or `--workers` on retrieval. Higher CPU/RAM use; no GPU or paid calls. | On melchior, the saved seed-20260927 sample of 2,048 references took 104.7 s with one worker and 11.9 s with 16 (8.8x), including startup. | Exact serial/parallel candidate and non-timing counter parity on all 2,048 references. The 22-reference 2024 smoke produced byte-identical CLI candidate files; startup makes this tiny batch slower (2.9 s versus 1.8 s). Twelve retrieval/yearly-runner tests pass, including worker failures. Full-year 2024 validation remains part of that run. |
| B2 | **Implemented and swept before 2024:** whole-workload token sorting; person batches 64/16/8 and title batches 64/16/4 for token ceilings 128/384/1,536. Existing window boundaries and overlap rules retained. | Full 2025 person extraction: 17,927 comments in 47.7 s. Title extraction: 291.7 s versus original 342 s. Batch curves, not merely successful runs, select these sizes. | Full frozen-output comparison: person +68/−76 spans across 131 comments; title +46/−64 span occurrences, changed references in 101/108,194 comments. Numerical changes include span-boundary selection, not just threshold flips. Twelve focused scheduler/title/yearly tests pass. See [current pipeline](pipeline-current.md) for settings, lengths and backend conclusions. |
| B3 | **`publish.py`: group children by `root_id` once**, instead of scanning all cases for each root. | Removes the quadratic scan, about 45 s per year. | Identical `checkpoint.sqlite` contents on 2024. |
| B4 | **Rust export in `hn_core`.** HTML decode and 2,048-token chunking with the `tokenizers` crate and rayon; a single writer numbers rows and batch-inserts into the existing `index.sqlite` schema. | About 5–10x (6.2 MB/s now), limited by the database scan. Also the first building block of the service. Off the backfill's critical path, so it can come after 2024. | Byte-for-byte parity with `data/comment-2025/index.sqlite` for `comments.text`, chunk offsets and token counts. **Consequence:** if the input checksum is computed in parallel, its definition changes; keep the old verifier for existing slices. |

Not required: `ready.py` reader-count dict (seconds), `select.py` coroutine per case (about 30k per year).

### S — service work, not this handoff

- Retrieval query shape (require an informative word) and a Rust per-hit loop; see [Retrieval](#retrieval-why-17-queriess-and-the-fix). The query change alters results, so compare it against 2025 candidate sets.
- NER in one worker process: test the fine-tuned title model on `person` first, then a joint fine-tune if needed; see [3060 service](#next-fitting-the-pieces-on-the-3060-service).
- Evaluate a smaller reranker with the existing harness. Recalibrate the popularity weight if the model changes.
- Zerank VRAM caps for the 3060.
- Request `base64` embeddings from the embedding client before any bulk traffic over the tailnet.
- A date-range slice option, so 2026 can catch up after its current snapshot.
- Explorer default `--base-url` → tailnet raw route (`tools/comment_explorer.py`; one line; dev loop only).

### Not needed for this rollout

These showed up in the code review. None of them blocks the rollout or pays for itself at year-sized shards:

- **FAISS or the comment explorer.** Nothing in the book pipeline imports them. The filter memory-maps `vectors.npy` and uses NumPy. The explorer is a dev and annotation tool that handles one slice at a time. Don't build an all-years explorer: 41M int8 vectors take about 40 GiB of RAM on a 60 GiB host.
- **A SQLite run store to replace the whole-stage JSON files.** At about 1.2× 2025 per year, these files stay near sizes that already worked (largest about 610 MiB). Revisit only for continuous processing or much larger shards.
- **Fingerprinted rerank-cache keys.** The current key (`case_id, work_id`) is only unsafe if a run root is reused after changing its inputs. The rule "new inputs, new run root" covers this.
- **Joint title/person NER, for the backfill specifically.** On the 5090 the saving is about 30–60 minutes of person NER. The saving that matters is VRAM on the 3060 service; see [Next: the 3060 service](#next-fitting-the-pieces-on-the-3060-service).
- **A job API, a live change-capture service.** These belong to the service step below, not the backfill.
- **A date index on `items`.** Exporting one year is one sequential scan (about 20 s). Twenty scans take about 7 minutes in total.

## Where to run embeddings

The two inference endpoints are an escalation ladder. Neither is a dependency the
other has to fall back on.

| Use | Endpoint | Why |
|---|---|---|
| Interactive and small jobs: explorer phrase queries, daily or incremental slices, anything under roughly 1M comments | Tailnet 3060, `https://magi06-inference.tail7a3eb.ts.net/vllm/embeddings`, background priority | Always on and shared with live story search. No setup. |
| Bulk backfill (this rollout) | Devbox 5090, `data/comment-5090-tuning/compose.yaml` on `127.0.0.1:18080` | Measured 6.8M tokens/min (about 1,500 chunks/s) on 2025-length comments. Start it for the backfill, then run `docker compose down` to free VRAM for NER and rerank. |

Both endpoints serve the same model revision, BF16 precision and mean pooling,
and both return raw pooled floats. The existing client applies the Pplx int8
transform either way, so switching hosts only changes `--base-url`; only the
servers' batch limits differ. The 3060's bulk throughput on comments hasn't been
measured. Its batch limits are smaller (64 sequences and 8,192 tokens, versus 512
and 32,768 on the 5090), and it would compete with live search, so a 3B-token
backfill doesn't belong there.

The explorer defaults to `127.0.0.1:18080`, so its phrase queries fail whenever
the 5090 server is down, which is most of the time. Change the default in
`tools/comment_explorer.py` to the tailnet raw route; that is a one-line change.

## Cost and time estimates for the other 38.07M comments

These scale the 2025 measurements linearly by 11.65×. Older comments may differ in length and book-mention rate, so treat the numbers as sizing, not promises.

| Stage | 2025 (measured) | 38.07M (estimated) | Resource |
|---|---|---|---|
| Export from the mirror | **3.3 min** (from the source snapshot to the start of embedding, recorded in `index.sqlite`) | about 40 min run back to back; a few minutes if years run in parallel | Single-threaded Python, measured at 6.2 MB/s of HTML (1.23 GB in 198 s). The work is independent per comment, and the only ordered steps (row numbering, the input checksum, one snapshot) are bookkeeping. Moving decode and chunking into Rust (`hn_core`), with the tokenizers crate and rayon, should give 5–10x. The limit then becomes the roughly 15–20 s database scan per year; 2.5 GbE moves a year's HTML in about 5 s. Check byte-for-byte parity against the stored 2025 text and chunk offsets. The database has no other significant load, so years can run concurrently. Runs ahead of embedding, so it is off the critical path. |
| Embedding | 36.5 min, 3.27M chunks | **about 7 h**, about 2.9B tokens | 5090 |
| Slice storage | 7.8 GiB | about 91 GiB | Disk |
| Quick filter | 34 s, 108,194 passes | about 7 min, about 1.26M passes | CPU |
| Title NER | 5.8 min (311 comments/s) | about 67 min | 5090. The original script confirms batch 16; its 8.6 GiB peak cannot be used to infer a larger batch. The separate benchmark reached 589 comments/s at batch 16, but that is not the measured full-slice throughput |
| Person NER | 17,927 comments at about 110/s (batch 1) | about 209k comments, about 30 min | 5090 |
| Catalog retrieval | 57 ms/query, about 17 queries/s in one thread: about 38 ms in Tantivy, about 19 ms Python decoding | about 5.8 h in one process; **about 25 min** with 16 workers (Tantivy searches measured at 273 queries/s on 16 threads) | CPU. Root cause and fix under [Retrieval](#retrieval-why-17-queriess-and-the-fix) |
| Rerank | 1.24M initial + 257k repair pairs; about 760 pairs/s in the runner | about 17.5M pairs, **4–5.5 h** plus about 80 s startup per invocation | 5090. Excluding model load, the repair round ran at about 880 pairs/s. The benchmark reached 1,620 pairs/s on 248-token pairs, which is about 1,290/s at the repair prompts' 312 tokens. Python prompt building and tokenization measured 48 µs per pair, only about 8% of GPU time. **The cause is the submission size.** The benchmark and the original 2025 runner (`resolver_rollout/rerank.py`) submit windows of 128 references, about 6,100 pairs per call; the original run's launch check recorded about 1,210 pairs/s. The later `resolver_retry` and `resolver_heal` runners submit 512 pairs per call, a size that was never benchmarked. Restoring 128-reference windows is a one-constant fix, giving about 4 h |
| Luna | 34,125 calls, $4.93 | about 398k calls, **about $57**, about 45 min at concurrency 512 | OpenRouter |
| Run directories | 4.9 GiB | up to about 57 GiB | Disk |

The total is roughly **13–17 h of mostly unattended runtime**, with the GPU stages
running one after another and retrieval run in parallel.

**VRAM schedule on the 5090 (32 GiB):** the embedding server plans for 35% of VRAM
and Zerank for 80%, so they can't run together. Title NER (8.6 GiB peak) plus Zerank
doesn't fit either. The order is: embed every year, stop the embedding server, then
run each year's book stages. The NER and rerank processes already exit when their
stage finishes.

## Plan for the rollout

1. R1–R5, with their 2025 checks. None of them makes a paid call.
2. Start the 5090 server. Run `comment_slice.py run data/comment-YYYY --year YYYY` for each year, then `docker compose down`. The 2026 slice is a snapshot; catching up later needs a date-range slice option, which is a small addition.
3. **Run 2024 end to end as the proof of value** (see below).
4. B1–B4, then the remaining years with a budget cap per year. Check each year's filter pass rate before its Luna stage.

### 2024 proof of value

2024 has 3,117,812 comments, 0.95× 2025, so it is the same size class as the run that already succeeded. Expected, scaled from 2025:

| Stage | Expected |
|---|---|
| Export and embedding | about 3 min + about 35 min |
| Filter | about 35 s, about 103k passes |
| Title and person NER | about 5 + 3 min |
| Retrieval | about 25k references + about 4.9k repairs, about 30 min in one process or about 2 min with 16 workers |
| Rerank | about 1.43M + 0.25M pairs, about 20–40 min |
| Luna | about 32.6k calls, **about $4.70**. The existing $12 cap is enough. |

About 2–2.5 h of wall time in total.

Treat these as success criteria, not only timing:
- The filter pass rate is near 2025's 3.3%.
- The reference count is near 25k.
- The share of roots with at least one selection is near 2025's 84%.
- Recorded spend is at most about $6.
- Spot-check about 50 random resolved roots in the review UI.

If any of these is well off, stop before starting the other years.

## Initial retrieval: why the top 50 changed (RCA)

Measured on melchior: 1,000 random 2025 initial references (seed 20260927), the new
`Catalog.search` against the saved old results (`first-ready.json`) and Luna's
initial picks (`round0-decisions.jsonl`).

1. **Scoring did not change.** The 47,067 works present in both old and new results have identical scores (title BM25 + 5 author bonus).
2. **Ties at the cutoff are the norm.** In 83% of references, a group of equally scored works straddles position 50: median 47 works tied, p90 211, maximum 2,053. Open Library holds many records with the same title; for example, 67 works are titled exactly "Hyperion". Title-only BM25 gives identical titles identical scores, and the author bonus only separates them when a detected person matches. About 12% of the current 50 slots hold records whose title and author set duplicate another slot.
3. **Both tie-breaks were arbitrary.** The old path kept whichever tied works came first in the old index's internal document order. `Catalog.search` sorts ties by descending work-ID string. Neither is a relevance signal, so 82% of references get a different set of 50.
4. **Membership matters because the reranker only sees the 50.** A book that loses a tie never reaches the reranker or Luna.
5. **The 10,000-title pool is a minor factor.** Keeping every tied work still misses 4 of 734 Luna picks (0.5%). Those are the pool boundary.

Options, measured against Luna's 734 initial picks in the sample:

| Selection of the 50 | Pick's record not in the list | No record with the same title and authors | Rerank pairs |
|---|---:|---:|---:|
| Descending work-ID string (current `Catalog.search`) | 43 (5.9%) | 17 (2.3%) | 47.4k |
| **Score, then popularity, then numeric ID** | **13 (1.8%)** | **8 (1.1%)** | **47.4k** |
| Popularity order, ties allowed past 50, cap 75 | 12 | 7 | 60.7k (+28%) |
| Popularity order, ties allowed past 50, cap 100 | 8 | 5 | 68.7k (+45%) |
| Collapse exact title+author duplicates, then popularity | 56 | 8 | 47.1k |
| Keep all ties (uncapped) | 4 | 4 | about 2.4× |

**Use popularity tie-breaking with 50 candidates.** It costs nothing extra and
recovers most of the loss. Allowing ties past 50 buys about 0.4 percentage points
for 28–45% more reranking. Collapsing duplicates adds nothing over popularity, and
the current pipeline deliberately avoids clustering records.

The benchmark is biased towards the old candidate lists: Luna picked from those
lists, so a pick missing from the new list may have an equally good replacement.
Treat the percentages as an upper bound on harm, not measured accuracy loss.

## Retrieval: why 17 queries/s, and the fix

"Retrieval" is `Catalog.search` in `tools/resolver_heal/catalog.py`. It takes one
extracted title, plus optional author and person names, and returns 50 Open
Library candidates for the reranker. Tantivy (Rust) runs the search. Python builds
the query, loops over the hits, fetches each hit's stored author list, decodes it
and applies the +5 author bonus.

Measured on 2025 repair queries on melchior:
- **About two thirds of the time is in Tantivy, not Python.** Across 5,113 queries the native search took 195 s and the Python loop 98 s. The Python part decodes 2,650 hits per query on average, at 7.3 µs each. The global author join is not involved.
- **Search cost follows the most common word in the title, not the pool size.** A query is an OR of the title's words, including words like "the", because the index keeps stopwords. "the" appears in 7.0M of 41.6M titles, and queries containing it took about 36 ms even at limit 50 (49 ms at 10,000). "annabel lee" (11k matching titles) took 0.3 ms. Tantivy cannot skip most of "the"'s matches, because they all score almost the same, so it effectively scores every one.
- **Searches parallelize well.** On 16 threads, native searches reached 273 queries/s, 11x one thread. The Python decoding part still needs separate processes.

Fixes, cheapest first:

1. **Run in parallel** (no change to results). With 16 workers, the full backfill's retrieval takes about 25 min, and 2024's about 2 min. This is enough for this rollout.
2. **Change the query shape: require at least one informative word, and let common words only add score.** A "common" word is one appearing in more than about 1% of titles, read from the index itself rather than a hand-written list. Tantivy then only scores documents that contain a rare word, so the cost drops from about 36 ms to the rare word's cost (typically 0.2–3 ms). Titles made only of common words ("It", "Us") keep the current query.
   - Consequence: this changes results slightly. Titles that match only common words can no longer fill the pool. Such titles score far below any title that also matches an informative word, so they rarely reach the top 50. Include this change in a comparison against 2025 candidate sets, as with R2.
3. **Move the per-hit loop into Rust**, using the `tantivy` crate in the existing workspace. That removes most of the Python decoding time (about 19 ms per query), which becomes the dominant cost once the query-shape change is in. Worth doing when retrieval moves into the service; not needed for the backfill.

The query-shape change and the Rust loop together should give roughly 5–10 ms per query on one thread. That is an estimate, not a measurement.

## Next: fitting the pieces on the 3060 service

The backfill can spend VRAM freely. A continuous service on the 3060 (12 GiB,
shared with live story embedding) cannot. Throughput is not the constraint: at
2025 rates, new comments arrive at about 9k/day. That yields about 300 filter
passes, about 70 references, about 4k rerank pairs and about 95 Luna calls
(about $0.015) per day. Even at batch size 1, that is seconds to minutes of GPU
time per day. **The constraint is what has to stay resident.**

| Piece | VRAM if resident | Basis |
|---|---|---|
| Pplx embedding (already deployed) | about 1.7–3.1 GiB | Measured: 1,735 MiB peak on the 3060 deployment test, 3.1 GiB resident on the 5090 |
| Title GLiNER, batch 1–4 | about 1.0–1.5 GiB | Measured on base GLiNER large v2.5, which has the same architecture ([extraction](entity-extraction.md)) |
| Separate person GLiNER | about +0.9 GiB | 459M parameters in BF16 |
| Zerank-2, FP8 | about 4–4.5 GiB of weights, plus 144 KiB of KV cache per token (about 1.1 GiB at 8k batched tokens), plus vLLM overhead | Calculated from the model config (Qwen3-4B shape: 36 layers, 8 KV heads) |
| CUDA context per extra process | about 0.3–0.5 GiB each | Typical figure; not measured here |

That adds up to roughly 9–11 GiB of 12, before allocator headroom. At this margin
the second NER backbone and its process are worth removing. The 8.6 GiB title-NER
peak from the backfill is not a service figure: it came from a large batch size.

VRAM levers, cheapest first:

1. **Small batches** (a configuration change). Batch sizes 1–4 still process 100–350 comments/s, far more than the service needs.
2. **One GLiNER instead of two, without training.** Prompt the fine-tuned title checkpoint with both `book title` and `person`. Score its person spans against the saved 2025 `names.jsonl` (17,927 comments of base-model spans) and check title quality on the reviewed test set. If both hold, one backbone is enough for free. This takes minutes.
3. **Joint fine-tune, if step 2 loses person recall.** Train on reviewed title spans plus the base model's person spans from `names.jsonl` as silver labels. Earlier fine-tunes took about 2 minutes on the 5090. Re-run the title quality check afterwards.
4. **Measure whether the person signal is needed at all.** Rerun 2025 retrieval with and without person spans offline and compare the top-3 sets that reach Luna. If they barely change, dropping person NER saves the backbone and a forward pass.
5. **Zerank, the largest piece.** Cap `max_model_len` and batched tokens near real prompt lengths instead of 32k. Or load it per micro-batch instead of keeping it resident: vLLM load took about 45 s and the daily work is about a minute. A smaller reranker saves the most, but requires redoing the rerank quality comparison.
   - Precision: Zerank-2 is 4B parameters, run with vLLM online FP8. Weights and activations are FP8 with dynamic per-tensor scales; attention and the KV cache stay BF16. It is not MXFP8 or NVFP4. The [throughput study](resolver-throughput.md) chose it over vLLM BF16 because it was 1.6x faster and kept the same top-3 answerability (172/201).
   - The 3060 is Ampere and has no FP8 tensor cores. As far as I know, vLLM falls back to weight-only FP8 there, so the memory saving remains but the speed-up does not. Check this on the card.
   - Smaller candidates (for example ZeroEntropy's 1.7B small model or the Qwen3-Reranker 0.6B/4B family) can be checked in minutes with the existing harness (`resolver_rollout/bench_vllm.py`, `bench_compare.py`) on its 201 labeled proposals.
   - Consequence of swapping: the popularity weight (0.1 × log2 of reader count) and any score-gap gate are calibrated to Zerank-2's logit scale, so they must be recalibrated.

In the service, run both NER recipes (or one joint model) behind one worker
process. That also covers the NER consolidation that the backfill can skip.

## Appendix: component inventory

Source paths are relative to `packages/search-research/`.

| Component | Where | State on 2026-09-27 | Role in the rollout |
|---|---|---|---|
| HN mirror | `searchhn-pg/searchhn_test` | Live, PG 17 | Read-only source for per-year exports |
| Comment export and embedding | `tools/comment_slice.py` (`--year`) | Works; resumes from checkpoints | Used as is; B4 later |
| Quick filter | `tools/quick_filter.py`, `src/search_research/quick_filter.py` | Frozen standalone inference; exact 2025 pass-ID parity | R4 complete |
| Title NER | `tools/resolver_heal/titles.py`; `data/probes/books-gliner-training-v1/final-refit/model`, threshold 0.17 | Complete; full 2025 parity verified | R1 complete |
| Person NER | `tools/resolver_retry/ner.py` | Whole-workload static batching; full 2025 run and output comparison complete | B2 complete |
| Catalog retrieval | `tools/resolver_heal/catalog.py`, Tantivy index `data/probes/books-catalog-tantivy-v1` (5.08 GB, 41.6M works) | Works for repairs | R2, B1 |
| Legacy initial retrieval | `tools/resolver_retry/retrieve.py`, `tools/resolver_heal/authors.py` | Removed; fresh runs use `Catalog.search` | R2 complete |
| Rerank | `tools/resolver_retry/rerank.py` and `tools/resolver_heal/rerank.py`, Zerank-2 FP8 via vLLM | Works; two near-duplicate scripts | R3, R5. Merging the two scripts can wait. |
| Luna selection | `tools/resolver_heal/select.py`, `journal.py` | Works; SQLite receipt journal and resume | Used as is, with a budget per year |
| Publication | `tools/resolver_heal/publish.py` | Baseline optional; new-year smoke verified | R3 complete; B3 later |
| Comment explorer (FAISS) | `src/search_research/comment_explorer.py`, melchior:18081 | Running idle, 3.5 GiB swapped; phrase queries hit a dead port | Not used by the pipeline |
| Review UI | `src/search_research/resolver_review_web.py`, port 8767 | Not running | Optional, for inspecting a year's results |
| Tailnet inference | `magi06-inference`, RTX 3060 12 GiB | Proxy is healthy; host `nvidia-smi` reports a driver/library mismatch (a separate ops issue) | Interactive and small jobs only |

Not measured: bulk comment throughput on the 3060, per-year token lengths and
filter pass rates, and quality of the initial retrieval after R2. R2's check and the
per-year pass-rate check cover the last two.
