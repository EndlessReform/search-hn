# GLiNER extraction: first throughput and quality check

**Historical raw-model audit.** For the completed supervised experiments and
current next step, see [Trained book extraction](entity-training.md). Input cleanup
suggestions below describe the early exploration, not the current work plan.

The first check supports using GLiNER as a fast candidate extractor, with further
input preparation work before fine-tuning. The current whole-comment recipe
finds some valid title in 28/29 reviewed title-bearing comments, but it also emits
book spans in 21/35 comments with no explicit book title. A positive NER result
alone does not establish that a real book title is present.

## Population and reproducibility

Run date: 2026-09-20. Host: melchior, RTX 5090. Model:
`gliner-community/gliner_large-v2.5`, GLiNER 0.2.29, PyTorch 2.14.0.
Labels match the user's playground: `book`, `author`, `url`, with thresholds
0.35, 0.50, 0.50. Nested spans and multiple labels are enabled.

Applying the frozen [25/75 quick filter](training.md#closing-decision-centroid--xgboost-quick-filter-2026-09-20)
to all 3,266,889 frozen 2025 comments passes **108,194 (3.3118%)**. Counting took
34.20 seconds. This uses the saved XGBoost best iteration, 293 (294 trees), rather
than all 344 trees retained in its early-stopped artifact. Checks reproduced
both the exact 310 passed IDs from the earlier 10k fixture and the held-out
393 positive / 313 negative passes. No model was refitted.

Seed 20260923 selects 512 passed comments for timing, then 32 positive and 32
negative examples from the held-out Luna-labeled passes. These are **recommendation
labels**, not title-presence labels. Four `gpt-5.6-luna` agents each reviewed 16
comments; parent spot-checks corrected review mistakes and checked prediction
accounting. The deliberately balanced 64-comment review is a diagnostic sample,
not a corpus-prevalence or corpus-precision estimate.

Artifacts are in `data/probes/books-gliner-v1/` on melchior. Review packets,
judgments, and summaries are also copied locally. Source databases are read-only;
no extraction has been run over the complete passed population yet.

## Precision and batch size

The original playground ran FP32. At batch 16, the same 512 comments took
2.071 seconds in FP32 versus 0.873 seconds in BF16: about **2.37× faster**.
BF16 changed decoded span sets in 8/512 comments. Matched span score differences
were at most 0.0303 (99th percentile 0.0225). Two comments changed the any-book
boolean; both lost generic false positives near 0.35 (`diary` and `hands-on
tutorial`). The separate 64-comment review had no any-book changes.

The live CUDA playground now uses BF16 and reports the dtype. CPU uses FP32.
No thresholds, ontologies, or text preparation were changed in production.

Timing includes GLiNER tokenization, device transfer, forward pass, and decoding,
with CUDA synchronization. Windows are sorted by text length to reduce padding.
Each batch size has three timed passes after warming; the table uses the median.
Other GPU services remained resident.

| Batch | Comments/second | Time for 512 comments | Peak allocated GPU GiB |
| ---: | ---: | ---: | ---: |
| 1 | 108 | 4.733 s | 1.05 |
| 2 | 197 | 2.605 s | 1.17 |
| 4 | 348 | 1.470 s | 1.45 |
| 8 | 517 | 0.990 s | 2.01 |
| **16** | **589** | **0.869 s** | **3.13** |
| 32 | 525 | 0.975 s | 5.36 |
| 64 | 386 | 1.327 s | 9.84 |
| 128 | 244 | 2.098 s | 18.78 |

The knee is **16**, defined here as the smallest batch within 90% of the highest
observed throughput. Batch 128 triggered allocator retries and reserved about
24.5 GiB; larger batches have no advantage here. Peak allocated memory excludes
other processes and differs from cached/reserved memory. At batch 16, unsorted
input took 2.066 seconds: length sorting matters substantially.

A second measurement includes SQLite text lookup, window construction/sorting,
span merging, and JSON serialization: **543.5 comments/second**, projecting
**199 seconds (3.3 minutes)** for the 108,194 passes. Model-only projection is
184 seconds. These are warm sample-based estimates, excluding model download,
startup and durable output writes; plan roughly 3–4 minutes for NER, plus the
34-second filter pass when needed. The full population's median/p95 lengths are
238/1,210 characters versus 224/1,052 in the timing sample, so the sample is
somewhat lighter. Very long comments can require more windows; the longest full
population comment is 19,391 characters.

## Four-agent review and parent checks

A title includes an explicit book/series name or usable acronym/nickname in the
comment. Author-only references and generic subjects do not qualify. URL-only
clues are kept separate: they may be useful to a resolver, but do not establish a
clean title span in the prose. Repeated mentions and overlapping predictions are
not treated as additional distinct book names.

| Review result | `book` ≥ 0.35 | `book title` ≥ 0.35 |
| --- | ---: | ---: |
| Title-bearing comments with at least one valid extracted title | 28/29 | 28/29 |
| Title-free comments incorrectly producing a title span | 21/35 | 15/35 |
| Title-bearing comments with every named title found | 26/29 | 26/29 |
| Every named title found and no extra invalid title spans | 24/29 | 25/29 |
| Distinct title names recovered, counted per comment | 65/76 | 63/76 |

Within the original Luna-positive 32, 28 actually contain an explicit title and
27 produce a valid title. The Luna-negative 32 contain one explicit title
(`Neuromancer`), correctly extracted. Thus recommendation labels and title-presence
labels cannot substitute for each other.

False positives include generic descriptions (`code design`, `dot-com bubble`),
software/model names (`DikuMUD`, `Opus`), a YouTube channel, a text-adventure game,
and URL slug fragments. Several have high scores. Parent review caught Luna
judgment mistakes too: `Spider and Web` is explicitly a game in its comment;
`Final Fantasy V` is a book in its Boss Fight context, confirmed by the
[publisher](https://www.simonandschuster.com/books/Final-Fantasy-V/Chris-Kohler/Boss-Fight-Books/9781940535715).
`UHH` and `Dice Book` are usable abbreviated book references, not automatic
false positives merely because they are noncanonical forms.

A post-hoc threshold check on these same reviewed outputs illustrates the tradeoff:

| `book` gate threshold | Title-bearing comments passed | Title-free comments passed |
| ---: | ---: | ---: |
| 0.35 | 28/29 | 21/35 |
| 0.50 | 28/29 | 14/35 |
| 0.70 | 28/29 | 8/35 |
| 0.85 | 27/29 | 6/35 |
| 0.95 | 19/29 | 3/35 |

These are exploratory numbers on the inspected sample, not a selected production
threshold. Raising a comment gate need not imply raising the extraction threshold
for all the other entities in an admitted comment.

## Why the obvious misses matter

- **Screenshot, comment 46392391:** full-comment inference finds 11/16 distinct
  titles. It misses `If This Is a Man`, `The Truce`, `Machine Vendetta`,
  `Future's Edge`, and `Blueshift`. The text has only 210 model words, so this is
  not truncation. Separate list-line inference recovers **16/16**, usually with
  scores above 0.95. But it also mistakes the introductory quoted theme for a
  title, demonstrating the cost of removing context indiscriminately.
- **Comment 42655045:** full and per-line inference miss all three prose titles
  wrapped in literal underscores. Replacing underscores with spaces in non-URL
  lines preserves offsets and recovers **all three** in the full comment, at
  about 0.84, 0.96 and 0.91. This was a diagnostic treatment, not deployed cleanup.
- **Comment 45711672:** whole-comment inference misses `1984`, `Bravzxe new world`,
  and `Kollocain`. Per-line inference recovers all ten distinct titles, while
  also hallucinating titles from a generic introductory sentence.

For complete extraction, the current recipe is not sufficient. However, these
failures justify testing emphasis cleanup and context-aware list segmentation
before fine-tuning. For routing to a stronger resolver, the current model has
promising sensitivity but substantial false positives; treat it as a candidate
gate and validate title identity at the next stage. A higher gate threshold around
0.70 is worth testing on fresh comments, separately from the span threshold.

## Re-run

Run model work on melchior. The sampling script deliberately refuses to overwrite
its output directory; choose a fresh experiment path in the scripts for a repeat.

```sh
uv run --locked --package search-research --extra ner --with xgboost-cpu python \
  packages/search-research/tools/comment_entity_samples.py
uv run --locked --package search-research --extra ner python \
  packages/search-research/tools/comment_entity_benchmark.py
uv run --locked --package search-research --extra ner python \
  packages/search-research/tools/comment_entity_checks.py
uv run --locked --package search-research --extra ner python \
  packages/search-research/tools/comment_entity_audit.py
```

`comment_entity_audit.py` requires the four `*_review.jsonl` files from the agent
review. The saved raw model outputs remain separate from the human/agent judgments.

## Representative follow-up and XXL comparison (2026-09-20)

The random-pass survey makes the routing limitation clearer: on 192 uniformly
sampled quick-filter passes, large/book/.35 forwards 72 comments, only 16 of which
contain an explicit book/series title. That is 22.2% precision with 16/17 (94.1%)
comment recall. Large/book-title/.70 forwards 42 with 15 real positives: 35.7%
precision, 88.2% recall. This can reduce a stronger model's workload, but does not
establish that a forwarded comment has valid books.

Large is 459M parameters, not the largest v2.5 model. The 1.61B XXL checkpoint was
also tested on the 5090 in BF16. XXL/book/.90 reaches 13/28 (46.4%) gate precision
and retains 13/17 title-bearing comments. On the previous title-rich sample at
.35, XXL recovers 72/87 titles versus large's 66/87, with clean complete extraction
on 20/31 versus 17/31 positive comments. Bigger helps some recall but does not
solve semantic false positives or completeness.

The representative sample has only 17 positives; current gate precision has a
Wilson 95% interval of 14.2–33.1%, recall 73.0–98.9%. A larger survey could narrow
those intervals, but these results already justify moving to a stronger verifier
or supervised task-specific training instead of more ad hoc zero-shot tweaks.
These are agent-reviewed annotations with parent corrections, not human gold.

Full methods, scope sensitivity, frozen inputs, model outputs, scoring scripts and
all operating points: `data/probes/books-gliner-survey-v1/README.md` and adjacent
artifacts. The production playground remains unchanged.

## Gemma structured extraction follow-up

[Gemma 4 E4B results](gemma4-extraction.md) cover the next-stage structured
`has_any_book` / `books[{title, author}]` comparison. At the measured concurrency
knee of 64, FP8 Gemma processes about 92 filter passes/s (20 minutes for 108,194),
versus GLiNER's 543.5/s. It recovers 154/163 reviewed title references across the
two book-heavy samples, versus 131/163 for GLiNER, but the representative gate is
only 44% precise and author association has clear errors. The note links exact
outputs, schema, scripts, startup memory results, and manual spot checks.
