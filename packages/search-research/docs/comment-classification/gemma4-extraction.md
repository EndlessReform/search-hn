# Gemma 4 E4B structured book extraction

The later [26B NVFP4 comparison](gemma4-26b-extraction.md) supersedes the live
serving state below: E4B and the embedding container are now stopped so the
larger model can use the full 5090. These E4B measurements remain the baseline.

Gemma recovers substantially more titles than raw GLiNER, but this first-pass
configuration still has too many false book detections and incorrect author
assignments to treat its output as final. Warm throughput is about 92 comments/s
on quick-filter passes: approximately 20 minutes for 108,194 passes, versus
GLiNER's 3.3 minutes. All 3,266,889 comments project to about 9.1 hours using a
separate representative full-corpus sample.

## Serving configuration

- Host: melchior, RTX5090. GLiNER worker unloaded; embedding service retained.
- Model: `google/gemma-4-E4B-it`, served as `gemma4-e4b`.
- Runtime: vLLM 0.23.0 in the installed `vllm/vllm-openai:latest` image. The exact
  image ID and arguments are preserved in the experiment's `server.json`.
- BF16 failed at a 50% GPU reservation: 14.4 GiB weights and negative available KV
  cache after working-memory profiling. This is a memory failure, not a speed result.
- Online FP8 weight quantization with BF16 activations and KV cache. FP8 at 50%
  also lacked cache space; 58% reservation succeeded. Actual measured model
  process usage was 17,460 MiB, about 54% of the GPU. Loaded weights: 10.62 GiB;
  allocated KV cache: 1.62 GiB. The separate embedding process used 3,158 MiB.
- Text only; thinking disabled; 16,384-token maximum context; 128 server sequence
  slots; 2,048-token prefill batch; asynchronous scheduling.
- Prefix caching disabled so replayed benchmark comments cannot inflate results.
- Container: `searchhn-gemma4-extraction`. Remote endpoint:
  `http://127.0.0.1:18082/v1`. It remains running. Failed trial containers removed.

The client sends one whole comment per request, with temperature zero and a
2,048-token output ceiling. JSON Schema constrains this object:

```json
{"has_any_book": true, "books": [{"title": "The Martian", "author": "Andy Weir"}]}
```

`author` is required but nullable. The prompt requests every explicit prose
book/series title or usable nickname, deduplicates repeated mentions, and asks for
an author only when stated and associated with that book in the comment. Films,
games, songs, articles, papers, chapters, generic unnamed books and URL-only titles
are excluded to retain the existing audit scope. Pydantic rejects inconsistent
boolean/list pairs and empty titles. Failed or truncated generations are saved,
counted as failures, and stop the measurement; no silent retry or truncation.

References: [vLLM recipe](https://docs.vllm.ai/projects/recipes/en/stable/Google/Gemma4.html)
and [model](https://huggingface.co/google/gemma-4-E4B-it).

## Throughput

A fixed 256-comment subset of the earlier uniform random quick-filter sample was
used for the concurrency sweep. Grammar/kernel warmup is excluded; HTTP, model
inference, constrained JSON decoding and validation are included. Input loading
is outside timing. Representative 512-comment runs confirm the knee.

| Concurrent requests | Comments/s, 256 comments |
|---:|---:|
| 1 | 8.7 |
| 2 | 15.2 |
| 4 | 25.8 |
| 8 | 40.2 |
| 16 | 57.0 |
| 32 | 75.8 |
| **64** | **89.5** |
| 128 | 91.7 |

On 512 comments: concurrency 32 = 77.5/s, 64 = 91.7/s, 128 = 93.5/s. At 64, p95 latency
is 1.18 s versus 1.99 s at 128. Thus 64 is the smallest tested concurrency within 90%
of peak and achieves 98% of peak in the confirmation.

| Workload | Warm measured rate | Projected runtime |
|---|---:|---:|
| 108,194 quick-filter passes, Gemma | 91.5/s | 19 m 42 s |
| Same passes, prior GLiNER BF16 pipeline | 543.5/s | 3 m 19 s |
| All 3,266,889 comments, Gemma | 99.8/s | 9 h 06 m |

Gemma takes 5.94 times as long on the matched filter-pass workload. Full-corpus timing uses
an independent uniform 512-comment reservoir sample (seed 20260925), measured
twice: 99.7 and 99.9/s. Filter-pass confirmations were 91.7 and 91.3/s. Average
input/output tokens per comment: 321.6/18.6 for passes and 322.9/15.4 for full corpus.
These are warm projections, not completed corpus jobs; they exclude startup and
future corpus I/O. Add the previously measured 34 s if rerunning the quick filter.
We did not measure a paired full-corpus GLiNER rate.

## Quality at concurrency 64

320 existing cases were run; all 320 returned complete schema-valid JSON with a
consistent gate/list. One previously unresolved annotation is excluded from the
primary scores, leaving 319. The three sample strata remain separate. Original
labels came from Luna reviews with parent corrections; they are not human gold.
The prompt was fixed before inference, with no subsequent prompt tuning.

| Sample | Gemma gate TP/FP/FN/TN | Reviewed title references recovered | Valid title strings / emitted strings |
|---|---|---:|---:|
| Representative 192 |15/19/2/156|21/23|21/42|
| Title-rich 63 |30/7/1/25|80/87|80/95|
| Earlier 64 |29/10/0/25|74/76|73/85|

Representative gate precision is 15/34=44.1%, recall 15/17=88.2%. Raw GLiNER large
at .35 was 16/72=22.2% precise with 16/17=94.1% recall. Gemma forwards 17.7% of the
random sample, versus GLiNER's 37.5%. All 15 correctly gated random comments yield
a valid title after accepting the recorded series-qualified alias.

The two book-heavy samples recover 154/163 (94.5%) reviewed title references,
versus GLiNER's 131/163 (80.4%). Complete recovery occurs in 55/60 title-bearing
comments; 50/60 have all titles and no extra invalid titles. These are title-only
scores: author errors are not included in those success counts. Do not interpret
book-heavy sample precision as representative corpus yield.

Strict matching before the new semantic alias decisions recovers 150/163 in those
two samples. `aliases.jsonl` records six decisions including straight/curly
apostrophes, series-qualified names, and Strunk & White resolved to The Elements
of Style. The earlier gold counts Dice Book and its full name as two references
to one book; a single canonical book object correctly covers both. Thus these
are reviewed name/reference counts, not a count of distinct bibliographic works.

Concurrency 16 and 64 have identical gate counts and alias-aware title recovery
counts on these strata, but some individual outputs differ. For example, the
initial run added URL-only Startup in the underscore example; the final run did
not. Raw outputs for both are retained.

## Manual spot checks

- Comment 46392391, the screenshot list: every title's words appear, but If This Is
  a Man / The Truce is merged into one object. Fourteen of 16 titles are separate
  correct objects. All 14 non-null author assignments in the 15 returned objects
  match the comment, and the unstated Tolkien author is correctly null.
- Comment 46396803: 18/19 title references recovered with correct author grouping
  for all 18 returned books. Southern Reach series omitted.
- Comment 42655045: all three underscore-marked prose titles and authors correct.
- Comment 43617144: correctly separates two books named The Emigrants by Moberg
  and W.G. Sebald, and connects The Rings of Saturn to Sebald.
- Wrong authors: Manning (publisher) assigned to Machine Learning for Drug
  Discovery; Gary Cooper (actor) assigned to The Fountainhead; Doctorow assigned
  to The Deluge; Van Vogt assigned to A voyage to Arcturus and The worm Ouroboros.
  Those should be null under the comment-only author policy. These are five
  concrete wrong assignments, not a complete author-accuracy benchmark.
- Remaining false books include Bookman Old Style (explicitly a font), this book,
  three books, film titles, and URL-only names despite the prompt exclusions.

Author checks: [The Deluge publisher](https://www.simonandschuster.com/books/The-Deluge/Stephen-Markley/9781982123109),
[AFI Fountainhead credits](https://catalog.afi.com/Film/25929-THE-FOUNTAINHEAD),
[Arcturus](https://www.gutenberg.org/ebooks/1329),
[Ouroboros](https://www.gutenberg.org/ebooks/67090).

## Reproduction and artifacts

- Client: `packages/search-research/tools/comment_book_llm.py`.
- Scorer: `packages/search-research/tools/comment_book_audit.py`.
- Artifacts: `data/probes/books-gemma4-v1/` locally and on melchior.
- Frozen samples: `passed512.jsonl`, `full512.jsonl`, `evaluation.jsonl`.
- Final quality: `fp8-evaluation-knee/c64.jsonl`, `final-audit/metrics.json`,
  `final-audit/cases.jsonl`, `final-strict-audit/`, `aliases.jsonl`.
- Throughput: `fp8-knee/`, `fp8-confirm/`, corresponding repeat directories,
  `fp8-full/`, `throughput.json`, `summarize.py`.
- Direct receipts: `spotchecks.jsonl`, exact `request.json` in each run,
  `server.json`, `server.log`, `bf16-startup.log`, `fp8-50-startup.log`.

Example on melchior, with the server running:

```sh
uv run --locked --package search-research python packages/search-research/tools/comment_book_llm.py \
  --input data/probes/books-gemma4-v1/passed512.jsonl \
  --out data/probes/books-gemma4-v1/new-run --concurrency 64
```

The output directory must not already exist, preserving each measurement.
