# First full book-resolution pipeline

**Historical baseline:** the current author-aware, popularity-boosted Luna rollout
and its repair loop are documented in [Current pipeline and handoff](pipeline-current.md).
The active run is `books-resolver-2025-heal-v2`, with no shortcut gate. Settings and
measurements below describe the earlier baseline unless explicitly reused there.

The completed 2025 repair run uses a combined title/author-metadata Tantivy index,
one fixed 10,000-title pool and a top-50 author-adjusted shortlist. See
[the implemented index and reuse contract](pipeline-current.md#reusing-the-catalog-and-retrieval-in-later-slices)
for the bounded build, query semantics and the remaining legacy initial-pass wiring.
[Final Luna verdicts](resolver-labels-handoff.md#final-luna-verdicts-after-repairs-2026-09-27)
locates the completed checkpoint and documents its plural work-ID schema. Do not
use this historical page's title-only search or singular-ID rollout contract as
the current repair implementation.

Decision, 2026-09-26: use **Tantivy over the full offline Open Library works
catalog**, followed by contextual Zerank and an optional final selector. The user
accepted Tantivy after the resolver spike. This is the first implementation design;
the complete pipeline is not yet a deployed service.

The 2025 labeling run sends every reference to both Luna and native DeepSeek V4.1 Flash,
including references the acceptance heuristic would skip. That doubles selector
coverage deliberately so the models and heuristic can be compared on the same
inputs. The user funded and authorized the run, and resumed it after throughput tuning.
[Tuning results and selected runtime](resolver-throughput.md) now specify native
vLLM FP8 with prefix caching (~13 minutes projected reranking, not 4–5 hours).
The saved rollout on melchior uses the frozen 26,221 references and
558,083 candidate metadata records. See the durable run contract below.

## Data flow and decisions

1. **Cheap comment filter:** reuse the existing embeddings and frozen 25% centroid /
   75% XGBoost blend from [training](training.md). Do not re-embed or retrain it.
2. **NER:** use the saved reference GLiNER at .17 from
   [entity training](entity-training.md). Preserve comment IDs and character spans;
   deduplicate repeated title references within a comment for processing.
3. **Abbreviation routing, intended first-version extension:** detect short spans
   with 2–8 ASCII letters/ampersands/periods and at least two uppercase letters.
   Resolve flagged unfamiliar abbreviations into a searchable title, preserving
   the original mention and context; otherwise pass the original title through.
   Store approved alias mappings with their provenance and context constraints.
   This shape rule flagged 12/250 proposals and caught 8/9 observed abbreviations;
   lowercase pmbok was missed. Detection does not itself identify the book.
   The translation-plus-reranking path has not been measured end to end. Smoke-test
   and version this extension separately before incorporating it into the full run;
   the completed baseline uses original spans only. No universal translation step.
4. **Lookup:** local Tantivy BM25 over all work titles; retrieve 50 candidates.
   The measured tokenizer lowercases, without stopword removal or stemming.
   Exact matching is not the retrieval algorithm. Author text, embeddings,
   popularity, typo expansion and extra query routes were not used in this baseline.
5. **Contextual reranking:** Zerank-2 with the [measured vLLM FP8 settings](resolver-throughput.md) scores the complete comment, with the
   target occurrence marked, against each candidate's title and catalog authors.
   Keep its top three work IDs. No date, description, subjects, popularity or BM25
   score is included in the model input. Individual pairs are scored independently.
6. **Optional automatic acceptance:** for pool 50, raw top score ≥10 and gap ≥2.
   The gap is to the best different normalized title+author group, collapsing only
   mechanically identical records. This is not general work canonicalization.
   The BF16 rule accepted 55/250, with 54 clean single-work matches and one fused span.
   Recalibrate on the new FP8 scores; do not assume the same thresholds transfer.
7. **Final selection:** full marked comment plus three titles/authors; choose one
   supplied ID or neither. Preserve abstentions for inspection. Series, nonbooks,
   fused spans and unresolved references must remain distinguishable in analysis.

Edition resolution is outside scope. Online lookup and cross-source fusion are
outside this first pipeline. The full catalog remains searchable; there is no
popularity cutoff. Global mention-only caching is not assumed safe for ambiguous
short names. Catalog version, original query, any translated query, candidates,
scores, model revision, final output and usage belong in the run receipts.

## Why this architecture

**Tantivy is the chosen first backend, not the winner of a completed engine
bakeoff.** It already supports the measured title retrieval and its complete index
is about 3 GiB. A small Rust executable embedding Tantivy is the preferred first
interface: persistent index reader, batched JSONL input/output, and local metadata
lookup. This avoids an additional search service for the offline research workload.
The spike currently uses Python bindings; the Rust executable is not implemented.

A dedicated search service would offer more ready-made query/typo controls,
management APIs and concurrent serving facilities, but adds deployment and service
operations. Embedded Tantivy leaves those query features and inspection facilities
to us. PG FTS was rejected by user preference and prior experience. No vector
retrieval substitution was selected. Add retrieval features one at a time with
measured gains rather than carrying an untested collection into the first version.

Zerank contextual scoring materially improved over title-only scoring and the BGE
baseline. Pool 50 is a useful first cost/coverage point: top-1 162/201 and top-3
172/201 versus pool 100 top-1 167/201 and top-3 175/201. Measured median rerank
latency approximately doubles, 603ms to 1198ms, on the same 12-request subset.

Luna is the current final-selection baseline because it rejects many more false
matches than the local Gemmas. On all 250 original spans:

| Selector after contextual top three from pool 50 | Correct | Uncredited selection | Abstain |
|---|---:|---:|---:|
| Gemma E4B, FP8 | 169 | 68 | 13 |
| Gemma 26B, NVFP4 | 169 | 45 | 36 |
| GPT-6 Luna, medium | 166 | 14 | 70 |
| GLiNER2.5-Decide, literal 1/2/3/neither | 92 | 93 | 65 |

Both Gemmas find 169 of the 172 targets present in the shortlist; Luna finds 166.
The principal difference is rejection when a suitable single-work choice is absent.
Raw+gap acceptance followed by Luna gives 167 correct /15 uncredited /68 abstentions,
skipping 55 calls. Bootstrap labels include Luna-assisted judgments and bounded
catalog review; the 250 are a development set, not independent human gold.

## 2025 workload and rollout contract

The frozen corpus contains 3,266,889 comments. The quick filter forwards 108,194;
reference NER yields 17,927 comments, 28,026 span occurrences and 26,221 references
after within-comment title deduplication. These are measured counts. Applying the
spike's final-link yield suggests roughly 17k correct links; it is a projection.
Upstream misses mean whole-corpus recall is not yet measured.

Run both selectors on the same saved top-three candidates, retaining the entire
comment and deterministically shuffling candidate presentation without ranks or
scores. Use GPT-6 Luna medium, matching the spike, and native DeepSeek
`deepseek-flash` with thinking enabled and `reasoning_effort=low`. The user
explicitly switched to native after third-party V4 latency proved poor. Native
currently serves V4.1 Flash. Keep the earlier OpenRouter V4 receipts under
`deepseek`; the final paired dataset uses `luna` and `deepseek-native`.

Native endpoint checks passed. Save explicit model/provider settings. The resumed
workers use rolling concurrency 128 for both Luna and native DeepSeek. Resume by
request identity, retain raw receipts and failures, and validate selected IDs
against the supplied candidates. Retry individual failures up to five times per
resume; double the output allowance on token exhaustion, capped at 65,536, and
save each changed request. Exhausted retries leave an unresolved item while the
rest of the queue continues. Never turn failed/truncated responses into abstentions. Keep human-reviewed labels
separate from proposed model selections. Score yield, emitted-link precision,
abstention, model disagreement and optional-gate replay on the same inputs.

## Cost estimate before funding

On 2026-09-26, 250 Luna selector calls used 113,617 input and 22,069 output tokens
(including billed reasoning), costing **$0.02269455**. That is approximately 454
input /88 output tokens per reference and **$2.38 for 26,221 references**.

The public OpenRouter catalog currently lists DeepSeek V4 Flash at $0.04704 per
million input tokens and $0.09408 per million output tokens. Using 454 input tokens
per request and no cache discount:

| DeepSeek generated tokens per reference | Projected full-run cost |
|---|---:|
| 88, same token-volume assumption as Luna | $0.78 |
| 500 | $1.79 |
| 2,000 | $5.49 |

These are scenarios, not measured DeepSeek selector usage. The combined estimate
is **about $3.20–$7.90**; a **$10 balance** gives working headroom. This covers one
selector call per reference per model, excluding translation, repeat experiments
and local GPU costs. No credit balance or secret was inspected. Pricing source:
[OpenRouter model catalog](https://openrouter.ai/api/v1/models). Check prices and
actual slice billing against saved receipts. The user authorized both full-run
labelers after funding and throughput tuning.

## Remaining bounded comparisons

Popularity has only been inspected as a cutoff, not measured as a ranking signal.
Reading-log counts at ranks 100k/500k/1M were 15/4/2: treat the low-count tail as
unranked noise. Compare a modest popularity boost against the fixed current
candidate ranking, preserving the full catalog, and report gains and regressions.
This can be replayed before expanding scope. Duplicated work records complicate
activity aggregation; no unmeasured identity merge is implied.

An operator inspection interface, typo-tolerant retrieval, broader aliases and
learned routing remain future iterations. The present design does not assume they
are all necessary. [Resolver iteration](resolver-iteration.md) owns the frozen
250-case setup; ignored receipts and runner copies live in
`data/probes/books-resolver-iteration-v1/final-pick/`.

## Durable run and resumption

The authoritative run directory on **melchior** is
`/home/ritsuko/projects/data/search-hn/data/research/books-resolver-2025-v1/`.
It is persistent research data, not a temporary directory. Its `run.sqlite` holds
frozen reference text/offsets, candidate metadata, all reranker scores, exact API
requests/responses, usage/cost, abstentions, failures and stage status. Model outputs
are proposed labels, not human gold. `manifest.json` pins source hashes; separate
`rerank-runtime.json`, `luna-config.json` and `deepseek-native-config.json` pin
current settings; `deepseek-config.json` preserves the superseded provider setup.
The `attempts` table keeps retry history, including paid failures after recovery.

Scripts live in `packages/search-research/tools/resolver_rollout/`. On melchior,
from the repository root, `bash packages/search-research/tools/resolver_rollout/run.sh`
resumes reranking if necessary, then both labelers. **Run it only after existing
workers exit**; each labeler also holds an exclusive process lock. Resumption skips
committed successes. Inspect `rerank.log`, `luna.log`, `deepseek-native.log` and
`progress.json`. Individual request errors are retained and retried; account-wide
authentication/funding failures drain and stop the worker. Incomplete work returns
a nonzero exit code. SIGTERM drains current requests before exit. Do not delete
completed rows to restart a model.

For a consistent portable copy, run
`uv run --no-sync --package search-research python packages/search-research/tools/resolver_rollout/snapshot.py --backup`.
This checks SQLite integrity and writes `checkpoint.sqlite` plus its SHA256 in
`progress.json`. Copy that checkpoint, not the live WAL database. A checkpoint and
configuration copies are retained in the same relative local research directory;
the live remote run remains authoritative until another snapshot is copied.

### Launch checkpoint, 2026-09-26

Both requested OpenRouter models passed eight initial calls using the strict JSON
schema and configured reasoning effort. Full workers are running at concurrency
32 per arm. At the first portable checkpoint: **2,560/26,221 reranked, 135 Luna
and 12 DeepSeek responses saved, zero recorded failures**. Reported spend was
$0.01306 Luna + $0.00234 DeepSeek; these initial samples are too small for a stable
final bill. The checkpoint is an early snapshot, not a completed calibration set.
The live database continues accumulating results. No quality scores for the full
2025 set are claimed from model agreement alone.

Reranking completed those 2,560 references in 100 seconds after model setup,
including first-window warmup. API completion time is separate; DeepSeek had a
slow initial response, so the reranking projection is not an API completion ETA.

### Recovery and provider correction

The first runner incorrectly stopped an entire model queue on one output-limit
response. Reranking completed, but only 711 Luna and 37 OpenRouter DeepSeek labels
were saved before those stops. The repaired worker tests cover continuation past
an exhausted item, transport retries, increasing output allowance, retaining retry
history, and resuming only missing results. The reranker scores are reused.

A six-input streaming diagnostic separated TTFT from generation. Median native
V4.1 TTFT was 0.60s, total latency 1.35s and output speed 219 tokens/s; the original
OpenInference V4 route measured 1.05s, 7.15s and 24 tokens/s. These are different
model versions, so this is an operational comparison, not a controlled model-quality
claim. Raw streaming receipts are in `latency-diagnosis.jsonl` inside the run.
The user selected native immediately; no 512-concurrency provider run was launched.

Luna has completed all 26,221 labels; native DeepSeek stopped at 10,736 labels
by user choice. See the handoff for exact final state. The older `deepseek` rows are superseded diagnostics, never merged into native
labels. Use `summarize.py` for counts and paired agreement, and `snapshot.py` for
usage across all recorded attempts. Native cost is reconstructed from tokens using
the saved official weekend pricing ($0.003/M cached input, $0.15/M fresh input,
$0.60/M output); it is distinct from OpenRouter's reported dollar charges.
