# Book resolver labels: final verdicts and historical baseline

For the current search algorithm and new-slice reuse boundaries, start with
[Current pipeline](pipeline-current.md) and
[Catalog reuse](pipeline-current.md#reusing-the-catalog-and-retrieval-in-later-slices).
The completed Luna-only repair run supersedes the original baseline for final
2025 verdicts. Both datasets remain available; their selection schemas differ.

## Final Luna verdicts after repairs, 2026-09-27

**Read `selection.work_ids` and `resolution_items` from the completed checkpoint,
not `decision.results` or the round-0 receipt export.** The publisher retains the
initial receipt in the same payload, so `decision` can still request a search even
though the final result has already resolved that search.

The authoritative completed run is on **melchior**:

```text
/home/ritsuko/projects/data/search-hn/data/research/books-resolver-2025-heal-v2/
```

| File | Purpose |
|---|---|
| `checkpoint.sqlite` | Frozen final verdicts, original references, candidate metadata, rankings and repair history; about 1.2 GB |
| `receipts.sqlite` | Authoritative per-attempt API journal, including errors and billed usage; separate from the published checkpoint |
| `round0-decisions.jsonl`, `round1-decisions.jsonl`, `round2-decisions.jsonl` | Journal exports by round; attempts, not one final verdict per original reference |
| `roundN-ready.json` | Exact candidate/input material for that round |
| `roundN-queries.json`, `roundN-reused.json` | Repair parent/child links and reused candidate lists |
| `manifest.json` | Original frozen population, settings and script hashes |
| `recovery-manifest-20260927.json` | Index change, selected concurrency, completion counts and updated script hashes |
| `stage-times.jsonl`, `selector-metrics-*.json` | Recovery wall-clock, throughput, latency, token and error measurements |
| `gate-backtest.json` | Offline shortcut agreement with final Luna sets; not accuracy or an enabled gate |
| `report.html` | Static final counts, top 100 books, examples, tokens, costs and timings |

The report also exists locally at
`/Users/ritsuko/projects/data/search-hn/data/research/books-resolver-2025-heal-v2/report.html`.
The 1.2 GB completed checkpoint has **not** been mirrored to the workstation as
part of this recovery. Older local checkpoints/UI defaults are different datasets.
To obtain the final frozen file without overwriting an earlier run, from the local
repository root:

```sh
mkdir -p data/research/books-resolver-2025-heal-v2
scp melchior:/home/ritsuko/projects/data/search-hn/data/research/books-resolver-2025-heal-v2/checkpoint.sqlite \
  data/research/books-resolver-2025-heal-v2/checkpoint.sqlite
```

This is a completed immutable snapshot. For the attempt journal, use a consistent
SQLite backup if writes are active; do not copy only a live WAL database's main
file or open it with SQLite's immutable option.

### Final checkpoint schema

All five tables store JSON in `payload`; IDs are original reference IDs or catalog
work IDs as indicated below.

- `refs(id PRIMARY KEY, ordinal, payload)`: source `comment_id`, original `title`,
  complete `context`, character `start`/`end`, NER/person spans, original retrieval
  and final `resolution_items`/`repair_count`. Offsets refer to the saved context.
- `selections(id, model, payload)`, primary key `(id, model)`: choose **`model='luna'`**.
  `selection.work_ids` is a deduplicated list of final selected IDs, possibly empty
  or multiple. `resolution_items` retains each final target's `title`, optional
  `author`, `action` (`select`/`abstain`), `work_id`, `reason` and `case_id`.
  A root can contain both selections and abstentions: an empty list means no
  selected work, not an API failure. `special:bible` is a synthetic work ID.
- `documents(id PRIMARY KEY, payload)`: work `id`, `title`, `authors` and frozen
  `readinglog_count`. `special:bible` need not have a document row; retain it in
  left joins. Distinct Open Library IDs are not canonicalized into one book.
- `rankings(id PRIMARY KEY, payload)`: original candidate `ids`, boosted `scores`,
  raw `original_scores` and `top3`. Parallel arrays are not necessarily sorted;
  these are initial rankings, not the final repair shortlist.
- `history(id PRIMARY KEY, payload)`: `initial_decision` and `repairs`, each repair
  holding its `case` and `receipt`. Match a terminal item's `case_id` to the root
  or repair case ID to find the actual deciding request and supplied candidates.

`model='luna-original'` is the preserved original 26,221-reference baseline in
this checkpoint. It uses singular `selection.work_id`; it is neither the final
verdict nor this repair run's initial round. In the older 2,500-case checkpoint,
the same comparison key denotes a different preceding run. Always retain the run
identity alongside labels. All labels here are model outputs, not human gold.

### Reading final verdicts with SQLite

Run these queries against the completed `checkpoint.sqlite`. The first retains
one row per original reference, including abstentions and split selections:

```sql
SELECT s.id,
       json_extract(r.payload, '$.comment_id') AS comment_id,
       json_extract(r.payload, '$.title') AS original_mention,
       json_extract(s.payload, '$.selection.work_ids') AS final_work_ids,
       json_extract(s.payload, '$.resolution_items') AS final_targets
FROM selections AS s JOIN refs AS r ON r.id = s.id
WHERE s.model = 'luna'
ORDER BY r.ordinal;
```

For one row per selected reference/work pair, retaining synthetic IDs:

```sql
SELECT s.id AS reference_id,
       json_extract(r.payload, '$.comment_id') AS comment_id,
       w.value AS work_id,
       CASE WHEN w.value = 'special:bible' THEN 'Bible'
            ELSE json_extract(d.payload, '$.title') END AS catalog_title,
       json_extract(d.payload, '$.authors') AS catalog_authors
FROM selections AS s
JOIN refs AS r ON r.id = s.id
JOIN json_each(s.payload, '$.selection.work_ids') AS w
LEFT JOIN documents AS d ON d.id = w.value
WHERE s.model = 'luna';
```

The examples were executed against the completed checkpoint: the first returns
26,221 roots; the second returns 22,666 deduplicated reference/work pairs. The
22,702 selected target items are a different count: multiple targets can resolve
to the same work within one reference.

The expanded query deliberately omits roots without selected works. For book
frequency, count distinct `comment_id` per work ID rather than reference rows;
multiple mentions in one comment otherwise inflate frequency. For reasons and
abstentions, expand `resolution_items` instead of `selection.work_ids`.

The checkpoint has 26,221 final roots: 22,013 with selections and 4,208 without;
22,702 selected terminal targets and 4,330 abstentions; 9,929 distinct selected work
IDs. All 5,113 round-1 and 2,791 round-2 cases completed. There are zero pending
cases. These counts describe yield, not selection correctness.

For exact per-attempt accounting, `receipts.sqlite` contains
`receipts(round, id, attempt, fingerprint, payload)`, primary key
`(round,id,attempt)`. A payload with `decision` is a successful attempt; `error`
marks a failed attempt. The complete run has 34,125 successful cases across rounds
and 14 failed attempts, all followed by success. Sum `response.usage.cost` across
**all attempts**, not the root-level `selections.response` fields, to obtain the
recorded $4.933901275 total. Eleven transport failures returned no usage receipt;
recorded spend is not an independently reconciled invoice. Never count retries
or child cases as additional original-reference labels.

After copying the final checkpoint, explicitly select it for the review UI:

```sh
uv run --no-sync --package search-research python -m search_research.resolver_review_web \
  --checkpoint data/research/books-resolver-2025-heal-v2/checkpoint.sqlite --port 8767
```

No model calls are needed to inspect this snapshot. For future slices, preserve
run ID, source text/offsets and final target provenance when consuming these labels;
a title-to-work lookup alone loses context and split/abstention semantics.

## Historical original baseline, 2026-09-26

Everything below describes `books-resolver-2025-v1`, before repair searches and
multi-work verdicts. The older non-Luna runs remain stopped. Its singular-ID SQL
must not be used to read the completed repair run above.

## Locations

Authoritative files are on **melchior** in
`/home/ritsuko/projects/data/search-hn/data/research/books-resolver-2025-v1/`.

- `run.sqlite`: complete saved run, including contexts, candidates and API receipts.
- `checkpoint.sqlite`: portable SQLite backup, verified with `PRAGMA integrity_check`.
- `summary.json`: label counts and agreement on the partial overlap.
- `progress.json`: stage status, usage and checkpoint hash.
- `manifest.json`: original inputs and source hashes.
- `luna-config.json`, `deepseek-native-config.json`, `deepseek-config.json`,
  `rerank-runtime.json`: exact model/runtime settings.
- `*-recovery-policy.json`: retry and concurrency settings.
- `latency-diagnosis.jsonl`: separate streaming diagnostic, not production labels.

A copy of the final checkpoint, summaries and configuration JSON files is at
`/Users/ritsuko/projects/data/search-hn/data/research/books-resolver-2025-v1/`.
Checkpoint SHA256:
`9ff18da46feff9c32badc11d14de37144a8ca6da4c43c120f0905951bf8af792`.
These data files are ignored by Git; this tracked-intended document is the locator.

## What is complete

| Selection model key | Saved labels | Selected work | Abstained | Status |
|---|---:|---:|---:|---|
| `luna` | 26,221 | 17,277 | 8,944 | Complete; zero unresolved failures |
| `deepseek-native` | 10,736 | 6,894 | 3,842 | Partial; 15,485 missing; stopped |
| `deepseek` | 981 | 644 | 337 | Superseded OpenRouter V4 run; keep separate |

All 26,221 references have reranker results; 314 had no retrieved candidates.
Luna selected 8,813 distinct work IDs. Native is V4.1 Flash (`deepseek-flash`),
whereas the older OpenRouter run is V4 Flash. Do not combine their labels.
The final native process exited after a 402 response during shutdown; its stored
status is `failed`. The user explicitly chose to stop native work. Its 495 failure
rows are request failures, not abstentions; successful partial labels remain usable.

## Reading the labels

`selections` is keyed by `(id, model)`. Each JSON `payload` contains:

- `selection.work_id`: chosen Open Library work ID, or JSON null for abstention.
- `request`: exact prompt, full marked comment and presented title/author candidates.
- `response`: raw model response and token usage; OpenRouter also reports cost.
- `seconds`: request wall time; `created_unix`: request timestamp.

Join `selections.id = refs.id` for the original comment ID, span offsets, NER score,
full comment and original top-50 candidates. Join `rankings.id = refs.id` for all
reranker scores and `top3`. `documents` maps catalog work IDs to titles/authors.
`attempts` retains successful and failed retry receipts; `failures` holds outstanding
failures. Do not count attempts as additional labels or infer abstention from failures.

Example SQLite query against the checkpoint:

```sql
SELECT s.id,
       json_extract(r.payload, '$.comment_id') AS comment_id,
       json_extract(r.payload, '$.title') AS mention,
       json_extract(s.payload, '$.selection.work_id') AS work_id
FROM selections s JOIN refs r ON r.id = s.id
WHERE s.model = 'luna';
```

These are model-generated calibration proposals, not independent human gold.
On 10,736 paired successful labels: 6,078 same work ID, 3,511 both abstain,
331 Luna-only selections, 183 native-only selections, 633 differing work IDs.
This is agreement, not an accuracy score; distinct IDs may describe the same book.

## Accounting and scripts

Recorded Luna spend including failed attempts: **$2.45118**. Superseded OpenRouter
DeepSeek spend: **$0.66688**. Native recorded token usage gives **$6.46016** at the
saved official weekend rates. Diagnostic calls and interrupted requests without a
receipt are separate from these production totals.

Scripts: `packages/search-research/tools/resolver_rollout/`. `summarize.py` and
`snapshot.py --backup` are read/summary operations; `selector.py` and `run.sh`
make paid calls and must not be run as part of simply inspecting these labels.
The repaired worker preserves successes, retries per item, and continues past
exhausted items. Future native work must account for the provider's balance-based
concurrency limit; raising concurrency did not fix the original route's slow decode.

See [architecture](resolver-architecture.md) and [throughput](resolver-throughput.md)
for the pipeline and measurements. No commit was created; unrelated search-agent
changes in the working tree are outside this resolver work.

## Local review UI

From the repository root:

```sh
uv run --no-sync --package search-research python -m search_research.resolver_review_web --port 8767
```

Open `http://127.0.0.1:8767`. This read-only HTMX interface uses the local frozen
`checkpoint.sqlite`; do not point immutable SQLite reads at a live WAL database.
`--checkpoint` accepts another frozen checkpoint. The page shows the full marked
source passage, all saved BM25 candidates, all reranker scores, the saved top three,
and Luna versus native V4.1 choices. Older OpenRouter V4 is a separate comparison.
Filters cover disagreements, missing outputs, mentions/IDs, and duplicate shortlist
metadata. Missing outputs are not abstentions. Exact requests and case JSON remain
available for inspection. No model calls or new dependencies are involved.
