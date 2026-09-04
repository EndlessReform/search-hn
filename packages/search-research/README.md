# Known-story retrieval research

This package evaluates the production search agent, without changing its query
syntax, ranking, tools or system prompt. `search_agent.runtime.SearchRuntime`
is shared with the TUI; `search_agent.headless` supplies a durable observer.

## Driver location and current run (2026-09-04)

**The laptop owns the driver, source, future trajectories and reports.** Only
Qwen/Gemma inference runs on `melchior`; Luna uses its API and PostgreSQL remains
remote. No code synchronization is needed for subsequent runs. The current run
was started remotely before this clarification and is being left undisturbed at
`/home/ritsuko/research-runs/fts-baseline-20260904/`, on the persistent `/home`
filesystem. `code/` is its isolated source snapshot; `plain-results/` contains its
active data. A partial copy is available locally at
`data/fts-baseline-20260904/plain-results/`. Do not start a duplicate suite against
the same local-model server while that run is active. The copied journals are a
snapshot, not live progress; one final artifact retrieval will collect later data.
Do not store trajectories in `/tmp` or tmpfs. Source revision before edits:
`8682850e09c97acf50ee30b1dc3adf22086d105c`.

The laptop's remote-model endpoint is `http://melchior-1:5000/v1`. Its exact model IDs
are `qwen-3.6-27b` and `gemma-4-31b-speculative`. Both advertise one inference
slot. Run local models sequentially, with concurrency 1; Luna may run alongside
with concurrency 2. Model switching can evict a loaded model. The experiment
does not change server settings, sampling temperatures or speculative decoding.

## Reproduce

From the repository root, use the committed UV lockfile:

```sh
uv sync --locked --package search-research
export HN_QUERY_DATABASE_URL=postgresql://readonly_hn_agent@searchhn-pg:5432/searchhn_test
export PGOPTIONS='-c statement_timeout=30000'
# Set OPENAI_API_KEY through your usual secret mechanism. Never put it in logs.
export EVAL_ROOT="$PWD/data/fts-next-run"

uv run --locked --package search-research hn-eval generate --root "$EVAL_ROOT"
uv run --locked --package search-research hn-eval curate --root "$EVAL_ROOT" \
  --exclusions packages/search-research/exclusions-20260904.json

# Orchestrate all three models with bounded concurrency (recommended).
uv run --locked --package search-research hn-eval suite --root "$EVAL_ROOT" --limit 3
# Inspect pilot journals and the report, then resume all frozen cases:
uv run --locked --package search-research hn-eval suite --root "$EVAL_ROOT"

# Alternatively, run each model individually:
uv run --locked --package search-research hn-eval run --root "$EVAL_ROOT" \
  --model gpt-5.6-luna --concurrency 2
uv run --locked --package search-research hn-eval run --root "$EVAL_ROOT" \
  --model qwen-3.6-27b --base-url http://melchior-1:5000/v1
uv run --locked --package search-research hn-eval run --root "$EVAL_ROOT" \
  --model gemma-4-31b-speculative --base-url http://melchior-1:5000/v1
uv run --locked --package search-research hn-eval report --root "$EVAL_ROOT"
```

Run the driver in a visible laptop terminal. Each finished case prints its model,
case outcome and terminal/expected progress count. Keep the laptop awake during
a run, or resume it later; server inference is accessed over HTTP, not SSH.
`suite` and `snapshot` accept `--local-base-url` to override the remote endpoint.
For an apples-to-apples rerun, copy the frozen `eval.jsonl` into a fresh local
root instead of generating new questions. Never reuse the live remote run's
root or overwrite its traces. The API key is inherited in memory; it is never journaled.
The driver uses a per-model advisory lock. Repeating a run skips terminal cases;
an interrupted case gets a new attempt file, leaving the old partial trace intact.
Terminal errors remain visible and are not silently retried. To deliberately
rerun a failed cohort after fixing an infrastructure issue, use a new experiment
root with a copy of the frozen `eval.jsonl`, and keep the original results.

Defaults: 10 model turns, 4096 output tokens per request, 600 seconds per
trajectory, HTTP timeout 180 seconds, one SDK HTTP retry. Consecutive API failures
trip a three-error circuit breaker. Ordinary budget/tool failures stay in the
denominator. Execution stops after three consecutive tool errors, as in the TUI.

## Dataset construction and limitations

Seed 20260904 fixes the sample. Sample distinct days from the preceding two years;
choose one story randomly from each day's top ten valid linked stories with
comments. Date equality uses `idx_items_story_day_score`, avoiding a corpus scan.
The initial pool contains 92 recent-day candidates and 80 older-day candidates.
Screening aims for 60 recent plus 40 older stories; the independent review must
retain at least 50 from the last three months. Stories are deduplicated
by URL during sampling. Obvious elections, anniversaries and calendar events are
excluded by a lexical screen and model review. Frozen source includes title,
URL, date, score, body and three top-level comments, not the external article.

Luna creates two questions per target: an entity-oriented question and a
paraphrase. A separate Luna call reviews them for identifiable story context,
source-supported answer detail, and date/ID/URL leakage. Reviews happen before
any retrieval results are seen. Questions ask for details in the discussion;
only question text enters the evaluated agent. Target metadata is journal-only.
Prompts are synthetic, and using Luna as generator/reviewer can bias the style.
Recency reduces potential training overlap; it cannot prove absence of leakage.

`candidates.jsonl` and `generation.jsonl` preserve sampling and rejection evidence.
`dataset.jsonl` is the initial selection; `review.jsonl` preserves review decisions;
`eval.jsonl` is the final frozen set. Do not change it after starting rollouts.
The first prompt set was judged too wordy and its rollout was stopped. Preserve
`results/` as that earlier version. The revised set in `plain-results/` keeps
exactly the same 98 targets (60 recent), with shorter, everyday questions:

```sh
uv run --locked --package search-research hn-eval rewrite \
  --source-root /home/ritsuko/research-runs/fts-baseline-20260904/results \
  --root /home/ritsuko/research-runs/fts-baseline-20260904/plain-results \
  --question-edits packages/search-research/plain-question-edits.json
```

`rewrite.jsonl` journals each revision and hashes its original frozen source and
instructions. Rewrites do not receive retrieval results or change target selection.
The checked-in question edits repair residual wordiness and missing topic clues;
their hash is stored with edited questions. `plain-draft/` preserves the unedited
model rewrite. To reproduce the exact frozen wording without stochastic regeneration,
copy the original `rewrite.jsonl` into a new output directory before this command.
Use `plain-results` as `EVAL_ROOT` for subsequent pilot/suite/report commands;
never pool the two prompt versions. Questions aim for 10–25 words, with a validated
35-word ceiling, while preserving identifying facts and source-supported answers.

The live mirror may change scores/content later: exact historical reranking uses
captured tool results, while new live runs measure the then-current database.

## Metrics: what k means

- **exposed**: target appears in a story result (or a validated comment lookup for
  that story) included in a successful subsequent
  model request. Merely returning a tool result on the last allowed turn does not
  count. Failed HTTP requests do not establish exposure either.
- **variant_pass@1**: mean exposure over the two fixed prompt variants per story.
  **variant_pass@2**: either variant exposes the target. These are fixed variants,
  not independent stochastic samples or an unbiased code-generation estimator.
- **query_pass@k**: at least one of the first k consumed search lists includes the
  target, at any returned rank. A batch of three queries counts as three lists.
- **recall@k / ndcg@k**: first-seen, deduplicated concatenation of consumed story
  result lists, truncated to k. This reflects cumulative context arrival order.
- **first_query_recall@k / first_query_ndcg@k**: rank within the first consumed
  result list; use these to distinguish initial retrieval from agent recovery.
- **cited**: the final answer contains the literal `【story:ID】` anchor. Citation
  does not imply retrieval or correct answer content.
- **cited_target_comment**: a final comment anchor belongs to a target comment
  actually consumed by the model. **cited_evidence** accepts either citation type.

There is one judged relevant story. NDCG is `1/log2(rank+1)` if within cutoff,
otherwise zero. Alternate genuinely relevant stories are unjudged, so this is
known-item NDCG, not a graded relevance benchmark. Incomplete/error trajectories
are reported separately and included as observed outcomes; a partial trajectory
can have exposed its target before failing. **In-flight runs do not enter aggregate
denominators** until terminal; they remain visible in `metrics.parquet` and the
explorer. Reports are provisional while runs are in flight. `progress.json` gives
started/terminal/error/in-progress counts. `matched_summary.csv` uses the terminal
case intersection across observed models; `pairwise_summary.csv` uses each pair's
intersection. Neither silently compares the faster model's larger sample with a
slower model's smaller one. Variant pass@k uses only stories with both variants done.

## Outputs and browsing

- `trajectories/MODEL/*.jsonl`: full model inputs/outputs, tool arguments/results,
  usage, completion/errors, timings, source and dataset hashes. Every event is
  flushed and fsynced. The report ignores an interrupted final JSONL fragment.
- `metrics.parquet`, `queries.parquet`, `attempted_calls.parquet`: convenient
  Polars/DuckDB analysis; attempted calls retain invalid/model-error arguments.
- `summary.csv`, `summary.json`, `strata.csv`, `query_syntax.csv`: aggregate metrics,
  recency/style slices, query length, Boolean/quote/prefix syntax, encoded arrays,
  date-filter frequency and empty result frequency.
- `explorer.html`: offline searchable view of prompts, ordered queries/results,
  target ranks and final responses. Open locally or serve on the remote host.

```sh
duckdb -c "SELECT model, query, target_rank FROM read_parquet('$EVAL_ROOT/queries.parquet') WHERE encoded_array"
uv run --package search-research --with pytest pytest packages/search-agent/tests packages/search-research/tests
```

Tests explicitly cover context consumption, false citations, batched list order,
deduplication, NDCG discounting, and core/presentation import isolation.
