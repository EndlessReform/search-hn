# FTS baseline: early findings

Partial snapshot collected on 2026-09-04 around 19:58 UTC. The full run remains
active on melchior; these are not final benchmark results. No retrieval algorithm
changes were made. Source, future drivers and analysis now live on the laptop.

## Useful result already

On the **same 52 finished questions (26 stories)**:

| Model | Context hit / variant pass@1 | Either variant / pass@2 | First-query NDCG@8 | Pooled NDCG@8 |
| --- | ---: | ---: | ---: | ---: |
| Luna | 76.9% | 92.3% | 0.363 | 0.483 |
| Qwen 3.6 27B | 50.0% | 76.9% | 0.197 | 0.286 |

This is a small, recent-heavy prefix, not a randomized model-ranking conclusion.
Gemma has only its six pilot cases in this snapshot: all three entity questions
hit, all three paraphrases missed. All three models had that same pilot pattern.
The local models run serially, so Gemma's full tranche starts after Qwen finishes.

Across each model's **different** available subsets, Luna has 81/98 context hits,
Qwen 26/52, and Gemma 3/6. Do not compare those unmatched rates as a bakeoff.
The 13 Luna, 27 Qwen and one Gemma terminal errors are all ten-turn budget
exhaustions, not HTTP failures. They remain outcomes; unfinished cases do not
enter denominators. A trajectory can expose its target and subsequently fail.

## Query behavior worth investigating

Of 314 observed Qwen search lists, 238 (75.8%) contain a JSON-encoded array **as a
string**, for example `"[\"ISS\", \"space station leak\"]"`. The tool accepts
real arrays, but a string is treated as one query, not decoded into alternatives.
This is a concrete interface mismatch to isolate in the next controlled tweak;
it does not establish that it explains every miss.

Luna's observed queries average 2.52 words; Qwen's 4.31 includes those encoded
arrays. Gemma's pilot queries average 3.05 words and 45.5% contain quotes, but its
66 lists are much too small a sample to generalize. No uppercase OR or `:*`
prefix syntax appeared in this snapshot. These syntax counts include valid
consumed lists from still-running cases, unlike terminal-only success aggregates.

The more natural wording did not remove the known-item difficulty. For example,
all models found the named Dark Hours story, but none recovered the anchor from
“Someone copied an astronomy app while building an astrology app. What was the
original called?” The distinction between title vocabulary and remembered article
details is worth preserving in the benchmark, not editing away after observing a miss.

## Metric contract and artifacts

- Exposure requires a target-bearing tool result in a successful subsequent model
  request. An unconsumed last-turn result does not count.
- Pass@1/@2 refer to two fixed question variants, not iid stochastic rollouts.
- NDCG judges one known relevant story. Other useful stories are unjudged; this is
  known-item retrieval, not graded relevance or answer correctness.
- Pooled rank deduplicates story IDs in first-seen context order; first-query rank
  is reported separately to distinguish initial retrieval from agent recovery.
- Direct story citations and consumed target-comment citations are separate bonuses.

Local artifacts: `data/fts-baseline-20260904/plain-results/` contains the frozen
questions, copied raw journals, environment snapshot, and generated reports.
`explorer.html` lets you filter model/outcome/text and expand prompts, raw calls,
ordered returned stories, final answers and errors. `progress.json` distinguishes
started/terminal/error/in-progress counts. `matched_summary.csv` and
`pairwise_summary.csv` restrict comparisons to terminal intersections.

Recompute locally with:

```sh
uv run --locked --package search-research hn-eval report \
  --root data/fts-baseline-20260904/plain-results
```

This local copy is a snapshot, not a live feed. One authorized follow-up will
collect another snapshot; there is no recurring monitor. See README.md for the
laptop driver commands and remote inference endpoint.
