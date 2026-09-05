# Completed FTS run: search behavior

Final results downloaded from melchior and reports regenerated on 2026-09-04.
The suite ended at 21:16:43 UTC: all 588 cases are terminal (98 stories, two
questions, three models). This supersedes the partial snapshot in archive/initial-findings.md.

| Model | Target in context | Either variant hits | First-query NDCG@8 | Pooled NDCG@8 |
| --- | ---: | ---: | ---: | ---: |
| Luna | 150/196 (76.5%) | 88/98 (89.8%) | 0.340 | 0.497 |
| Gemma 4 31B speculative | 127/196 (64.8%) | 81/98 (82.7%) | 0.284 | 0.543 |
| Qwen 3.6 27B | 111/196 (56.6%) | 77/98 (78.6%) | 0.192 | 0.380 |

Luna has 26 ten-turn exhaustions and 13 terminal rate-limit failures. Gemma's 65
and Qwen's 93 errors are ten-turn exhaustion. A failed trajectory may still have
exposed its target. These are operational harness results, not isolated model
ability. Fixed question variants are not iid pass@k samples; pooled NDCG uses
first-seen deduplicated context order, with one known relevant story.

## Important prompt bias

Checked the recorded model_input.system_prompt in a journal from each model.
All explicitly warn against prose and semantic paraphrases and request short
named-entity/generic anchor lookups. Empty-result guidance reinforces anchors.
Thus keyword behavior is adaptation to this interface, not evidence of an innate
preference. User-question rewrites did not remove this system-level bias.

## Aggregate syntax

Raw attempted fetch_stories calls and consumed search lists have different
denominators: one genuine batch can produce several consumed lists.

| Model | Genuine array batches / attempted search calls | Mean words / consumed query | Queries with quotes | Empty lists |
| --- | ---: | ---: | ---: | ---: |
| Luna | 477/686 (69.5%) | 2.51 | 0.8% | 47.3% |
| Gemma | 700/779 (89.9%) | 3.03 | 36.7% | 74.3% |
| Qwen | 0/1085 | 4.51* | 78.1%* | 71.5% |

Qwen instead emits array-shaped strings in 892/1085 attempted calls (82.2%),
and 825/1093 consumed queries (75.5%). Example:
`{"query":"[\"ISS\", \"space station leak\"]"}`.
The backend does not decode these into alternatives: their words become one
conjunctive plain-text query. Qwen's starred length/quote figures mostly measure
serialization, not prose or phrase-search preference. No consumed queries use
uppercase OR or `:*`. Gemma's quotes do not enable phrase matching under
plainto_tsquery. Qwen alone attempted daily-top lookup (89 calls).

## Worked spot-checks

Selected three stories, both variants, across models to inspect anchors,
recovery, and vocabulary mismatch. These are illustrative, not a random sample.
Sequences count consumed lists, not necessarily sequential tool calls.

- ISS leak (48413464): Luna starts `ISS leak NASA tool external leaks`,
  eventually broadens to `ISS leak`, and hits rank 1 on list four. It continues
  exploring instruments and names, including `mass spectrometer ISS leak` and
  `RELL`. Gemma similarly moves from `ISS leak detection` to `ISS leak` and
  hits on list four. Qwen's encoded alternatives, including
  `["ISS", "space station leak"]`, miss this variant. Qwen does find the target
  in the other variant using the ordinary string `ISS leak`.
- Dark Hours (49231154): all models find the named version; all miss
  "Someone copied an astronomy app while building an astrology app. What was
  the original called?" Luna explores 33 lists, including `horoscope`,
  `zodiac`, `Co-Star`, `Sky Guide`, `Star Walk`, and `Stellarium`. Gemma's 26
  lists favor local rephrasings and quotes, including `"astrology app"`,
  `astronomy "rip-off"`, and `"ephemeris" cloned`. Qwen attempts encoded bundles
  such as `["sky map", "planetarium app", "star chart"]`. None bridges the
  clue to the headline "Mea Culpa – Dark Hours".
- OpenCiv3 (46918612): Gemma changes `OpenCiv3 macOS damaged` (miss) to
  `OpenCiv3` (rank 1). For the paraphrase it drops macOS/remake terms, reaching
  `Civilization III` on list four (rank 1). Qwen's named question immediately
  hits `OpenCiv3`; its paraphrase only reaches the target on list nine with
  `["Civilization"]`. Luna has no consumed searches for either variant:
  rate-limit failures, not evidence about search behavior.

In these traces Luna explores broader lexical alternatives, Gemma often uses
quoted local reformulations, and Qwen attempts batching with a serialization
mismatch. Entity/paraphrase context-hit rates are 80.6%/72.4% Luna,
73.5%/56.1% Gemma, and 67.3%/45.9% Qwen.

## Next comparison, not performed here

Use a small matched prompt ablation: current guidance versus neutral search
guidance, holding questions/settings/budgets fixed. Remove keyword steering from
both system instructions and empty-result guidance. Isolate Qwen's argument
contract as a separate factor. Then compare neutral prompting against hybrid
retrieval, avoiding simultaneous prompt/interface/ranking changes.

Artifacts are in data/fts-baseline-20260904/plain-results/: raw trajectories,
metrics.parquet, queries.parquet, attempted_calls.parquet, summary.csv, and
explorer.html. Reproduce the report locally:

```sh
uv run --locked --package search-research hn-eval report \
  --root data/fts-baseline-20260904/plain-results
```

No extra model trajectories or retrieval changes were made for this analysis.

## Live PostgreSQL cutoff sweep

Replayed all 4,604 consumed search lists on 2026-09-04, preserving the literal
query strings and score/date/domain filters. Target eligibility is checked by
primary key; eligible targets receive an exact uncapped rank under the production
score DESC, day DESC, id DESC ordering (NULLS LAST). All 438 previously observed
target-bearing list ranks reproduced exactly. This is a live DB check, not a
historical snapshot or a rerun of adaptive model behavior.

The tool default is 8, schema maximum 20 (repository helper maximum 100).
Gemma used 8 for 1,951/1,954 lists. Luna used 20 for 965/1,557 and 10 for 570.
Qwen used 8 for 848/1,093. Ranking is by popularity, not text relevance.

Additional previously missed cases whose targets would enter a returned list:

| Model | Cutoff 20 | Cutoff 50 | Cutoff 100 | Unlimited |
| --- | ---: | ---: | ---: | ---: |
| Luna | 0 | 6 | 7 | 7 |
| Gemma | 3 | 6 | 6 | 6 |
| Qwen | 4 | 12 | 16 | 19 |

Of 200 observed missed cases: 154 have no target match under any recorded search
predicate before optional filters; four match before optional filters but are
excluded by them; 32 are cutoff losses; ten have no consumed search lists.
Daily-top predicates include their requested day in the initial matching test.
Thus 81% of the 190 misses with searches cannot match the target even before
optional filters. Raising to 20 rescues seven cases, 50 rescues 24, and even
unlimited rescues only 32. These are mechanical fixed-query opportunities, not
guaranteed outcomes from new agent trajectories with different context.

Real ranking misses exist: Amazon layoffs is at best rank 84 for Luna and Qwen;
Qwen's AI Aesthetic paraphrase reaches only rank 641. But matching dominates.
Target exposure may also occur through comment lookup, so search-only replay
totals must not replace the original context-exposure metric.

Reproduce with:

```sh
uv run --locked --package search-research python -m search_research.cutoff_sweep
```

Outputs: cutoff-sweep.jsonl and cutoff-sweep.parquet in the main results directory.
The sweep uses one DB request at a time, a 55-second statement timeout, and cached
duplicate predicates. A broad-query EXPLAIN confirmed the FTS index is used.
