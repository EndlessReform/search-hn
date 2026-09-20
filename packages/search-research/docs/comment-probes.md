# Comment linear probes

Run the scripts through UV; their script locks keep CPU PyTorch separate from the
explorer environment. Artifacts live under `data/probes/` on melchior and locally.
No script below calls a labeling vendor or modifies the annotation database.

## Distribution experiment

`books-mixes-v1` freezes 3,891 accepted labels. Its original test split has 919
comments; the remaining 2,972 are split into 2,377 fit and 595 validation rows.
All six trials use a linear layer, AdamW (LR .001, weight decay .01), batch 64,
850 epochs and seed 42. Epochs have equal numbers of draws and optimizer updates.
The baseline shuffles without replacement; the five mixture trials draw with
replacement. Reported mixture proportions are expected draw shares, not new data.
Random-negative membership means source rule 2 in this specific book pool; nearby
negative ranks are <=5,500 and deeper negatives are the remaining nonrandom picks.
This source ID is specific to this experiment, not a portable taxonomy definition.

Selection uses validation negative rejection at >=99% positive recall, with BCE
as tie breaker. Test and random-fixture scores are computed only after selection.
The saved model retains the validation holdout rather than refitting, so its
validation-derived thresholds apply to that exact checkpoint. There is no norm,
optimizer, threshold-objective, or epoch sweep in this slice.

```sh
uv run --locked --script packages/search-research/tools/comment_probe_mixes.py \
  --slice-dir data/comment-2025 \
  --fixture data/probes/books-wild-1000-v1/predictions.jsonl \
  --output data/probes/NEW-mixes-run
```

## Reusable real-corpus sanity check

The fixture is 1,000 fixed unique random comment IDs, seed 20260920, originally
sampled outside every rollout pick. Reuse it rather than redrawing between models.
The scorer refuses overlap with the supplied label snapshot. The mixture runner
excludes any fixture overlap before splitting or fitting, and records exclusions.

```sh
uv run --locked --script packages/search-research/tools/comment_probe_wild.py \
  --slice-dir data/comment-2025 \
  --fixture data/probes/books-wild-1000-v1/predictions.jsonl \
  --checkpoint data/probes/books-mixes-v1/random_emphasis.pt \
  --exclude-labels data/probes/books-mixes-v1/labels.jsonl \
  --output data/probes/NEW-fixture-check
```

Outputs include full comment text and scores in `predictions.jsonl`, plus pass
counts in `summary.json`. Checkpoint thresholds are used by default. For older
checkpoints supply `--threshold99` and `--threshold95` explicitly. These names
refer to calibration targets, not measured recall on this unlabeled fixture.
Do not treat fewer passes alone as better performance: inspect retained positives
and false positives, and use labeled test recall alongside the fixture.

`books-mixes-v1/metrics.json` contains every trial, validation selection, source
and taxonomy metrics, and test operating points. `split_ids.json`, `labels.jsonl`,
and `provenance.json` preserve the exact dataset and label origins. Checkpoints
and per-model test/fixture predictions are saved alongside them. Because the old
and expanded experiments use different test populations and training partitions,
their aggregate metrics are not a controlled before/after data comparison.

## Converged logistic regression

`comment_probe_logistic.py` reuses the frozen `books-mlp-v1` labels and split IDs.
It compares natural weighting with the existing 40/35/15/10 mix using normalized
sample weights, avoiding duplicate draws. L-BFGS uses tolerance 1e-8 and a 3,000
iteration ceiling; convergence warnings fail the run. The C grid is .01 through
10,000 in powers of ten (smaller C means stronger L2). Selection remains validation
negative rejection at 99% recall, then validation BCE. Test-matched-recall curves
are explicitly descriptive and do not select the model. Models export PyTorch
linear state dictionaries compatible with the fixture scorer.

The four audit packets contain disjoint subsets of the bottom 40 validation
positives under the previous linear model. Reviewer judgments are advisory, not
annotation changes; this selected tail cannot estimate overall labeling error.

## MRL prefixes and tree baseline

`comment_probe_mrl.py` reuses the same frozen labels and splits. It truncates each
native INT8 chunk to 128/256/512/1024 dimensions before normalization and mean
pooling. Each dimension uses weighted converged logistic regression with
C in [.01,.1,1,10,100], selected by the same validation screening criterion.
This tests the prefixes described in arXiv:2602.11151v2 Appendix B; it does not
re-embed text or alter stored vectors. The fixture scorer accepts these checkpoints
and applies their dimension before pooling.

One XGBoost CPU baseline uses depth 3, learning rate .05, row/column sampling .8,
L2 1, seed 42, histogram trees and up to 1,000 rounds, with 50-round validation
log-loss early stopping. Its model is saved as `xgboost.ubj`. Results and fixture
predictions are in `data/probes/books-mrl-v1`. All test measurements occur after
validation selection; the random fixture remains unlabeled.

## Closing decision: centroid + XGBoost quick filter (2026-09-20)

Freeze the exploratory recipe at a 25% centroid / 75% XGBoost blend, using the
looser validation-calibrated cutoff. Existing embeddings are reused; this adds
one cosine score and a scalar weighted sum to learned inference. The next work
is book extraction/resolution, rather than further filter tuning. This recipe
is recorded here; it has not been wired into the explorer or a production job.

Exact frozen scoring recipe:

- XGBoost artifact: `data/probes/books-xgb-sweep-v1/depth4_child1.ubj`.
- XGBoost input: normalize each 1024-dimensional native chunk, average the chunks
  for a comment, then normalize the average (the existing probe representation).
- Centroid: pool 1's frozen 60-positive `anchor_json.query` in
  `data/comment-2025/annotations.sqlite`; cosine uses the best chunk per comment.
- `zc = (cosine - 0.4018084356464245) / 0.20650860033072777`.
- `zx = (logit(p) + 1.6561394556670892) / 4.033332085834178`, where `p` is XGBoost's
  positive score; clip to `[1e-7, 1-1e-7]` before taking log-odds.
- Pass when `0.25 * zc + 0.75 * zx >= -0.48585514643850203`.

Location and scale came from the frozen fit partition. The cutoff came from
validation positives, targeting 99% recall there; it is not a corpus recall claim.
The common snapshot contains 4,891 labels: 2,989 fit, 748 validation, 1,154 test.

Ten ensemble configurations were tried: cosine blend weights .1/.25/.5/.75/.9,
three scaled maximum-score combinations, and two fixed OR rules. Each blend/max
was calibrated at validation recall targets .99 and .95. Standalone centroid and
XGBoost were controls. The final choice incorporates the wild audit; it is an
exploratory operating-point decision, not an untouched final evaluation. The
validation-only winners were blend .75 at .99 and blend .25 at .95, and did not
retain as many audited wild positives as the chosen operating point.

| Configuration | Test positives /398 | Test negatives rejected /756 | Wild passed /10,000 | Audited positives retained /17 |
| --- | ---: | ---: | ---: | ---: |
| XGBoost, .95 target | 386 | 573 | 109 | 14 |
| XGBoost, .99 target | 394 | 449 | 743 | 15 |
| 50/50 blend, .99 target | 393 | 447 | 100 | 15 |
| **25/75 blend, .99 target** | **393** | **443** | **310** | **17** |
| Loose centroid OR tighter XGBoost | 394 | 427 | 128 | 15 |

The fresh 10k fixture used seed 20260922, excluding the frozen labeled snapshot,
current rollout picks, frozen anchor exclusions, and the previous three wild
fixtures. Luna reviewers read all 752 comments passing either standalone loose
filter, plus 150 rejected by both (75 from the nearest 1,000 rejects by normalized
threshold proximity and 75 from the remaining rejects). Parent inspection checked
positive/borderline calls. No clear positives appeared in those 150 sampled
rejects. Another 73 newly admitted comments were read during the ensemble sweep;
this found a clear `Mastering Emacs` recommendation, bringing the total to 17.
These are reviewed positive counts, not an estimate that the entire 10k has only
17 positives. Ambiguous mentions and unresolved context remain separate.

XGBoost recovered recommendations for Caro's Lyndon Johnson books, the Roald
Amundsen diaries, Plunder, AIMA, Neuromancer, and Scott Adams' How to Fail that the
loose centroid missed. The centroid rescued praise for Why's Poignant Guide to
Ruby. The selected blend additionally rescued Mastering Emacs. The 50/50 blend
is a leaner alternative when forwarding 1% rather than 3.1% is more valuable than
retaining those two extra audited recommendations.

Artifacts remain in ignored experiment directories on the research machines:

- `data/probes/books-filter-comparison-v1`: taxonomy/source comparison and texts.
- `data/probes/books-wild-audit-10k-v1`: scored fixture, review packets, raw Luna
  reviews, parent-adjusted `reviewed.jsonl`, and the original 16 positives.
- `data/probes/books-ensemble-v1`: all configuration metrics, wild scores, and
  `extra_reviewed.jsonl` containing the additional positive. Local metrics include
  the merged audit counts; the scorer itself produces numeric model comparisons.

Reproduce the model comparisons on melchior from these frozen artifacts:

```sh
uv run --locked --script packages/search-research/tools/comment_filter_comparison.py
uv run --locked --script packages/search-research/tools/comment_probe_ensemble.py
```

Both scripts deliberately refuse to overwrite their existing output directory.
Choose a fresh output path in the script when repeating the run. The separate
`comment_probe_norm.py` preserves the earlier explicit squared-L2 experiment;
that regularized linear model is not part of the final recipe.
