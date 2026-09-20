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
