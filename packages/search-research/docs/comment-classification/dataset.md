# Sampling, labeling, and provenance

[Workflow overview](README.md) · [Usage](usage.md) · [Training](training.md)

## Sampling strata are not target classes

A positive-mean ranking is a candidate generator. High similarity does not label
a comment positive, and a random draw is not automatically negative. The LLM or
human makes that judgment after sampling. Keep these two axes separate:

| Axis | Examples | Purpose |
| --- | --- | --- |
| Sampling source | Top ranks, deeper rank band, similarity band, corpus-random | Record where examples came from and control collection/training mixtures |
| Content label/taxonomy | Book + author, book without author, general reading discussion | Train the binary target and explain its errors |

The target is rare in the corpus. Nearby hard negatives teach distinctions within
the topic; random examples teach the broad background distribution. A dataset
rich in the former but poor in the latter can look good on retrieved examples
while forwarding too much unrelated material at corpus scale. Conversely, random
negatives alone do not teach the difficult distinctions among highly activating
comments. Retain both, and inspect positives found by the random source for styles
not represented by the seed centroid.

The experiment's training mixture used expected draw/weight shares of 40%
positives, 35% random-source negatives, 15% nearby negatives, and 10% deeper
negatives. This is a training choice, not an estimate of prevalence or a fixed
quota imposed on every dataset. In that specific pool, source rule 2 identified
the random source and rank <=5500 identified nearby nonrandom negatives. Do not
reuse those identifiers as universal definitions.

## Frozen pool and incremental rules

A pool stores the corpus identity, seed, split fraction, seed positive IDs,
excluded manual IDs, and actual plain-mean query vector. Sampling uses this frozen
ranking even while the annotator continues editing the example set. New manual
labels are also excluded during sampling.

Rules support inclusive rank intervals, an open-ended start rank with a count,
cosine-at-or-below followed downward, and seeded uniform random order. The current
UI edits ranges/counts directly; an earlier Continue action survives in backend
support but is not the intended UI workflow. Extending a range adds missing picks.
Overlapping rules add source memberships rather than duplicate training rows.

Deleting or narrowing a rule removes its uncovered unsent memberships and prunes
picks with no remaining source. Already attempted, queued, or running picks are
preserved. Invalidation archives the entire pool generation; it does not erase
paid run history. These operations are distinct from rejecting a model label.

Train/test assignment is a deterministic seeded hash of the picked comment across
all sources. The default test fraction is 300/1300; it is approximate, not an exact
quota. Corpus-random examples occur in both train and test. The probe experiments
split the training portion again into fit/validation, leaving original test IDs
unchanged. A separate wild fixture excludes known picks; it is a corpus sanity
check, not a replacement for the labeled test split.

## Few-shot labeling contract

Classifier drafts belong to named example sets. They contain the category
description, example selections/rationale choices, and taxonomy entries. Current
saved labels are resolved on compilation; removing a selected teaching example
causes an explicit compile error rather than silently changing the prompt.

The compiler renders Markdown taxonomy descriptions and grouped positive/negative
examples. Each taxon's polarity is model-visible. It instructs the model that
`is_positive` means instantiating the category, not merely mentioning the topic
or resembling a positive example. The boolean must agree with the chosen taxon.
The current schema requires both the boolean and an enum taxonomy value; taxonomy
can be coarse if detailed error analysis is not needed.

Rollouts freeze the compiled prompt/schema and routing settings per run. The
OpenRouter default is `openai/gpt-5.6-luna`, batch limit 50, concurrency 8, and
4096 maximum output tokens. Partial dispatch alternates sampling sources and
preserves their pick order. Each comment gets one request with zero automatic
SDK retries. Valid consistent results are accepted immediately; malformed output
or boolean/taxonomy disagreement is recorded as an error. Accept/reject/requeue
are explicit review actions. Stopping a run drains in-flight calls. Unknown
in-flight attempts after restart remain interrupted until explicitly retried.

## Provenance and export

All mutable tables below live in the separate `annotations.sqlite`; frozen text
and vectors stay in `index.sqlite` and `vectors.npy`.

| Store | Contents |
| --- | --- |
| Named sets, positive memberships, `negatives` | Human labels, set revisions, negative notes |
| `classifier_drafts` | Editable category, taxonomy, selected examples, rationale modes |
| `rollout_pools` | Frozen anchor/corpus, seed, test fraction, creation time, active generation |
| `rollout_rules`, `rollout_rule_changes`, `rollout_deleted_rules` | Current rule specification and edit/retirement history |
| `rollout_picks`, `rollout_sources` | Unique picked IDs, rank, similarity, split, pick time, state, all source memberships |
| `classifier_runs` | Model, immutable prompt/schema/settings snapshot, start/end/status |
| `classifier_attempts` | Comment/run, timestamps, raw response, parsed label, usage/cost in response, error |
| `rollout_reviews` | Accept/reject/requeue actions and time |

`GET /api/sets/{set_id}/rollouts/export` streams **current accepted picks from the
active pool** as JSONL, with text, label, split, rank/score, latest attempt/run,
model, completion time, run snapshot, frozen anchor, and contributing source rules.
It does not export every historical attempt. Source specs come from current rule
rows; use rule-change history in SQLite when reconstructing an earlier sampling
action. Training drivers also save frozen `labels.jsonl`, split IDs, and provenance
with their model artifacts, so comparisons can use the exact same dataset.

## Reading results without confusing populations

Report binary recall alongside negative rejection/candidate volume. Break mistakes
down by taxonomy and by sampling source. An enriched test set is useful for hard
cases, but its class prevalence is not the corpus prevalence; its accuracy or
precision should not be advertised as a corpus-wide rate.

The fresh 10k audit inspected every comment passing either loose standalone
filter, a sample of their joint rejects, and the ensemble's additional hits.
The 17 confirmed positives are those found in this inspected subset. The remainder
of the 10k was not exhaustively labeled. Reviewer judgments were saved separately;
they did not overwrite the original annotations. The final blend was chosen with
that audit in view, and repeated test comparisons make small differences exploratory.
These limits do not prevent using the filter to collect candidates for the next
extraction/resolution layer.
