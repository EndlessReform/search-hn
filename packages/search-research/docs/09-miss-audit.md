# Phase 0 gut check: what remains after title/URL retrieval?

## Outcome

The residual questions are **source-grounded, but often weak known-title lookup
tests**. That does not justify either deleting them or indexing every comment.

| Manual judgment | Cases / 18 |
|:--|--:|
| Answer evidence exists in frozen source | 18 |
| Title/URL identification plausible from the question | 8 |
| Title/URL identification weak | 10 |
| Explicitly flagged ambiguous target | 1 |
| Reasonable alternative answer under the question wording | 7 |
| Unsupported/unanswerable question established | 0 |

This covers **all 18 distinct questions missed by either dense or hybrid**: ten
missed by both, eight found by one treatment. It includes 28 failed new trajectories
and their successful counterparts, plus the original FTS trajectory for context.
It is not a representative sample of all 196 questions or of production searches.

## Worked examples and implications

| Case | What the records show | Implication for title/URL-only search |
|:--|:--|:--|
| `43296918-paraphrase` — rlama | The question supplies `rlama`, and target URL is `rlama.dev`. Hybrid's short `rlama` query finds it at rank 7; dense exhausts turns without consuming it. | A concrete recovery opportunity using information the question already supplies; no comment index required. |
| `49231154-paraphrase` — Dark Hours | The astronomy/astrology copying clue is supported by the comment, but the headline is “Mea Culpa – Dark Hours.” Models guess other astronomy apps. | Knowing the answer lets a reviewer type Dark Hours; that is not a legitimate query suggestion from the question. Weak representation signal, not proof of bad ranking. |
| `44166102-entity` — SFSU/Plato | Frozen comment explicitly describes an “obstacle course with AI.” Finals invent a student-driven philosophy redesign. Headline only says AI makes the humanities stranger/more important. | Valid question, weak headline identification, unsupported final answers. These are three different judgments. |
| `49214008-paraphrase` — DeepSeek | A generic local-hardware question anchors to V4 Flash, but the agent finds an explicitly titled R1 hardware thread. The numerical answers differ. | Explicit ambiguity flag: a reasonable search can return the wrong labeled anchor. Preserve the case, review wording in a future version. |
| `46290916-paraphrase` — ALPR | Both QR proposals exist in different license-plate surveillance threads: live camera stream versus a vehicle-owner endpoint. Dense finds the anchor; hybrid selects the other. | A semantically reasonable alternative may satisfy the broad wording while failing single-anchor recall. Do not automatically call the alternate answer false. |
| `49355606-paraphrase` — OpenLogi | The Linux/Logitech question can be answered with Solaar from another directly titled story, even when OpenLogi is not exposed. | Anchor recall and useful answer quality can diverge. Target-aware audits should not erase that distinction. |

## What to do next

1. Keep title/URL indexing as the next baseline's boundary. Start a small recovery
   test with question-provided anchors such as rlama, not hidden target-title probes.
2. Keep all original questions/scores unchanged. Treat this as a diagnostic layer.
   “Keep” means retain for regression, not certify unique title-level solvability.
3. In a new untouched set, distinguish a *remembered story lookup* from a *question
   about a detail somewhere in a discussion*. Both are valid user needs, but only
   the former reliably provides a title-level identifier. The existing “entity”
   label sometimes names a detail rather than the story's entity/title.
4. Audit answer grounding separately: a successful exposure can still yield a wrong
   answer, and a missed anchor can still yield a reasonable answer. Do not convert
   these subjective annotations into a revised pass rate.
5. The remaining retrieval/ranking and query/stopping explanations are hypotheses,
   not demonstrated mechanical diagnoses. No new queries or uncapped-rank probes
   were run during this manual inspection.

## Method, calibration and limitations

Four **Codex GPT-5.6 Luna subagents**, one per disjoint shard (5/5/4/4 questions),
inspected the frozen prompts, target title/URL, body/three source comments, consumed
search lists and result titles, final answers, and original FTS context. Assignment
is lexical case-ID order distributed round-robin, not cherry-picked examples.
One reviewer per case is not four independent votes or inter-rater agreement.

The parent reviewed the annotations and requested bounded corrections: exact-title
suggestions must not smuggle in the known answer; source support is not unique
target identification; one review reversed dense/hybrid attribution; one confused
an unsupported final answer with an unsupported question. Final shards retain
specific evidence and hindsight warnings. Parent checks included the rlama mode
attribution, SFSU source quote, Nix alternate-answer trace and ALPR alternatives.

Reviewers knew the target and are language models, so judgments are target-aware
and fallible. All-answer-supported means supported by the **frozen source snippets**,
not independently verified external facts. No external articles or full comment
trees were fetched; absent snippet evidence would not prove a whole thread lacked
an answer. Small counts and subjective categories warrant no significance claims.
The supplied Algolia screenshot does not establish how Algolia indexes content.

## Reproduce / inspect

Restore Garage release `research-20260904-v3` via [artifact instructions](01-artifacts.md).
The directory `data/luna-semantic-20260904/miss-audit/` contains selection, packets,
four raw final review shards, validated annotations (JSONL/Parquet), summary JSON,
and `review.md` with the complete per-case evidence. No eval labels were edited.

```sh
# Deterministic packet preparation from preserved traces; no inference/DB access.
uv run --locked --package search-research python -m search_research.miss_audit
# Validate and regenerate outputs from the preserved manual annotation shards.
uv run --locked --package search-research python -m search_research.miss_audit_report
```

Packet regeneration is deterministic; fresh subjective reviews need not reproduce
the same labels. The preserved shards are the record of this audit, not a claim
that the model's judgments are ground truth.
