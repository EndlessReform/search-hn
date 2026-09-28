# Book resolver: 250-proposal iteration set

This is a reusable development set for quick retrieval/reranking iterations, not
held-out evaluation or human-verified work identities. The 250 original proposals
stay fixed. Label corrections and additional equivalent work IDs are explicit
revisions, rather than changes to the sampled inputs.

## Frozen inputs and labels

[Fixture](assets/resolver-iteration-v1.json) records sample IDs, source partitions,
comment IDs, span offsets, context SHA-256 hashes, translation labels, relevance
status, and known acceptable work IDs. Comments reconstruct exactly from
`data/comment-2025/index.sqlite` on melchior; all 250 contexts were checked.
Full comments are not duplicated in the versioned fixture.

The set contains 173 proposals from the older test partition and 77 from fresh-300,
across 163 comments. One long list supplies 29 proposals. Report both combined and
fresh-300 results. The 201 clear work/reference targets form the forced-choice
ranking denominator; the other 49 remain available for inspecting nonbook,
series/fused-reference, and ambiguous inputs. Known alternative work IDs count as
the same target. They do not earn repeated gain in NDCG.

The initial relevance labels came from three Luna reviewers using shuffled unions
of original/translated top-20 candidates. Subsequent bounded catalog inspections
added equivalent IDs outside those pools. These judgments are incomplete; newly
promoted candidates must be checked before calling them ranking errors.

Correction during this iteration: sample 197, SICP, was incorrectly labeled direct.
It is now indirect. The original smoke artifacts remain unchanged.

## Translation taxonomy and cheap routing candidates

[Per-case taxonomy](assets/resolver-translation-taxonomy-v1.json) covers all 24
indirect labels after that correction:

| Type | Occurrences | Examples | Cheap route to assess |
|---|---:|---|---|
| Abbreviation / author initials | 9 | SICP, OED, PMBOK, K&R, LotR | Detect abbreviation shape before identity; resolve and cache aliases afterward. |
| Spelling, word spacing, plural | 4 | Davinci, Engeineering, Eoad, Confidentials | Lexical typo tolerance / small edit-distance candidates. |
| Partial title | 4 | Linear Algebra, Leviathan, Atlas, Finn | Recover fuller text in context where present; otherwise author/context disambiguation. |
| Topic + book | 2 | rust book, Git book | Detect the phrase cheaply; identity still depends on context. |
| Transliteration / alternate-language title | 1 | Hikam of al-Iskandari | Known aliases or contextual resolution. |
| Dictionary-title completion | 1 | Webster's 1913 | Reference-specific alias; no edition selection needed. |
| Unnecessary subtitle expansion | 1 | Manna | Preserve the usable title. |
| Series scope adjustment | 2 | Culture, Dune series | Keep separate from single-work resolution. |

### Detect escalation before knowing the identity

A surface-only rule (2–8 ASCII letters, ampersands or periods, at least two
uppercase letters) flags 12/250 proposals and catches 8/9 abbreviation occurrences.
It misses lowercase `pmbok`. The four other flags are two occurrences of the real
title `JR`, nonbook `REVERSAL`, and ambiguous `CORE`. No canonical title or alias
lookup participates in this rule. This is development-set measurement, not unseen
validation. Detecting abbreviation shape does not supply its expansion: resolve
an unfamiliar abbreviation, then retain an approved alias for subsequent lookups.

A separate `word + book` pattern flags Git, Rust, and the unresolved Ruby reference.
Together these rules flag 15/250 proposals. They cannot recognize every opaque
nickname or distinguish ordinary-looking partial titles such as Atlas and Finn.
GLiNER hidden-state separability remains unmeasured; no classifier was trained.

Among five context/association cases, translation brings Git book, rust book, and
Finn from absent in the original top 20 to ranks 1, 3, and 1. Hikam already ranks
first. Atlas expands to Atlas Shrugged but still misses the top 20: a retrieval
failure remains after a useful expansion. These are not five failed translations.
Contextual reranking over the original top 100 already resolves Git, Finn, and
Hikam; Rust and Atlas still lack an accepted candidate in that pool.

## Selective acceptance from contextual Zerank

There is a provisional bend near a 2-logit gap, provided the winner also clears
an absolute score floor. At pool 100, top score ≥9 and gap ≥2 accepts 61/250:
60 match the current single-work labels; one resolves only the first book of a
fused three-title span. At pool 50, score ≥10 and gap ≥2 accepts 55/250, with the
same fused-span exception. Both models used BF16.

| Pool 100, minimum top score 9 | Accepted / 250 | Credited single-work matches |
|---|---:|---:|
| Gap ≥1 | 82 | 76 |
| Gap ≥1.5 | 70 | 66 |
| Gap ≥2 | 61 | 60 |
| Gap ≥3 | 48 | 47 |

The gap compares the winner with the highest-scoring different normalized
(title, author-names) group; this mechanical grouping uses no relevance labels.
Normalization is NFKC, case folding, and word token extraction; author names are
sorted. Records without authors remain separate by ID. This limited duplicate
collapse does not solve work canonicalization. Scores are raw Yes-token logits,
not probabilities. An empty pool is always rejected.

Gap alone fails when the correct book is absent: `On the Eoad` selects a companion
with gap 10.53 but score 6.41; `When We Cease…` selects a summary with gap 11.36 but
score 8.69. The score floor rejects both.

A coarse score/gap grid selected the pool-100 9/2 rule on the 173 older proposals
at a ≥95% agreement target; applying it to the 77 fresh-subset proposals accepts
19, all agreeing with their labels. The subsets have already been used during
development, and proposals can share comments. This is not a held-out error-rate
estimate. A stricter 11/2 rule accepts 41/250 with no uncredited results here;
that small selected sample does not establish perfect precision.

“Correct” means the selected ID is among the reviewed IDs for the intended work,
using title, author and comment context. These are Luna/bootstrap judgments plus
bounded catalog review, not authoritative human gold. All 250 proposals enter
routing analysis; the 49 nonbook, series, fused or uncertain proposals cannot earn
single-work acceptance credit. Collection/volume boundaries remain a source of
label uncertainty.

Deeper contextual pools improve top-1 from 156/201 to 162/201 to 167/201. Moving
20→50 produces ten gains and four losses: nine gains expose a previously absent
target and one promotes another record of an already available work. Moving
50→100 adds five gains with no losses: two newly available targets and three
better-ranking alternative records. The added yield therefore combines retrieval
coverage and catalog duplication. Known candidate coverage is 167/178/180.

## Reranker experiment contract

Use the existing full-catalog Tantivy title BM25 index, with the original span as
query. Retrieve 100 candidates once; depths 20 and 50 are strict prefixes. The
first 20 IDs were verified identical to the previous smoke run for every sample.
No translation, popularity prior, author retrieval, or extra retrieval route is
added to this comparison.

Both models receive the same candidate document: title plus author names. The
query arms are (1) the bare extracted span and (2) that span plus the complete
comment with its target occurrence marked. Scores at depth 100 are reused for
prefix evaluation, without allowing candidates beyond the selected depth to win.

Models run sequentially on melchior's RTX 5090 in BF16 with SDPA, using full input
text and no truncation. Zerank uses its shipped chat template and raw Yes-token
logit, matching its current published scoring contract. BGE uses its sequence
classification logit. Both revisions are pinned in the runner. The embedding
service remains running; roughly 29 GiB was already available before inference.

Separately timed 20/50/100 requests use the same 12 seeded sample IDs for both
models and query modes. Report model load/warmup separately. These are local
inference timings, including tokenization and device transfers, excluding catalog
retrieval and service/network overhead. The small latency subset does not provide
a stable production tail-latency estimate.

## Reproduction

Run from the repository on melchior, using its existing GPU environment. The
requested Hugging Face snapshots must already be cached at the pinned revisions.

```sh
uv run --no-sync --with tantivy --with duckdb python packages/search-research/tools/comment_book_resolver_prepare.py
uv run --no-sync --package search-research python packages/search-research/tools/comment_book_resolver_rerank.py bge
uv run --no-sync --package search-research python packages/search-research/tools/comment_book_resolver_rerank.py zerank
uv run --no-sync --package search-research python packages/search-research/tools/comment_book_resolver_report.py
```

Ignored receipts, candidates, runtime metadata, and summaries live in
`data/probes/books-resolver-iteration-v1/`. The earlier snapshot/projections remain
in `data/probes/books-resolver-smoke-v1/`. No production resolver is changed.

Model sources: [zerank-2-reranker](https://huggingface.co/zeroentropy/zerank-2-reranker),
[BGE reranker v2 m3](https://huggingface.co/BAAI/bge-reranker-v2-m3).

## Measured results, 2026-09-26

Contextual zerank over 100 candidates reaches 167/201 top-1 (83.1%) and
175/201 top-3 (87.1%), compared with BM25 at 106/201 and 134/201. No translation
is added in this comparison. Candidate documents always contain title and author.

| Model / query | Top-1, pool 20 | Pool 50 | Pool 100 | Median latency, 20 / 50 / 100 |
|---|---:|---:|---:|---|
| BGE / title | 56.2% | 57.7% | 55.7% | 5 / 10 / 20 ms |
| BGE / context | 60.7% | 63.2% | 60.2% | 23 / 57 / 108 ms |
| zerank-2 / title | 65.2% | 65.7% | 64.7% | 41 / 91 / 173 ms |
| zerank-2 / context | 77.6% | 80.6% | 83.1% | 236 / 603 / 1198 ms |

Contextual zerank NDCG@3 is .795 / .836 / .855 at depths 20/50/100.
Known-correct candidate coverage is 167 / 178 / 180 of 201. At depth 100, 21
uncredited targets have no known correct candidate in the pool; 13 have a known
correct candidate but select another record first (including two uncertain winners).

Fresh-300 subset, 62 clear targets: contextual zerank top-1 is 48 / 51 / 51,
versus BM25 34. Thus depth 100 improves the combined sample but not this smaller
subset relative to depth 50. BGE contextual top-1 is 34 / 37 / 37 on that subset.

Before final scoring, new top-three candidates were pooled across both models,
both query modes, and every depth. Luna reviewed them without model/rank labels.
The [pooled judgment ledger](assets/resolver-rerank-judgments-v1.json) adds 148
acceptable IDs across 154 reviewed samples and leaves 22 candidate IDs uncertain.
All configurations use the same expanded label revision. This is an iteration
set with incomplete model judgments, not an independent estimate of production
accuracy. Nonbook/series/ambiguous abstention is not evaluated by forced ranking.

### Shortlists for a final picker

Contextual zerank top-3 / top-7 target coverage is 162/166 at pool 20,
172/175 at pool 50, and 175/176 at pool 100, each out of 201. These describe
availability to a final picker, not its decision accuracy. Fresh subset at pool
100 is 53/54 out of 62. Both rerankers used BF16 inference.

[Additional rank-4–7 review](assets/resolver-top7-judgments-v1.json) records the
bounded check used for this follow-up. Label revision 3 adds six alternate
Bible/dictionary IDs across four samples. Earlier top-1/top-3 scores are unchanged.
The top-7 audit focuses on contextual zerank cases where top 3 lacks a credited
match, which are the cases that can change its shortlist success rate.

Translation routing summary: 16 of 24 indirect labels plausibly admit mechanical
repair or lookup; five require contextual association/alternate-title knowledge;
three are unnecessary subtitle or series-scope changes. This categorization does
not measure the performance of an implemented shortcut pipeline.
