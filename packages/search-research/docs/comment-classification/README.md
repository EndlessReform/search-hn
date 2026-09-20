# From a comment corpus to a cheap concept filter

This workflow turns an embedded Hacker News corpus into a small stream of
candidate comments for more expensive interpretation. Book recommendations are
the first worked example; the same process can start from another concept.

The current result is a **25% centroid / 75% XGBoost score blend**. On the fresh
10,000-comment fixture it forwarded 310 comments and retained all 17 clear
recommendations found during the audit. That is a useful quick filter, not a
claim that all 10,000 comments were labeled or that corpus recall is 100%.
Book extraction, attribution, and resolution are the next layer. The recipe is
frozen in the research artifacts and is not yet a production inference service.

**Start here:** read the five stages below, then follow [Usage](usage.md).

| Reference | What it owns |
| --- | --- |
| [Usage](usage.md) | Commands and the Corpus → Classifier → Rollouts workflow |
| [Corpus](corpus.md) | Extraction, embedding recipe, checkpoints, storage, and measured sizing |
| [Explorer](explorer.md) | Search math, API, index behavior, and runtime measurements |
| [Dataset and labeling](dataset.md) | Sampling, imbalance, few-shot prompts, provenance, and export |
| [Training and results](training.md) | Experiment methods, reproducible artifacts, and exact chosen formula |

## 1. Initial corpus ingest and embedding

Freeze a read-only PostgreSQL selection into a self-contained comment slice.
Decode HTML while preserving paragraph breaks; omit unusable bodies and record
those exclusions. Long comments become contiguous chunks of at most 2,048 model
tokens, with character offsets back into the comment. We do not prepend story
text or silently truncate long comments.

Embed with the pinned Pplx 0.6B recipe. The durable outputs are:

- `vectors.npy`: one exactly sized array of 1,024-dimensional native signed-int8
  chunk vectors, written incrementally with committed checkpoints.
- `index.sqlite`: full text, source metadata, comment/chunk-to-vector mappings,
  recipe identity, input hashes, and completion records.
- `tokenizer.json`: the tokenizer used to construct the frozen chunk inputs.

At explorer startup, verify the completed slice and build an **in-memory FAISS
signed-int8 index**. There is no separate persisted FAISS index to synchronize.
SQLite supplies text and metadata for displayed results. Phrase queries use exact
cosine scores, retaining the best chunk for each comment.

The completed 2025 slice on melchior has **3,266,889 comments / 3,266,991 chunks**;
embedding took **36.50 minutes**. See [Corpus](corpus.md) for the pinned recipe,
commands, and measured storage/throughput.

## 2. Iterative human refinement

Start with a broad text anchor such as “a comment recommending a book.” Its job
is to find a useful neighborhood, not to define the class perfectly. Read highly
activating comments and save actual positives in a named set. Save tempting but
wrong examples as negatives, optionally noting why: requests for recommendations,
author-only praise, or general discussion of reading are useful distinctions.

Switch the query source from free text to the positive mean. Each comment first
gets a normalized mean of its normalized chunks; selected comments then contribute
equally to the positive mean `p`. This steers retrieval toward examples the human
actually considers instances of the category. The text phrase is ignored in mean
modes; it is not added as another weighted signal.

Optionally subtract a corpus background mean:

```text
query = normalize(p - gamma * b)
```

Here `b` is the mean of a seeded random sample of comments. The UI offers nested
samples of 100, 1,000, or 10,000 comments. Preserve the magnitudes of `p` and `b`
until after subtraction; gamma zero gives the plain positive mean. This is corpus
centering, not probability calibration. Those random comments are not presumed
negative labels.

Repeat search → inspect → label → apply until the seed set expresses the concept
well enough to collect a candidate dataset. Inspect beyond the first page too.
Negatives teach the later classifier; they do not alter the search centroid.
The rollout pool freezes a **plain positive-mean** anchor, independent of any
background subtraction being explored in the Corpus screen.

Fractional centroid queries use streaming float32 accumulation over the existing
int8 vectors: FAISS's direct-int8 query path would quantize those coordinates.
There is no full float32 corpus copy, although mapped vector pages can add resident
RAM beside the FAISS index. See [Explorer](explorer.md) for this implementation.

## 3. Dataset construction: near positives, hard negatives, and random comments

Sample from a frozen ranking using explicit rank intervals or a starting cosine
and a count moving downward. Extend a band when more examples are needed. The
sampling pool deduplicates comment IDs while retaining every contributing source.
Similarity determines where to look; it does not supply the target label.

**Collect both hard negatives and corpus-random examples. They solve different
problems in this imbalanced classifier:**

- Hard negatives are close to the concept but do not instantiate it. They teach
  distinctions such as a book request versus an actual recommendation.
- Corpus-random examples expose the much larger background population. A model
  trained only around the positive cluster can score ordinary unrelated comments
  too highly when applied across millions of comments. Random examples also
  reveal positive styles missed by the anchor; keep those as positives.

Do not substitute one source for the other or treat every random draw as negative.
Keep source provenance separate from taxonomy: sampling stratum describes how we
found a comment; taxonomy describes the classifier's judgment about its content.
Training can rebalance these sources without pretending their proportions equal
natural corpus prevalence.

Assign train/test **randomly across all selected sources**. The random-corpus
source belongs in both splits; it is not a special random-only test set. The
current pool uses a stable seeded assignment, defaulting to about 1000:300.
Training scripts further hold out validation from training for schedules/cutoffs.
These are comment-level splits, not story-group or near-duplicate-disjoint splits.

## 4. LLM few-shot labeling

Attach a classifier draft to the same named set. Describe the category explicitly
and choose teaching examples from the handpicked positive and negative comments.
For each example, include its saved rationale, omit the rationale, or write custom
prompt-only wording. The compiler groups examples into readable Markdown sections.

Taxonomy can provide useful diagnostic detail alongside the binary decision.
The current workbench requires at least one named entry; a minimal positive and
negative pair works when no finer taxonomy is needed. Every entry has a description
and model-visible polarity. The structured response is:

```json
{"is_positive": true, "taxonomy": "book_plus_author"}
```

Preview the compiled prompt, schema, and `o200k_base` token counts before dispatch.
Run bounded batches through OpenRouter, defaulting to `openai/gpt-5.6-luna`.
This is brute-force per-comment labeling of the sampled pool, not of the whole
corpus. Valid outputs are accepted by default; schema failures and contradictory
boolean/taxonomy labels remain errors for explicit retry. Review can reject or
requeue a label. Each run preserves the prompt, model, parameters, timestamps,
responses, and reported usage/cost. Export accepted rows to JSONL for downstream
work without depending on the UI.

## 5. Training, validation, and the resulting filter

Reuse the existing embeddings. Begin with a binary linear probe, then compare
changes on frozen splits. The book experiments covered sampling mixtures,
regularization, a small GELU MLP, converged logistic regression, MRL dimension
prefixes, shallow XGBoost, centroid cutoffs, and ten simple ensemble configurations.
Taxonomy and sampling-source breakdowns distinguish hard-negative mistakes from
background-distribution failures; aggregate accuracy alone hides that distinction.

The final comparison used **4,891 labels: 2,989 fit / 748 validation / 1,154 test**.
Thresholds were calibrated on validation positives. A fresh 10k random fixture
excluded the labeled pool and earlier wild fixtures. Review covered all standalone
filter hits, sampled rejects, and new ensemble hits; it found 17 clear positives.
The final operating point was chosen with that audit in view, so these are
exploratory results rather than a new untouched test of the selected recipe.

| Filter | Test positives retained /398 | Test negatives rejected /756 | Wild comments forwarded /10k | Audited positives retained /17 |
| --- | ---: | ---: | ---: | ---: |
| XGBoost, tighter | 386 | 573 | 109 | 14 |
| XGBoost, looser | 394 | 449 | 743 | 15 |
| 50/50 blend, looser | 393 | 447 | 100 | 15 |
| **25% similarity / 75% XGBoost, looser** | **393** | **443** | **310** | **17** |

The blend combines standardized cosine with standardized XGBoost log-odds, not raw
probabilities with raw cosine. [Training and results](training.md#closing-decision-centroid--xgboost-quick-filter-2026-09-20)
records the exact constants, cutoff, artifacts, and audit coverage. Freeze this
recipe as the starting filter and proceed to extracting and resolving books.
