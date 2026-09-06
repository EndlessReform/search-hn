# Jina small-retrieval first bench — 2026-09-05

The user authorized the official premerged text-small retrieval model, with the
same 64,638-story corpus and 196 original questions used for Perplexity and Qwen.
The comparison uses native 1024 dimensions and cached 256/512/768/1024 prefixes,
exact cosine and the unchanged title-only BM25/RRF recipe. No new agent loops.

**Decision criterion:** the NC license is acceptable for this personal experiment
but remains adoption friction. A marginal tie with the permissively licensed
alternatives is not a compelling reason to choose Jina. This preference is fixed
before inspecting the results; no arbitrary significance threshold is being added.

**Cleanup remains OPEN:** archive and checksum-verify the experiment in Garage,
then reap completed laptop and inference-VM caches. See the mandatory checklist
at the top of [the main design doc](00-design.md).

## Recipe and validation

- Model: `jinaai/jina-embeddings-v5-text-small-retrieval`, revision
  `6856e76bb72982e58de0620458a4e8b3614da340`. This is the premerged retrieval
  adapter, not the multi-adapter base or the omni wrapper.
- TEI 1.9.3, the same pinned Ampere image and RTX 3060 as earlier runs, FP16
  FlashQwen3, last-token pooling. Complete settings in `compose.jina.yaml`.
- Both roles are prompted exactly once: `Query: ` for questions, `Document: `
  for title + newline + URL. The trailing spaces are significant. The official
  SentenceTransformer configuration was checked for both prefixes.
- Requests disable truncation and normalization. Float responses are cached as
  float32, with prefix shortening then L2 normalization on both sides at scoring.
- Official SentenceTransformer CPU FP32/SDPA comparison across five smoke inputs
  (two queries, three documents) has minimum cosine agreement **0.99999928**.
  The reference includes a Normalize module, so raw output scales differ; cosine
  agreement is what matters here. Singleton/batch smoke checks also passed.
- Eleven focused tests pass, including role-prefix transport, output validation,
  cached resume semantics, stable top-k ties and the frozen fusion recipe.

## Throughput and quality

**Completed: 402.78 seconds (6m 43s) for all 64,638 documents, 160.48 stories/s.**
All 196 questions took another 1.05 seconds. The journal has 4,040 completed
document requests and 13 query requests, zero failures/retries, and complete
ordered coverage. At halfway, the projected total was 6m 44s, well below the
15-minute threshold. No additional GPU host was needed.

The seeded 256-story pilot found batch sizes 16/32/64
effectively tied around 163 stories/s, projecting roughly 6m 36s. Selected 16.
First launch through Ready took about 53 seconds with the TEI image already cached.
The weight download accounted for 44.41 seconds. Peak sampled GPU memory was
1,315 MiB and temperature 63°C. The float32 document payload is 252.5 MiB; the
whole local experiment occupies approximately 283 MiB including evidence and
small-file overhead. Prefix-aware counts: 2,372,031 document tokens (max 353),
4,305 query tokens (max 34). Every request disabled truncation.

### Combined comparison

Each retrieval cell is **target hits out of 196 / NDCG@20**. All local models
are at native 1024 dimensions; TE3-1024 provides the equal-width comparison and
TE3-1536 remains the historical second-Luna control.

| Model | Dimensions | Dense @20 | Hybrid @20 | Full document pass |
| --- | ---: | ---: | ---: | ---: |
| Perplexity 0.6B | 1024 | **153 / .6002** | 157 / .6094 | 14m 5s |
| Qwen 0.6B | 1024 | 149 / .5815 | 156 / **.6206** | 6m 36s |
| Jina small-retrieval | 1024 | 148 / .5678 | 156 / .5899 | 6m 43s |
| TE3-large | 1024 | 151 / .5663 | 157 / .6057 | Cached control |
| TE3-large | 1536 | 154 / .5873 | 160 / .6166 | Cached control |

At @8, Jina dense retrieves 133 targets versus Qwen's 137 and Perplexity's 136;
Jina hybrid retrieves 137 versus 142 and 144. Jina's @20 style split is 81/98
entity and 67/98 paraphrase for dense, 86/98 and 70/98 for hybrid. Equal aggregate
hit counts do not imply the same target cases were retrieved.

Jina is about as fast as Qwen in the supported FP16 TEI recipe, but gives no
native quality advantage in these primary metrics. Its native hybrid NDCG@20
is lower than Qwen by .0307 with the same hit count; Perplexity leads Jina in both
dense and hybrid @20 hits and NDCG. This is evidence about this known-item sample,
not a claim that Jina is universally worse or a statistical significance result.

### Cached dimension sweep

| Jina dimensions | Dense hits / NDCG@20 | Hybrid hits / NDCG@20 |
| ---: | ---: | ---: |
| 256 | 127 / .4835 | 141 / .5450 |
| 512 | 142 / .5361 | 150 / .5762 |
| 768 | 146 / .5548 | 156 / .5842 |
| 1024 | 148 / .5678 | 156 / .5899 |

Smaller dimensions do not produce a compelling overall advantage. Perplexity has
better dense @20 hits and NDCG at every matched dimension, and better hybrid
NDCG throughout. Jina-768 has one extra hybrid hit over Perplexity-768 (156 vs 155),
but lower NDCG (.5842 vs .6001). The complete four-model sweep is saved in
`combined-dimensions.csv`; all cutoffs and per-case results remain available.
Native Jina and TE3 anchors reproduced exactly before the sweep outputs were
written. Corpus/query hashes matched the frozen inputs.

**Recommendation: drop Jina from the current text-search shortlist.** It has not
earned the additional license friction on this workload. Continue with Perplexity
and Qwen for recorded-query replay and reserve fresh agent loops for finalists.
Its documented omni compatibility is useful, but is not a measured benefit for
our present title/URL task. Preserve these results for future multimodal work.

## Reproduction

Artifacts: `data/sovereign-embeddings-20260905/jina-1024/`, including manifests,
ordered shards, request journal, timing and GPU receipts, source snapshot,
official reference check, native results and `dimension-sweep/` outputs.
No Jina artifacts have been archived to Garage yet.

```sh
uv run --locked --package search-research python -m search_research.sovereign_run pilot --model jina
uv run --locked --package search-research python -m search_research.sovereign_run embed --model jina --batch-size 16
OPENBLAS_NUM_THREADS=4 VECLIB_MAXIMUM_THREADS=4 uv run --locked --package search-research python -m search_research.sovereign_score --root data/sovereign-embeddings-20260905/jina-1024
OPENBLAS_NUM_THREADS=4 VECLIB_MAXIMUM_THREADS=4 uv run --locked --package search-research python -m search_research.sovereign_score --root data/sovereign-embeddings-20260905/jina-1024 --dimension-sweep
```

The [Jina text/omni compatibility investigation](10-sovereign-embeddings.md)
records the supported future multimodal path and why this particular text variant
works in TEI. Omni compatibility is documented by Jina, not tested by this bench.
