# Perplexity 0.6B first bench — completed 2026-09-05

**The RTX 3060 embedded the full 64,638-story corpus in 14m 5s.** Perplexity at
native 1024 dimensions is competitive with the second Luna run's TE3-1536 static
control, with mixed quality differences. This run does not select a final model.

Scope: original 196 questions, one native dimension, exact dense and the second
run's fixed PG hybrid. No new Luna trajectories, dimension sweep, query rewriting,
Qwen/Jina runs, or paid embedding calls. See the [overall plan](10-sovereign-embeddings.md).
All results are in `data/sovereign-embeddings-20260905/pplx-1024/` on the laptop.

Follow-up: the [cached equal-dimension sweep](#cached-equal-dimension-follow-up)
below extends this first bench without changing its artifacts.

## Cached equal-dimension follow-up

Completed 2026-09-05 after the native run. Both models were compared at 256, 512,
768 and 1024 dimensions, retaining TE3-1536 as the historical anchor. Prefixes of
the cached document and query vectors were L2-normalized in float32 before exact
cosine search. The original lexical candidates, questions and RRF recipe were
reused. No new embeddings, GPU inference, paid calls or Luna trajectories.

Each cell shows **target hits out of 196 / NDCG@20**:

| Dimensions | Perplexity dense | TE3 dense | Perplexity hybrid | TE3 hybrid |
| ---: | ---: | ---: | ---: | ---: |
| 256 | 131 / .4948 | 123 / .4859 | 142 / .5605 | 137 / .5600 |
| 512 | 147 / .5689 | 141 / .5450 | 154 / .5923 | 154 / .6073 |
| 768 | 153 / .5950 | 149 / .5641 | 155 / .6001 | 155 / .6098 |
| 1024 | 153 / .6002 | 151 / .5663 | 157 / .6094 | 157 / .6057 |
| 1536 | — | 154 / .5873 | — | 160 / .6166 |

Perplexity's dense Recall@20 and NDCG@20 exceed TE3 at each matched dimension in
this sample. Hybrid recall ties from 512 upward; hybrid NDCG favors TE3 at 512/768
and Perplexity at 1024. This does not establish a statistically reliable winner,
especially on an already inspected set with two questions per target story.

**Decision: retain Perplexity-1024 and evaluate Qwen next.** Perplexity-768 is a
possible storage tradeoff: 25% fewer coordinates preserves dense @20 hits, but
reduces dense @8 hits from 136 to 133 and hybrid @20 hits from 157 to 155. At 512,
dense @20 falls to 147 hits. There is no present storage pressure that warrants
accepting that reduction before comparing the next model.

Equal dimensions compare coordinate count, not equal training capacity or
inference cost. Stored precision also matters: Perplexity's native int8 payload
uses one byte per coordinate, while the current TE3 cache uses float32. The
25% saving above compares Perplexity with itself, excluding index/shard overhead;
prefix shortening does not avoid the model's full embedding computation.

The scorer verifies the frozen input SHA256 hashes and reproduces both original
native anchors exactly, case by case, before writing sweep results. All original
native results remain in place; new results (including all five cutoffs and
dimension-specific strata) live under `dimension-sweep/`. Reproduce with:

```sh
OPENBLAS_NUM_THREADS=4 VECLIB_MAXIMUM_THREADS=4 \
  uv run --locked --package search-research \
  python -m search_research.sovereign_score --dimension-sweep
```

## Timing and batch-size knee

| Stage | Measured time |
| --- | ---: |
| First launch through HTTP Ready, including downloads | 286.05 s (4m 46s) |
| Weight download alone, included above | 274.48 s (4m 34s) |
| Weight download completion through Ready, included above | 7.76 s |
| Later cached launch through Ready | 9.21 s |
| Full 64,638-document pass, including transport/shard writes | 845.46 s (14m 5s) |
| All 196 questions, batched | 2.06 s |

The one-time download is separate from the user's 15-minute document-backfill
threshold. The pilot predicted 13m 56s; the observed pass was about nine seconds
longer, at **76.45 stories/s**. The midway update at 30,160 stories projected
14m 2s, so execution continued on the 3060. No 5090 access was needed or attempted.
The question timing is a batched throughput measurement, not interactive p95.

Two repetitions of the same seeded 256-story random sample, after warmup:

| Client batch size | Mean stories/s | Projected full-corpus minutes |
| --- | ---: | ---: |
| 1 | 38.02 | 28.33 |
| 8 | 74.94 | 14.38 |
| **16, selected** | **77.31** | **13.94** |
| 32 | 76.53 | 14.08 |
| 64 | 75.78 | 14.22 |

The knee is around 8–16; 32/64 did not improve throughput in this pilot. These
short measurements do not establish a universally optimal batch size. Concurrency
was one HTTP request throughout, with normal TEI internal batching.

An initial batch-64 warmup returned 429 because the server's concurrent-input cap
was 32. It was a configuration limit, not OOM. The old container/logs were retained,
the cap raised to 128, and only the uncompleted batch-64 pilot was rerun. All
completed pilot measurements remain in `pilot.json`; the final server settings
are in `backfill-config.json` and `active-launch.txt`.

## Static retrieval quality

Same frozen >=25-vote/two-year snapshot, title + newline + URL, and questions as
the second Luna run. Dense is exact cosine over all 64,638 stories. Hybrid adds
the unchanged title-only English pg_textsearch BM25 top 100 with RRF k=60,
dense weight 1 and lexical weight .5. No ANN or reranking.

Before comparing Perplexity, the scorer reproduced **every archived TE3 question's
dense/hybrid Recall@8/@20 and NDCG@8/@20** (196 cases × two modes × four metrics).
The scratch corpus's IDs, titles, URLs, scores and dates also matched exactly.

| Model / retrieval | Dimensions | Recall@8 | NDCG@8 | Recall@20 | NDCG@20 |
| --- | ---: | ---: | ---: | ---: | ---: |
| TE3-large dense | 1536 | 68.4% (134/196) | .5604 | 78.6% (154/196) | .5873 |
| Perplexity dense | 1024 | **69.4% (136/196)** | **.5772** | 78.1% (153/196) | **.6002** |
| TE3-large hybrid | 1536 | 72.4% (142/196) | .5923 | **81.6% (160/196)** | **.6166** |
| Perplexity hybrid | 1024 | **73.5% (144/196)** | .5915 | 80.1% (157/196) | .6094 |
| BM25 only, common control | — | 63.3% (124/196) | .5388 | 68.9% (135/196) | .5533 |

Perplexity gains two net hits at @8 in each mode. At @20 it loses one net hit in
dense and three in hybrid. These are different sets of cases: dense @8 gains 17
and loses 15 against TE3; hybrid gains 16 and loses 14. At @20 the gains/losses
are 14/15 dense and 9/12 hybrid. Small net differences do not establish superiority.

Question style is worth retaining in the next comparison. At @20, Perplexity
dense retrieves 87/98 entity and 66/98 paraphrase targets, versus TE3's 84/98 and
70/98. Perplexity hybrid retrieves 88/98 and 69/98, versus TE3's 85/98 and 75/98.
The aggregate similarity conceals stronger entity coverage and weaker paraphrase
coverage in this sample. All cutoffs 1/5/8/10/20 and style/recency strata are saved.

This is known-item single-search evaluation on a heavily inspected development
set, with one labeled target and unjudged alternatives. It is not the 92.9% Luna
exposure metric, a fresh agent comparison, or proof of answer correctness.

## Recipe, validation and resources

- `perplexity-ai/pplx-embed-v1-0.6b` revision
  `2c4d510dd4a732063c31a0f70193e35067b51fd8`, native 1024 dimensions, no prefixes,
  bidirectional mean pooling, FP32 compute and native integer-valued output.
- TEI 1.9.3, Ampere-86 image digest
  `sha256:a7d82dfef16c3bf1a95e93f5b226f358312512dbb0d585b48c3cf886f9d470a9`.
  Driver 610.57.04, RTX 3060 12 GiB; direct router entrypoint as documented in
  [preflight receipts](evidence/20260905-cuda-preflight.md).
- Server: max batch tokens 2048, max batch requests/client batch size 64,
  concurrent-input cap 128. Every embedding request sends `normalize:false` and
  `truncate:false`. The server allows truncation by default, but the request
  override rejects overlength inputs; all 64,834 inputs were below the cap.
- Documents: 2,250,749 local-model tokens, mean 34.82, maximum 351. Questions:
  3,913 tokens, mean 19.96, maximum 32. Tokenizer hash and input hashes are frozen
  in `manifest.json`. Scores use float32 cosine normalization of native int8.
- Five fixed smoke inputs matched the pinned official SentenceTransformer CPU
  FP32/SDPA implementation with minimum cosine .9999845 and maximum absolute
  int8-coordinate difference 1. Singleton/mixed-batch checks also passed.
  Numeric agreement is not bitwise equality. The isolated reference uses UV;
  its script, pyproject and lockfile are preserved with the results.
- 4,040 document requests and 13 query requests completed, exactly covering all
  inputs. **Zero backfill errors or retries**. Native shape, finiteness, nonzero
  rows, integer range and shard inventory were validated. Pilot 429 is separate.
- GPU samples every 30 seconds observed a maximum **2685 MiB** used and 67°C.
  That is a sampled maximum, not continuous peak instrumentation. Native document
  payload is ~63.1 MiB; 4,040 small shard files occupy ~79 MiB on this filesystem.
  The normalized float32 search matrix is ~252.5 MiB.

## Reproduction and current server state

The server remains running, bound only to `127.0.0.1:8080` on the VM. The laptop
accesses it through an SSH tunnel on `127.0.0.1:58080`. GPU sampling was stopped.
The saved [Compose recipe](../compose.pplx.yaml) is also at
`/opt/searchhn-embeddings/compose.yaml`; it matches the manually launched container.
Do not start a second container with that same name over the running instance.

```sh
# If a tunnel is not already running:
ssh -N -L 127.0.0.1:58080:127.0.0.1:8080 maya@magi06-inference

# On the laptop, from the repository root:
uv run --locked --package search-research python -m search_research.sovereign_run pilot
uv run --locked --package search-research python -m search_research.sovereign_run embed --batch-size 16
OPENBLAS_NUM_THREADS=4 VECLIB_MAXIMUM_THREADS=4 \
  uv run --locked --package search-research python -m search_research.sovereign_score
```

The existing run resumes matching shards and preserves completed timing. Use a
new root for a new measured experiment. The scorer needs the unchanged local
scratch PG instance and cached TE3 artifacts; it makes no API embedding calls.
Key outputs: `summary.csv/json`, `ranks.parquet`, `neighbors.parquet`, `strata.csv`,
`pilot.csv/json`, `requests.jsonl`, `timing-audit.json`, `load-timing.json`, native
shards, server logs, GPU samples, reference receipts and source snapshots.
These new artifacts are local; no new Garage release was published in this step.
