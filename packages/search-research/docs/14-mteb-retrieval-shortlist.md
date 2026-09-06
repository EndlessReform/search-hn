# English retrieval model review — 2026-09-05

**Recommendation after the user's publisher constraint: investigate NVIDIA
Nemotron-3-Embed-1B-BF16 if adding one more model.** Keep Perplexity and Qwen as
the measured finalists. Snowflake is a credible efficiency comparison but the
retrieval leaderboard does not indicate a large quality upgrade. Do not pursue
obscure publishers solely because of a leaderboard lead.

Publisher clarification: the user does not require a giant corporation. Credible
specialist teams and research authors with a track record remain eligible; avoid
an open-ended investigation of unfamiliar leaderboard submitters. Mixedbread,
Nomic and LightOn belong in that middle tier. Their reviewed small models do not
show a large English-retrieval upgrade over Qwen. Jasper/Stella's published
research lineage is also relevant: Jasper-Token-Compression-600M is the one
material exception worth retaining as an optional experiment (+4.36 on the same
ten tasks), with custom serving code and training-overlap caveats. Nemotron remains
the preferred additional candidate; Jasper is deferred, not dismissed solely for
publisher size.

This was a read-only review. No additional weights, servers or inference runs.

## Method

1. Read the official leaderboard API schema and saved the benchmark/model data.
   Primary suite: **MTEB(eng, v2), Retrieval only**, not the overall average.
2. Restricted to open-weight, text-only, dense models with known **total** parameter
   count >0 and <=2B. Active-parameter counts do not hide a large resident MoE.
3. Required all ten retrieval tasks and recomputed their mean NDCG@10. There are
   336 rows in the English-v2 response and 154 models meeting those conditions.
4. Inspected individual tasks and declared training overlap, and compared BEIR
   separately. Neither absent tasks nor absent means are treated as zeros.
5. Inspected English RTEB to catch newer retrieval-focused releases with incomplete
   English-v2 coverage. Only compared scores on identical task intersections;
   these custom means are not official full-benchmark scores.
6. Applied the user's subsequent requirement: an established publisher with a
   track record. This rules out chasing Yuan, Octen and similar obscure entries.

Sources: [English-v2 leaderboard](https://mteb-leaderboard.hf.space/benchmark/MTEB%28eng%2C%20v2%29),
[official score API](https://mteb-leaderboard-backend.hf.space/v1/benchmarks/MTEB%28eng%2C%20v2%29/scores),
[BEIR API](https://mteb-leaderboard-backend.hf.space/v1/benchmarks/BEIR/scores),
[English RTEB API](https://mteb-leaderboard-backend.hf.space/v1/benchmarks/RTEB%28eng%2C%20beta%29/scores).

## Established-publisher comparison

These numbers are **English-v2 Retrieval mean NDCG@10 × 100**, each over the same
ten tasks. They are neither HN target Recall@20 nor the general MTEB average.

| Model | Total parameters | Retrieval score | Delta vs Qwen |
| --- | ---: | ---: | ---: |
| Qwen3-Embedding-0.6B | 596M | 61.83 | — |
| Jina v5 text-small | 596M as listed by MTEB | 60.07 | -1.75 |
| Snowflake Arctic Embed L | 335M | 59.04 | -2.79 |
| Snowflake Arctic Embed L v2.0 | 568M | 58.56 | -3.27 |
| Snowflake Arctic Embed M v2.0 | 305M | 58.41 | -3.42 |
| IBM Granite embedding English r2 | 149M | 56.43 | -5.39 |
| Google EmbeddingGemma | 308M | 55.69 | -6.14 |
| BAAI BGE-large-en-v1.5 | 335M | 55.44 | -6.39 |

Perplexity has only three of these ten tasks in this snapshot, so there is no
valid full-suite score to add. Its missing leaderboard coverage does not outweigh
our own favorable HN results. Microsoft Harrier also lacks full retrieval coverage
here; its strong advertised multilingual overall score answers a different question.

The historical BEIR ranking differs: Snowflake Arctic L scores 55.98 versus
Qwen's 55.52, a small advantage, while L-v2.0 scores 55.22. This is why a change
in benchmark version/task selection should not be mistaken for a universal win.
Snowflake's smaller encoders and compression support could be attractive for
cost/latency, but that is a different hypothesis from markedly higher quality.
[Snowflake card](https://huggingface.co/Snowflake/snowflake-arctic-embed-l-v2.0).

## NVIDIA candidate

`nvidia/Nemotron-3-Embed-1B-BF16` is 1.14B parameters with 2048-dimensional MRL
embeddings. NVIDIA reports RTEB 72.38 on 16 public tasks; that number is not directly
comparable to the English-v2 table above. The model uses bidirectional Ministral3
and mean pooling, with Transformers/vLLM serving. It is not supported by our
current TEI 1.9.3 Candle architecture dispatch. A separate serving check is needed.
Its OpenMDW-1.1 license permits commercial use per the card; it is not Apache/MIT.
[Official card](https://huggingface.co/nvidia/Nemotron-3-Embed-1B-BF16).

On the eight English-RTEB tasks shared with Qwen, our recomputed means are
**75.25 versus 68.56**, a +6.70-point lead. However, only two of those eight tasks
are public; six are the private finance/health/code tasks. RTEB's private column
was removed over unequal access to evaluation data, so this is supporting evidence
for a test, not a clean public benchmark victory or an accusation of misuse.
[Maintainer decision](https://github.com/embeddings-benchmark/mteb/issues/3934).

The frozen snapshot and `rteb-paired.json` list every shared task. Avoid extrapolating
parameter count into a measured RTX 3060 throughput or memory claim.

## Broad search findings, retained for audit rather than recommendation

Before the user added the publisher requirement, the leading sub-2B text-dense
entries were Yuan-embedding-2.0-en (596M, 70.69) and Jasper-Token-Compression-600M
(607M, 66.19). Both have complete ten-task coverage. Yuan wins 7/10 against Qwen,
with +8.86 mean points; removing its unusually large SCIDOCS gain still leaves
+5.91. Jasper wins 8/10, +4.36 mean points. This justifies curiosity, not adopting
either on an unfamiliar publisher's reported benchmark alone.

Yuan is a Qwen fine-tune with standard modules and instructions in the MTEB
adapter; its brief card does not provide an independent replication. Jasper has
custom token-compression code and task-specific prompts, so a plain Qwen TEI
loader would not faithfully reproduce it. Metadata reports training overlap on
2/10 retrieval tasks for Yuan and 4/10 for Jasper; declarations are not an audit
of all pretraining/distillation exposure. No claim of benchmark misconduct.
[Yuan card](https://huggingface.co/IEITYuan/Yuan-embedding-2.0-en),
[Jasper card](https://huggingface.co/infgrad/Jasper-Token-Compression-600M).

Other checks: Geevec-lite's full retrieval score is 62.23, only +0.40 versus Qwen,
despite 4096-dimensional native output. Inf-retriever 1.5B is 60.83 on English-v2
retrieval, below Qwen, despite a stronger historical BEIR result. Octen-0.6B only
shares two English RTEB tasks with Qwen; its model card's public score cannot be
compared with the adjacent Qwen private score. None earns another experiment
under the user's revised publisher requirement.

## Reproduction and limits

Saved evidence: `data/mteb-retrieval-review-20260905/`, including raw responses,
cards, MTEB adapters, source hashes, ranked CSV and per-task deltas. Recompute with:

```sh
uv run --locked --package search-research python data/mteb-retrieval-review-20260905/analyze.py
```

MTEB evaluates passage retrieval across domains; our short title/URL known-item
task is related but distinct. The next decision should come from the frozen HN
questions or recorded-query replay, not from treating these public scores as
guaranteed application gains. This review introduces no new agent loops.

Include these small review artifacts in the end-of-bakeoff archive inventory;
the outstanding archive-and-reap checklist in `00-design.md` still applies.
