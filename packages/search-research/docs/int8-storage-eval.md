# Pplx int8 storage: measured results

2026-09-06. **No additional target-recall or nDCG penalty was measured for native
int8 storage or the float16 index on our 196-question eval.** The native bytes are
usable in PostgreSQL, but querying them through pgvector is noticeably less
convenient than a plain `halfvec` column.

## What ran

Restored only the selected BF16 evidence from `research-20260906-v4`, checking its
manifest SHA256 and every downloaded file. Used all 64,638 frozen stories and 196
questions, with the archived title-only BM25 candidates and lexical weight 0.125.
No new embeddings, paid agent calls or application-database writes were needed.

The exact native reference computes cosine from the integer coordinates using
float64 arithmetic. Compare this with the paper's normalized float32 PostgreSQL
vectors, unnormalized integer coordinates stored in PostgreSQL `halfvec`, and
native signed bytes stored in `bytea`. Every stored byte vector was decoded and
verified equal to its corresponding halfvec. All int8 values fit in float16 exactly.

Three HNSW indexes used m=16, ef_construction=128 and ef_search=1000. Rows were loaded
in the same order, with a reset random seed and parallel index building disabled.
The bytea index uses a SQL byte-to-halfvec conversion expression. The graphs were
built separately: seed control does not make their approximate candidates identical.

This ran in a disposable local **ARM64 PostgreSQL 17.10 / pgvector 0.8.2** container,
not on the x86 production database or the paper's timing VM. Timings cover SQL plus
fetching 100 IDs at concurrency one, excluding query inference and BM25. ANN has
two shuffled passes (392 samples/layout); exact paths have one (196 samples).
EXPLAIN verified index use for ANN and scans for exact search.

## Retrieval scores

All hit counts are out of 196 questions. nDCG is at 20.

| Path | Dense hits@8 | Dense hits@20 | Dense nDCG@20 | Hybrid hits@8 | Hybrid hits@20 | Hybrid nDCG@20 |
|---|---:|---:|---:|---:|---:|---:|
| Native int8, exact cosine | 135 | 153 | .601478 | 143 | 158 | .621483 |
| Float32 PostgreSQL, exact | 135 | 153 | .601478 | 143 | 158 | .621483 |
| Float16 PostgreSQL, exact | 135 | 153 | .601478 | 143 | 158 | .621483 |
| Float32 HNSW | 134 | 151 | .596880 | 141 | 156 | .615713 |
| Float16 HNSW | 134 | 151 | .596880 | 141 | 156 | .615713 |
| Int8 bytes + float16 HNSW | 134 | 151 | .596880 | 141 | 156 | .615713 |

Exact float16 and native reference returned identical ordered top-100 lists on all
questions. Float32 exact returned the same sets, with one ordering difference and
no target-rank changes. It reproduces the paper's exact metrics.

ANN candidate recovery against native exact top-100 averaged 99.153% for float32,
99.128% for float16 and 99.133% for bytes+float16. Candidate lists did differ; equal
headline metrics do not mean identical retrieval. There was no additional loss at
the evaluated cutoffs from using smaller storage. All three local graphs lost two
hybrid targets relative to exact. The paper's different VM graph reached 157/196;
compare storage layouts within this matched run rather than attributing that
build-to-build difference to int8. This remains one tuned eval set, not proof for
all future queries.

## Time and disk space

| Layout | ANN median / p95 | Table incl. TOAST | HNSW index | Total incl. primary key |
|---|---:|---:|---:|---:|
| Float32 table + float32 index | 21.56 / 28.78 ms | 344.70 MiB | 503.82 MiB | 849.91 MiB |
| Float16 table + float16 index | 11.85 / 15.62 ms | 174.93 MiB | 167.95 MiB | 344.27 MiB |
| Int8 bytes + float16 index | 20.36 / 23.96 ms | 126.20 MiB | 167.95 MiB | 295.55 MiB |

These are actual relations containing only ID and embedding; title/BM25 storage
is separate. PostgreSQL page/TOAST overhead means these differ from coordinate-only
estimates. The float32 control uses the paper's EXTERNAL storage setting; other
columns use defaults. This was not an exhaustive storage-setting tuning exercise.

The bytes layout saved **48.73 MiB (14.2%)** against plain float16, but its ANN query
was **72% slower** in this run. Index construction took 61.3s for float32, 42.7s for
float16, and 47.9s for bytes after fixing the helper described below.

Scaling these observed sizes linearly to the live count of 509,118 eligible stories
suggests 6.54 GiB / 2.65 GiB / 2.27 GiB respectively. Those are estimates, not measured
full-history sizes. The byte-versus-half saving would be about **0.38 GiB**.

## Does int8 cut against PostgreSQL's grain?

**Against pgvector's native query interface, yes, moderately.** Saving/loading the
bytes is straightforward, and no values are lost. The annoyance is searching them:

Terminology: PostgreSQL's SQL type named `int8` means eight-byte `bigint`, not the
model's one-byte int8 coordinates. This experiment uses `bytea` for packed native
bytes and `halfvec` for the two-byte alternative.

1. **A custom conversion helper is required.** It decodes two's-complement bytes
   using `get_byte`, constructs an integer array and casts to halfvec. The first
   index build failed because the SQL function's unqualified `halfvec` reference
   could not resolve during index-expression inlining. Qualifying it as
   `public.halfvec` fixed that. The failure and final SQL are retained in evidence.
2. **Query spelling becomes part of the performance contract.** The tested index
   expression is `unpack_int8(v)::halfvec(1024)`. Removing the dimension cast from
   the query changed its plan to sequential scan plus sort. A shared SQL helper
   can hide this, but someone simplifying the SQL can accidentally lose the index.
3. **Exact fallback is expensive.** One bytea exact-search probe took **5,176 ms**.
   Plain halfvec exact search took **90.5 ms median** over the eval. This is a
   roughly 57x contrast, although the bytea figure is a single probe, not a full
   latency distribution. Converting all 64,638 rows in SQL is costly. Avoiding that
   cost requires more bespoke code or another representation.

The source/index limitations are documented in the
[pgvector 0.8.2 operator definitions](https://github.com/pgvector/pgvector/blob/v0.8.2/sql/vector.sql).
This experiment does not test every PostgreSQL extension or a custom C converter.
Those could change the tradeoff, but neither is needed to ship the selected model.

**Recommendation:** use unnormalized native integer coordinates in a plain
`halfvec(1024)` column. It keeps every model coordinate exactly, cut measured total
space by 59.5% versus float32, and uses pgvector's ordinary SQL without an unpacking
helper. Native bytea remains a viable explicit space-first option, now with a
measured saving and maintenance cost. There is no measured reason to retain
float32 table storage for quality on this eval.

## Reproduction and evidence

Runner: [`pplx_int8_storage_eval.py`](../tools/pplx_int8_storage_eval.py).
Small results, per-case metrics, configuration, image identity and failure receipt
are retained in [`evidence/int8-storage-20260906/`](evidence/int8-storage-20260906/).
Full query journals/plans/rankings and restored inputs are under the ignored
`data/int8-eval-20260906/` and original frozen-data paths. Result hashes identify
those files; the source archive manifest and selected restoration inventory are
also retained there.

Start a fresh scratch database; the script refuses to overwrite existing tables:

```sh
docker run -d --name searchhn-int8-eval-20260906 \
  -e POSTGRES_PASSWORD=local-eval-only -p 127.0.0.1:55436:5432 --shm-size=1g \
  pgvector/pgvector@sha256:feb68f4f15446397d8cac7f4fe48fe4586de83160d1fc48b46283312d1a33966
uv run --locked --package search-research python packages/search-research/tools/pplx_int8_storage_eval.py
```

The final runner contains the schema-qualified fix. In this recorded run, the two
completed control indexes were preserved after the failed bytea build; only the
corrected bytea index was built before running the full evaluation. No failed
measurements or attempts were silently replaced.
