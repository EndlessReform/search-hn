# Full-corpus sizing before ANN decision

Measured 2026-09-06 directly from `searchhn-pg:5432/searchhn_test` using the
read-only account. The earlier recommendation to defer ANN based on the
64,638-story benchmark understated deployment scale and is superseded here.

Eligible rows are `type='story'`, not dead or deleted (NULL flags treated as
false), with score at least the cutoff. No date restriction or URL requirement;
self/text posts remain included. One title+URL vector per eligible story.
The qualifying mirror spans 2006-10-09 through 2026-09-06 UTC. These are current
mirror counts; scores and ingestion coverage can change.

| Score cutoff | Stories / vectors | Multiple of 64,638 | Native int8 payload GiB | Float32 vector payload GiB |
|---|---:|---:|---:|---:|
| >=10 | 776,244 | 12.01x | .740 | 2.961 |
| >=25 | 509,107 | 7.88x | .486 | 1.942 |
| >=50 | 353,173 | 5.46x | .337 | 1.347 |

Payload estimates use 1024 coordinates and exclude PostgreSQL row/TOAST metadata,
indexes, HNSW graph overhead, other columns and copies. BF16 model computation
and native int8 cache output do not make the current pgvector `vector` storage
int8: that representation uses float32 coordinates. Broad exact search evaluates
hundreds of thousands of vectors; warm-cache CPU/memory work and concurrency
must be measured on the intended weaker database host, not inferred from the
small local slice. Counts alone do not establish actual latency.

Recommendation: make a bounded full-scale ANN-versus-exact benchmark a gate
before settling the production retrieval path. Use the chosen model and actual
admission cutoff; build one HNSW configuration and vary only query-time search
breadth initially. Include vote/date/domain filters, measure candidate recovery
at 100 and final hybrid quality at 8/20, p50/p95 latency, footprint and build time.
Keep exact search as the reference and possible selective-filter path. Do not
launch an unrestricted HNSW parameter grid or infer that ANN is required solely
from vector count. No indexes or full-corpus embedding jobs were started here.

Evidence: `data/full-corpus-sizing-20260906/counts.sql` and `counts.csv`.
EXPLAIN selected `idx_items_story_day_score` with a score>=10 condition, followed
by eligibility filters and one aggregate. The read-only count finished within
its 60-second statement timeout.
