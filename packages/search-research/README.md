# Search research

Start with [the proposed production design](docs/00-design.md). This package is
the reproducible research record, **not** the production embedding service.
Garage is the source of truth for frozen datasets, rollouts, vectors and reports;
local `data/` is a working copy. Git owns source, the UV lockfile and these docs.

## Reading / reproduction order

0. [Design and conclusions](docs/00-design.md) — proposed, awaiting review.
1. [Artifact releases](docs/01-artifacts.md) — restore data before running analyses.
2. [Evaluation protocol](docs/02-evaluation.md) — sampling, prompts, trajectories, metrics.
3. [Original FTS findings](docs/03-fts-findings.md) — model behavior and cutoff sweep.
4. [Dense and DuckDB BM25](docs/04-embeddings.md) — TE3 dimension baseline.
5. [PG/DuckDB comparison](docs/05-engine-bakeoff.md) — lexical, fusion and fixed-query replay.
6. [Reranker](docs/06-reranker.md) — quality and remote batching results.
7. [Fresh Luna trajectories](docs/07-semantic-luna.md) — pagination, dense versus hybrid.

All commands run from the repository root with `uv run --locked --package search-research`.
The driver runs here; remote GPU inference is accessed over HTTP. Never run two
drivers against the same output directory. Do not regenerate the frozen evaluation
questions to reproduce a result. Earlier partial findings are in `docs/archive/`.

## Code map

- `dataset`, `curate`, `rewrite`, `manifest`: frozen question construction/provenance.
- `rollouts`, `suite`, `report`: original trajectories and context-consumption metrics.
- `embedding_*`, `hybrid_baseline`: cached embeddings and DuckDB experiments.
- `engine_*`, `pg_lexical_sweep`, `textsearch_*`, `fusion_sweep`: engine comparisons.
- `rerank_*`: isolated remote reranker experiment.
- `semantic_*`, `rate_retry`: fresh Luna experiment and rate-limit recovery.
- `artifacts`: versioned Garage publication and checksum-verified restore.

Experiment module names remain stable so recorded commands still work. The
snapshot-specific `SemanticStoryRepository` in search-agent is experimental;
its provider/dimension choices must not become the production client contract.

```sh
uv sync --locked --package search-research
uv run --locked --package search-research pytest packages/search-agent/tests packages/search-research/tests
```
