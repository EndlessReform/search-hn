# Search research

The [whitepaper](../../output/pdf/search-hn-retrieval-whitepaper.pdf) summarizes
the completed retrieval study. The selected starting point is Pplx 0.6B BF16 via
stock vLLM, 1024 dimensions, light PostgreSQL BM25 fusion, and HNSW ef=1000.

- [Reproduction guide](docs/reproduction.md): Garage restore, frozen inputs,
  serving recipes, harness commands, budget/resume behavior, and cleanup.
- [Production design](docs/production-design.md): ownership, index lifecycle,
  open decisions, and implementation acceptance checks.
- [Paper source](whitepaper/search-hn.typ): methods, evaluation, and conclusions.

The numbered experiment diaries are preserved in Git at `27f572f` and in Garage.
Results belong in the paper; operating instructions belong in the reproduction
guide. Keep new run-specific records with their artifact release.

## Run and build

Commands run from the repository root. Restore the required data first using the
reproduction guide; bulk arrays and trajectories live in Garage. Several scripts
retain frozen paths and endpoint settings, so inspect them before a new run.
Use a fresh output directory unless explicitly resuming the same manifest.

```sh
uv sync --locked --package search-research
uv run --locked --package search-research pytest packages/search-agent/tests packages/search-research/tests
```

Build the paper with Typst 0.15.1 and Times New Roman installed. Its source pins
`bloated-neurips` 0.8.0; Typst downloads the package on first use. The pinned
third-party template is also archived in the Garage closeout release.

```sh
typst compile packages/search-research/whitepaper/search-hn.typ output/pdf/search-hn-retrieval-whitepaper.pdf
```

## Code map

| Area | Modules / files |
|---|---|
| Frozen questions and provenance | `dataset`, `curate`, `rewrite`, `manifest`; root question-edits/exclusions JSON |
| Original dataset/FTS CLI | `hn-eval` / `cli`, `rollouts`, `suite`, `report` |
| Static retrieval and database comparisons | `embedding_*`, `hybrid_baseline`, `engine_*`, `textsearch_*`, `pg_lexical_sweep`, `fusion_sweep` |
| Local embedding comparison | `tei_embeddings`, `sovereign_run`, `sovereign_score` |
| Agent experiments | `semantic_*`, `sovereign_repository`, `sovereign_rollouts`, `rollout_budget`, `sovereign_e2e_report`, `rate_retry` |
| Reranking and miss analysis | `rerank_*`, `miss_audit*`, `cutoff_sweep` |
| BF16, fusion, ANN, and timing tools | `tools/pplx_*` |
| Serving recipes | Shared Pplx: `../../deploy/inference/`; experiments: `compose*.yaml`, `install-textsearch.sh`, `tools/nemotron_server.py` |
| Archive and restore | `artifacts`, `tools/research_closeout.py` |

`src/search_research/` contains the Python package; `tools/` contains later
experiment drivers and SQL analyses; `tests/` contains focused tests. The
snapshot-specific repository in `search-agent` is experimental. Production API
and indexing responsibilities are specified in the design, not inferred from
this harness's direct database access.
