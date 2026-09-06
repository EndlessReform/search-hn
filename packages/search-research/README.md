# Search research

Start with the [five-page research whitepaper](../../output/pdf/search-hn-retrieval-whitepaper.pdf)
and its [Typst source](whitepaper/search-hn.typ). The conclusion is Pplx BF16 via
stock vLLM, 1024 dimensions, light PG BM25 fusion, and provisional HNSW ef=1000.
Full-corpus/filtered behavior and the combined agent recipe remain implementation
acceptance checks. This package is research evidence, not the production service.

[Archive/restore instructions](docs/01-artifacts.md) and
[closeout, cleanup, and the TEI workaround](docs/26-research-closeout.md) are the
operational handoff. Garage is canonical; restore data before rerunning analysis.

Build the paper from repository root with Typst 0.15.1 and Times New Roman
installed. The source pins `bloated-neurips` 0.8.0; Typst downloads that package
on the first build. The pinned template is also preserved in the Garage closeout.

```sh
typst compile packages/search-research/whitepaper/search-hn.typ output/pdf/search-hn-retrieval-whitepaper.pdf
```

## Supporting experiment record

<details>
<summary>Notes 00–25: historical conditions, results, and reproduction commands</summary>


0. [Design and conclusions](docs/00-design.md) — research recommendation and historical implementation proposal.
1. [Artifact releases](docs/01-artifacts.md) — restore data before running analyses.
2. [Evaluation protocol](docs/02-evaluation.md) — sampling, prompts, trajectories, metrics.
3. [Original FTS findings](docs/03-fts-findings.md) — model behavior and cutoff sweep.
4. [Dense and DuckDB BM25](docs/04-embeddings.md) — TE3 dimension baseline.
5. [PG/DuckDB comparison](docs/05-engine-bakeoff.md) — lexical, fusion and fixed-query replay.
6. [Reranker](docs/06-reranker.md) — quality and remote batching results.
7. [Fresh Luna trajectories](docs/07-semantic-luna.md) — pagination, dense versus hybrid.
8. [Topline ROI tables](docs/08-topline-roi.md) — gains and resource use versus untuned FTS, including question-style splits.
9. [Phase 0 miss audit](docs/09-miss-audit.md) — four Luna reviewers on all residual missed questions, with evidence and caveats.
10. [Sovereign embedding bake-off](docs/10-sovereign-embeddings.md) — Perplexity/Qwen/Jina recipes, frozen comparison conditions, dimension and throughput tests, and inference VM prerequisites.
11. [Perplexity first bench](docs/11-pplx-first-bench.md) — native 1024-dimensional quality, measured 3060 backfill, load time and batch-size knee.
12. [Qwen first bench](docs/12-qwen-first-bench.md) — instructed-query recipe, FP16 TEI throughput, native quality and cached dimension sweep.
13. [Jina first bench and combined comparison](docs/13-jina-first-bench.md) — premerged small-retrieval, exact role prefixes, throughput and quality against Qwen, Perplexity and TE3.
14. [English retrieval leaderboard review](docs/14-mteb-retrieval-shortlist.md) — matched-task MTEB/BEIR/RTEB evidence under 2B parameters, with an established-publisher requirement.
15. [Nemotron 1B final static bench](docs/15-nemotron-first-bench.md) — official BF16 serving, precision alternatives, throughput, and the final comparison before E2E.
16. [Final bakeoff manifest and E2E sequence](docs/16-final-bakeoff-manifest.md) — three native dense finalists, full 196-question coverage, optional winner-only hybrid, and the cumulative $8 execution guard.
17. [Luna throughput and routing plan](docs/17-luna-throughput-plan.md) — audited saved traces, input-token capacity targets, concurrency requirements and OpenRouter preflight.
18. [OpenRouter capacity probe](docs/18-openrouter-capacity-probe.md) — measured 2–32 concurrency ramp, confirmed operating point, cost and revised E2E estimate.
19. [Sovereign finalist E2E results](docs/19-sovereign-e2e.md) — all 588 sessions, $3.34 actual driver cost, paired exposure/citation results, streaming audit and resumable budget control.
20. [Pplx BF16 vLLM gate](docs/20-pplx-vllm-gate.md) — stock serving, batch 64 at 1,735 MiB, full backfill in 3m45s, and frozen quality comparison.
21. [Pplx hybrid weight sweep](docs/21-pplx-hybrid-weight-sweep.md) — cached static results, paired comparisons, and light lexical weighting candidates; no E2E runs.
22. [Full-corpus sizing](docs/22-full-corpus-sizing.md) — actual whole-mirror counts at score cutoffs 10/25/50 and revised ANN evaluation scope.
23. [Pplx HNSW accuracy](docs/23-pplx-hnsw-accuracy.md) — two-year slice, exact-neighbor recovery and hybrid target loss across query search breadth; no latency benchmark.
24. [Inference-VM PG latency](docs/24-pplx-vm-latency.md) — Ryzen CPU comparison of serial/parallel exact versus HNSW; temporary VM database cleaned up.
25. [HNSW ef=1000 follow-up](docs/25-pplx-ef1000.md) — retained-graph accuracy and matched VM timing versus 800 and exact search.

</details>

Cleanup and release verification are tracked in [the closeout record](docs/26-research-closeout.md).

All commands run from the repository root with `uv run --locked --package search-research`.
The driver runs here; remote GPU inference is accessed over HTTP. Never run two
drivers against the same output directory. Do not regenerate the frozen evaluation
questions to reproduce a result. Earlier partial findings are in `docs/archive/`.

## Code map

- `dataset`, `curate`, `rewrite`, `manifest`: frozen question construction/provenance.
- `rollouts`, `suite`, `report`: original trajectories and context-consumption metrics.
- `embedding_*`, `hybrid_baseline`: cached embeddings and DuckDB experiments.
- `tei_embeddings`, `sovereign_run`, `sovereign_score`: local-provider transport,
  throughput pilot/resumable backfill, and matched second-run static scoring.
- `sovereign_repository`, `sovereign_rollouts`, `rollout_budget`, `sovereign_e2e_report`:
  isolated vector tables, streaming finalist sessions, durable credit guard and paired results.
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
