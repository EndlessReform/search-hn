"""Compare fresh, consumed Luna trajectories against the archived FTS baseline."""

import argparse
import json

import polars as pl

from search_research.engine_data import TRACES
from search_research.report import report
from search_research.semantic_snapshot import ROOT


def compare():
    """Report all terminal cases and a paired-case view while a run is incomplete."""
    frames = []
    for mode, root in [
        ("original-fts", TRACES),
        ("dense", ROOT / "dense"),
        ("hybrid", ROOT / "hybrid"),
    ]:
        if mode != "original-fts":
            report(root)
        frame = (
            pl.read_parquet(root / "metrics.parquet")
            .filter(
                (pl.col("model") == "gpt-5.6-luna")
                & pl.col("status").is_in(["complete", "error"])
            )
            .with_columns(pl.lit(mode).alias("treatment"))
        )
        frames.append(frame)
    common = set.intersection(*(set(f["case"]) for f in frames))
    metrics = [
        "exposed",
        "cited_evidence",
        "query_count",
        "elapsed",
        "input_tokens",
        "output_tokens",
        "query_pass@1",
        "query_pass@3",
        "query_pass@5",
        "query_pass@8",
        "query_pass@10",
        "first_query_recall@8",
        "first_query_recall@20",
        "first_query_ndcg@8",
        "first_query_ndcg@20",
    ]
    combined = pl.concat(
        [f.select("treatment", "case", "status", *metrics) for f in frames]
    )
    combined.write_parquet(ROOT / "comparison.parquet")
    for name, frame in [
        ("all", combined),
        ("paired", combined.filter(pl.col("case").is_in(sorted(common)))),
    ]:
        summary = frame.group_by("treatment").agg(
            pl.len().alias("n"),
            (pl.col("status") == "error").sum().alias("errors"),
            pl.col(metrics).mean(),
        )
        summary.write_csv(ROOT / f"comparison-{name}.csv")
        print(
            name,
            summary.select(
                "treatment",
                "n",
                "errors",
                "exposed",
                "cited_evidence",
                "query_pass@1",
                "query_pass@5",
                "query_count",
                "elapsed",
            ),
        )
    print(json.dumps({"paired_cases": len(common), "expected_per_treatment": 196}))


def static():
    """Reuse frozen question embeddings to isolate the refreshed corpus effect."""
    from search_agent.semantic_search import SemanticStoryRepository

    from search_research.embedding_baseline import shortened
    from search_research.engine_data import BASE, load_vectors
    from search_research.hybrid_baseline import measurements
    from search_research.semantic_rollouts import LIVE, SCRATCH

    questions = pl.read_parquet(BASE / "questions.parquet")
    vectors = shortened(load_vectors(BASE, "queries", questions), 1536)
    rows = []
    for mode in ("dense", "hybrid"):
        repository = SemanticStoryRepository(SCRATCH, LIVE, mode=mode)
        try:
            repository.embeddings.update(
                {
                    r["input"]: v.tolist()
                    for r, v in zip(questions.to_dicts(), vectors, strict=True)
                }
            )
            for q in questions.to_dicts():
                hits = repository.search_stories(q["input"], limit=100)
                ids = [h.id for h in hits]
                rank = ids.index(q["target_id"]) + 1 if q["target_id"] in ids else None
                rows.append({**measurements(q, mode, 1536, rank), "ids": ids})
            assert repository.stats["embedding_requests"] == 0
        finally:
            repository.dispose()
    frame = pl.DataFrame(rows)
    frame.write_parquet(ROOT / "static.parquet")
    summary = frame.group_by("method").agg(
        pl.col("recall@8", "recall@20", "ndcg@8", "ndcg@20").mean()
    )
    summary.write_csv(ROOT / "static-summary.csv")
    print(summary)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--static", action="store_true")
    args = parser.parse_args()
    static() if args.static else compare()
