"""Cheap in-sample fusion-weight sensitivity using already-measured rankings."""

import polars as pl

from search_research.engine_data import OUT
from search_research.hybrid_baseline import measurements


def weighted_fuse(dense, lexical, weight):
    """Dense weight one, lexical weight varied; zero excludes lexical-only IDs."""
    scores = {story: 1 / (60 + rank) for rank, story in enumerate(dense, 1)}
    if weight:
        for rank, story in enumerate(lexical, 1):
            scores[story] = scores.get(story, 0) + weight / (60 + rank)
    return sorted(scores, key=lambda story: (-scores[story], story))


def run():
    source = pl.read_parquet(OUT / "comparison.parquet")
    lexical = {
        (r["engine"], r["case"]): r["ids"]
        for r in source.filter(pl.col("method") == "lexical").to_dicts()
    }
    rows = []
    for q in source.filter(
        (pl.col("method") == "dense") & pl.col("dimensions").is_in([1536, 3072])
    ).to_dicts():
        engine = "pg-tuned" if q["engine"] == "pg" else "duckdb"
        for weight in [0, 0.25, 0.5, 1, 2]:
            ids = weighted_fuse(q["ids"], lexical[engine, q["case"]], weight)
            rank = ids.index(q["target_id"]) + 1 if q["target_id"] in ids else None
            rows.append(
                {
                    **measurements(q, "weighted-hybrid", q["dimensions"], rank),
                    "engine": engine,
                    "lexical_weight": weight,
                }
            )
    frame = pl.DataFrame(rows)
    frame.write_parquet(OUT / "fusion-sweep.parquet")
    frame.group_by("engine", "dimensions", "lexical_weight").agg(
        pl.col("recall@8", "ndcg@8", "recall@20").mean()
    ).sort("engine", "dimensions", "lexical_weight").write_csv(OUT / "fusion-sweep.csv")


if __name__ == "__main__":
    run()
