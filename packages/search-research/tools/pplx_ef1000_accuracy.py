"""Add ef=1000 to the retained accuracy graph without rebuilding it."""

import json
from pathlib import Path

import polars as pl
import psycopg2
from pplx_hybrid_sweep import fuse
from search_research.engine_data import DSN

ROOT = Path("data/pplx-vllm-gate-20260905/bf16-full")
OUT = Path("data/pplx-ef1000-20260906")
TABLE = "pplx_bf16_hnsw_accuracy_20260906"


def run():
    """Query the retained scratch index and write its comparison artifacts."""
    questions = json.loads((OUT / "queries.json").read_text())
    lexical = dict(pl.read_parquet(ROOT / "lexical.parquet").iter_rows())
    exact = {
        r["case"]: r["rankings"]["0"]
        for r in map(
            json.loads, (ROOT / "hnsw-accuracy/queries.jsonl").read_text().splitlines()
        )
    }
    rows = []
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SET hnsw.ef_search=1000")
        cur.execute("SET hnsw.iterative_scan='off'")
        sql = f"SELECT id FROM (SELECT id,embedding <=> %s::vector distance FROM {TABLE} ORDER BY embedding <=> %s::vector LIMIT 100) c ORDER BY distance,id"
        literal = json.dumps(questions[0]["vector"])
        cur.execute("EXPLAIN (FORMAT JSON) " + sql, (literal, literal))
        plan = cur.fetchone()[0]
        assert TABLE + "_idx" in json.dumps(plan)
        (OUT / "local-plan.json").write_text(json.dumps(plan, indent=2))
        for q in questions:
            literal = json.dumps(q["vector"])
            cur.execute(sql, (literal, literal))
            ids = [r[0] for r in cur.fetchall()]
            assert len(ids) == 100
            ranking = fuse(ids, lexical[q["case"]], 0.125)
            rank = (
                ranking.index(q["target_id"]) + 1 if q["target_id"] in ranking else 1000
            )
            rows.append(
                {
                    "case": q["case"],
                    "ids": ids,
                    "overlap100": len(set(ids) & set(exact[q["case"]])) / 100,
                    "hybrid_hits8": int(rank <= 8),
                    "hybrid_hits20": int(rank <= 20),
                }
            )
    frame = pl.DataFrame(rows)
    frame.write_parquet(OUT / "local-accuracy.parquet")
    frame.select(
        pl.col("overlap100").mean(), pl.col("hybrid_hits8", "hybrid_hits20").sum()
    ).write_csv(OUT / "local-summary.csv")


if __name__ == "__main__":
    run()
