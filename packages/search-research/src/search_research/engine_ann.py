"""Measure HNSW search effort against the exact PG top-100 baseline."""

import json
import time
from contextlib import closing

import polars as pl
from search_agent.journal import Journal

from search_research.embedding_baseline import shortened
from search_research.engine_backends import Postgres, timed
from search_research.engine_data import BASE, OUT, load_vectors
from search_research.hybrid_baseline import measurements


def run():
    questions = pl.read_parquet(BASE / "questions.parquet")
    vectors = shortened(load_vectors(BASE, "queries", questions), 1536)
    exact = {
        r["case"]: r["ids"]
        for r in pl.read_parquet(OUT / "static-summary.parquet")
        .filter(
            (pl.col("engine") == "pg")
            & (pl.col("method") == "dense")
            & (pl.col("dimensions") == 1536)
        )
        .to_dicts()
    }
    pg = Postgres()
    pg.cur.execute("SET statement_timeout='0'")
    start = time.perf_counter()
    pg.cur.execute(
        "CREATE INDEX IF NOT EXISTS vectors_1536_hnsw ON vectors_1536 USING hnsw (embedding vector_cosine_ops) WITH (m=16, ef_construction=128)"
    )
    build_seconds = time.perf_counter() - start
    pg.cur.execute("ANALYZE vectors_1536")
    pg.cur.execute(
        "SELECT pg_relation_size('vectors_1536_hnsw'),pg_total_relation_size('vectors_1536')"
    )
    sizes = pg.cur.fetchone()
    (OUT / "ann-config.json").write_text(
        json.dumps(
            {
                "dimensions": 1536,
                "m": 16,
                "ef_construction": 128,
                "build_seconds": build_seconds,
                "index_bytes": sizes[0],
                "table_and_index_bytes": sizes[1],
            },
            indent=2,
        )
    )
    rows = []
    with closing(Journal(OUT / "ann.jsonl")) as journal:
        for ef in [100, 200, 400]:
            pg.cur.execute(f"SET hnsw.ef_search={ef}")
            for q, vector in zip(questions.to_dicts(), vectors):
                literal = json.dumps(vector.tolist())

                def search(literal=literal):
                    # Inner ordering matches the index; stable outer tie-break.
                    pg.cur.execute(
                        "SELECT id FROM (SELECT id,embedding <=> %s::vector distance FROM vectors_1536 ORDER BY embedding <=> %s::vector LIMIT 100) c ORDER BY distance,id",
                        (literal, literal),
                    )
                    return [r[0] for r in pg.cur.fetchall()]

                ids, ms = timed(search)
                rank = ids.index(q["target_id"]) + 1 if q["target_id"] in ids else None
                row = {
                    **measurements(q, "hnsw", 1536, rank),
                    "ef_search": ef,
                    "milliseconds": ms,
                    "overlap100": len(set(ids) & set(exact[q["case"]])) / 100,
                    "ids": ids,
                }
                rows.append(row)
                journal.write("result", **row)
            print(
                json.dumps({"hnsw_ef_search": ef, "questions": questions.height}),
                flush=True,
            )
        pg.cur.execute(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT id FROM vectors_1536 ORDER BY embedding <=> %s::vector LIMIT 100",
            (json.dumps(vectors[0].tolist()),),
        )
        (OUT / "ann-plan.json").write_text(json.dumps(pg.cur.fetchone()[0], indent=2))
    pg.close()
    frame = pl.DataFrame(rows)
    frame.write_parquet(OUT / "ann.parquet")
    frame.group_by("ef_search").agg(
        pl.col("recall@8", "ndcg@8", "recall@20", "overlap100").mean(),
        pl.col("milliseconds").median().alias("median_ms"),
        pl.col("milliseconds").quantile(0.95).alias("p95_ms"),
    ).sort("ef_search").write_csv(OUT / "ann-summary.csv")


if __name__ == "__main__":
    run()
