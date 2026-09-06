"""Isolated PG HNSW accuracy sweep on cached Pplx BF16; no latency benchmark."""

import hashlib
import io
import json
import struct
from pathlib import Path

import polars as pl
import psycopg2
from pplx_hybrid_sweep import fuse
from search_agent.journal import Journal
from search_research.embedding_baseline import shortened
from search_research.engine_data import DSN
from search_research.hybrid_baseline import measurements
from search_research.sovereign_run import CORPUS, HASHES, QUESTIONS
from search_research.sovereign_score import load_native

ROOT = Path("data/pplx-vllm-gate-20260905/bf16-full")
OUT = ROOT / "hnsw-accuracy"
TABLE = "pplx_bf16_hnsw_accuracy_20260906"
INDEX = TABLE + "_idx"
EFS = (100, 200, 400, 800)


def prepare(conn, corpus, vectors):
    """Load a separate transactional scratch table with a checked vector identity.

    The manifest is committed with the table, so a failed COPY cannot leave a
    partially reusable dataset. Existing model tables and indexes are untouched.
    """
    manifest = {
        "corpus": HASHES["corpus"],
        "matrix_sha256": hashlib.sha256(vectors.tobytes()).hexdigest(),
    }
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM semantic_stories ORDER BY id")
        assert [r[0] for r in cur.fetchall()] == corpus["id"].to_list()
        cur.execute("SELECT to_regclass(%s)", (TABLE,))
        exists = cur.fetchone()[0]
        if exists:
            cur.execute("SELECT obj_description(%s::regclass)", (TABLE,))
            assert json.loads(cur.fetchone()[0]) == manifest
            cur.execute(f"SELECT count(*) FROM {TABLE}")
            assert cur.fetchone()[0] == corpus.height
        else:
            cur.execute(
                f"CREATE TABLE {TABLE} (id bigint PRIMARY KEY, embedding vector(1024))"
            )
            cur.execute(
                f"ALTER TABLE {TABLE} ALTER COLUMN embedding SET STORAGE EXTERNAL"
            )
            for start in range(0, corpus.height, 512):
                buf = io.BytesIO()
                buf.write(b"PGCOPY\n\xff\r\n\x00" + struct.pack("!ii", 0, 0))
                for sid, vector in zip(
                    corpus["id"][start : start + 512],
                    vectors[start : start + 512],
                    strict=True,
                ):
                    payload = (
                        struct.pack("!hh", 1024, 0) + vector.astype(">f4").tobytes()
                    )
                    buf.write(struct.pack("!hiqi", 2, 8, sid, len(payload)))
                    buf.write(payload)
                buf.write(struct.pack("!h", -1))
                buf.seek(0)
                cur.copy_expert(f"COPY {TABLE} FROM STDIN WITH BINARY", buf)
            cur.execute(f"COMMENT ON TABLE {TABLE} IS %s", (json.dumps(manifest),))
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS {INDEX} ON {TABLE} USING hnsw (embedding vector_cosine_ops) WITH (m=16, ef_construction=128)"
        )
        cur.execute(f"ANALYZE {TABLE}")
    conn.commit()


def run():
    """Compare ANN to PG exact candidates and known-target dense/hybrid metrics.

    Exact scans use distance+0 to prevent the HNSW index from satisfying ordering.
    HNSW uses native distance ordering then a stable outer ID tie-break. Rankings
    are journaled per query; this experiment holds the original eligibility slice
    fixed, without introducing additional date/domain filter scenarios.
    """
    OUT.mkdir(exist_ok=True)
    for name, path in (("corpus", CORPUS), ("questions", QUESTIONS)):
        assert hashlib.sha256(path.read_bytes()).hexdigest() == HASHES[name]
    corpus, questions = pl.read_parquet(CORPUS), pl.read_parquet(QUESTIONS)
    docs = shortened(load_native(ROOT, "documents", corpus.height), 1024)
    queries = shortened(load_native(ROOT, "queries", questions.height), 1024)
    lexical = pl.read_parquet(ROOT / "lexical.parquet")
    assert lexical["case"].to_list() == questions["case"].to_list()
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout='0'")
            cur.execute("SET maintenance_work_mem='1GB'")
            cur.execute("SET max_parallel_maintenance_workers=0")
        print(
            "Loading isolated BF16 table and building HNSW m=16, ef_construction=128",
            flush=True,
        )
        prepare(conn, corpus, docs)
        print("Index ready; checking exact and HNSW query plans", flush=True)
        exact_sql = (
            f"SELECT id FROM {TABLE} ORDER BY (embedding <=> %s::vector)+0,id LIMIT 100"
        )
        ann_sql = f"SELECT id FROM (SELECT id,embedding <=> %s::vector AS distance FROM {TABLE} ORDER BY embedding <=> %s::vector LIMIT 100) c ORDER BY distance,id"
        rows, overlaps, plans = [], [], {}
        with conn.cursor() as cur:
            cur.execute("SELECT extname,extversion FROM pg_extension ORDER BY extname")
            versions = cur.fetchall()
            cur.execute("SET statement_timeout='60s'")
            cur.execute("SET hnsw.iterative_scan='off'")
            literal = json.dumps(queries[0].tolist())
            cur.execute("EXPLAIN (FORMAT JSON) " + exact_sql, (literal,))
            plans["exact"] = cur.fetchone()[0]
            assert INDEX not in json.dumps(plans["exact"])
            for ef in EFS:
                cur.execute(f"SET hnsw.ef_search={ef}")
                cur.execute("EXPLAIN (FORMAT JSON) " + ann_sql, (literal, literal))
                plans[str(ef)] = cur.fetchone()[0]
                assert INDEX in json.dumps(plans[str(ef)]), (
                    f"ANN index unused at ef={ef}"
                )
            (OUT / "plans.json").write_text(json.dumps(plans, indent=2))
            journal = Journal(OUT / "queries.jsonl")
            try:
                for n, q in enumerate(questions.to_dicts()):
                    literal = json.dumps(queries[n].tolist())
                    cur.execute(exact_sql, (literal,))
                    exact = [r[0] for r in cur.fetchall()]
                    assert len(exact) == 100
                    rankings = {0: exact}
                    for ef in EFS:
                        cur.execute(f"SET hnsw.ef_search={ef}")
                        cur.execute(ann_sql, (literal, literal))
                        ann = [r[0] for r in cur.fetchall()]
                        assert len(ann) == len(set(ann))
                        rankings[ef] = ann
                        overlaps.append(
                            {
                                "case": q["case"],
                                "ef_search": ef,
                                "returned": len(ann),
                                **{
                                    f"overlap@{k}": len(set(ann[:k]) & set(exact[:k]))
                                    / k
                                    for k in (8, 20, 100)
                                },
                            }
                        )
                    for ef, ranking in rankings.items():
                        for weight in (0, 0.125, 0.25):
                            fused = fuse(ranking, lexical["ids"][n].to_list(), weight)
                            rank = (
                                fused.index(q["target_id"]) + 1
                                if q["target_id"] in fused
                                else None
                            )
                            rows.append(
                                {
                                    "ef_search": ef,
                                    **measurements(q, str(weight), 1024, rank),
                                }
                            )
                    journal.write("query", case=q["case"], rankings=rankings)
                    if (n + 1) % 25 == 0:
                        print(
                            f"Completed {n + 1}/{questions.height} queries", flush=True
                        )
            finally:
                journal.close()
    frame = pl.DataFrame(rows)
    metrics = [c for c in frame.columns if "@" in c]
    frame.write_parquet(OUT / "ranks.parquet")
    frame.group_by("ef_search", "method").agg(
        pl.len().alias("questions"), pl.col(metrics).mean()
    ).sort("method", "ef_search").write_csv(OUT / "summary.csv")
    overlap = pl.DataFrame(overlaps)
    overlap.write_parquet(OUT / "overlaps.parquet")
    overlap.group_by("ef_search").agg(
        pl.col("overlap@8", "overlap@20", "overlap@100").mean(),
        pl.col("overlap@100").min().alias("min_overlap100"),
        (pl.col("overlap@100") == 1).sum().alias("perfect100"),
        pl.col("returned").min().alias("min_returned"),
    ).sort("ef_search").write_csv(OUT / "overlap-summary.csv")
    controls = pl.read_parquet(ROOT / "hybrid-weight-sweep/ranks.parquet")
    differences = []
    for weight in ("0", "0.125", "0.25"):
        base = frame.filter(
            (pl.col("ef_search") == 0) & (pl.col("method") == weight)
        ).sort("case")
        cached = controls.filter(pl.col("method") == weight).sort("case")
        differences.append(
            {
                "method": weight,
                "changed_metric_cells": int(
                    (
                        base.select(metrics).to_numpy()
                        != cached.select(metrics).to_numpy()
                    ).sum()
                ),
            }
        )
    (OUT / "cached-control-check.json").write_text(json.dumps(differences, indent=2))
    (OUT / "recipe.json").write_text(
        json.dumps(
            {
                "source": json.loads((ROOT / "manifest.json").read_text()),
                "hashes": HASHES,
                "table": TABLE,
                "index": INDEX,
                "m": 16,
                "ef_construction": 128,
                "ef_search": EFS,
                "iterative_scan": "off",
                "candidate_depth": 100,
                "lexical_weights": [0, 0.125, 0.25],
                "extensions": versions,
                "scope": "Original two-year >=25 slice, no extra query filters; one graph build; no latency measurement",
            },
            indent=2,
        )
    )
    print("Accuracy sweep complete", flush=True)


if __name__ == "__main__":
    run()
