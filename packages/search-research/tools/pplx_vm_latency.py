# /// script
# requires-python = ">=3.11"
# dependencies = ["psycopg[binary]==3.2.9"]
# ///
"""VM-local warm PG vector latency, isolated from SSH/model/lexical processing."""

import json
import random
import sys
import time
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parent
EXACT = "SELECT id FROM vectors ORDER BY (embedding <=> %s::vector)+0,id LIMIT 100"
ANN = "SELECT id FROM (SELECT id,embedding <=> %s::vector distance FROM vectors ORDER BY embedding <=> %s::vector LIMIT 100) c ORDER BY distance,id"
MODES = (
    "exact_serial",
    "exact_planner2",
    "exact_forced2",
    "hnsw100",
    "hnsw200",
    "hnsw400",
    "hnsw800",
)
if "--ef1000" in sys.argv:
    MODES = ("exact_serial", "hnsw800", "hnsw1000")


def configure(cur, mode):
    """Change only search/parallel settings; leave the dataset and index fixed."""
    cur.execute(
        "SET max_parallel_workers_per_gather="
        + ("0" if mode == "exact_serial" else "2")
    )
    for setting, value in (
        ("min_parallel_table_scan_size", "0"),
        ("parallel_setup_cost", "0"),
        ("parallel_tuple_cost", "0"),
    ):
        cur.execute(
            f"SET {setting}={value}" if mode == "exact_forced2" else f"RESET {setting}"
        )
    if mode.startswith("hnsw"):
        cur.execute("SET hnsw.ef_search=" + mode[4:])


def query(cur, mode, literal):
    cur.execute(
        ANN if mode.startswith("hnsw") else EXACT,
        (literal, literal) if mode.startswith("hnsw") else (literal,),
    )
    return [r[0] for r in cur.fetchall()]


def run():
    questions = json.loads((ROOT / "queries.json").read_text())
    for q in questions:
        q["literal"] = json.dumps(q.pop("vector"))
    with (
        psycopg.connect(
            "host=127.0.0.1 port=55433 dbname=bench user=postgres",
            autocommit=True,
            prepare_threshold=None,
        ) as conn,
        conn.cursor() as cur,
    ):
        # Reuse only this disposable table when rerunning timing controls.
        if "--measure-only" not in sys.argv:
            cur.execute("CREATE EXTENSION vector")
            cur.execute(
                "CREATE TABLE vectors(id bigint PRIMARY KEY, embedding vector(1024))"
            )
            cur.execute(
                "ALTER TABLE vectors ALTER COLUMN embedding SET STORAGE EXTERNAL"
            )
            with (
                cur.copy("COPY vectors FROM STDIN WITH BINARY") as copy,
                (ROOT / "vectors.copy").open("rb") as f,
            ):
                while chunk := f.read(1024 * 1024):
                    copy.write(chunk)
            cur.execute("SELECT count(*) FROM vectors")
            assert cur.fetchone()[0] == 64638
            print("Loaded 64,638 vectors; building index", flush=True)
            cur.execute("SET max_parallel_maintenance_workers=0")
            cur.execute(
                "CREATE INDEX vectors_hnsw ON vectors USING hnsw(embedding vector_cosine_ops) WITH(m=16,ef_construction=128)"
            )
            cur.execute("ANALYZE vectors")
        cur.execute("SELECT version()")
        version = cur.fetchone()[0]
        cur.execute("SELECT extversion FROM pg_extension WHERE extname='vector'")
        vector_version = cur.fetchone()[0]
        cur.execute(
            "SELECT pg_relation_size('vectors_hnsw'),pg_total_relation_size('vectors')"
        )
        sizes = cur.fetchone()
        print("Index ready; warming each path", flush=True)
        for mode in MODES:
            configure(cur, mode)
            for q in questions[:16]:
                assert len(query(cur, mode, q["literal"])) == 100
        plans = {}
        for mode in MODES:
            configure(cur, mode)
            sql = ANN if mode.startswith("hnsw") else EXACT
            literal = questions[0]["literal"]
            cur.execute(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql,
                (literal, literal) if mode.startswith("hnsw") else (literal,),
            )
            plans[mode] = cur.fetchone()[0]
            assert ("vectors_hnsw" in json.dumps(plans[mode])) == mode.startswith(
                "hnsw"
            )
        (ROOT / "plans.json").write_text(json.dumps(plans, indent=2))
        rng = random.Random(20260906)
        with (ROOT / "samples.jsonl").open("w") as out:
            for repeat in range(2):
                order = list(range(len(questions)))
                rng.shuffle(order)
                for n, ix in enumerate(order):
                    q = questions[ix]
                    modes = list(MODES)
                    rng.shuffle(modes)
                    for mode in modes:
                        configure(cur, mode)
                        start = time.perf_counter()
                        ids = query(cur, mode, q["literal"])
                        elapsed = (time.perf_counter() - start) * 1000
                        assert len(ids) == 100
                        out.write(
                            json.dumps(
                                {
                                    "repeat": repeat,
                                    "case": q["case"],
                                    "target_id": q["target_id"],
                                    "mode": mode,
                                    "ms": elapsed,
                                    "ids": ids,
                                }
                            )
                            + "\n"
                        )
                    out.flush()
                    if (n + 1) % 49 == 0:
                        print(f"Repeat {repeat + 1}: {n + 1}/196", flush=True)
        (ROOT / "config.json").write_text(
            json.dumps(
                {
                    "version": version,
                    "pgvector": vector_version,
                    "index_bytes": sizes[0],
                    "table_index_bytes": sizes[1],
                    "modes": MODES,
                    "repeats": 2,
                    "concurrency": 1,
                    "seed": 20260906,
                    "scope": "Warm VM-loopback dense top-100; no model or BM25/fusion; settings outside timers; EXPLAIN separate",
                },
                indent=2,
            )
        )
    print("Complete", flush=True)


if __name__ == "__main__":
    run()
