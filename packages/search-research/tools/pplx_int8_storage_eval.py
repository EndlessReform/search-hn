"""Compare native-int8 PG storage with the frozen paper recipe in scratch PG.

No inference or source-database writes. Reuse the archived lexical candidates so
only vector storage/index representation changes. Journal each query before report.
"""

import hashlib
import io
import json
import struct
import time
from pathlib import Path

import numpy as np
import polars as pl
import psycopg2

from pplx_hybrid_sweep import fuse
from search_research.embedding_baseline import shortened
from search_research.hybrid_baseline import measurements
from search_research.sovereign_run import CORPUS, HASHES, QUESTIONS
from search_research.sovereign_score import load_native, top_ids

ROOT = Path("data/pplx-vllm-gate-20260905/bf16-full")
OUT = Path("data/int8-eval-20260906")
DSN = "host=127.0.0.1 port=55436 dbname=postgres user=postgres password=local-eval-only"
ARMS = {
    "f32": ("vector(1024)", "v", "vector", "vector_cosine_ops"),
    "half": ("halfvec(1024)", "v", "halfvec", "halfvec_cosine_ops"),
    "bytes": (
        "bytea",
        "unpack_int8(v)::halfvec(1024)",
        "halfvec",
        "halfvec_cosine_ops",
    ),
}
UNPACK = """CREATE FUNCTION unpack_int8(b bytea) RETURNS halfvec
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE AS $$
 SELECT ARRAY(SELECT CASE WHEN get_byte(b,i)>127 THEN get_byte(b,i)-256
                         ELSE get_byte(b,i) END
              FROM generate_series(0,octet_length(b)-1) AS i ORDER BY i)::public.halfvec
$$"""


def save(name, value):
    (OUT / name).write_text(json.dumps(value, indent=2))


def copy_matrix(cur, arm, ids, matrix):
    """Binary COPY keeps transport size representative of the actual column type."""
    for start in range(0, len(ids), 512):
        stream = io.BytesIO(b"PGCOPY\n\xff\r\n\x00" + struct.pack("!ii", 0, 0))
        stream.seek(0, 2)
        for sid, vector in zip(
            ids[start : start + 512], matrix[start : start + 512], strict=True
        ):
            if arm == "bytes":
                payload = vector.tobytes()
            else:
                payload = (
                    struct.pack("!hh", 1024, 0)
                    + vector.astype(">f4" if arm == "f32" else ">f2").tobytes()
                )
            stream.write(struct.pack("!hiqi", 2, 8, int(sid), len(payload)) + payload)
        stream.write(struct.pack("!h", -1))
        stream.seek(0)
        cur.copy_expert(f"COPY eval_{arm} FROM STDIN WITH BINARY", stream)


def prepare(conn, ids, native, normalized):
    """Build three isolated, single-worker graphs in the same ID order and seed.

    Resetting the seed reduces graph-build variation; it does not establish that
    different representations must create the same graph. Never modify app tables.
    """
    timings = {}
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute("SELECT to_regclass('eval_f32')")
        assert cur.fetchone()[0] is None, "Use a fresh scratch database for a fresh run"
        cur.execute(UNPACK)
        cur.execute("SET maintenance_work_mem='768MB'")
        cur.execute("SET max_parallel_maintenance_workers=0")
        cur.execute("SET max_parallel_workers_per_gather=0")
        for arm, (typ, expression, _, opclass) in ARMS.items():
            cur.execute(
                f"CREATE TABLE eval_{arm}(id bigint PRIMARY KEY, v {typ} NOT NULL)"
            )
            if arm == "bytes":
                cur.execute("ALTER TABLE eval_bytes ADD CHECK(octet_length(v)=1024)")
            if arm == "f32":
                cur.execute("ALTER TABLE eval_f32 ALTER COLUMN v SET STORAGE EXTERNAL")
            started = time.perf_counter()
            copy_matrix(cur, arm, ids, normalized if arm == "f32" else native)
            conn.commit()
            timings[arm] = {"copy_seconds": time.perf_counter() - started}
            cur.execute("SELECT setseed(0.42)")
            started = time.perf_counter()
            cur.execute(
                f"CREATE INDEX eval_{arm}_hnsw ON eval_{arm} USING hnsw (({expression}) {opclass}) WITH(m=16, ef_construction=128)"
            )
            conn.commit()
            timings[arm]["build_seconds"] = time.perf_counter() - started
            cur.execute(f"ANALYZE eval_{arm}")
            print(json.dumps({"ready": arm, **timings[arm]}), flush=True)
            save("build.json", timings)
        cur.execute("SELECT version()")
        version = cur.fetchone()[0]
        cur.execute("SELECT extversion FROM pg_extension WHERE extname='vector'")
        save("server.json", {"postgres": version, "pgvector": cur.fetchone()[0]})
    conn.commit()


def sql_for(arm, exact=False):
    _, expression, typ, _ = ARMS[arm]
    distance = f"{expression} <=> %s::{typ}"
    if exact:
        return f"SELECT id FROM eval_{arm} ORDER BY ({distance})+0,id LIMIT 100"
    return f"SELECT id FROM (SELECT id,{distance} AS distance FROM eval_{arm} ORDER BY {distance} LIMIT 100) c ORDER BY distance,id"


def native_reference(ids, docs, queries):
    """Exact cosine of integer coordinates with float64 accumulation, no re-quantization.

    All coordinates/products/sums are exactly representable here. Normalizing the
    float32 baseline can change ties through rounding, so compare metrics and IDs.
    """
    d = docs.astype(np.float64)
    q = queries.astype(np.float64)
    cosine = (q @ d.T) / (
        np.linalg.norm(q, axis=1)[:, None] * np.linalg.norm(d, axis=1)[None, :]
    )
    return [top_ids(row, ids) for row in cosine]


def evaluate(conn, questions, ids, docs, queries):
    """Measure the full eval at concurrency one; cache lexical lists unchanged."""
    lexical = pl.read_parquet(ROOT / "lexical.parquet")
    assert lexical["case"].to_list() == questions["case"].to_list()
    native = native_reference(ids, docs, queries)
    normalized = shortened(queries, 1024)
    rankings = {"native_exact": native}
    plans, timings = {}, []
    tasks = [("f32_exact", "f32", True), ("half_exact", "half", True)] + [
        (a + "_ann", a, False) for a in ARMS
    ]
    with conn.cursor() as cur, (OUT / "queries.jsonl").open("x") as journal:
        cur.execute("SET statement_timeout='60s'")
        cur.execute("SET hnsw.ef_search=1000")
        cur.execute("SET hnsw.iterative_scan='off'")
        for label, arm, exact in tasks:
            literals = [
                json.dumps(row.tolist())
                for row in (normalized if arm == "f32" else queries)
            ]
            query_sql = sql_for(arm, exact)
            params = (literals[0],) if exact else (literals[0], literals[0])
            cur.execute("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + query_sql, params)
            plans[label] = cur.fetchone()[0]
            assert (f"eval_{arm}_hnsw" in json.dumps(plans[label])) != exact
            rankings[label] = [None] * len(questions)
            # Two shuffled warm passes; full-corpus exact gets one pass to bound cost.
            for repeat in range(1 if exact else 2):
                for n in np.random.default_rng(20260906 + repeat).permutation(
                    len(questions)
                ):
                    value = literals[n]
                    params = (value,) if exact else (value, value)
                    started = time.perf_counter()
                    cur.execute(query_sql, params)
                    found = [r[0] for r in cur.fetchall()]
                    elapsed = (time.perf_counter() - started) * 1000
                    assert len(found) == 100 and len(set(found)) == 100
                    if rankings[label][n] is not None:
                        assert rankings[label][n] == found
                    rankings[label][n] = found
                    row = {
                        "method": label,
                        "case": questions["case"][int(n)],
                        "repeat": repeat,
                        "ms": elapsed,
                        "ids": found,
                    }
                    journal.write(json.dumps(row) + "\n")
                    journal.flush()
                    timings.append({k: v for k, v in row.items() if k != "ids"})
            print(json.dumps({"completed": label}), flush=True)
            save("plans.json", plans)
    rows, overlap, changes = [], [], []
    for label, lists in rankings.items():
        for n, q in enumerate(questions.to_dicts()):
            overlap.append(
                {
                    "method": label,
                    "overlap100": len(set(lists[n]) & set(native[n])) / 100,
                }
            )
            for mode, ranking in [
                ("dense", lists[n]),
                ("hybrid", fuse(lists[n], lexical["ids"][n].to_list(), 0.125)),
            ]:
                target = q["target_id"]
                rank = ranking.index(target) + 1 if target in ranking else None
                rows.append(measurements(q, label + "_" + mode, 1024, rank))
                reference = (
                    fuse(native[n], lexical["ids"][n].to_list(), 0.125)
                    if mode == "hybrid"
                    else native[n]
                )
                base = reference.index(target) + 1 if target in reference else None
                if (rank is not None and rank <= 20) != (
                    base is not None and base <= 20
                ):
                    changes.append(
                        {
                            "method": label,
                            "mode": mode,
                            "case": q["case"],
                            "target_id": target,
                            "native_rank": base,
                            "rank": rank,
                        }
                    )
    frame = pl.DataFrame(rows)
    frame.write_parquet(OUT / "metrics.parquet")
    summary = (
        frame.group_by("method")
        .agg(pl.col("recall@8", "recall@20").sum(), pl.col("ndcg@8", "ndcg@20").mean())
        .sort("method")
    )
    summary.write_csv(OUT / "summary.csv")
    save("summary.json", summary.to_dicts())
    pl.DataFrame(timings).write_parquet(OUT / "timings.parquet")
    timing = (
        pl.DataFrame(timings)
        .group_by("method")
        .agg(
            pl.col("ms").median().alias("median_ms"),
            pl.col("ms").quantile(0.95).alias("p95_ms"),
        )
        .sort("method")
    )
    save("timing-summary.json", timing.to_dicts())
    save("target-changes.json", changes)
    save(
        "overlap.json",
        pl.DataFrame(overlap)
        .group_by("method")
        .agg(pl.col("overlap100").mean())
        .to_dicts(),
    )
    save("rankings.json", rankings)
    print(summary)
    print(timing)


def friction_probes(conn, query):
    """Measure representation-specific costs; do not mistake bytea for native ANN.

    EXPLAIN checks whether an apparently innocent query spelling changes the plan.
    A bounded exact bytea query exposes SQL-unpacking cost without a 196-case slog.
    """
    conn.commit()
    conn.autocommit = True
    results = {}
    with conn.cursor() as cur:
        for arm in ARMS:
            cur.execute(
                f"SELECT pg_table_size('eval_{arm}'),pg_relation_size('eval_{arm}_hnsw'),pg_total_relation_size('eval_{arm}')"
            )
            results[arm] = dict(
                zip(("table_bytes", "hnsw_bytes", "total_bytes"), cur.fetchone())
            )
        cur.execute(
            "SELECT bool_and(unpack_int8(b.v)=h.v) FROM eval_bytes b JOIN eval_half h USING(id)"
        )
        assert cur.fetchone()[0], "Byte conversion must reproduce every halfvec exactly"
        raw = json.dumps(query.tolist())
        cur.execute("SET statement_timeout='15s'")
        started = time.perf_counter()
        try:
            cur.execute(sql_for("bytes", True), (raw,))
            cur.fetchall()
            results["bytea_exact_probe"] = {
                "ms": (time.perf_counter() - started) * 1000,
                "timed_out": False,
            }
        except psycopg2.errors.QueryCanceled:
            results["bytea_exact_probe"] = {
                "ms": (time.perf_counter() - started) * 1000,
                "timed_out": True,
            }
        cur.execute(
            "EXPLAIN (FORMAT JSON) SELECT id FROM eval_bytes ORDER BY unpack_int8(v) <=> %s::halfvec LIMIT 100",
            (raw,),
        )
        results["without_dimension_cast_plan"] = cur.fetchone()[0]
    save("storage-and-friction.json", results)
    print(json.dumps(results), flush=True)


def main():
    OUT.mkdir(exist_ok=True)
    for name, path in [("corpus", CORPUS), ("questions", QUESTIONS)]:
        assert hashlib.sha256(path.read_bytes()).hexdigest() == HASHES[name]
    corpus, questions = pl.read_parquet(CORPUS), pl.read_parquet(QUESTIONS)
    docs = load_native(ROOT, "documents", len(corpus))
    queries = load_native(ROOT, "queries", len(questions))
    assert np.array_equal(docs.astype(np.float16).astype(np.int8), docs)
    save(
        "run-config.json",
        {
            "corpus": HASHES["corpus"],
            "questions": HASHES["questions"],
            "documents_sha256": hashlib.sha256(docs.tobytes()).hexdigest(),
            "queries_sha256": hashlib.sha256(queries.tobytes()).hexdigest(),
            "stories": len(corpus),
            "questions_count": len(questions),
            "ef_search": 1000,
            "m": 16,
            "ef_construction": 128,
            "lexical_weight": 0.125,
            "seed": 0.42,
            "unpack_sql": UNPACK,
        },
    )
    with psycopg2.connect(DSN) as conn:
        prepare(conn, corpus["id"].to_numpy(), docs, shortened(docs, 1024))
        evaluate(conn, questions, corpus["id"].to_numpy(), docs, queries)
        friction_probes(conn, queries[0])


if __name__ == "__main__":
    main()
