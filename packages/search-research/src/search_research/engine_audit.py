"""Check native-engine parity and locate lexical misses before/after ranking."""

import argparse
import json

import duckdb
import polars as pl

from search_research.embedding_baseline import shortened
from search_research.engine_backends import Postgres
from search_research.engine_data import BASE, OUT, TRACES, load_vectors


def static_audit():
    """Assert all exact dense metrics reproduce; compare lexical eligibility."""
    frame = pl.read_parquet(OUT / "comparison.parquet")
    original = pl.read_parquet(BASE / "ranks.parquet")
    metrics = [c for c in original.columns if "@" in c]
    for engine in ["pg", "duckdb"]:
        actual = frame.filter(
            (pl.col("engine") == engine) & (pl.col("method") == "dense")
        )
        joined = actual.join(original, on=["case", "dimensions"], suffix="_original")
        assert joined.height == 1176
        for metric in metrics:
            assert joined.filter(
                (pl.col(metric) - pl.col(metric + "_original")).abs() > 1e-10
            ).is_empty(), (engine, metric)
    existing = pl.read_parquet(BASE / "hybrid-ranks.parquet").filter(
        pl.col("method") == "hybrid"
    )
    duck_hybrid = frame.filter(
        (pl.col("engine") == "duckdb") & (pl.col("method") == "hybrid")
    ).join(existing, on=["case", "dimensions"], suffix="_original")
    assert duck_hybrid.height == 1176
    for metric in metrics:
        assert duck_hybrid.filter(
            (pl.col(metric) - pl.col(metric + "_original")).abs() > 1e-10
        ).is_empty(), metric
    pg = Postgres()
    questions = pl.read_parquet(BASE / "questions.parquet")
    vector = shortened(load_vectors(BASE, "queries", questions), 1536)[0]
    pg.cur.execute(
        "EXPLAIN (FORMAT JSON) SELECT v.id FROM vectors_1536 v JOIN documents d USING(id) ORDER BY (v.embedding <=> %s::vector)+0,v.id LIMIT 100",
        (json.dumps(vector.tolist()),),
    )
    plan = pg.cur.fetchone()[0]
    assert "vectors_1536_hnsw" not in json.dumps(plan), "Exact baseline used ANN index"
    (OUT / "exact-plan.json").write_text(json.dumps(plan, indent=2))
    duck = duckdb.connect(str(BASE / "bm25.duckdb"), read_only=True)
    duck.execute("LOAD fts; SET threads=2")
    rows = []
    for q in pl.read_parquet(BASE / "questions.parquet").to_dicts():
        pg.cur.execute(
            "SELECT string_agg(quote_literal(x),' | ')::tsquery FROM unnest(tsvector_to_array(to_tsvector('english',%s))) x",
            (q["input"],),
        )
        query = pg.cur.fetchone()[0]
        pg.cur.execute(
            "SELECT tsv @@ %s::tsquery FROM documents WHERE id=%s",
            (query, q["target_id"]),
        )
        pg_match = bool(pg.cur.fetchone()[0])
        duck_match = duck.execute(
            "SELECT fts_main_documents.match_bm25(?, ?, conjunctive:=false) IS NOT NULL",
            [q["target_id"], q["input"]],
        ).fetchone()[0]
        rows.append(
            {"case": q["case"], "pg_match": pg_match, "duckdb_match": duck_match}
        )
    pg.close()
    duck.close()
    pl.DataFrame(rows).write_parquet(OUT / "lexical-eligibility.parquet")
    counts = (
        pl.DataFrame(rows)
        .select(pl.col("pg_match", "duckdb_match").sum())
        .to_dicts()[0]
    )
    (OUT / "audit.json").write_text(
        json.dumps(
            {
                "native_dense_case_metrics_match_original": True,
                "duck_hybrid_case_metrics_match_original": True,
                "lexical_target_eligibility": counts,
            },
            indent=2,
        )
    )
    print(counts)


def replay_audit():
    """Every consumed list must have all six results; compare dense target hits."""
    actual = pl.read_parquet(OUT / "replay.parquet")
    source = pl.read_parquet(TRACES / "queries.parquet")
    keys = ["model", "case", "query_number"]
    assert actual.height == source.height * 6
    assert actual.unique(subset=[*keys, "engine", "method"]).height == actual.height
    assert (
        source.select(keys)
        .join(actual.select(keys).unique(), on=keys, how="anti")
        .is_empty()
    )
    dense = actual.filter(pl.col("method") == "dense")
    paired = dense.filter(pl.col("engine") == "pg").join(
        dense.filter(pl.col("engine") == "duckdb"), on=keys, suffix="_duck"
    )
    counts = {}
    for k in [8, 20]:
        mismatch = paired.filter(
            (pl.col("rank").le(k).fill_null(False))
            != (pl.col("rank_duck").le(k).fill_null(False))
        )
        counts[f"target_hit_disagreements_at_{k}"] = mismatch.height
    (OUT / "replay-audit.json").write_text(
        json.dumps({"lists": source.height, "rows": actual.height, **counts}, indent=2)
    )
    print(counts)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["static", "replay"])
    {"static": static_audit, "replay": replay_audit}[parser.parse_args().action]()
