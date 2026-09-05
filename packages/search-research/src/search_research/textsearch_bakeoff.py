"""Bounded pg_textsearch experiment: combined, title-only, and 2x title scores.

No new embeddings or ANN tuning. Reuse the original dense top-100 lists for
hybrid scoring. The boosted variant scores the whole frozen corpus, so it is
not accidentally limited to a union of shallow per-field candidate lists.
"""

import argparse
import json
import time
from contextlib import closing

import polars as pl
import psycopg2
from psycopg2.extras import execute_values
from search_agent.journal import Journal

from search_research.dataset import read_jsonl
from search_research.engine_data import BASE, DSN, OUT
from search_research.fusion_sweep import weighted_fuse
from search_research.hybrid_baseline import measurements


def setup():
    """Create three ordinary text indexes only in the disposable local DB."""
    corpus = pl.read_parquet(BASE / "corpus.parquet")
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_textsearch")
        cur.execute("SELECT extversion FROM pg_extension WHERE extname='pg_textsearch'")
        assert cur.fetchone()[0] == "1.4.0"
        cur.execute(
            "CREATE TABLE IF NOT EXISTS textsearch_docs(id bigint PRIMARY KEY,title text NOT NULL,url text NOT NULL,input text NOT NULL)"
        )
        cur.execute("SELECT count(*) FROM textsearch_docs")
        count = cur.fetchone()[0]
        assert count in (0, corpus.height)
        if not count:
            with conn:
                execute_values(
                    cur,
                    "INSERT INTO textsearch_docs(id,title,url,input) VALUES %s",
                    [
                        (r["id"], r["title"] or "", r["url"] or "", r["input"])
                        for r in corpus.to_dicts()
                    ],
                    page_size=1000,
                )
        builds = []
        for field in ["input", "title", "url"]:
            start = time.perf_counter()
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS textsearch_{field}_idx ON textsearch_docs USING bm25({field}) WITH (text_config='english',k1=1.2,b=0.75)"
            )
            cur.execute("SELECT pg_relation_size(%s)", (f"textsearch_{field}_idx",))
            builds.append(
                {
                    "field": field,
                    "seconds": time.perf_counter() - start,
                    "bytes": cur.fetchone()[0],
                }
            )
            print(json.dumps(builds[-1]), flush=True)
        cur.execute("ANALYZE textsearch_docs")
    conn.close()
    (OUT / "textsearch-config.json").write_text(
        json.dumps(
            {
                "version": "1.4.0",
                "text_config": "english",
                "k1": 1.2,
                "b": 0.75,
                "variants": ["combined", "title-only", "title2-url1"],
                "boost_definition": "2*BM25(title)+BM25(url), independently normalized field indexes",
                "builds": builds,
                "dense": "cached exact rankings; unchanged",
                "fusion_weights": [1, 0.5],
            },
            indent=2,
        )
    )


def retrieve(cur, query, variant):
    """Negative BM25 scores sort ascending; zero-score nonmatches are excluded."""
    if variant in ("combined", "title-only"):
        field = "input" if variant == "combined" else "title"
        cur.execute(
            f"""SELECT id,score FROM (SELECT id,{field} <@> to_bm25query(%s,%s) score
            FROM textsearch_docs ORDER BY {field} <@> to_bm25query(%s,%s) LIMIT 100) candidates
            WHERE score<0 ORDER BY score,id""",
            (query, f"textsearch_{field}_idx", query, f"textsearch_{field}_idx"),
        )
    else:
        assert variant == "title2-url1"
        cur.execute(
            """WITH scores AS MATERIALIZED (
            SELECT id,2*(title <@> to_bm25query(%s,'textsearch_title_idx'))+
                (url <@> to_bm25query(%s,'textsearch_url_idx')) score FROM textsearch_docs)
            SELECT id,score FROM scores WHERE score<0 ORDER BY score,id LIMIT 100""",
            (query, query),
        )
    return cur.fetchall()


def run():
    questions = pl.read_parquet(BASE / "questions.parquet").to_dicts()
    dense = {
        (r["case"], r["dimensions"]): r["ids"]
        for r in pl.read_parquet(OUT / "comparison.parquet")
        .filter((pl.col("engine") == "pg") & (pl.col("method") == "dense"))
        .to_dicts()
    }
    path = OUT / "textsearch.jsonl"
    previous = read_jsonl(path) if path.exists() else []
    done = {
        (r["case"], r["method"], r["dimensions"], r["variant"], r["lexical_weight"])
        for r in previous
    }
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    with conn.cursor() as cur, closing(Journal(path)) as journal:
        cur.execute("SET statement_timeout='55s'")
        for variant in ["combined", "title-only", "title2-url1"]:
            for i, q in enumerate(questions):
                if (q["case"], "hybrid", 3072, variant, 0.5) in done:
                    continue
                start = time.perf_counter()
                results = retrieve(cur, q["input"], variant)
                ms = (time.perf_counter() - start) * 1000
                ids = [r[0] for r in results]
                rank = ids.index(q["target_id"]) + 1 if q["target_id"] in ids else None
                journal.write(
                    "result",
                    variant=variant,
                    lexical_weight=1.0,
                    ids=ids,
                    milliseconds=ms,
                    **measurements(q, "lexical", 0, rank),
                )
                for dim in [1536, 3072]:
                    for weight in [1.0, 0.5]:
                        hybrid = weighted_fuse(dense[q["case"], dim], ids, weight)
                        rank = (
                            hybrid.index(q["target_id"]) + 1
                            if q["target_id"] in hybrid
                            else None
                        )
                        journal.write(
                            "result",
                            variant=variant,
                            lexical_weight=weight,
                            ids=hybrid,
                            milliseconds=ms,
                            **measurements(q, "hybrid", dim, rank),
                        )
                if (i + 1) % 20 == 0:
                    print(
                        json.dumps({"variant": variant, "questions": i + 1}), flush=True
                    )
            frame = pl.DataFrame(read_jsonl(path))
            frame.write_parquet(OUT / "textsearch.parquet")
            frame.group_by("variant", "method", "dimensions", "lexical_weight").agg(
                pl.len().alias("n"),
                pl.col("recall@8", "ndcg@8", "recall@20").mean(),
                pl.col("milliseconds").median().alias("lexical_median_ms"),
            ).sort("method", "dimensions", "variant", "lexical_weight").write_csv(
                OUT / "textsearch-summary.csv"
            )
        cur.execute(
            "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT id FROM textsearch_docs ORDER BY input <@> to_bm25query(%s,'textsearch_input_idx') LIMIT 100",
            (questions[0]["input"],),
        )
        (OUT / "textsearch-plan.json").write_text(
            json.dumps(cur.fetchone()[0], indent=2)
        )
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["setup", "run"])
    {"setup": setup, "run": run}[parser.parse_args().action]()
