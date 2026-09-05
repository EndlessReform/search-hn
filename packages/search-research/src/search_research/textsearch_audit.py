"""Check whether URL scoring actually contributes to the field experiment."""

import json

import polars as pl
import psycopg2

from search_research.engine_data import BASE, DSN, OUT


def run():
    frame = pl.read_parquet(OUT / "textsearch.parquet")
    assert frame.height == 2940
    assert (
        frame.unique(
            subset=["case", "variant", "method", "dimensions", "lexical_weight"]
        ).height
        == 2940
    )
    conn = psycopg2.connect(DSN)
    rows = []
    with conn.cursor() as cur:
        for q in pl.read_parquet(BASE / "questions.parquet").to_dicts():
            cur.execute(
                "SELECT url <@> to_bm25query(%s,'textsearch_url_idx') score FROM textsearch_docs ORDER BY url <@> to_bm25query(%s,'textsearch_url_idx') LIMIT 1",
                (q["input"], q["input"]),
            )
            best = cur.fetchone()
            rows.append(
                {
                    "case": q["case"],
                    "best_url_score": best[0] if best else None,
                    "url_has_match": bool(best and best[0] < 0),
                }
            )
    conn.close()
    result = pl.DataFrame(rows)
    result.write_parquet(OUT / "textsearch-url-audit.parquet")
    summary = {
        "questions": result.height,
        "queries_matching_any_url": result["url_has_match"].sum(),
    }
    (OUT / "textsearch-audit.json").write_text(json.dumps(summary, indent=2))
    print(summary)


if __name__ == "__main__":
    run()
