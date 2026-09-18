"""Exploratory built-in ranker sweep on the frozen questions (not held out)."""

import json
from contextlib import closing

import polars as pl
from search_agent.journal import Journal

from search_research.engine_backends import Postgres, timed
from search_research.engine_data import BASE, OUT
from search_research.hybrid_baseline import measurements


def run():
    pg = Postgres()
    questions = pl.read_parquet(BASE / "questions.parquet").to_dicts()
    rows = []
    with closing(Journal(OUT / "pg-lexical-sweep.jsonl")) as journal:
        for function in ["ts_rank", "ts_rank_cd"]:
            for normalization in [0, 1, 2, 8, 16, 10]:
                for q in questions:
                    pg.cur.execute(
                        "SELECT string_agg(quote_literal(x),' | ')::tsquery FROM unnest(tsvector_to_array(to_tsvector('english',%s))) x",
                        (q["input"],),
                    )
                    query = pg.cur.fetchone()[0]

                    def search(
                        function=function, normalization=normalization, query=query
                    ):
                        pg.cur.execute(
                            f"SELECT id FROM documents WHERE tsv @@ %s::tsquery ORDER BY {function}(tsv,%s::tsquery,{normalization}) DESC,id LIMIT 100",
                            (query, query),
                        )
                        return [r[0] for r in pg.cur.fetchall()]

                    ids, ms = timed(search)
                    rank = (
                        ids.index(q["target_id"]) + 1 if q["target_id"] in ids else None
                    )
                    row = {
                        **measurements(q, function, normalization, rank),
                        "ids": ids,
                        "milliseconds": ms,
                    }
                    rows.append(row)
                    journal.write("result", **row)
                print(
                    json.dumps({"ranker": function, "normalization": normalization}),
                    flush=True,
                )
    pg.close()
    frame = pl.DataFrame(rows)
    frame.write_parquet(OUT / "pg-lexical-sweep.parquet")
    frame.group_by("method", "dimensions").agg(
        pl.col("recall@8", "ndcg@8", "recall@20").mean()
    ).sort("ndcg@8", descending=True).write_csv(OUT / "pg-lexical-sweep.csv")


if __name__ == "__main__":
    run()
