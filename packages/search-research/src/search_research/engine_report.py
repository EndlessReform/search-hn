"""Combine cached rankings into tuned comparisons and a small offline report."""

import html

import polars as pl

from search_research.engine_data import OUT
from search_research.hybrid_baseline import fuse, measurements


def run():
    static = pl.read_parquet(OUT / "static-summary.parquet")
    sweep = pl.read_parquet(OUT / "pg-lexical-sweep.parquet")
    selected = sweep.filter(
        (pl.col("method") == "ts_rank") & (pl.col("dimensions") == 1)
    )
    lexical = {r["case"]: r for r in selected.to_dicts()}
    tuned = []
    for row in selected.to_dicts():
        tuned.append(
            {**row, "engine": "pg-tuned", "method": "lexical", "dimensions": 0}
        )
    for row in static.filter(
        (pl.col("engine") == "pg") & (pl.col("method") == "dense")
    ).to_dicts():
        lex = lexical[row["case"]]
        ids = fuse(row["ids"], lex["ids"])
        rank = ids.index(row["target_id"]) + 1 if row["target_id"] in ids else None
        tuned.append(
            {
                **measurements(row, "hybrid", row["dimensions"], rank),
                "engine": "pg-tuned",
                "milliseconds": row["milliseconds"] + lex["milliseconds"],
                "ids": ids,
            }
        )
    frame = pl.concat([static, pl.DataFrame(tuned)], how="diagonal_relaxed")
    frame.write_parquet(OUT / "comparison.parquet")
    metrics = [c for c in frame.columns if "@" in c]
    summary = (
        frame.group_by("engine", "method", "dimensions")
        .agg(pl.len().alias("n"), pl.col(metrics).mean())
        .sort("method", "dimensions", "engine")
    )
    summary.write_csv(OUT / "comparison.csv")

    def table(data):
        cols = data.columns
        return (
            "<table><thead><tr>"
            + "".join(f"<th>{html.escape(c)}</th>" for c in cols)
            + "</tr></thead><tbody>"
            + "".join(
                "<tr>"
                + "".join(
                    f"<td>{v:.3f}</td>"
                    if isinstance(v, float)
                    else f"<td>{html.escape(str(v))}</td>"
                    for v in row
                )
                + "</tr>"
                for row in data.iter_rows()
            )
            + "</tbody></table>"
        )

    content = '<!doctype html><meta charset="utf-8"><title>PG vs DuckDB retrieval</title><style>body{font:15px system-ui;margin:32px;color:#17232b;background:#f7f7f2}table{border-collapse:collapse;background:white;margin-bottom:28px}td,th{padding:9px 14px;text-align:right;border-bottom:1px solid #ddd}h1{font-size:25px}p{max-width:1000px;line-height:1.5}</style><h1>PostgreSQL vs DuckDB: frozen retrieval bakeoff</h1><p>105,081 stories · 196 questions · existing text-embedding-3-large vectors · exact cosine · top-100 candidates · equal RRF k=60. pg-tuned uses English OR with ts_rank normalization=1, selected from a 12-setting exploratory sweep on these same questions.</p>'
    content += "<h2>Single question retrieval</h2>" + table(
        summary.filter(
            (pl.col("dimensions").is_in([0, 1536, 3072]))
            & (pl.col("engine") != "pg").or_(
                (pl.col("engine") == "pg")
                & (pl.col("method") == "dense")
                & pl.col("dimensions").is_in([1536, 3072])
            )
        ).select("engine", "method", "dimensions", "recall@8", "ndcg@8", "recall@20")
    )
    if (OUT / "textsearch-summary.csv").exists():
        content += (
            "<h2>pg_textsearch: bounded BM25 / field comparison</h2><p>Version 1.4.0, English, k1=1.2, b=0.75. Three fixed variants only. title2-url1 sums independently normalized BM25 field scores with title weight 2 and URL weight 1; it is not repeated text or BM25F. Dense rankings and HNSW are unchanged.</p>"
            + table(
                pl.read_csv(OUT / "textsearch-summary.csv").select(
                    "variant",
                    "method",
                    "dimensions",
                    "lexical_weight",
                    "n",
                    "recall@8",
                    "ndcg@8",
                    "recall@20",
                    "lexical_median_ms",
                )
            )
        )
    if (OUT / "replay-summary.csv").exists():
        replay = pl.read_csv(OUT / "replay-summary.csv")
        content += (
            "<h2>Trajectory search budgets</h2><p>Fixed recorded queries and filters; 1,536 dimensions. K counts individual search lists, including batch alternatives. Cutoff is results per search, not the search budget. All 196 cases per model remain in the denominator, including no-search cases.</p>"
            + table(
                replay.filter(
                    (pl.col("cutoff") == 8)
                    & (pl.col("search_budget").is_in([1, 3, 5, 10, 20]))
                ).select(
                    "model", "engine", "method", "search_budget", "hits", "n", "pass"
                )
            )
        )
        from search_research.replay_inspector import render

        content += render()
    if (OUT / "fusion-sweep.csv").exists():
        content += (
            "<h2>Exploratory fusion-weight sweep</h2><p>Dense weight 1; lexical weight varied using cached rankings. Same evaluation questions, not held-out tuning. Trajectory replay above retains equal weights.</p>"
            + table(pl.read_csv(OUT / "fusion-sweep.csv"))
        )
    if (OUT / "ann-summary.csv").exists():
        content += "<h2>PG HNSW at 1,536 dimensions</h2>" + table(
            pl.read_csv(OUT / "ann-summary.csv")
        )
    (OUT / "comparison.html").write_text(content)
    print(summary.select("engine", "method", "dimensions", "recall@8", "ndcg@8"))


if __name__ == "__main__":
    run()
