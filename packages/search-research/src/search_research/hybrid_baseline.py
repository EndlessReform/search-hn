"""Frozen BM25 + cosine baseline: top-100 lists, equal-weight RRF with k=60."""

import argparse
import hashlib
import html
import json
from pathlib import Path

import duckdb
import numpy as np
import polars as pl

from search_research.embedding_api import batches
from search_research.embedding_baseline import exact_ranks, shortened


def fuse(left, right, k=60):
    """Union candidates; missing-list contributions are zero, ties use story ID."""
    scores = {}
    for ranking in (left, right):
        assert len(ranking) == len(set(ranking)), "Duplicate candidate in ranking"
        for rank, story_id in enumerate(ranking, 1):
            scores[story_id] = scores.get(story_id, 0.0) + 1 / (k + rank)
    return sorted(scores, key=lambda story_id: (-scores[story_id], story_id))


def measurements(question, method, dim, rank):
    row = {
        "case": question["case"],
        "target_id": question["target_id"],
        "style": question["style"],
        "cohort": question["cohort"],
        "method": method,
        "dimensions": dim,
        "rank": rank,
    }
    for k in (1, 5, 8, 10, 20):
        hit = rank is not None and rank <= k
        row[f"recall@{k}"] = float(hit)
        row[f"ndcg@{k}"] = float(1 / np.log2(rank + 1)) if hit else 0.0
    return row


def render(root, summary):
    """Two adjacent result tables, stacking on narrow screens; no remote assets."""

    def table(method, title):
        body = "".join(
            f"<tr><td>{r['dimensions']:,}</td><td>{r['recall@8']:.1%}</td>"
            f"<td>{r['ndcg@8']:.3f}</td><td>{r['recall@20']:.1%}</td></tr>"
            for r in summary
            if r["method"] == method
        )
        return f"<section><h2>{html.escape(title)}</h2><table><thead><tr><th>Dimensions</th><th>Recall@8</th><th>NDCG@8</th><th>Recall@20</th></tr></thead><tbody>{body}</tbody></table></section>"

    bm25 = next(r for r in summary if r["method"] == "bm25")
    content = """<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Cosine vs BM25 hybrid</title><style>body{font:16px system-ui;margin:32px;color:#17232b;background:#f7f7f2;max-width:1300px}h1{font-size:26px}h2{font-size:19px}.tables{display:grid;grid-template-columns:1fr 1fr;gap:28px}table{width:100%;border-collapse:collapse;background:white}th,td{text-align:right;padding:12px;border-bottom:1px solid #ddd}th:first-child,td:first-child{text-align:left}p{line-height:1.5} @media(max-width:850px){.tables{grid-template-columns:1fr}}</style>
<h1>Same stories. Same questions. One extra retrieval signal.</h1>
<p>105,081 title+URL documents · 196 frozen questions · exact cosine search · no new API calls.</p><div class="tables">"""
    content += table("cosine", "Just use vectors + cosine sim bruh")
    content += table("hybrid", "BM25 + vectors: simple rank fusion") + "</div>"
    content += f"<p>BM25 alone: Recall@8 <b>{bm25['recall@8']:.1%}</b> · NDCG@8 <b>{bm25['ndcg@8']:.3f}</b> · Recall@20 <b>{bm25['recall@20']:.1%}</b>.</p>"
    content += "<p>Hybrid: equal-weight reciprocal-rank fusion, k=60, top 100 candidates from each retriever. DuckDB BM25: Porter stemming, English stopwords, digits retained, any-term matching, k₁=1.2, b=0.75. Settings fixed before results, not tuned.</p><p>Single-shot known-item retrieval, not agent pass@k. Other relevant stories remain unjudged. Vector inputs and question wording are unchanged.</p></html>"
    (root / "comparison.html").write_text(content)


def run(root):
    manifest = json.loads((root / "manifest.json").read_text())
    assert (
        hashlib.sha256((root / "corpus.parquet").read_bytes()).hexdigest()
        == manifest["corpus_sha256"]
    )
    corpus, questions = (
        pl.read_parquet(root / "corpus.parquet"),
        pl.read_parquet(root / "questions.parquet"),
    )
    ids = corpus["id"].to_numpy()
    positions = {value: i for i, value in enumerate(ids)}
    targets = np.array([positions[v] for v in questions["target_id"]])
    con = duckdb.connect(str(root / "bm25.duckdb"))
    con.execute("SET memory_limit='8GB'; SET threads=4; INSTALL fts; LOAD fts;")
    con.execute(
        "CREATE OR REPLACE TABLE documents AS SELECT id,input FROM read_parquet(?)",
        [str(root / "corpus.parquet")],
    )
    con.execute(
        "PRAGMA create_fts_index('documents','id','input',stemmer='porter',stopwords='english',ignore='[^a-z0-9]+',strip_accents=1,lower=1,overwrite=1)"
    )
    config = {
        "duckdb": duckdb.__version__,
        "corpus_sha256": manifest["corpus_sha256"],
        "questions_sha256": hashlib.sha256(
            (root / "questions.parquet").read_bytes()
        ).hexdigest(),
        "stemmer": "porter",
        "stopwords": "english",
        "ignore": "[^a-z0-9]+",
        "conjunctive": False,
        "bm25_k": 1.2,
        "bm25_b": 0.75,
        "candidate_depth": 100,
        "rrf_k": 60,
        "weights": [1, 1],
    }
    (root / "hybrid-config.json").write_text(json.dumps(config, indent=2))
    rows, neighbors, lexical = [], [], []
    question_rows = questions.to_dicts()
    for i, q in enumerate(question_rows):
        result = con.execute(
            "SELECT id,fts_main_documents.match_bm25(id,?,k:=1.2,b:=0.75,conjunctive:=false) AS score FROM documents WHERE score IS NOT NULL ORDER BY score DESC,id LIMIT 100",
            [q["input"]],
        ).fetchall()
        ranking = [r[0] for r in result]
        lexical.append(ranking)
        rank = ranking.index(q["target_id"]) + 1 if q["target_id"] in ranking else None
        rows.append(measurements(q, "bm25", 0, rank))
        neighbors.append(
            {"case": q["case"], "method": "bm25", "dimensions": 0, "ids": ranking}
        )
        if (i + 1) % 20 == 0:
            print(
                json.dumps({"bm25_questions": i + 1, "total": len(question_rows)}),
                flush=True,
            )
    con.close()

    def load(kind, frame):
        return np.concatenate(
            [
                np.load(root / kind / f"{a:07d}-{b:07d}.npy", mmap_mode="r")
                for a, b in batches(frame)
            ]
        )

    documents, queries = load("documents", corpus), load("queries", questions)
    original = pl.read_parquet(root / "ranks.parquet")
    for dim in (256, 512, 768, 1024, 1536, 3072):
        scores = shortened(queries, dim) @ shortened(documents, dim).T
        ranks = exact_ranks(scores, targets)
        expected = dict(
            original.filter(pl.col("dimensions") == dim)
            .select("case", "rank")
            .iter_rows()
        )
        for i, q in enumerate(question_rows):
            assert int(ranks[i]) == expected[q["case"]], "Cosine baseline changed"
            # Stable exact top-100 even when duplicates tie at the cutoff.
            threshold = np.partition(scores[i], -100)[-100]
            above = np.flatnonzero(scores[i] > threshold)
            tied = np.flatnonzero(scores[i] == threshold)[: 100 - len(above)]
            best = np.concatenate((above, tied))
            best = best[np.lexsort((ids[best], -scores[i, best]))]
            dense = ids[best].tolist()
            hybrid = fuse(dense, lexical[i])
            rank = (
                hybrid.index(q["target_id"]) + 1 if q["target_id"] in hybrid else None
            )
            rows.extend(
                [
                    measurements(q, "cosine", dim, int(ranks[i])),
                    measurements(q, "hybrid", dim, rank),
                ]
            )
            neighbors.append(
                {
                    "case": q["case"],
                    "method": "hybrid",
                    "dimensions": dim,
                    "ids": hybrid,
                }
            )
        del scores
        print(json.dumps({"dimension_complete": dim}), flush=True)
    frame = pl.DataFrame(rows)
    frame.write_parquet(root / "hybrid-ranks.parquet")
    pl.DataFrame(neighbors).write_parquet(root / "hybrid-neighbors.parquet")
    metrics = [c for c in frame.columns if "@" in c]
    summary = (
        frame.group_by("method", "dimensions")
        .agg(pl.len().alias("questions"), pl.col(metrics).mean())
        .sort("method", "dimensions")
    )
    summary.write_csv(root / "hybrid-summary.csv")
    (root / "hybrid-summary.json").write_text(json.dumps(summary.to_dicts(), indent=2))
    frame.group_by("method", "dimensions", "style", "cohort").agg(
        pl.len().alias("n"), pl.col(metrics).mean()
    ).write_csv(root / "hybrid-strata.csv")
    render(root, summary.to_dicts())
    print(summary.select("method", "dimensions", "recall@8", "ndcg@8"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path("data/te3-large-baseline-20260904")
    )
    run(parser.parse_args().root)
