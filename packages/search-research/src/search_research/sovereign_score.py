"""Score native Perplexity against the second Luna run's cached TE3 control.

Exact cosine runs locally; lexical candidates come from the unchanged scratch PG
title-only BM25 index. The cached TE3 per-question metrics must reproduce before
new-model scores are published. No OpenAI requests or agent trajectories occur.
"""

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import polars as pl
import psycopg2

from search_research.embedding_baseline import exact_ranks, shortened
from search_research.engine_data import BASE, DSN, load_vectors
from search_research.hybrid_baseline import measurements
from search_research.sovereign_run import CORPUS, HASHES, QUESTIONS, ROOT
from search_research.tei_embeddings import EmbeddingRecipe


def top_ids(scores, ids, limit=100):
    """Stable top-k even when duplicate documents tie across the cutoff."""
    assert len(scores) == len(ids) and 0 < limit <= len(ids)
    threshold = np.partition(scores, -limit)[-limit]
    above = np.flatnonzero(scores > threshold)
    tied = np.flatnonzero(scores == threshold)
    tied = tied[np.argsort(ids[tied])][: limit - len(above)]
    selected = np.concatenate((above, tied))
    selected = selected[np.lexsort((ids[selected], -scores[selected]))]
    return ids[selected].tolist()


def hybrid(dense, lexical):
    """The second Luna run's fixed RRF: dense 1, lexical .5, k=60."""
    scores = {i: 1 / (60 + r) for r, i in enumerate(dense, 1)}
    for r, i in enumerate(lexical, 1):
        scores[i] = scores.get(i, 0) + 0.5 / (60 + r)
    return sorted(scores, key=lambda i: (-scores[i], i))


def lexical_lists(root, corpus, questions):
    """Validate the frozen scratch table and retrieve the same lexical candidates."""
    rows = []
    with psycopg2.connect(DSN) as conn:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout='30s'")
            cur.execute(
                "SELECT id,title,url,score,day FROM semantic_stories ORDER BY id"
            )
            assert (
                cur.fetchall()
                == corpus.select("id", "title", "url", "score", "day").rows()
            ), "Scratch snapshot changed"
            cur.execute("SELECT extname,extversion FROM pg_extension ORDER BY extname")
            (root / "pg-extensions.json").write_text(
                json.dumps(cur.fetchall(), indent=2)
            )
            for q in questions.to_dicts():
                cur.execute(
                    "SELECT id FROM semantic_stories WHERE score>=25 "
                    "AND title <@> to_bm25query(%s,'semantic_title_bm25') < 0 "
                    "ORDER BY title <@> to_bm25query(%s,'semantic_title_bm25'),id LIMIT 100",
                    (q["input"].strip(), q["input"].strip()),
                )
                rows.append({"case": q["case"], "ids": [r[0] for r in cur.fetchall()]})
    pl.DataFrame(rows).write_parquet(root / "lexical.parquet")
    return [r["ids"] for r in rows]


def load_native(root, kind, n):
    """Reject missing, overlapping, malformed or unexpected shards."""
    size = json.loads((root / "backfill-config.json").read_text())["batch_size"]
    paths = [
        root / kind / f"{a:07d}-{min(a + size, n):07d}.npy" for a in range(0, n, size)
    ]
    assert set(paths) == set((root / kind).glob("*.npy")), "Shard inventory differs"
    recipe = EmbeddingRecipe.model_validate(
        json.loads((root / "manifest.json").read_text())["recipe"]
    )
    shards = []
    for start, path in zip(range(0, n, size), paths, strict=True):
        shard = np.load(path, allow_pickle=False)
        assert shard.shape == (min(size, n - start), recipe.dimensions), (
            f"Shard row range differs: {path}"
        )
        assert shard.dtype == recipe.numpy_dtype, f"Shard dtype differs: {path}"
        shards.append(shard)
    result = np.concatenate(shards)
    assert result.shape == (n, recipe.dimensions) and result.dtype == recipe.numpy_dtype
    assert np.isfinite(result).all() and np.any(result != 0, axis=1).all()
    return result


def score(root: Path, baseline_only: bool = False, dimension_sweep: bool = False):
    """Compare cached prefixes; sweep outputs never overwrite the native run.

    A sweep reuses the original lexical candidates and verifies both native
    anchors case by case. Equal dimensions compare storage/quality, not model
    training budgets or inference cost. Both sides are renormalized after slicing.
    """
    assert not (baseline_only and dimension_sweep)
    output = root / "dimension-sweep" if dimension_sweep else root
    output.mkdir(parents=True, exist_ok=True)
    for name, path in (("corpus", CORPUS), ("questions", QUESTIONS)):
        assert hashlib.sha256(path.read_bytes()).hexdigest() == HASHES[name], name
    corpus, questions = pl.read_parquet(CORPUS), pl.read_parquet(QUESTIONS)
    recipe = EmbeddingRecipe.model_validate(
        json.loads((root / "manifest.json").read_text())["recipe"]
    )
    model_name = recipe.model_id.split("/")[-1]
    if dimension_sweep or root != ROOT:
        cached = pl.read_parquet(
            (root if dimension_sweep else ROOT) / "lexical.parquet"
        )
        assert cached["case"].to_list() == questions["case"].to_list()
        lexical = cached["ids"].to_list()
        if not dimension_sweep:
            cached.write_parquet(root / "lexical.parquet")
    else:
        lexical = lexical_lists(root, corpus, questions)
    ids = corpus["id"].to_numpy()
    positions = {i: n for n, i in enumerate(ids)}
    targets = np.array([positions[i] for i in questions["target_id"]])
    te3_docs = np.load(CORPUS.parent / "vectors.npy", mmap_mode="r")
    te3_queries = load_vectors(BASE, "queries", questions)
    archived = pl.read_parquet(CORPUS.parent / "static.parquet")
    rows, neighbors = [], []
    models = [("te3-large", 1536)]
    if not baseline_only:
        models.append((model_name, recipe.dimensions))
    if dimension_sweep:
        models += [
            (model, dim)
            for model in ("te3-large", model_name)
            for dim in (256, 512, 768, 1024)
            if (model, dim) not in models
        ]
    native = (
        (
            load_native(root, "documents", corpus.height),
            load_native(root, "queries", questions.height),
        )
        if not baseline_only
        else None
    )
    for model, dim in models:
        docs, queries = (te3_docs, te3_queries) if model == "te3-large" else native
        scores = shortened(queries, dim) @ shortened(docs, dim).T
        ranks = exact_ranks(scores, targets)
        for n, q in enumerate(questions.to_dicts()):
            dense = top_ids(scores[n], ids)
            fused = hybrid(dense, lexical[n])
            for method, ranking in (
                ("dense", dense),
                ("hybrid", fused),
                ("bm25", lexical[n]),
            ):
                rank = (
                    int(ranks[n])
                    if method == "dense"
                    else (
                        ranking.index(q["target_id"]) + 1
                        if q["target_id"] in ranking
                        else None
                    )
                )
                row = {"model": model, **measurements(q, method, dim, rank)}
                if model == "te3-large" and dim == 1536 and method != "bm25":
                    old = archived.filter(
                        (pl.col("case") == q["case"]) & (pl.col("method") == method)
                    ).to_dicts()
                    assert len(old) == 1
                    for key in ("recall@8", "recall@20", "ndcg@8", "ndcg@20"):
                        assert abs(row[key] - old[0][key]) < 1e-12, (
                            q["case"],
                            method,
                            key,
                            row[key],
                            old[0][key],
                        )
                rows.append(row)
                neighbors.append(
                    {
                        "model": model,
                        "dimensions": dim,
                        "method": method,
                        "case": q["case"],
                        "ids": ranking[:20],
                    }
                )
    frame = pl.DataFrame(rows)
    if dimension_sweep:
        original = pl.read_parquet(root / "ranks.parquet")
        anchors = frame.filter(
            ((pl.col("model") == "te3-large") & (pl.col("dimensions") == 1536))
            | (
                (pl.col("model") == model_name)
                & (pl.col("dimensions") == recipe.dimensions)
            )
        )
        keys = ["model", "method", "dimensions", "case"]
        assert anchors.sort(keys).equals(original.sort(keys)), "Native anchors changed"
        (output / "recipe.json").write_text(
            json.dumps(
                {
                    "input_sha256": HASHES,
                    "model_dimensions": models,
                    "lexical_sha256": hashlib.sha256(
                        (root / "lexical.parquet").read_bytes()
                    ).hexdigest(),
                    "normalization": "prefix then L2, float32 exact cosine",
                    "native_anchors_verified": True,
                },
                indent=2,
            )
        )
    if baseline_only:
        frame.write_parquet(root / "verified-te3-ranks.parquet")
        print(
            "Verified all 196 archived TE3 cases for dense and hybrid, four metrics each."
        )
        return
    metrics = [c for c in frame.columns if "@" in c]
    frame.write_parquet(output / "ranks.parquet")
    pl.DataFrame(neighbors).write_parquet(output / "neighbors.parquet")
    summary = (
        frame.group_by("model", "method", "dimensions")
        .agg(pl.len().alias("questions"), pl.col(metrics).mean())
        .sort("model", "method", "dimensions")
    )
    summary.write_csv(output / "summary.csv")
    (output / "summary.json").write_text(json.dumps(summary.to_dicts(), indent=2))
    frame.group_by("model", "method", "dimensions", "style", "cohort").agg(
        pl.len().alias("questions"), pl.col(metrics).mean()
    ).write_csv(output / "strata.csv")
    print(
        summary.select(
            "model",
            "method",
            "dimensions",
            "recall@8",
            "ndcg@8",
            "recall@20",
            "ndcg@20",
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--baseline-only", action="store_true")
    parser.add_argument("--dimension-sweep", action="store_true")
    args = parser.parse_args()
    score(args.root, args.baseline_only, args.dimension_sweep)
