"""Quick exact-search dimension sweep, deliberately not a new agent benchmark."""

import argparse
import asyncio
import fcntl
import json
import time
from pathlib import Path

import numpy as np
import polars as pl

from search_research.embedding_api import batches, embed
from search_research.embedding_data import prepare


def shortened(matrix, dimensions):
    """Matryoshka prefix plus L2 normalization; no additional API calls."""
    result = np.array(matrix[:, :dimensions], dtype=np.float32, copy=True)
    norms = np.linalg.norm(result, axis=1, keepdims=True)
    assert np.all(norms > 0), "Zero embedding"
    result /= norms
    return result


def exact_ranks(scores, target_positions):
    """Full-corpus rank, breaking equal-score ties by ascending corpus row/id."""
    truth = scores[np.arange(len(target_positions)), target_positions, None]
    return (
        1
        + (scores > truth).sum(axis=1)
        + (
            (scores == truth)
            & (np.arange(scores.shape[1])[None, :] < target_positions[:, None])
        ).sum(axis=1)
    )


def evaluate(root, corpus, questions, dimensions):
    def load(kind, frame):
        chunks = [
            np.load(root / kind / f"{a:07d}-{b:07d}.npy", mmap_mode="r")
            for a, b in batches(frame)
        ]
        return np.concatenate(chunks)

    documents, queries = load("documents", corpus), load("queries", questions)
    ids = corpus["id"].to_numpy()
    positions = {value: i for i, value in enumerate(ids)}
    targets = np.array([positions[value] for value in questions["target_id"]])
    rows, neighbors, summaries = [], [], []
    for dim in dimensions:
        started = time.perf_counter()
        doc = shortened(documents, dim)
        query = shortened(queries, dim)
        scores = query @ doc.T
        ranks = exact_ranks(scores, targets)
        elapsed = time.perf_counter() - started
        for i, question in enumerate(questions.iter_rows(named=True)):
            row = {k: v for k, v in question.items() if k not in ("tokens",)}
            row.update(dimensions=dim, rank=int(ranks[i]))
            for k in (1, 5, 8, 10, 20):
                row[f"recall@{k}"] = float(ranks[i] <= k)
                row[f"ndcg@{k}"] = (
                    float(1 / np.log2(ranks[i] + 1)) if ranks[i] <= k else 0.0
                )
            rows.append(row)
            best = np.argpartition(-scores[i], 20)[:20]
            best = best[np.lexsort((ids[best], -scores[i, best]))]
            neighbors.append(
                {
                    "case": question["case"],
                    "dimensions": dim,
                    "ids": ids[best].tolist(),
                    "scores": scores[i, best].tolist(),
                }
            )
        frame = pl.DataFrame(rows).filter(pl.col("dimensions") == dim)
        metric_cols = [c for c in frame.columns if "@" in c]
        summary = {
            "dimensions": dim,
            "questions": questions.height,
            "vectors_mib": doc.nbytes / 2**20,
            "batch_search_seconds": elapsed,
            **frame.select(pl.col(metric_cols).mean()).to_dicts()[0],
        }
        summary["either_variant_recall@8"] = (
            frame.group_by("target_id")
            .agg(pl.col("recall@8").max())
            .select(pl.col("recall@8").mean())
            .item()
        )
        summaries.append(summary)
        print(json.dumps(summary), flush=True)
        del doc, query, scores
    pl.DataFrame(rows).write_parquet(root / "ranks.parquet")
    pl.DataFrame(neighbors).write_parquet(root / "neighbors.parquet")
    pl.DataFrame(summaries).write_csv(root / "dimensions.csv")
    (root / "dimensions.json").write_text(json.dumps(summaries, indent=2))
    pl.DataFrame(rows).group_by("dimensions", "style", "cohort").agg(
        pl.len().alias("n"), pl.col("recall@8").mean(), pl.col("ndcg@8").mean()
    ).write_csv(root / "strata.csv")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path("data/te3-large-baseline-20260904")
    )
    parser.add_argument(
        "--eval-set",
        type=Path,
        default=Path("data/fts-baseline-20260904/plain-results/eval.jsonl"),
    )
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument(
        "--dimensions", type=int, nargs="+", default=[256, 512, 768, 1024, 1536, 3072]
    )
    args = parser.parse_args()
    assert all(0 < d <= 3072 for d in args.dimensions)
    args.root.mkdir(parents=True, exist_ok=True)
    with (args.root / "driver.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        corpus, questions = prepare(args.root, args.eval_set)
        if not args.prepare_only:
            asyncio.run(embed(args.root, corpus, questions))
            evaluate(args.root, corpus, questions, args.dimensions)


if __name__ == "__main__":
    main()
