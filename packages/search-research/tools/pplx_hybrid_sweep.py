"""Cached BF16 weighted-RRF tuning; no server, DB, or paid inference required."""

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import polars as pl
from search_research.embedding_baseline import shortened
from search_research.hybrid_baseline import measurements
from search_research.sovereign_run import CORPUS, HASHES, QUESTIONS
from search_research.sovereign_score import load_native, top_ids

WEIGHTS = (0, 0.125, 0.25, 0.5, 1, 2, 4)


def fuse(dense, lexical, weight):
    """Zero weight is strictly dense; absent-leg candidates get no contribution."""
    if weight == 0:
        return dense
    scores = {sid: 1 / (60 + rank) for rank, sid in enumerate(dense, 1)}
    for rank, sid in enumerate(lexical, 1):
        scores[sid] = scores.get(sid, 0) + weight / (60 + rank)
    return sorted(scores, key=lambda sid: (-scores[sid], sid))


def run(root):
    """Freeze candidates once, verify native controls, then report paired changes.

    Metrics evaluate one known target per query. The two styles belonging to each
    target remain grouped in bootstrap resampling; intervals describe development
    set variability, not an independent confirmation after tuning.
    """
    for name, path in (("corpus", CORPUS), ("questions", QUESTIONS)):
        assert hashlib.sha256(path.read_bytes()).hexdigest() == HASHES[name]
    corpus, questions = pl.read_parquet(CORPUS), pl.read_parquet(QUESTIONS)
    lexical = pl.read_parquet(root / "lexical.parquet")
    assert lexical["case"].to_list() == questions["case"].to_list()
    docs = shortened(load_native(root, "documents", corpus.height), 1024)
    queries = shortened(load_native(root, "queries", questions.height), 1024)
    scores = queries @ docs.T
    ids = corpus["id"].to_numpy()
    rows, candidates = [], []
    for n, q in enumerate(questions.to_dicts()):
        dense = top_ids(scores[n], ids, 100)
        lex = lexical["ids"][n].to_list()
        assert len(lex) <= 100 and len(lex) == len(set(lex))
        candidates.append({"case": q["case"], "dense": dense, "lexical": lex})
        for label, ranking in [(str(w), fuse(dense, lex, w)) for w in WEIGHTS] + [
            ("lexical", lex)
        ]:
            rank = (
                ranking.index(q["target_id"]) + 1 if q["target_id"] in ranking else None
            )
            rows.append(measurements(q, label, 1024, rank))
    frame = pl.DataFrame(rows)
    metrics = [c for c in frame.columns if "@" in c]
    original = pl.read_parquet(root / "ranks.parquet").filter(
        pl.col("model") == "pplx-embed-v1-0.6b"
    )
    for label, method in (("0", "dense"), ("0.5", "hybrid"), ("lexical", "bm25")):
        assert (
            frame.filter(pl.col("method") == label)
            .sort("case")
            .select(metrics)
            .equals(
                original.filter(pl.col("method") == method).sort("case").select(metrics)
            )
        ), f"Native control changed: {method}"
    output = root / "hybrid-weight-sweep"
    output.mkdir(exist_ok=True)
    frame.write_parquet(output / "ranks.parquet")
    pl.DataFrame(candidates).write_parquet(output / "candidates.parquet")
    summary = frame.group_by("method", maintain_order=True).agg(
        pl.len().alias("questions"), pl.col(metrics).mean()
    )
    summary.write_csv(output / "summary.csv")
    for stratum in ("style", "cohort"):
        frame.group_by("method", stratum, maintain_order=True).agg(
            pl.len().alias("questions"), pl.col(metrics).mean()
        ).write_csv(output / f"by-{stratum}.csv")
    paired = []
    for baseline in ("0", "0.5"):
        base = frame.filter(pl.col("method") == baseline)
        for label in [str(w) for w in WEIGHTS] + ["lexical"]:
            joint = frame.filter(pl.col("method") == label).join(
                base, on="case", suffix="_base"
            )
            grouped = (
                joint.group_by("target_id")
                .agg(
                    [(pl.col(m) - pl.col(m + "_base")).mean().alias(m) for m in metrics]
                )
                .sort("target_id")
            )
            rng = np.random.default_rng(20260906)
            samples = rng.integers(0, grouped.height, size=(5000, grouped.height))
            for metric in ("recall@8", "ndcg@8", "recall@20", "ndcg@20"):
                delta = joint[metric] - joint[metric + "_base"]
                draws = grouped[metric].to_numpy()[samples].mean(axis=1)
                lo, hi = np.quantile(draws, [0.025, 0.975])
                paired.append(
                    {
                        "baseline": baseline,
                        "method": label,
                        "metric": metric,
                        "gains": int((delta > 0).sum()),
                        "losses": int((delta < 0).sum()),
                        "delta": delta.mean(),
                        "bootstrap_lo": lo,
                        "bootstrap_hi": hi,
                    }
                )
    pl.DataFrame(paired).write_csv(output / "paired.csv")
    (output / "recipe.json").write_text(
        json.dumps(
            {
                "source_manifest": json.loads((root / "manifest.json").read_text()),
                "hashes": HASHES,
                "lexical_sha256": hashlib.sha256(
                    (root / "lexical.parquet").read_bytes()
                ).hexdigest(),
                "dimensions": 1024,
                "dense_weight": 1,
                "lexical_weights": WEIGHTS,
                "rrf_k": 60,
                "candidate_depth": 100,
                "native_controls_verified": True,
                "bootstrap": {
                    "unit": "target_id",
                    "repetitions": 5000,
                    "seed": 20260906,
                },
            },
            indent=2,
        )
    )
    print(summary)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path("data/pplx-vllm-gate-20260905/bf16-full")
    )
    run(parser.parse_args().root)
