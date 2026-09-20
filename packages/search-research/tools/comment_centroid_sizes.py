# /// script
# requires-python = ">=3.13"
# dependencies = ["numpy>=2.4", "torch>=2.8", "scikit-learn>=1.7"]
# [tool.uv.sources]
# torch = { index = "pytorch-cpu" }
# [[tool.uv.index]]
# name = "pytorch-cpu"
# url = "https://download.pytorch.org/whl/cpu"
# explicit = true
# ///
"""Compare centroid size and seeded member variability on fixed evaluation sets."""

import json
import math
from pathlib import Path

import numpy as np
from comment_linear_probe import embeddings, readonly

root = Path("data/comment-2025")
base = Path("data/probes/books-mlp-v1")
out = Path("data/probes/books-centroid-sizes-v1")
out.mkdir(exist_ok=False)
rows = [json.loads(l) for l in (base / "labels.jsonl").read_text().splitlines()]
splits = json.loads((base / "split_ids.json").read_text())
lookup = {r["comment_id"]: i for i, r in enumerate(rows)}
fit = np.array([lookup[c] for c in splits["fit"]])
val = np.array([lookup[c] for c in splits["validation"]])
test = np.array([lookup[c] for c in splits["test"]])
y = np.array([r["is_positive"] for r in rows])
positive = fit[y[fit]]
reps = embeddings(root, rows).numpy()
# Cosine scoring follows the explorer: best chunk per comment, not pooled score.
vectors = np.load(root / "vectors.npy", mmap_mode="r")


def chunks(records):
    allchunks = []
    starts = []
    with readonly(root / "index.sqlite") as db:
        for row in records:
            starts.append(len(allchunks))
            idx = [
                r[0]
                for r in db.execute(
                    "select vector_row from inputs where comment_id=? order by chunk",
                    (row["comment_id"],),
                )
            ]
            a = vectors[idx].astype(np.float32)
            a /= np.linalg.norm(a, axis=1, keepdims=True)
            allchunks.extend(a)
    return np.array(allchunks), np.array(starts)


a, starts = chunks(rows)
wild = {}
for name in ["original", "new1", "new2"]:
    r = [
        json.loads(l)
        for l in Path("data/probes/books-xgb-sweep-v1", name + "-fixture.jsonl")
        .read_text()
        .splitlines()
    ]
    wild[name] = chunks(r)


def stats(ids, s, t):
    pred = s[ids] >= t
    truth = y[ids]
    tp = int((pred & truth).sum())
    tn = int((~pred & ~truth).sum())
    fp = int((pred & ~truth).sum())
    fn = int((~pred & truth).sum())
    return {
        "recall": tp / (tp + fn),
        "negative_rejection": tn / (tn + fp),
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
    }


results = []
for size in [5, 15, 50, 150, len(positive)]:
    for seed in [42] if size == len(positive) else [42, 43, 44, 45, 46]:
        chosen = np.random.default_rng(seed).choice(positive, size=size, replace=False)
        q = reps[chosen].mean(0)
        q /= np.linalg.norm(q)
        scores = np.maximum.reduceat(a @ q, starts)
        ws = {k: np.maximum.reduceat(v @ q, st) for k, (v, st) in wild.items()}
        pos = sorted(scores[val[y[val]]], reverse=True)
        ops = {}
        for target in [0.99, 0.95, 0.9]:
            threshold = float(pos[math.ceil(len(pos) * target) - 1])
            ops[str(target)] = {
                "threshold": threshold,
                "validation": stats(val, scores, threshold),
                "test": stats(test, scores, threshold),
                "wild_passed": {k: int((v >= threshold).sum()) for k, v in ws.items()},
            }
        results.append(
            {
                "size": size,
                "seed": seed,
                "positive_ids": [rows[i]["comment_id"] for i in chosen],
                "ops": ops,
            }
        )
        print(size, seed, json.dumps(ops), flush=True)
(out / "metrics.json").write_text(
    json.dumps(
        {
            "positive_fit_available": len(positive),
            "selection": "centroid members from fit positives only; thresholds from validation positives; test untouched",
            "results": results,
        },
        indent=2,
    )
)
