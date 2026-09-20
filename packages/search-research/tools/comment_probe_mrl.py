# /// script
# requires-python = ">=3.13"
# dependencies = ["numpy>=2.4", "torch>=2.8", "scikit-learn>=1.7", "xgboost-cpu>=3.0"]
# [tool.uv.sources]
# torch = { index = "pytorch-cpu" }
# [[tool.uv.index]]
# name = "pytorch-cpu"
# url = "https://download.pytorch.org/whl/cpu"
# explicit = true
# ///
"""Converged L2 logistic regression on the fixed MLP experiment splits."""

import json
import warnings
from pathlib import Path

import numpy as np
import torch
from comment_linear_probe import embeddings, metrics, readonly
from comment_probe_mixes import at_threshold, operating_points
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier

warnings.simplefilter("error", ConvergenceWarning)
torch.set_num_threads(4)
root = Path("data/comment-2025")
base = Path("data/probes/books-mlp-v1")
out = Path("data/probes/books-mrl-v1")
out.mkdir(exist_ok=False)
rows = [json.loads(l) for l in (base / "labels.jsonl").read_text().splitlines()]
split = json.loads((base / "split_ids.json").read_text())
lookup = {r["comment_id"]: i for i, r in enumerate(rows)}
fit = np.array([lookup[c] for c in split["fit"]])
val = np.array([lookup[c] for c in split["validation"]])
test = np.array([lookup[c] for c in split["test"]])
x = embeddings(root, rows).numpy().astype("float64")
y = np.array([int(r["is_positive"]) for r in rows])
yt = torch.tensor(y, dtype=torch.float32)
wild = [
    {"comment_id": r["comment_id"], "text": r["text"]}
    for r in map(
        json.loads,
        Path("data/probes/books-wild-1000-v1/predictions.jsonl")
        .read_text()
        .splitlines(),
    )
]
wx = embeddings(root, wild).numpy().astype("float64")


def group(i):
    r = rows[i]
    if r["is_positive"]:
        return "positive"
    if 2 in r["sources"]:
        return "random"
    return "near" if r["rank"] <= 5500 else "deep"


shares = {"positive": 0.4, "random": 0.35, "near": 0.15, "deep": 0.1}
counts = {g: sum(group(i) == g for i in fit) for g in shares}
weighted = np.array([shares[group(i)] / counts[group(i)] * len(fit) for i in fit])


def prefix_vectors(records, dimensions):
    """Truncate native chunk vectors, normalize, then pool as in the baseline."""
    vectors = np.load(root / "vectors.npy", mmap_mode="r")
    values = []
    with readonly(root / "index.sqlite") as db:
        for row in records:
            indices = [
                r[0]
                for r in db.execute(
                    "SELECT vector_row FROM inputs WHERE comment_id=? ORDER BY chunk",
                    (row["comment_id"],),
                )
            ]
            chunks = vectors[indices, :dimensions].astype(np.float32)
            norms = np.linalg.norm(chunks, axis=1, keepdims=True)
            assert (norms > 0).all()
            mean = (chunks / norms).mean(0)
            assert np.linalg.norm(mean) > 0
            values.append(mean / np.linalg.norm(mean))
    return np.stack(values).astype("float64")


results = {}
models = {}
features = {}
for dim in [128, 256, 512, 1024]:
    dx = prefix_vectors(rows, dim)
    dw = prefix_vectors(wild, dim)
    features[dim] = (dx, dw)
    for c in [0.01, 0.1, 1, 10, 100]:
        name = f"prefix{dim}_C{c:g}"
        model = LogisticRegression(C=c, solver="lbfgs", max_iter=3000, tol=1e-8)
        model.fit(dx[fit], y[fit], sample_weight=weighted)
        logits = torch.tensor(model.decision_function(dx), dtype=torch.float32)
        thresholds = operating_points(yt[val], logits[val])
        results[name] = {
            "dimensions": dim,
            "C": c,
            "iterations": int(model.n_iter_[0]),
            "validation": metrics(yt[val], logits[val]),
            "thresholds": thresholds,
            "validation_ops": {
                k: at_threshold(yt[val], logits[val], t) for k, t in thresholds.items()
            },
        }
        models[name] = (model, logits, model.predict_proba(dw)[:, 1])
        torch.save(
            {
                "state_dict": {
                    "weight": torch.tensor(model.coef_, dtype=torch.float32),
                    "bias": torch.tensor(model.intercept_, dtype=torch.float32),
                },
                "dimensions": dim,
                "thresholds": thresholds,
            },
            out / (name + ".pt"),
        )
        print(name, flush=True)
# One deliberately plain tree baseline, not a tree hyperparameter sweep.
model = XGBClassifier(
    n_estimators=1000,
    max_depth=3,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    reg_lambda=1,
    objective="binary:logistic",
    eval_metric="logloss",
    tree_method="hist",
    n_jobs=4,
    random_state=42,
    early_stopping_rounds=50,
)
model.fit(
    x[fit], y[fit], sample_weight=weighted, eval_set=[(x[val], y[val])], verbose=False
)
logits = torch.tensor(model.predict(x, output_margin=True), dtype=torch.float32)
thresholds = operating_points(yt[val], logits[val])
name = "xgboost_1024"
results[name] = {
    "dimensions": 1024,
    "best_iteration": model.best_iteration,
    "parameters": {
        k: v
        for k, v in model.get_params().items()
        if isinstance(v, (str, int, float, bool))
    },
    "validation": metrics(yt[val], logits[val]),
    "thresholds": thresholds,
    "validation_ops": {
        k: at_threshold(yt[val], logits[val], t) for k, t in thresholds.items()
    },
}
models[name] = (model, logits, model.predict_proba(wx)[:, 1])
model.save_model(out / "xgboost.ubj")
key = lambda n: (
    -results[n]["validation_ops"]["0.99"]["negative_rejection"],
    results[n]["validation"]["bce"],
)
selected = {
    str(d): min([n for n in results if n.startswith(f"prefix{d}_")], key=key)
    for d in features
}
winner = min(results, key=key)
for name, (model, logits, wp) in models.items():
    r = results[name]
    r["test"] = metrics(yt[test], logits[test])
    r["test_ops"] = {
        k: at_threshold(yt[test], logits[test], t) for k, t in r["thresholds"].items()
    }
    r["test_matched_recall_descriptive"] = {
        k: at_threshold(yt[test], logits[test], t)
        for k, t in operating_points(yt[test], logits[test]).items()
    }
    r["wild_passed"] = {k: int((wp >= t).sum()) for k, t in r["thresholds"].items()}
    (out / (name + "-wild.jsonl")).write_text(
        "".join(
            json.dumps(row | {"probability": float(p)}) + "\n"
            for row, p in zip(wild, wp)
        )
    )
report = {
    "winner": winner,
    "selected_by_dimension": selected,
    "baseline": str(base),
    "selection": "validation negative rejection at 99% recall, then BCE",
    "results": results,
}
(out / "metrics.json").write_text(json.dumps(report, indent=2))
print("WINNER", winner, flush=True)
