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
from xgboost import XGBClassifier

warnings.simplefilter("error", ConvergenceWarning)
torch.set_num_threads(4)
root = Path("data/comment-2025")
base = Path("data/probes/books-mlp-v1")
out = Path("data/probes/books-xgb-sweep-v1")
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


# Two new disjoint fixtures, excluding the entire current rollout pool.
excluded = {r["comment_id"] for r in rows} | {r["comment_id"] for r in wild}
with readonly(root / "annotations.sqlite") as db:
    excluded.update(
        r[0] for r in db.execute("SELECT DISTINCT comment_id FROM rollout_picks")
    )
with readonly(root / "index.sqlite") as db:
    corpus_ids = np.fromiter(
        (
            r[0]
            for r in db.execute("SELECT comment_id FROM comments ORDER BY comment_id")
        ),
        dtype=np.int64,
    )
    eligible = corpus_ids[~np.isin(corpus_ids, list(excluded))]
    chosen = np.random.default_rng(20260921).choice(eligible, 2000, replace=False)
    fixtures = {"original": wild}
    for batch in range(2):
        fixtures[f"new{batch + 1}"] = [
            {
                "comment_id": int(cid),
                "text": db.execute(
                    "SELECT text FROM comments WHERE comment_id=?", (int(cid),)
                ).fetchone()[0],
            }
            for cid in chosen[batch * 1000 : (batch + 1) * 1000]
        ]
for name, fixture in fixtures.items():
    (out / (name + "-fixture.jsonl")).write_text(
        "".join(json.dumps(r) + "\n" for r in fixture)
    )
features = {
    name: embeddings(root, fixture).numpy() for name, fixture in fixtures.items()
}
results = {}
models = {}
for depth in [2, 3, 4]:
    for child in [1, 10]:
        name = f"depth{depth}_child{child}"
        model = XGBClassifier(
            n_estimators=1000,
            max_depth=depth,
            min_child_weight=child,
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
            x[fit],
            y[fit],
            sample_weight=weighted,
            eval_set=[(x[val], y[val])],
            verbose=False,
        )
        logits = torch.tensor(model.predict(x, output_margin=True), dtype=torch.float32)
        thresholds = operating_points(yt[val], logits[val])
        results[name] = {
            "depth": depth,
            "min_child_weight": child,
            "best_iteration": model.best_iteration,
            "validation": metrics(yt[val], logits[val]),
            "thresholds": thresholds,
            "validation_ops": {
                k: at_threshold(yt[val], logits[val], t) for k, t in thresholds.items()
            },
        }
        models[name] = (model, logits)
        model.save_model(out / (name + ".ubj"))
        print(name, flush=True)
key = lambda n: (
    -results[n]["validation_ops"]["0.99"]["negative_rejection"],
    results[n]["validation"]["bce"],
)
winner = min(results, key=key)
for name, (model, logits) in models.items():
    r = results[name]
    r["test"] = metrics(yt[test], logits[test])
    r["test_ops"] = {
        k: at_threshold(yt[test], logits[test], t) for k, t in r["thresholds"].items()
    }
    r["wild_passed"] = {}
    for batch, fixture in fixtures.items():
        wp = model.predict_proba(features[batch])[:, 1]
        r["wild_passed"][batch] = {
            k: int((wp >= t).sum()) for k, t in r["thresholds"].items()
        }
        (out / (name + "-" + batch + ".jsonl")).write_text(
            "".join(
                json.dumps(row | {"probability": float(p)}) + "\n"
                for row, p in zip(fixture, wp)
            )
        )
report = {
    "winner": winner,
    "baseline": str(base),
    "fixture_seed": 20260921,
    "excluded_ids": len(excluded),
    "selection": "validation negative rejection at 99% recall, then BCE",
    "results": results,
}
(out / "metrics.json").write_text(json.dumps(report, indent=2))
print("WINNER", winner, flush=True)
