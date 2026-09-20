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

warnings.simplefilter("error", ConvergenceWarning)
torch.set_num_threads(4)
root = Path("data/comment-2025")
base = Path("data/probes/books-mlp-v1")
out = Path("data/probes/books-logistic-v1")
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
# Audit the bottom 40 validation positives under the previous linear checkpoint.
old = torch.nn.Linear(1024, 1)
old.load_state_dict(torch.load(base / "linear.pt", weights_only=True)["state_dict"])
with torch.no_grad():
    oldp = torch.sigmoid(old(torch.tensor(x, dtype=torch.float32)).squeeze(1)).numpy()
audit = sorted([i for i in val if y[i]], key=lambda i: oldp[i])[:40]
with readonly(root / "index.sqlite") as db:
    packets = [[] for _ in range(4)]
    for j, i in enumerate(audit):
        row = rows[i] | {
            "score": float(oldp[i]),
            "text": db.execute(
                "select text from comments where comment_id=?", (rows[i]["comment_id"],)
            ).fetchone()[0],
        }
        packets[j % 4].append(row)
    for j, packet in enumerate(packets):
        (out / f"audit-{j + 1}.json").write_text(json.dumps(packet, indent=2))


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
results = {}
models = {}
for mode, weights in [("natural", np.ones(len(fit))), ("weighted", weighted)]:
    for c in [0.01, 0.1, 1, 10, 100, 1000, 10000]:
        name = f"{mode}_C{c:g}"
        model = LogisticRegression(C=c, solver="lbfgs", max_iter=3000, tol=1e-8)
        model.fit(x[fit], y[fit], sample_weight=weights)
        logits = torch.tensor(model.decision_function(x), dtype=torch.float32)
        thresholds = operating_points(yt[val], logits[val])
        results[name] = {
            "C": c,
            "mode": mode,
            "iterations": int(model.n_iter_[0]),
            "weight_norm": float(np.linalg.norm(model.coef_)),
            "validation": metrics(yt[val], logits[val]),
            "thresholds": thresholds,
            "validation_ops": {
                k: at_threshold(yt[val], logits[val], t) for k, t in thresholds.items()
            },
        }
        models[name] = (model, logits)
        print(name, results[name]["validation"]["bce"], flush=True)
winner = min(
    results,
    key=lambda n: (
        -results[n]["validation_ops"]["0.99"]["negative_rejection"],
        results[n]["validation"]["bce"],
    ),
)
for name, (model, logits) in models.items():
    r = results[name]
    r["test"] = metrics(yt[test], logits[test])
    r["test_ops"] = {
        k: at_threshold(yt[test], logits[test], t) for k, t in r["thresholds"].items()
    }
    r["descriptive_test_matched_recall"] = {
        k: at_threshold(yt[test], logits[test], t)
        for k, t in operating_points(yt[test], logits[test]).items()
    }
    wp = model.predict_proba(wx)[:, 1]
    r["wild_passed"] = {k: int((wp >= t).sum()) for k, t in r["thresholds"].items()}
    state = {
        "weight": torch.tensor(model.coef_, dtype=torch.float32),
        "bias": torch.tensor(model.intercept_, dtype=torch.float32),
    }
    torch.save(
        {
            "state_dict": state,
            "thresholds": r["thresholds"],
            "architecture": "linear",
            "C": r["C"],
            "mode": r["mode"],
        },
        out / (name + ".pt"),
    )
    (out / (name + "-wild.jsonl")).write_text(
        "".join(
            json.dumps(row | {"probability": float(p)}) + "\n"
            for row, p in zip(wild, wp)
        )
    )
report = {
    "winner": winner,
    "baseline": str(base),
    "fit_n": len(fit),
    "validation_n": len(val),
    "test_n": len(test),
    "selection": "validation negative rejection at 99% recall, then BCE",
    "results": results,
}
(out / "metrics.json").write_text(json.dumps(report, indent=2))
print("WINNER", winner, flush=True)
