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

"""Ten fixed centroid/XGBoost combinations; validation selects operating points."""
import json, math
from pathlib import Path
import numpy as np
from xgboost import XGBClassifier
from comment_linear_probe import embeddings

root = Path("data/comment-2025")
base = Path("data/probes/books-mlp-v1")
out = Path("data/probes/books-ensemble-v1")
out.mkdir(exist_ok=False)
rows = list(map(json.loads, (base / "labels.jsonl").read_text().splitlines()))
spl = json.loads((base / "split_ids.json").read_text())
masks = {
    k: np.array([r["comment_id"] in set(ids) for r in rows]) for k, ids in spl.items()
}
y = np.array([r["is_positive"] for r in rows])
model = XGBClassifier()
model.load_model("data/probes/books-xgb-sweep-v1/depth4_child1.ubj")
x = model.predict_proba(embeddings(root, rows).numpy())[:, 1].astype(float)
c = np.array([r["score"] for r in rows])
wild = list(
    map(
        json.loads,
        Path("data/probes/books-wild-audit-10k-v1/predictions.jsonl")
        .read_text()
        .splitlines(),
    )
)
wx = np.array([r["xgboost"] for r in wild])
wc = np.array([r["centroid"] for r in wild])
# Fixed score mixtures require comparable scales. Fit rows set location/scale only.
logit = lambda p: np.log(np.clip(p, 1e-7, 1 - 1e-7) / np.clip(1 - p, 1e-7, 1 - 1e-7))
normalization = {}
zs = []
for name, a, b in [("cosine", c, wc), ("xgb_logit", logit(x), logit(wx))]:
    mu = float(a[masks["fit"]].mean())
    sd = float(a[masks["fit"]].std())
    normalization[name] = {"mean": mu, "std": sd}
    zs.append(((a - mu) / sd, (b - mu) / sd))
(zc, wzc), (zx, wzx) = zs
configs = {"centroid": (c, wc), "xgboost": (x, wx)}
for weight in [0.1, 0.25, 0.5, 0.75, 0.9]:
    configs[f"blend_cosine_{weight}"] = (
        (1 - weight) * zx + weight * zc,
        (1 - weight) * wzx + weight * wzc,
    )
for weight in [0.5, 1.0, 2.0]:
    configs[f"max_cosine_{weight}"] = (
        np.maximum(zx, weight * zc),
        np.maximum(wzx, weight * wzc),
    )
old = json.loads(
    Path("data/probes/books-filter-comparison-v1/metrics.json").read_text()
)["thresholds"]
for ct, xt in [("0.99", "0.95"), ("0.95", "0.95")]:
    configs[f"union_c{ct}_x{xt}"] = (
        np.maximum(c / old["centroid"][ct], x / old["xgboost"][xt]),
        np.maximum(wc / old["centroid"][ct], wx / old["xgboost"][xt]),
    )


def stats(mask, score, t):
    truth = y[mask]
    pred = score[mask] >= t
    tp = int((truth & pred).sum())
    fn = int((truth & ~pred).sum())
    fp = int((~truth & pred).sum())
    tn = int((~truth & ~pred).sum())
    return dict(
        tp=tp,
        fn=fn,
        fp=fp,
        tn=tn,
        recall=tp / (tp + fn),
        negative_rejection=tn / (tn + fp),
    )


results = {}
for name, (score, ws) in configs.items():
    points = {}
    for target in [0.99, 0.95]:
        pos = np.sort(score[masks["validation"] & y])[::-1]
        threshold = (
            1.0
            if name.startswith("union")
            else float(pos[math.ceil(len(pos) * target) - 1])
        )
        points[str(target)] = {
            "threshold": threshold,
            "validation": stats(masks["validation"], score, threshold),
            "test": stats(masks["test"], score, threshold),
            "wild_pass": int((ws >= threshold).sum()),
        }
    results[name] = points
winners = {
    t: max(
        (n for n in configs if results[n][t]["validation"]["recall"] >= float(t)),
        key=lambda n: results[n][t]["validation"]["negative_rejection"],
    )
    for t in ["0.99", "0.95"]
}
(out / "metrics.json").write_text(
    json.dumps(
        {"normalization": normalization, "results": results, "winners": winners},
        indent=2,
    )
)
with (out / "wild_scores.jsonl").open("w") as f:
    for i, r in enumerate(wild):
        f.write(
            json.dumps(
                {
                    "comment_id": r["comment_id"],
                    "scores": {n: float(a[1][i]) for n, a in configs.items()},
                }
            )
            + "\n"
        )
print(json.dumps({"results": results, "winners": winners}))
