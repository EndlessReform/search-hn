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
import json, math
from pathlib import Path
import numpy as np, torch
from xgboost import XGBClassifier
from comment_linear_probe import embeddings, readonly

base = Path("data/probes/books-mlp-v1")
out = Path("data/probes/books-filter-comparison-v1")
out.mkdir(exist_ok=False)
rows = [json.loads(l) for l in (base / "labels.jsonl").read_text().splitlines()]
spl = json.loads((base / "split_ids.json").read_text())
val = np.array([r["comment_id"] in set(spl["validation"]) for r in rows])
test = np.array([r["comment_id"] in set(spl["test"]) for r in rows])
y = np.array([r["is_positive"] for r in rows])
x = embeddings(Path("data/comment-2025"), rows).numpy()
scores = {"centroid": np.array([r["score"] for r in rows])}
thresholds = {}
pos = sorted(scores["centroid"][val & y], reverse=True)
thresholds["centroid"] = {
    str(t): pos[math.ceil(t * len(pos)) - 1] for t in [0.99, 0.95]
}
for name, path in [
    ("logistic", "data/probes/books-logistic-v1/weighted_C0.1.pt"),
    ("adamw", "data/probes/books-mlp-v1/linear.pt"),
]:
    ck = torch.load(path, weights_only=True)
    model = torch.nn.Linear(1024, 1)
    model.load_state_dict(ck["state_dict"])
    with torch.no_grad():
        scores[name] = torch.sigmoid(model(torch.from_numpy(x)).squeeze(1)).numpy()
    thresholds[name] = ck["thresholds"]
m = json.loads(Path("data/probes/books-xgb-sweep-v1/metrics.json").read_text())
model = XGBClassifier()
model.load_model("data/probes/books-xgb-sweep-v1/depth4_child1.ubj")
scores["xgboost"] = model.predict_proba(x)[:, 1]
thresholds["xgboost"] = m["results"]["depth4_child1"]["thresholds"]
report = {"thresholds": thresholds, "taxonomy": {}, "random_source": {}}
for tax in sorted({r["taxonomy"] for r in rows}):
    mask = test & np.array([r["taxonomy"] == tax for r in rows])
    truth = y[mask]
    report["taxonomy"][tax] = {
        "n": int(mask.sum()),
        "positive": int(truth.sum()),
        "models": {
            name: {
                target: int(((s[mask] >= thresholds[name][target]) == truth).sum())
                for target in ["0.99", "0.95"]
            }
            for name, s in scores.items()
        },
    }
random = np.array([2 in r["sources"] for r in rows])
report["random_source"] = {
    "all_positive": int((random & y).sum()),
    "test_positive": int((test & random & y).sum()),
    "test_negative": int((test & random & ~y).sum()),
    "models": {
        name: {
            t: {
                "tp": int((s[test & random & y] >= thresholds[name][t]).sum()),
                "fp": int((s[test & random & ~y] >= thresholds[name][t]).sum()),
            }
            for t in ["0.99", "0.95"]
        }
        for name, s in scores.items()
    },
}
with readonly(Path("data/comment-2025/index.sqlite")) as db:
    inspections = []
    for i, r in enumerate(rows):
        if (random[i] and y[i]) or (
            test[i]
            and y[i]
            and any(scores[n][i] < thresholds[n]["0.95"] for n in scores)
        ):
            inspections.append(
                r
                | {
                    "split_detail": "test"
                    if test[i]
                    else "validation"
                    if val[i]
                    else "fit",
                    "text": db.execute(
                        "select text from comments where comment_id=?",
                        (r["comment_id"],),
                    ).fetchone()[0],
                    "predictions": {
                        n: {
                            "score": float(s[i]),
                            "pass99": bool(s[i] >= thresholds[n]["0.99"]),
                            "pass95": bool(s[i] >= thresholds[n]["0.95"]),
                        }
                        for n, s in scores.items()
                    },
                    "random_source": bool(random[i]),
                }
            )
(out / "inspect.jsonl").write_text("".join(json.dumps(r) + "\n" for r in inspections))
(out / "metrics.json").write_text(json.dumps(report, indent=2))
print(json.dumps(report))
for r in inspections:
    if r["random_source"]:
        print("RANDOM", json.dumps(r))
