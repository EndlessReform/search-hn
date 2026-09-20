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
"""Fixed CPU linear-probe baseline over a snapshot of accepted rollout labels.

Run with `uv run --script ... --slice-dir data/comment-2025 --output DIR`.
The saved split is never reassigned. Only training rows affect optimization;
no checkpoint selection or threshold tuning uses test labels. Multi-chunk
comments use normalized-chunk mean pooling followed by unit normalization,
matching the explorer's comment-centroid representation.
"""

import argparse
import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import torch
from sklearn.metrics import average_precision_score, roc_auc_score
from torch import nn


def readonly(path):
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)


def snapshot(root, set_id):
    """Read accepted picks and their run provenance in one SQLite transaction."""
    with readonly(root / "annotations.sqlite") as db:
        db.row_factory = sqlite3.Row
        db.execute("BEGIN")
        pool = db.execute(
            "SELECT * FROM rollout_pools WHERE set_id=? AND active=1", (set_id,)
        ).fetchone()
        assert pool is not None, "No active pool"
        rows = [
            dict(r)
            for r in db.execute(
                """
            SELECT p.comment_id,p.split,p.rank,p.score,p.latest_attempt,a.run_id,
                   a.label_json,r.model,r.snapshot_json
            FROM rollout_picks p JOIN classifier_attempts a ON a.id=p.latest_attempt
            JOIN classifier_runs r ON r.id=a.run_id
            WHERE p.pool_id=? AND p.status='accepted' ORDER BY p.comment_id
        """,
                (pool["id"],),
            )
        ]
        runs = {}
        for row in rows:
            runs[str(row["run_id"])] = json.loads(row.pop("snapshot_json"))
            row.update(json.loads(row.pop("label_json")))
            row["sources"] = [
                r[0]
                for r in db.execute(
                    "SELECT rule_id FROM rollout_sources WHERE pool_id=? AND comment_id=?",
                    (pool["id"], row["comment_id"]),
                )
            ]
        return rows, {"pool": dict(pool), "runs": runs}


def embeddings(root, rows, dimensions=None):
    """Gather only sampled vectors, leaving the full corpus memory mapped."""
    vectors = np.load(root / "vectors.npy", mmap_mode="r")
    result = []
    with readonly(root / "index.sqlite") as db:
        for row in rows:
            indices = [
                r[0]
                for r in db.execute(
                    "SELECT vector_row FROM inputs WHERE comment_id=? ORDER BY chunk",
                    (row["comment_id"],),
                )
            ]
            assert indices, f"Missing embedding: {row['comment_id']}"
            chunks = vectors[indices, :dimensions].astype(np.float32)
            norms = np.linalg.norm(chunks, axis=1, keepdims=True)
            assert (norms > 0).all()
            mean = (chunks / norms).mean(axis=0)
            assert np.linalg.norm(mean) > 0
            result.append(mean / np.linalg.norm(mean))
    return torch.from_numpy(np.stack(result))


def metrics(y, logits):
    """Binary loss and fixed-threshold metrics; ranking metrics need both classes."""
    p = torch.sigmoid(logits).numpy()
    actual = y.numpy().astype(bool)
    predicted = p >= 0.5
    tp = int((predicted & actual).sum())
    tn = int((~predicted & ~actual).sum())
    fp = int((predicted & ~actual).sum())
    fn = int((~predicted & actual).sum())
    return {
        "n": len(y),
        "positive": int(actual.sum()),
        "bce": nn.functional.binary_cross_entropy_with_logits(logits, y).item(),
        "accuracy": (tp + tn) / len(y),
        "precision": tp / (tp + fp) if tp + fp else None,
        "recall": tp / (tp + fn) if tp + fn else None,
        "f1": 2 * tp / (2 * tp + fp + fn) if 2 * tp + fp + fn else None,
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "auroc": roc_auc_score(actual, p) if len(set(actual)) == 2 else None,
        "average_precision": average_precision_score(actual, p)
        if len(set(actual)) == 2
        else None,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slice-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--set-id", type=int, default=1)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    torch.set_num_threads(4)
    torch.manual_seed(42)
    torch.use_deterministic_algorithms(True)
    rows, provenance = snapshot(args.slice_dir, args.set_id)
    assert len({r["comment_id"] for r in rows}) == len(rows)
    frozen = "".join(json.dumps(r) + "\n" for r in rows)
    (args.output / "labels.jsonl").write_text(frozen)
    config = {
        "seed": 42,
        "epochs": 30,
        "batch_size": 64,
        "lr": 0.001,
        "weight_decay": 0.01,
        "optimizer": "AdamW",
        "threshold": 0.5,
        "pooling": "unit-chunks-mean-unit-comment-v1",
        "created_at": datetime.now(UTC).isoformat(),
        "torch": torch.__version__,
        "labels_sha256": hashlib.sha256(frozen.encode()).hexdigest(),
    }
    (args.output / "provenance.json").write_text(json.dumps(provenance, indent=2))
    x = embeddings(args.slice_dir, rows)
    y = torch.tensor([float(r["is_positive"]) for r in rows])
    train = torch.tensor([i for i, r in enumerate(rows) if r["split"] == "train"])
    test = torch.tensor([i for i, r in enumerate(rows) if r["split"] == "test"])
    assert len(train) > 0 and len(test) > 0 and len(train) + len(test) == len(rows)
    assert len(y[train].unique()) == 2, "Training requires both classes"
    model = nn.Linear(x.shape[1], 1)
    optimizer = torch.optim.AdamW(
        model.parameters(), lr=config["lr"], weight_decay=config["weight_decay"]
    )
    history = []
    for epoch in range(config["epochs"]):
        model.train()
        for ids in train[torch.randperm(len(train))].split(config["batch_size"]):
            optimizer.zero_grad()
            loss = nn.functional.binary_cross_entropy_with_logits(
                model(x[ids]).squeeze(1), y[ids]
            )
            loss.backward()
            optimizer.step()
        with torch.no_grad():
            history.append(
                nn.functional.binary_cross_entropy_with_logits(
                    model(x[train]).squeeze(1), y[train]
                ).item()
            )
    model.eval()
    with torch.no_grad():
        logits = model(x).squeeze(1)
    prevalence = y[train].mean()
    constant = torch.full_like(y[test], torch.logit(prevalence).item())
    report = {
        "config": config,
        "train": metrics(y[train], logits[train]),
        "test": metrics(y[test], logits[test]),
        "constant_train_prevalence": metrics(y[test], constant),
        "train_loss": history,
        "by_taxonomy": {},
    }
    for taxonomy in sorted({r["taxonomy"] for r in rows}):
        ids = torch.tensor(
            [i for i in test.tolist() if rows[i]["taxonomy"] == taxonomy],
            dtype=torch.long,
        )
        if len(ids):
            report["by_taxonomy"][taxonomy] = metrics(y[ids], logits[ids])
    torch.save(
        {"state_dict": model.state_dict(), "config": config, "dimensions": x.shape[1]},
        args.output / "probe.pt",
    )
    (args.output / "metrics.json").write_text(json.dumps(report, indent=2))
    with (args.output / "test_predictions.jsonl").open("w") as out:
        for i in test.tolist():
            out.write(
                json.dumps(rows[i] | {"probability": torch.sigmoid(logits[i]).item()})
                + "\n"
            )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
