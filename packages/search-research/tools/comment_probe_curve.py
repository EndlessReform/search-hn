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
"""Measure a longer learning curve using validation drawn only from frozen train IDs."""

import argparse
import copy
import json
from pathlib import Path

import numpy as np
import torch
from comment_linear_probe import embeddings, metrics
from sklearn.model_selection import train_test_split
from torch import nn


def fit(x, y, ids, epochs, validation=None):
    """Use the original optimizer setup; select epochs only by validation BCE."""
    torch.manual_seed(42)
    model = nn.Linear(x.shape[1], 1)
    optimizer = torch.optim.AdamW(model.parameters(), lr=0.001, weight_decay=0.01)
    curve, best, best_state = [], float("inf"), None
    best_epoch = epochs
    for epoch in range(1, epochs + 1):
        for batch in ids[torch.randperm(len(ids))].split(64):
            optimizer.zero_grad()
            loss = nn.functional.binary_cross_entropy_with_logits(
                model(x[batch]).squeeze(1), y[batch]
            )
            loss.backward()
            optimizer.step()
        with torch.no_grad():
            train_loss = nn.functional.binary_cross_entropy_with_logits(
                model(x[ids]).squeeze(1), y[ids]
            ).item()
            row = {"epoch": epoch, "train_bce": train_loss}
            if validation is not None:
                val_loss = nn.functional.binary_cross_entropy_with_logits(
                    model(x[validation]).squeeze(1), y[validation]
                ).item()
                row["validation_bce"] = val_loss
                if val_loss < best:
                    best, best_epoch, best_state = (
                        val_loss,
                        epoch,
                        copy.deepcopy(model.state_dict()),
                    )
            curve.append(row)
    return model, curve, best_epoch, best_state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slice-dir", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--epochs", type=int, default=2000)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    torch.set_num_threads(4)
    torch.use_deterministic_algorithms(True)
    rows = [
        json.loads(line)
        for line in (args.baseline / "labels.jsonl").read_text().splitlines()
    ]
    x = embeddings(args.slice_dir, rows)
    y = torch.tensor([float(row["is_positive"]) for row in rows])
    train = np.array([i for i, r in enumerate(rows) if r["split"] == "train"])
    test = torch.tensor([i for i, r in enumerate(rows) if r["split"] == "test"])
    fit_ids, val_ids = train_test_split(
        train, test_size=0.2, random_state=42, stratify=y[train].numpy()
    )
    fit_ids, val_ids = torch.tensor(fit_ids), torch.tensor(val_ids)
    _, curve, best_epoch, best_state = fit(x, y, fit_ids, args.epochs, val_ids)
    # Refit all original training rows for the epoch budget selected above.
    model, _, _, _ = fit(x, y, torch.tensor(train), best_epoch)
    with torch.no_grad():
        logits = model(x).squeeze(1)
    report = {
        "baseline": str(args.baseline),
        "fit_n": len(fit_ids),
        "validation_n": len(val_ids),
        "best_epoch": best_epoch,
        "max_epochs": args.epochs,
        "curve": curve,
        "test": metrics(y[test], logits[test]),
        "by_taxonomy": {},
    }
    for name in sorted({r["taxonomy"] for r in rows}):
        ids = torch.tensor(
            [i for i in test.tolist() if rows[i]["taxonomy"] == name], dtype=torch.long
        )
        if len(ids):
            report["by_taxonomy"][name] = metrics(y[ids], logits[ids])
    (args.output / "metrics.json").write_text(json.dumps(report, indent=2))
    (args.output / "validation_ids.json").write_text(
        json.dumps([rows[i]["comment_id"] for i in val_ids.tolist()])
    )
    torch.save(
        {"state_dict": model.state_dict(), "epochs": best_epoch},
        args.output / "probe.pt",
    )
    torch.save(best_state, args.output / "validation_probe.pt")
    with (args.output / "test_predictions.jsonl").open("w") as out:
        for i in test.tolist():
            out.write(
                json.dumps(rows[i] | {"probability": torch.sigmoid(logits[i]).item()})
                + "\n"
            )
    print(json.dumps({k: v for k, v in report.items() if k != "curve"}, indent=2))
    print(
        "CURVE",
        json.dumps(
            [
                r
                for r in curve
                if r["epoch"]
                in [1, 30, 50, 100, 200, 300, 400, 500, 1000, 1500, 2000, best_epoch]
            ]
        ),
    )


if __name__ == "__main__":
    main()
