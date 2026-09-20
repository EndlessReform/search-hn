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
"""Rescore a frozen corpus fixture with any compatible linear-probe checkpoint.

Example: uv run --script comment_probe_wild.py --slice-dir data/comment-2025
--fixture data/probes/books-wild-1000-v1/predictions.jsonl --checkpoint MODEL.pt
--output NEW_DIRECTORY --exclude-labels TRAINING_SNAPSHOT.jsonl
Thresholds default to those saved in the checkpoint; older checkpoints require
--threshold99 and --threshold95. Scores are not ground-truth labels or recall.
"""

import argparse
import json
from pathlib import Path

import torch
from comment_linear_probe import embeddings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slice-dir", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--exclude-labels", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--threshold99", type=float)
    parser.add_argument("--threshold95", type=float)
    args = parser.parse_args()
    rows = [
        {"comment_id": r["comment_id"], "text": r["text"]}
        for r in map(json.loads, args.fixture.read_text().splitlines())
    ]
    ids = {r["comment_id"] for r in rows}
    assert len(ids) == len(rows), "Duplicate fixture IDs"
    used = {
        r["comment_id"]
        for r in map(json.loads, args.exclude_labels.read_text().splitlines())
    }
    assert not ids & used, (
        "Fixture overlaps label snapshot; exclude fixture IDs from training first"
    )
    checkpoint = torch.load(args.checkpoint, map_location="cpu", weights_only=True)
    thresholds = checkpoint.get("thresholds", {}).copy()
    if args.threshold99 is not None:
        thresholds["0.99"] = args.threshold99
    if args.threshold95 is not None:
        thresholds["0.95"] = args.threshold95
    assert all(k in thresholds for k in ["0.99", "0.95"]), (
        "Supply both thresholds for an older checkpoint"
    )
    assert all(0 <= v <= 1 for v in thresholds.values())
    args.output.mkdir(parents=True, exist_ok=False)
    torch.set_num_threads(4)
    x = embeddings(args.slice_dir, rows)
    model = torch.nn.Linear(x.shape[1], 1)
    model.load_state_dict(checkpoint["state_dict"])
    model.eval()
    with torch.no_grad():
        probabilities = torch.sigmoid(model(x).squeeze(1)).tolist()
    (args.output / "predictions.jsonl").write_text(
        "".join(
            json.dumps(r | {"probability": p}) + "\n"
            for r, p in zip(rows, probabilities)
        )
    )
    summary = {
        "fixture": str(args.fixture),
        "checkpoint": str(args.checkpoint),
        "n": len(rows),
        "thresholds": thresholds,
        "passed": {
            k: sum(p >= t for p in probabilities) for k, t in thresholds.items()
        },
    }
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
