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
"""Compare six prespecified sampling distributions with fixed optimization.

Only training rows are resampled. Validation selects the mix and thresholds;
test is evaluated after that decision. The random fixture is never trained on.
"""

import argparse
import json
import math
from pathlib import Path

import numpy as np
import torch
from comment_linear_probe import embeddings, metrics, snapshot
from sklearn.model_selection import train_test_split


def operating_points(y, logits):
    probabilities = torch.sigmoid(logits)
    positives = sorted(probabilities[y == 1].tolist(), reverse=True)
    assert positives
    return {
        str(target): positives[math.ceil(target * len(positives)) - 1]
        for target in [0.99, 0.95, 0.90]
    }


def at_threshold(y, logits, threshold):
    positive = y == 1
    passed = torch.sigmoid(logits) >= threshold
    tp = int((positive & passed).sum())
    fp = int((~positive & passed).sum())
    fn = int((positive & ~passed).sum())
    tn = int((~positive & ~passed).sum())
    return {
        "threshold": threshold,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "recall": tp / (tp + fn) if tp + fn else None,
        "negative_rejection": tn / (tn + fp) if tn + fp else None,
        "accuracy": (tp + tn) / len(y),
        "precision": tp / (tp + fp) if tp + fp else None,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slice-dir", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    torch.set_num_threads(4)
    torch.use_deterministic_algorithms(True)
    rows, provenance = snapshot(args.slice_dir, 1)
    wild = [
        {"comment_id": r["comment_id"], "text": r["text"]}
        for r in map(json.loads, args.fixture.read_text().splitlines())
    ]
    wild_ids = {r["comment_id"] for r in wild}
    excluded = [r["comment_id"] for r in rows if r["comment_id"] in wild_ids]
    rows = [r for r in rows if r["comment_id"] not in wild_ids]
    (args.output / "labels.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in rows)
    )
    (args.output / "provenance.json").write_text(json.dumps(provenance, indent=2))
    x = embeddings(args.slice_dir, rows)
    wx = embeddings(args.slice_dir, wild)
    y = torch.tensor([float(r["is_positive"]) for r in rows])
    train = np.array([i for i, r in enumerate(rows) if r["split"] == "train"])
    test = torch.tensor([i for i, r in enumerate(rows) if r["split"] == "test"])
    fit, val = train_test_split(
        train, test_size=0.2, random_state=42, stratify=y[train].numpy()
    )
    fit = torch.tensor(fit)
    val = torch.tensor(val)

    # Group negative rows using recorded random source membership, otherwise rank.
    def group(i):
        r = rows[i]
        if r["is_positive"]:
            return "positive"
        if 2 in r["sources"]:
            return "random_negative"
        if r["rank"] <= 5500:
            return "near_negative"
        return "deep_negative"

    groups = {
        g: torch.tensor([i for i in fit.tolist() if group(i) == g])
        for g in ["positive", "random_negative", "near_negative", "deep_negative"]
    }
    assert all(len(ids) for ids in groups.values())
    mixes = {
        "existing": None,
        "balanced": {
            "positive": 0.50,
            "random_negative": 0.50
            * len(groups["random_negative"])
            / (len(fit) - len(groups["positive"])),
            "near_negative": 0.50
            * len(groups["near_negative"])
            / (len(fit) - len(groups["positive"])),
            "deep_negative": 0.50
            * len(groups["deep_negative"])
            / (len(fit) - len(groups["positive"])),
        },
        "random_emphasis": {
            "positive": 0.40,
            "random_negative": 0.35,
            "near_negative": 0.15,
            "deep_negative": 0.10,
        },
        "deep_emphasis": {
            "positive": 0.40,
            "random_negative": 0.20,
            "near_negative": 0.15,
            "deep_negative": 0.25,
        },
        "negative_heavy": {
            "positive": 0.25,
            "random_negative": 0.35,
            "near_negative": 0.20,
            "deep_negative": 0.20,
        },
        "hard_negative_emphasis": {
            "positive": 0.40,
            "random_negative": 0.15,
            "near_negative": 0.30,
            "deep_negative": 0.15,
        },
    }
    results = {}
    saved = {}
    for name, mix in mixes.items():
        torch.manual_seed(42)
        model = torch.nn.Linear(x.shape[1], 1)
        optimizer = torch.optim.AdamW(model.parameters(), lr=0.001, weight_decay=0.01)
        # Every mix gets exactly the same number of draws and updates per epoch.
        weights = torch.ones(len(fit))
        if mix:
            for j, i in enumerate(fit.tolist()):
                weights[j] = mix[group(i)] / len(groups[group(i)])
        curve = []
        for epoch in range(850):
            order = (
                fit[torch.randperm(len(fit))]
                if mix is None
                else fit[torch.multinomial(weights, len(fit), replacement=True)]
            )
            for batch in order.split(64):
                optimizer.zero_grad()
                loss = torch.nn.functional.binary_cross_entropy_with_logits(
                    model(x[batch]).squeeze(1), y[batch]
                )
                loss.backward()
                optimizer.step()
            if epoch in [29, 99, 299, 499, 849]:
                with torch.no_grad():
                    curve.append(
                        {
                            "epoch": epoch + 1,
                            "fit_bce": torch.nn.functional.binary_cross_entropy_with_logits(
                                model(x[fit]).squeeze(1), y[fit]
                            ).item(),
                            "validation_bce": torch.nn.functional.binary_cross_entropy_with_logits(
                                model(x[val]).squeeze(1), y[val]
                            ).item(),
                        }
                    )
        with torch.no_grad():
            logits = model(x).squeeze(1)
        thresholds = operating_points(y[val], logits[val])
        results[name] = {
            "mix": mix,
            "validation": metrics(y[val], logits[val]),
            "thresholds": thresholds,
            "validation_operating_points": {
                k: at_threshold(y[val], logits[val], t) for k, t in thresholds.items()
            },
            "curve": curve,
        }
        saved[name] = (model, logits)
        print(name, json.dumps(results[name]), flush=True)
    # Predeclared selection: most validation negatives rejected at >=99% recall;
    # tie break by validation BCE. Neither test nor wild scores select the mix.
    winner = min(
        results,
        key=lambda name: (
            -results[name]["validation_operating_points"]["0.99"]["negative_rejection"],
            results[name]["validation"]["bce"],
        ),
    )
    for name, (model, logits) in saved.items():
        result = results[name]
        thresholds = result["thresholds"]
        result["test"] = metrics(y[test], logits[test])
        result["test_operating_points"] = {
            k: at_threshold(y[test], logits[test], t) for k, t in thresholds.items()
        }
        result["test_by_source"] = {}
        for source in ["positive", "random_negative", "near_negative", "deep_negative"]:
            ids = torch.tensor(
                [i for i in test.tolist() if group(i) == source], dtype=torch.long
            )
            if len(ids):
                result["test_by_source"][source] = metrics(y[ids], logits[ids])
        result["test_by_taxonomy"] = {}
        for tax in sorted({r["taxonomy"] for r in rows}):
            ids = torch.tensor(
                [i for i in test.tolist() if rows[i]["taxonomy"] == tax],
                dtype=torch.long,
            )
            if len(ids):
                result["test_by_taxonomy"][tax] = metrics(y[ids], logits[ids])
        with torch.no_grad():
            wp = torch.sigmoid(model(wx).squeeze(1)).tolist()
        result["wild_passed"] = {
            k: sum(p >= t for p in wp) for k, t in thresholds.items()
        }
        torch.save(
            {
                "state_dict": model.state_dict(),
                "thresholds": thresholds,
                "epochs": 850,
                "mix": result["mix"],
            },
            args.output / (name + ".pt"),
        )
        (args.output / (name + "-wild.jsonl")).write_text(
            "".join(json.dumps(r | {"probability": p}) + "\n" for r, p in zip(wild, wp))
        )
        (args.output / (name + "-test.jsonl")).write_text(
            "".join(
                json.dumps(rows[i] | {"probability": torch.sigmoid(logits[i]).item()})
                + "\n"
                for i in test.tolist()
            )
        )
    report = {
        "winner": winner,
        "selection": "validation negative rejection at 99% recall, then BCE",
        "seed": 42,
        "epochs": 850,
        "lr": 0.001,
        "weight_decay": 0.01,
        "batch_size": 64,
        "fit_n": len(fit),
        "validation_n": len(val),
        "test_n": len(test),
        "fit_groups": {g: len(ids) for g, ids in groups.items()},
        "fixture_overlaps_excluded": excluded,
        "results": results,
    }
    (args.output / "split_ids.json").write_text(
        json.dumps(
            {
                "fit": [rows[i]["comment_id"] for i in fit.tolist()],
                "validation": [rows[i]["comment_id"] for i in val.tolist()],
                "test": [rows[i]["comment_id"] for i in test.tolist()],
            }
        )
    )
    (args.output / "metrics.json").write_text(json.dumps(report, indent=2))
    print("WINNER", winner, flush=True)


if __name__ == "__main__":
    main()
