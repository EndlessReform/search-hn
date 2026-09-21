"""Render saved development curves; no training or test-based selection.

Run with: uv run --locked --package search-research --with matplotlib python
packages/search-research/tools/comment_entity_plot.py {wsd,positive-silver}
"""

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("experiment", choices=["wsd", "positive-silver"])
    args = parser.parse_args()
    base = Path("data/probes/books-gliner-wsd-v1")
    if args.experiment == "wsd":
        runs = [(base / "linear", "Linear"), (base / "wsd", "WSD")]
        xkey, xlabel = "epoch", "Epoch"
        destination = base / "scheduler-comparison.png"
        title = "Linear vs WSD · corrected gold · identical sample exposure"
    else:
        root = Path("data/probes/books-gliner-positive-silver-v1")
        runs = [(base / "linear", "Gold only"), (root, "Gold + silver positives")]
        xkey, xlabel = "step", "Optimizer updates"
        destination = root / "positive-silver-comparison.png"
        title = "Adding silver positives · fixed 135-update budget"
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.3), sharey=True)
    for (path, label), color in zip(runs, ["#2b66d9", "#d77800"], strict=True):
        curve = json.loads((path / "curve.json").read_text())
        for ax, tuned in zip(axes, [False, True], strict=True):
            values = [
                max(g["f1"] for g in r["grid"])
                if tuned
                else next(g["f1"] for g in r["grid"] if g["threshold"] == 0.5)
                for r in curve
            ]
            ax.plot(
                [r[xkey] for r in curve],
                [100 * v for v in values],
                "-o",
                label=label,
                color=color,
            )
            ax.set_xticks([r[xkey] for r in curve])
            ax.set_xlabel(xlabel)
            ax.grid(alpha=0.2)
            ax.legend()
    axes[0].set_title("Threshold 0.5")
    axes[1].set_title("Threshold selected on development")
    axes[0].set_ylabel("Development exact-span F1 (%)")
    fig.suptitle(title)
    fig.text(
        0.5,
        0.015,
        "147 development comments · effective batch 32 · 135 updates per arm",
        ha="center",
        fontsize=9,
    )
    fig.tight_layout(rect=[0, 0.045, 1, 0.95])
    fig.savefig(destination, dpi=160)
    print(destination)


if __name__ == "__main__":
    main()
