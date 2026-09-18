"""Freeze manual-review packets for every question missed by dense or hybrid.

This is a diagnostic annotation layer, never an edit of the evaluation labels.
Reviewers get complete recorded search lists and source snippets, not new retrieval.
"""

import json

import polars as pl

from search_research.dataset import read_jsonl
from search_research.engine_data import TRACES
from search_research.report import evaluate
from search_research.semantic_snapshot import ROOT

AUDIT = ROOT / "miss-audit"


def main():
    targets = {r["id"]: r for r in read_jsonl(TRACES / "eval.jsonl")}
    frames = {
        mode: pl.read_parquet(root / "metrics.parquet").filter(
            pl.col("model") == "gpt-5.6-luna"
        )
        for mode, root in (
            ("dense", ROOT / "dense"),
            ("hybrid", ROOT / "hybrid"),
            ("original-fts", TRACES),
        )
    }
    cases = sorted(
        set().union(
            *(
                set(frames[m].filter(~pl.col("exposed"))["case"])
                for m in ("dense", "hybrid")
            )
        )
    )
    AUDIT.mkdir(exist_ok=True)
    packets = []
    for case in cases:
        runs = {}
        for mode, frame in frames.items():
            row = frame.filter(pl.col("case") == case).row(0, named=True)
            root = TRACES if mode == "original-fts" else ROOT / mode
            path = root / row["trajectory"]
            parsed = evaluate(read_jsonl(path))
            runs[mode] = {
                "trajectory": str(path),
                "exposed": row["exposed"],
                "status": row["status"],
                "error": row["error"],
                "final": row["final"],
                "model_turns": row["model_requests"],
                "searches": parsed["queries"],
            }
        packets.append(
            {
                "case": case,
                "question": row["prompt"],
                "style": row["style"],
                "target": targets[row["target_id"]],
                "runs": runs,
            }
        )
    for shard in range(4):
        path = AUDIT / f"packet-{shard + 1}.jsonl"
        path.write_text(
            "".join(json.dumps(p, ensure_ascii=False) + "\n" for p in packets[shard::4])
        )
    (AUDIT / "selection.json").write_text(
        json.dumps(
            {
                "selection": "All question cases with no exposure in either final dense or hybrid trajectory",
                "case_count": len(cases),
                "cases": cases,
                "assignment": "Lexically sorted case IDs, round-robin into four shards",
                "review_model": "gpt-5.6-luna",
                "labels_frozen": True,
            },
            indent=2,
        )
    )
    print(json.dumps({"cases": len(cases), "packets": 4}))


if __name__ == "__main__":
    main()
