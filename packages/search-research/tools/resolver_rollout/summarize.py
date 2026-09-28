"""Summarize completion, emitted links and paired agreement without claiming accuracy.

Only Luna and native DeepSeek form the final paired dataset. Earlier third-party
DeepSeek V4 labels remain available under their original model key for inspection.
"""

import json
import statistics
from collections import Counter

from common import RUN, connect


def main():
    db = connect()
    total = db.execute("SELECT count(*) FROM refs").fetchone()[0]
    models = {}
    picks = {}
    for model in ("luna", "deepseek-native"):
        rows = [
            json.loads(p)
            for (p,) in db.execute(
                "SELECT payload FROM selections WHERE model=?", (model,)
            )
        ]
        selected = {r["id"]: r["selection"]["work_id"] for r in rows}
        picks[model] = selected
        latency = sorted(r["seconds"] for r in rows)
        models[model] = {
            "completed": len(rows),
            "missing": total - len(rows),
            "selected": sum(v is not None for v in selected.values()),
            "abstained": sum(v is None for v in selected.values()),
            "unique_selected_work_ids": len(
                {v for v in selected.values() if v is not None}
            ),
            "median_seconds": statistics.median(latency) if latency else None,
            "p95_seconds": latency[int((len(latency) - 1) * 0.95)] if latency else None,
            "outstanding_failures": db.execute(
                "SELECT count(*) FROM failures WHERE stage=?", (model,)
            ).fetchone()[0],
        }
    paired = picks["luna"].keys() & picks["deepseek-native"].keys()
    counts = Counter()
    for ident in paired:
        a, b = picks["luna"][ident], picks["deepseek-native"][ident]
        label = (
            "both_abstain"
            if a is None and b is None
            else "same_work_id"
            if a == b
            else "luna_only"
            if b is None
            else "deepseek_only"
            if a is None
            else "different_work_ids"
        )
        counts[label] += 1
    result = {
        "references": total,
        "ranked": db.execute("SELECT count(*) FROM rankings").fetchone()[0],
        "no_candidates": db.execute(
            "SELECT count(*) FROM rankings WHERE json_array_length(json_extract(payload,'$.ids'))=0"
        ).fetchone()[0],
        "models": models,
        "paired": len(paired),
        "agreement": dict(counts),
        "accuracy": "Not measured: these are model proposals without independent full-set judgments.",
    }
    (RUN / "summary.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
