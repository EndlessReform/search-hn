"""Freeze blinded review packets for every differing final choice.

Reviewers see selections A/B with per-case random assignment, never method names.
Identical decisions are counted separately; they cannot decide the comparison.
"""

import json
import random
from pathlib import Path

ROOT = Path("data/research/books-resolver-popularity-bakeoff-v1")


def main():
    data = json.loads((ROOT / "cases.json").read_text())
    receipts = [
        json.loads(l) for l in (ROOT / "receipts.jsonl").read_text().splitlines()
    ]
    outputs = {
        (r["id"], r["method"]): r["selection"]["work_id"]
        for r in receipts
        if "selection" in r
    }
    assert len(outputs) == len(data["cases"]) * 2, (
        "Finish both calls for every case before final comparison"
    )
    rng = random.Random(20260929)
    review = []
    mapping = {}
    same = 0
    both_null = 0
    for c in data["cases"]:
        a, b = outputs[c["id"], "boost"], outputs[c["id"], "substitution"]
        if a == b:
            same += 1
            both_null += a is None
            continue
        methods = ["boost", "substitution"]
        rng.shuffle(methods)
        mapping[c["id"]] = dict(zip(["A", "B"], methods, strict=True))
        docs = {d["id"]: d for d in c["candidates"]}
        selected = {
            label: outputs[c["id"], method]
            for label, method in mapping[c["id"]].items()
        }
        review.append(
            {
                "id": c["id"],
                "reference": c["reference"],
                "choices": {
                    label: None
                    if key is None
                    else {k: docs[key][k] for k in ("id", "title", "authors")}
                    for label, key in selected.items()
                },
            }
        )
    for i in range(3):
        (ROOT / f"review-{i + 1}.json").write_text(
            json.dumps(review[i::3], indent=2) + "\n"
        )
    (ROOT / "review-mapping.json").write_text(json.dumps(mapping, indent=2) + "\n")
    summary = {
        "cases": len(data["cases"]),
        "identical_final_choices": same,
        "both_abstain": both_null,
        "differing_final_choices": len(review),
        "recorded_cost_usd": sum(
            r.get("response", {}).get("usage", {}).get("cost", 0) for r in receipts
        ),
        "requests": len(receipts),
    }
    (ROOT / "outcome-counts.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
