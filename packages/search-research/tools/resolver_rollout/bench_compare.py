"""Summarize throughput and candidate-ranking agreement from saved scores."""

import json
from pathlib import Path

import numpy as np

OUT = Path("data/research/books-resolver-throughput-v1")


def main():
    for dataset in ["corpus128", "gold"]:
        data = json.loads((OUT / f"{dataset}.json").read_text())
        reference = OUT / f"{dataset}-bf16-original.json"
        if dataset == "gold":
            receipts = {
                r["sample_id"]: r
                for r in map(
                    json.loads,
                    Path("data/probes/books-resolver-iteration-v1/zerank-scores.jsonl")
                    .read_text()
                    .splitlines(),
                )
                if r["depth"] == 100 and r["mode"] == "context"
            }
            baseline = [
                dict(
                    zip(
                        receipts[int(p["ref"])]["ids"],
                        receipts[int(p["ref"])]["scores"],
                    )
                )[p["work"]]
                for p in data["pairs"]
            ]
        else:
            if not reference.exists():
                continue
            baseline = json.loads(reference.read_text())["scores"]
        for path in sorted(OUT.glob(f"{dataset}-*.json")):
            result = json.loads(path.read_text())
            if "scores" not in result:
                continue
            scores = result["scores"]
            assert len(scores) == len(baseline)
            top1 = top3 = eligible = correct = hit3 = 0
            for row, group in zip(data["rows"], data["groups"], strict=True):
                if not group:
                    continue
                a = sorted(group, key=lambda j: -baseline[j])
                b = sorted(group, key=lambda j: -scores[j])
                top1 += a[0] == b[0]
                top3 += set(a[:3]) == set(b[:3])
                if dataset == "gold" and row["judgment"]["status"] in [
                    "matched",
                    "none_of_candidates",
                ]:
                    eligible += 1
                    correct += data["pairs"][b[0]]["work"] in row["acceptable_ids"]
                    hit3 += any(
                        data["pairs"][j]["work"] in row["acceptable_ids"] for j in b[:3]
                    )
            delta = np.abs(np.array(scores) - np.array(baseline))
            summary = {
                "file": path.name,
                "seconds": result["seconds"],
                "pairs_per_second": result["pairs_per_second"],
                "top1_same": top1,
                "top3_set_same": top3,
                "nonempty_refs": sum(bool(g) for g in data["groups"]),
                "score_delta_mean": float(delta.mean()),
                "score_delta_p99": float(np.quantile(delta, 0.99)),
                "score_delta_max": float(delta.max()),
                "labeled_top1": correct,
                "labeled_top3": hit3,
            }
            print(json.dumps(summary))


if __name__ == "__main__":
    main()
