"""Measure work recovery separately from ranking quality on the iteration set.

Alternative IDs represent the same target, so only the first correct ID earns
NDCG gain. The 49 non-single-work/uncertain/nonbook cases stay outside this
forced-choice score; their outputs remain available for inspection.
"""

import argparse
import json
import math
import statistics
from pathlib import Path


def main():
    """Require complete scoring runs before emitting comparable summaries."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    out = args.root / "data/probes/books-resolver-iteration-v1"
    fixture = json.loads(
        (
            args.root
            / "packages/search-research/docs/comment-classification/assets/resolver-iteration-v1.json"
        ).read_text()
    )
    candidates = json.loads((out / "candidates.json").read_text())
    outputs = {}
    latency = []
    for model in ["bge", "zerank"]:
        records = [json.loads(line) for line in (out / f"{model}-scores.jsonl").open()]
        runtime = json.loads((out / f"{model}-runtime.json").read_text())
        indexed = {(r["sample_id"], r["mode"], r["depth"]): r for r in records}
        assert len(indexed) == len(records), "Duplicate score receipts"
        outputs[model] = indexed
        for mode in ["title", "context"]:
            assert all((i, mode, 100) in indexed for i in range(250)), (
                "Incomplete scoring run"
            )
            for depth in [20, 50, 100]:
                times = sorted(
                    indexed[(i, mode, depth)]["milliseconds"]
                    for i in runtime["latency_ids"]
                )
                latency.append(
                    {
                        "model": model,
                        "mode": mode,
                        "depth": depth,
                        "n": len(times),
                        "median_ms": statistics.median(times),
                        "p95_ms": times[math.ceil(0.95 * len(times)) - 1],
                        "mean_ms": statistics.mean(times),
                    }
                )
    metrics = []
    for source in ["all", "fresh300"]:
        rows = [
            s
            for s in fixture["rows"]
            if s["judgment"]["status"] in ["matched", "none_of_candidates"]
            and (source == "all" or s["source"] == source)
        ]
        for depth in [20, 50, 100]:
            for model in ["bm25", "bge", "zerank"]:
                for mode in ["title"] if model == "bm25" else ["title", "context"]:
                    ranks = []
                    for sample in rows:
                        i = sample["sample_id"]
                        ids = [c["id"] for c in candidates[i]["candidates"][:depth]]
                        if model != "bm25":
                            record = outputs[model][(i, mode, 100)]
                            scores = dict(
                                zip(record["ids"], record["scores"], strict=True)
                            )
                            assert all(math.isfinite(scores[k]) for k in ids)
                            ids.sort(key=lambda k: -scores[k])
                        rank = next(
                            (
                                n
                                for n, key in enumerate(ids, 1)
                                if key in sample["acceptable_ids"]
                            ),
                            None,
                        )
                        ranks.append(rank)
                    row = {
                        "source": source,
                        "depth": depth,
                        "model": model,
                        "mode": mode,
                        "n": len(rows),
                        "candidate_recall": sum(r is not None for r in ranks),
                    }
                    for k in [1, 3, 7, 10]:
                        row[f"hit{k}"] = sum(r is not None and r <= k for r in ranks)
                        row[f"ndcg{k}"] = sum(
                            1 / math.log2(r + 1)
                            for r in ranks
                            if r is not None and r <= k
                        ) / len(rows)
                    metrics.append(row)
    (out / "metrics.json").write_text(json.dumps(metrics, indent=2))
    (out / "latency.json").write_text(json.dumps(latency, indent=2))
    print(json.dumps([m for m in metrics if m["source"] == "all"], indent=2))


if __name__ == "__main__":
    main()
