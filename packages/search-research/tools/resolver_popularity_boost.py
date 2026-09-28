"""Sweep an additive log-reader boost over saved scores, without grouping IDs.

The weight has reranker-logit units per doubling of (1 + reading-log rows).
This is a diagnostic sensitivity sweep, not a fitted or validated ranking model.
All 50 candidates remain independent. Original search order breaks exact ties.
"""

import json
import math
from pathlib import Path

ROOT = Path("data/research/books-resolver-popularity-pilot-v1")
WEIGHTS = [0, 0.05, 0.1, 0.25, 0.5, 1, 2, 4]


def sweep(cases):
    results = []
    for weight in WEIGHTS:
        rows = []
        for case in cases:
            ranked = []
            for doc in case["candidates"]:
                count = doc["popularity"]["readinglog_count"]
                assert isinstance(count, int) and count >= 0, (
                    "Complete snapshot counts required"
                )
                boost = weight * math.log2(1 + count)
                ranked.append(
                    {
                        k: doc[k]
                        for k in (
                            "id",
                            "title",
                            "authors",
                            "rerank_score",
                            "search_rank",
                        )
                    }
                    | {
                        "reader_count": count,
                        "boost": boost,
                        "score": doc["rerank_score"] + boost,
                    }
                )
            ranked.sort(key=lambda d: (-d["score"], d["search_rank"]))
            if weight == 0:
                assert [d["id"] for d in ranked[:3]] == case["ranking"]["top3"]
            rows.append(
                {
                    "id": case["id"],
                    "mention": case["reference"]["title"],
                    "top3": ranked[:3],
                }
            )
        results.append({"weight": weight, "cases": rows})
    return results


def main():
    data = json.loads((ROOT / "cases.json").read_text())
    results = sweep(data["cases"])
    baseline = {r["id"]: [d["id"] for d in r["top3"]] for r in results[0]["cases"]}
    for result in results:
        result["changed_first"] = sum(
            r["top3"][0]["id"] != baseline[r["id"]][0] for r in result["cases"]
        )
        result["changed_top3_set"] = sum(
            {d["id"] for d in r["top3"]} != set(baseline[r["id"]])
            for r in result["cases"]
        )
        print(json.dumps({k: v for k, v in result.items() if k != "cases"}))
        for r in result["cases"]:
            print(
                r["mention"],
                " / ".join(
                    f"{d['title']} ({'; '.join(d['authors'])}; {d['reader_count']} readers; {d['id']})"
                    for d in r["top3"]
                ),
            )
    output = {
        "formula": "rerank_score + weight * log2(1 + readinglog_count)",
        "grouping": False,
        "sample_method": data["sample_method"],
        "readinglog_source": data["readinglog_source"],
        "sweep": results,
    }
    (ROOT / "boost-sweep.json").write_text(json.dumps(output, indent=2) + "\n")


if __name__ == "__main__":
    main()
