"""Summarize a bounded pilot without treating ID convergence as accuracy."""

import json
from pathlib import Path

ROOT = Path("data/research/books-resolver-popularity-pilot-v1")


def main():
    cases = json.loads((ROOT / "cases.json").read_text())["cases"]
    receipts = [
        json.loads(line)
        for line in (ROOT / "selector-receipts.jsonl").read_text().splitlines()
    ]
    rerank = json.loads((ROOT / "rerank-results.json").read_text())
    rows = []
    for case in cases:
        docs = {d["id"]: d for d in case["candidates"]}
        selected = {
            r["arm"]: r["selection"]["work_id"]
            for r in receipts
            if r["id"] == case["id"] and "selection" in r
        }
        scores = {
            version: sorted(
                [
                    r
                    for r in rerank["records"]
                    if r["case_id"] == case["id"] and r["variant"] == version
                ],
                key=lambda r: -r["score"],
            )
            for version in ("baseline", "popularity")
        }

        def explain(key, docs=docs):
            if key is None:
                return None
            d = docs[key]
            return {k: d[k] for k in ("id", "title", "authors", "popularity")}

        rows.append(
            {
                "id": case["id"],
                "mention": case["reference"]["title"],
                "luna": {k: explain(v) for k, v in selected.items()},
                "reranker": {
                    v: [
                        explain(r["work_id"]) | {"score": r["score"]}
                        for r in ordered[:3]
                    ]
                    for v, ordered in scores.items()
                },
                "grouping": {
                    k: {"groups": len(v["groups"]), "top3": v["top3"]}
                    for k, v in case["algorithms"].items()
                },
            }
        )
    result = {
        "sample": "10 deliberately selected diagnostic cases, not a representative accuracy sample",
        "group_counts": {
            rule: sum(len(c["algorithms"][rule]["groups"]) for c in cases)
            for rule in ("exact", "normalized", "author_tokens")
        },
        "luna_changed_from_fresh_control": {
            version: sum(r["luna"][version] != r["luna"]["control"] for r in rows)
            for version in ("popularity", "grouped_popularity")
        },
        "reranker_changed_top1": sum(
            r["reranker"]["baseline"][0]["id"] != r["reranker"]["popularity"][0]["id"]
            for r in rows
        ),
        "reranker_changed_top3_set": sum(
            {d["id"] for d in r["reranker"]["baseline"]}
            != {d["id"] for d in r["reranker"]["popularity"]}
            for r in rows
        ),
        "luna_recorded_cost_usd": sum(
            r["response"].get("usage", {}).get("cost", 0) for r in receipts
        ),
        "deepseek_status": "Provider returned 402 Payment Required on initial probe; no new successful outputs",
        "cases": rows,
    }
    (ROOT / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k != "cases"}, indent=2))


if __name__ == "__main__":
    main()
