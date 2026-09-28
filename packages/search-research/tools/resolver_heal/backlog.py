"""Materialize all distinct repair requests with source and parent provenance."""

import argparse
import json
import os
from pathlib import Path

from schema import child_case, query_key

ROOT = Path(os.environ.get("RESOLVER_RUN_ROOT", "data/research/books-resolver-heal-v1"))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--round", type=int, required=True)
    args = p.parse_args()
    cases = json.loads((ROOT / f"round{args.round}-ready.json").read_text())["cases"]
    decisions = {
        r["id"]: r["decision"]
        for l in (ROOT / f"round{args.round}-decisions.jsonl").open()
        if "decision" in (r := json.loads(l))
    }
    assert decisions.keys() == {c["id"] for c in cases}, "Incomplete selector round"
    seen = {}
    for number in range(1, args.round + 1):
        for c in json.loads((ROOT / f"round{number}-ready.json").read_text())["cases"]:
            seen[
                (
                    c.get("root_id", c["id"]),
                    query_key(c["query_title"], c.get("query_author")),
                )
            ] = c
    queued = {}
    reused = {}
    links = []
    for c in cases:
        for i, result in enumerate(decisions[c["id"]]["results"]):
            if result["action"] != "search":
                continue
            child = child_case(c, result, args.round + 1)
            key = (
                child["root_id"],
                query_key(child["query_title"], child["query_author"]),
            )
            duplicate = key in seen
            if duplicate:
                previous = seen[key]
                final_id = previous["id"] + ":final"
                reused[final_id] = previous | {
                    "id": final_id,
                    "parent_id": c["id"],
                    "round": args.round + 1,
                    "reused_candidates": True,
                }
            links.append(
                {
                    "parent_id": c["id"],
                    "result_index": i,
                    "child_id": final_id if duplicate else child["id"],
                    "status": "final_decision_existing_candidates"
                    if duplicate
                    else "queued",
                }
            )
            if not duplicate:
                queued[child["id"]] = child
    (ROOT / f"round{args.round + 1}-queries.json").write_text(
        json.dumps({"cases": list(queued.values()), "links": links})
    )
    (ROOT / f"round{args.round + 1}-reused.json").write_text(
        json.dumps({"cases": list(reused.values())})
    )
    print(
        json.dumps(
            {
                "round": args.round + 1,
                "queries": len(queued),
                "reused_candidate_lists": len(reused),
            }
        )
    )


if __name__ == "__main__":
    main()
