"""Publish completed two-pass results as a separate review checkpoint.

The comparison baseline is the completed popularity/Bible run. First-pass
requests and rankings remain inspectable when a title retry replaces candidates.
Original comments and marked offsets are never rewritten.
"""

import json
import os
import sqlite3
from pathlib import Path

ROOT = Path(
    os.environ.get("RESOLVER_RUN_ROOT", "data/research/books-resolver-retry-v1")
)


def decisions(stage):
    return {
        r["id"]: r
        for l in (ROOT / f"{stage}-decisions.jsonl").open()
        if "decision" in (r := json.loads(l))
    }


def main():
    first = {
        c["id"]: c for c in json.loads((ROOT / "first-ready.json").read_text())["cases"]
    }
    second = {
        c["id"]: c for c in json.loads((ROOT / "retry-ready.json").read_text())["cases"]
    }
    a, b = decisions("first"), decisions("retry")
    assert (
        len(first) == len(a) == len(json.loads((ROOT / "references.json").read_text()))
    )
    expected = {k for k, v in a.items() if v["decision"]["action"] == "retry"}
    assert expected == second.keys() == b.keys()
    target = ROOT / "checkpoint.sqlite"
    assert not target.exists(), "Checkpoint already published"
    old = sqlite3.connect(
        "file:data/research/books-resolver-popularity-rerun-v1/checkpoint.sqlite?mode=ro",
        uri=True,
    )
    db = sqlite3.connect(target)
    db.executescript(
        "CREATE TABLE refs(id TEXT PRIMARY KEY,ordinal INTEGER,payload TEXT); CREATE TABLE documents(id TEXT PRIMARY KEY,payload TEXT); CREATE TABLE rankings(id TEXT PRIMARY KEY,payload TEXT); CREATE TABLE selections(id TEXT,model TEXT,payload TEXT,PRIMARY KEY(id,model)); CREATE TABLE history(id TEXT PRIMARY KEY,payload TEXT);"
    )
    for ordinal, (ident, c) in enumerate(first.items()):
        final = second.get(ident, c)
        receipt = b.get(ident, a[ident])
        decision = receipt["decision"]
        assert decision["action"] != "retry"
        ref = final["reference"] | {
            "candidates": final["retrieved"],
            "person_spans": final["person_spans"],
            "query_title": final["query_title"],
            "retry_title": a[ident]["decision"]["rewritten_title"],
            "author_bonus": final["author_bonus"],
        }
        ds = final["candidates"]
        ranking = {
            "ids": [d["id"] for d in ds],
            "scores": [d["score"] for d in ds],
            "original_scores": [d["raw_score"] for d in ds],
            "top3": final["top3"],
        }
        baseline = json.loads(
            old.execute(
                "SELECT payload FROM selections WHERE id=? AND model='luna'", (ident,)
            ).fetchone()[0]
        )
        baseline_id = baseline["selection"]["work_id"]
        if baseline_id and baseline_id != "special:bible":
            payload = old.execute(
                "SELECT payload FROM documents WHERE id=?", (baseline_id,)
            ).fetchone()[0]
            db.execute(
                "INSERT OR REPLACE INTO documents VALUES (?,?)", (baseline_id, payload)
            )
        for d in ds:
            db.execute(
                "INSERT OR REPLACE INTO documents VALUES (?,?)",
                (d["id"], json.dumps(d)),
            )
        db.execute("INSERT INTO refs VALUES (?,?,?)", (ident, ordinal, json.dumps(ref)))
        db.execute("INSERT INTO rankings VALUES (?,?)", (ident, json.dumps(ranking)))
        db.execute(
            "INSERT INTO selections VALUES (?,?,?)",
            (ident, "luna-original", json.dumps(baseline)),
        )
        db.execute(
            "INSERT INTO selections VALUES (?,?,?)",
            (
                ident,
                "luna",
                json.dumps(receipt | {"selection": {"work_id": decision["work_id"]}}),
            ),
        )
        db.execute(
            "INSERT INTO history VALUES (?,?)",
            (
                ident,
                json.dumps(
                    {
                        "first": c,
                        "first_decision": a[ident]["decision"],
                        "final_decision": decision,
                    }
                ),
            ),
        )
    db.commit()
    db.close()
    print(f"Published {len(first)} cases")


if __name__ == "__main__":
    main()
