"""Publish source-level groups without losing split targets, retries, or abstentions."""

import json
import os
import sqlite3
from pathlib import Path

import duckdb
from search_research.resolver_counts import COUNTS_PATH

ROOT = Path(os.environ.get("RESOLVER_RUN_ROOT", "data/research/books-resolver-heal-v1"))


def copy_baseline_documents(old, db, counts_path):
    """Copy comparison-only works and obtain counts from the frozen count table.

    The older checkpoint predates popularity metadata. Current candidates already
    carry counts; preserve those rows and fetch counts only for missing work IDs.
    An absent row in the count table means zero, as in selector preparation.
    """
    documents = {}
    for (key,) in old.execute(
        "SELECT DISTINCT json_extract(payload,'$.selection.work_id') "
        "FROM selections WHERE model='luna'"
    ):
        if not key or key == "special:bible":
            continue
        if db.execute("SELECT 1 FROM documents WHERE id=?", (key,)).fetchone():
            continue
        payload = old.execute(
            "SELECT payload FROM documents WHERE id=?", (key,)
        ).fetchone()[0]
        documents[key] = json.loads(payload)
    if not documents:
        return
    with duckdb.connect() as counts_db:
        counts = dict(
            counts_db.execute(
                "SELECT work_id,readinglog_count FROM read_parquet(?) "
                "WHERE work_id IN (SELECT unnest(?))",
                [str(counts_path), list(documents)],
            ).fetchall()
        )
    for key, doc in documents.items():
        payload = {k: doc[k] for k in ("id", "title", "authors")}
        payload["readinglog_count"] = counts.get(key, 0)
        db.execute("INSERT INTO documents VALUES (?,?)", (key, json.dumps(payload)))


def main():
    cases = {}
    receipts = {}
    links = {}
    for number in range(3):
        path = ROOT / f"round{number}-ready.json"
        if not path.exists():
            continue
        current = {c["id"]: c for c in json.loads(path.read_text())["cases"]}
        results = {
            r["id"]: r
            for l in (ROOT / f"round{number}-decisions.jsonl").open()
            if "decision" in (r := json.loads(l))
        }
        assert current.keys() == results.keys(), f"Incomplete round {number}"
        cases.update(current)
        receipts.update(results)
        queries = ROOT / f"round{number + 1}-queries.json"
        if queries.exists():
            for link in json.loads(queries.read_text())["links"]:
                links[(link["parent_id"], link["result_index"])] = link["child_id"]
    roots = json.loads((ROOT / "round0-ready.json").read_text())["cases"]

    def flatten(ident):
        output = []
        for i, r in enumerate(receipts[ident]["decision"]["results"]):
            if r["action"] == "search":
                child = links[(ident, i)]
                if child is not None:
                    output.extend(flatten(child))
                else:
                    output.append(
                        r
                        | {
                            "action": "unresolved",
                            "reason": r["reason"]
                            + "; identical query already attempted",
                            "case_id": ident,
                        }
                    )
            else:
                output.append(r | {"case_id": ident})
        return output

    baseline_path = os.environ.get("RESOLVER_BASELINE")
    old = (
        sqlite3.connect(Path(baseline_path).resolve().as_uri() + "?mode=ro", uri=True)
        if baseline_path
        else None
    )
    target = ROOT / "checkpoint.sqlite"
    assert not target.exists(), "Already published"
    temporary = target.with_suffix(".sqlite.tmp")
    if temporary.exists():
        temporary.unlink()
    db = sqlite3.connect(temporary)
    db.executescript(
        "CREATE TABLE refs(id TEXT PRIMARY KEY,ordinal INTEGER,payload TEXT); CREATE TABLE documents(id TEXT PRIMARY KEY,payload TEXT); CREATE TABLE rankings(id TEXT PRIMARY KEY,payload TEXT); CREATE TABLE selections(id TEXT,model TEXT,payload TEXT,PRIMARY KEY(id,model)); CREATE TABLE history(id TEXT PRIMARY KEY,payload TEXT);"
    )
    for c in cases.values():
        for d in c["candidates"]:
            doc = {k: d[k] for k in ("id", "title", "authors", "readinglog_count")}
            db.execute(
                "INSERT OR REPLACE INTO documents VALUES (?,?)",
                (d["id"], json.dumps(doc)),
            )
    if old is not None:
        copy_baseline_documents(old, db, COUNTS_PATH)
    for ordinal, c in enumerate(roots):
        ident = c["id"]
        items = flatten(ident)
        ids = list(
            dict.fromkeys(r["work_id"] for r in items if r["action"] == "select")
        )
        children = [v for v in cases.values() if v.get("root_id") == ident]
        ref = c["reference"] | {
            "candidates": c["retrieved"],
            "person_spans": c["person_spans"],
            "query_title": c["query_title"],
            "retry_title": None,
            "author_bonus": c["author_bonus"],
            "resolution_items": items,
            "repair_count": len(children),
        }
        ds = c["candidates"]
        ranking = {
            "ids": [d["id"] for d in ds],
            "scores": [d["score"] for d in ds],
            "original_scores": [d["raw_score"] for d in ds],
            "top3": c["top3"],
        }
        db.execute("INSERT INTO refs VALUES (?,?,?)", (ident, ordinal, json.dumps(ref)))
        db.execute("INSERT INTO rankings VALUES (?,?)", (ident, json.dumps(ranking)))
        if old is not None:
            row = old.execute(
                "SELECT payload FROM selections WHERE id=? AND model='luna'", (ident,)
            ).fetchone()
            assert row is not None, f"Baseline lacks reference {ident}"
            db.execute(
                "INSERT INTO selections VALUES (?,?,?)",
                (ident, "luna-original", row[0]),
            )
        db.execute(
            "INSERT INTO selections VALUES (?,?,?)",
            (
                ident,
                "luna",
                json.dumps(
                    receipts[ident]
                    | {"selection": {"work_ids": ids}, "resolution_items": items}
                ),
            ),
        )
        db.execute(
            "INSERT INTO history VALUES (?,?)",
            (
                ident,
                json.dumps(
                    {
                        "initial_decision": receipts[ident]["decision"],
                        "repairs": [
                            {"case": v, "receipt": receipts[v["id"]]} for v in children
                        ],
                    }
                ),
            ),
        )
    db.commit()
    db.close()
    if old is not None:
        old.close()
    temporary.replace(target)
    print("Published", len(roots), "source cases")


if __name__ == "__main__":
    main()
