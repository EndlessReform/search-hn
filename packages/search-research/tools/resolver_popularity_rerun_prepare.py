"""Prepare the complete fixed-boost rerun, preserving the original checkpoint.

The new SQLite database uses rollback journaling so the review server can read
committed results while the selector writes. Original scores and selections are
retained for comparison. No retrieval or reranker model calls are repeated.
"""

import json
import math
import random
import sqlite3
from pathlib import Path

import duckdb

ROOT = Path("data/research/books-resolver-popularity-rerun-v1")
SOURCE = Path("data/research/books-resolver-2025-v1/checkpoint.sqlite")


def main():
    ROOT.mkdir(parents=True, exist_ok=True)
    target = ROOT / "checkpoint.sqlite"
    assert not target.exists(), "Rerun already prepared; resume the selector instead"
    source = sqlite3.connect(SOURCE.resolve().as_uri() + "?immutable=1", uri=True)
    db = sqlite3.connect(target)
    source.backup(db)
    source.close()
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("DELETE FROM selections WHERE model != 'luna'")
    db.execute("UPDATE selections SET model='luna-original'")
    counts = dict(
        duckdb.sql(
            "SELECT work_id,readinglog_count FROM '/tmp/resolver-reading-log-counts.parquet'"
        ).fetchall()
    )
    docs = {k: json.loads(p) for k, p in db.execute("SELECT id,payload FROM documents")}
    for key, doc in docs.items():
        doc["readinglog_count"] = counts.get(key, 0)
        db.execute("UPDATE documents SET payload=? WHERE id=?", (json.dumps(doc), key))
    cases = []
    for ident, payload, ref_payload in db.execute(
        "SELECT k.id,k.payload,r.payload FROM rankings k JOIN refs r ON r.id=k.id ORDER BY r.ordinal"
    ).fetchall():
        ranking = json.loads(payload)
        ranking["original_scores"] = ranking["scores"]
        ranking["original_top3"] = ranking["top3"]
        ranking["scores"] = [
            s + 0.1 * math.log2(1 + counts.get(k, 0))
            for k, s in zip(ranking["ids"], ranking["scores"], strict=True)
        ]
        ranking["top3"] = sorted(
            ranking["ids"], key=lambda k: -ranking["scores"][ranking["ids"].index(k)]
        )[:3]
        db.execute(
            "UPDATE rankings SET payload=? WHERE id=?", (json.dumps(ranking), ident)
        )
        ref = json.loads(ref_payload)
        if ranking["ids"]:
            cases.append(
                {
                    "id": ident,
                    "reference": ref,
                    "candidates": [docs[c["id"]] for c in ref["candidates"]],
                    "shortlists": {"boost": ranking["top3"]},
                }
            )
        else:
            db.execute(
                "INSERT INTO selections(id,model,payload) VALUES (?,?,?)",
                (
                    ident,
                    "luna",
                    json.dumps(
                        {
                            "selection": {"work_id": None},
                            "request": {"messages": []},
                            "reason": "No retrieved candidates",
                        }
                    ),
                ),
            )
    cases = random.Random(20260926).sample(cases, 5000)
    keep = {c["id"] for c in cases}
    for (ident,) in db.execute("SELECT id FROM refs").fetchall():
        if ident not in keep:
            for table in ("refs", "rankings", "selections"):
                db.execute(f"DELETE FROM {table} WHERE id=?", (ident,))
    db.commit()
    db.close()
    (ROOT / "cases.json").write_text(
        json.dumps(
            {
                "cases": cases,
                "formula": "reranker_score + 0.1 * log2(1 + readinglog_count)",
            }
        )
    )
    print(json.dumps({"model_requests": len(cases), "checkpoint": str(target)}))


if __name__ == "__main__":
    main()
