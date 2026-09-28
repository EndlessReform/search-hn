"""Commit each paid response before dispatching more work; export for later stages."""

import hashlib
import json
import sqlite3


def fingerprint(body):
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


class Journal:
    def __init__(self, root):
        self.root = root
        self.db = sqlite3.connect(root / "receipts.sqlite")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS receipts(round INTEGER,id TEXT,attempt INTEGER,"
            "fingerprint TEXT,payload TEXT,PRIMARY KEY(round,id,attempt))"
        )
        self.total_cost = self.db.execute(
            "SELECT coalesce(sum(json_extract(payload,'$.response.usage.cost')),0) FROM receipts"
        ).fetchone()[0]

    def records(self, number):
        return [
            json.loads(r[0])
            for r in self.db.execute(
                "SELECT payload FROM receipts WHERE round=? ORDER BY rowid", (number,)
            )
        ]

    def save(self, record):
        with self.db:
            self.db.execute(
                "INSERT INTO receipts VALUES(?,?,?,?,?)",
                (
                    record["round"],
                    record["id"],
                    record["attempt"],
                    fingerprint(record["request"]),
                    json.dumps(record),
                ),
            )
        self.total_cost += (
            record.get("response", {}).get("usage", {}).get("cost", 0) or 0
        )

    def cost(self):
        return self.total_cost

    def export(self, number):
        target = self.root / f"round{number}-decisions.jsonl"
        temporary = target.with_suffix(".jsonl.tmp")
        with temporary.open("w") as out:
            for record in self.records(number):
                out.write(json.dumps(record) + "\n")
        temporary.replace(target)
