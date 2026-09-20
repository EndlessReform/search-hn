"""Durable labeled sets and reproducible background samples, outside the corpus.

A store belongs to one frozen corpus. Its identity includes the input manifest and
checkpoint hashes, so reusing a filename for different vectors fails explicitly.
Each edit increments a set revision for ranking-cache invalidation.
"""

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path


class AnnotationStore:
    def __init__(self, path: Path, corpus_id: str):
        self.path = path
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS corpus(identity TEXT PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS sets(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE,
                    revision INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS members(
                    set_id INTEGER REFERENCES sets(id) ON DELETE CASCADE,
                    comment_id INTEGER NOT NULL, PRIMARY KEY(set_id,comment_id));
                CREATE TABLE IF NOT EXISTS negatives(
                    set_id INTEGER REFERENCES sets(id) ON DELETE CASCADE,
                    comment_id INTEGER NOT NULL, note TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY(set_id,comment_id));
                CREATE TABLE IF NOT EXISTS baselines(
                    seed INTEGER PRIMARY KEY, comment_ids TEXT NOT NULL);
            """)
            row = db.execute("SELECT identity FROM corpus").fetchone()
            if row is None:
                db.execute("INSERT INTO corpus VALUES (?)", (corpus_id,))
            elif row[0] != corpus_id:
                raise ValueError(
                    "Annotation database belongs to a different frozen corpus"
                )

    @contextmanager
    def connect(self):
        """One short transaction per operation, with enforced member ownership."""
        db = sqlite3.connect(self.path)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            with db:
                yield db
        finally:
            db.close()

    def list_sets(self):
        with self.connect() as db:
            return [
                dict(row)
                for row in db.execute("""
                SELECT s.*,count(m.comment_id) AS count,
                (SELECT count(*) FROM negatives n WHERE n.set_id=s.id) AS negative_count
                FROM sets s
                LEFT JOIN members m ON m.set_id=s.id GROUP BY s.id ORDER BY s.id
            """)
            ]

    def get(self, set_id):
        with self.connect() as db:
            row = db.execute("SELECT * FROM sets WHERE id=?", (set_id,)).fetchone()
            if row is None:
                raise KeyError("Set does not exist")
            ids = [
                r[0]
                for r in db.execute(
                    "SELECT comment_id FROM members WHERE set_id=? ORDER BY comment_id",
                    (set_id,),
                )
            ]
            negatives = [dict(r) for r in db.execute(
                "SELECT comment_id,note FROM negatives WHERE set_id=? ORDER BY comment_id",
                (set_id,),
            )]
            return dict(row) | {"comment_ids": ids, "count": len(ids),
                                "negatives": negatives, "negative_count": len(negatives)}

    def create(self, name):
        with self.connect() as db:
            return db.execute("INSERT INTO sets(name) VALUES (?)", (name,)).lastrowid

    def rename(self, set_id, name):
        with self.connect() as db:
            if not db.execute(
                "UPDATE sets SET name=? WHERE id=?", (name, set_id)
            ).rowcount:
                raise KeyError("Set does not exist")

    def delete(self, set_id):
        with self.connect() as db:
            if not db.execute("DELETE FROM sets WHERE id=?", (set_id,)).rowcount:
                raise KeyError("Set does not exist")

    def member(self, set_id, comment_id, positive):
        """Idempotent membership edits only invalidate rankings on actual changes."""
        with self.connect() as db:
            if (
                db.execute("SELECT id FROM sets WHERE id=?", (set_id,)).fetchone()
                is None
            ):
                raise KeyError("Set does not exist")
            if positive:
                db.execute("DELETE FROM negatives WHERE set_id=? AND comment_id=?",
                           (set_id, comment_id))
                changed = db.execute(
                    "INSERT OR IGNORE INTO members VALUES (?,?)", (set_id, comment_id)
                ).rowcount
            else:
                changed = db.execute(
                    "DELETE FROM members WHERE set_id=? AND comment_id=?",
                    (set_id, comment_id),
                ).rowcount
            if changed:
                db.execute("UPDATE sets SET revision=revision+1 WHERE id=?", (set_id,))

    def negative(self, set_id, comment_id, note):
        """Save a negative and optional rationale atomically, replacing a positive.

        Negative-only edits do not invalidate positive rankings. Converting a
        positive does, and takes effect on the next fresh Apply query.
        """
        with self.connect() as db:
            if db.execute("SELECT id FROM sets WHERE id=?", (set_id,)).fetchone() is None:
                raise KeyError("Set does not exist")
            changed = db.execute(
                "DELETE FROM members WHERE set_id=? AND comment_id=?",
                (set_id, comment_id),
            ).rowcount
            db.execute(
                "INSERT INTO negatives VALUES (?,?,?) ON CONFLICT(set_id,comment_id) "
                "DO UPDATE SET note=excluded.note", (set_id, comment_id, note),
            )
            if changed:
                db.execute("UPDATE sets SET revision=revision+1 WHERE id=?", (set_id,))

    def remove_negative(self, set_id, comment_id):
        with self.connect() as db:
            db.execute("DELETE FROM negatives WHERE set_id=? AND comment_id=?",
                       (set_id, comment_id))

    def baseline(self, seed, make_sample):
        """Persist actual IDs as well as the seed, independent of future RNG changes."""
        with self.connect() as db:
            row = db.execute(
                "SELECT comment_ids FROM baselines WHERE seed=?", (seed,)
            ).fetchone()
            if row is None:
                ids = make_sample()
                db.execute(
                    "INSERT INTO baselines VALUES (?,?)", (seed, json.dumps(ids))
                )
                return ids
            return json.loads(row[0])
