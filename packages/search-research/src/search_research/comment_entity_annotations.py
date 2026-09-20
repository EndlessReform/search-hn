"""Durable title review on frozen rollout batches, using the annotation ledger.

Original model predictions stay in the batch snapshot. Editable entities are
separate rows: deletion is reversible, manual additions retain their origin,
and an append-only event log records each change. Item revisions reject stale
browser writes instead of silently overwriting another review session.
"""

import json
import re
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from search_research.comment_rollout_store import now


def locate_title(text, title):
    """Find literal occurrences without marking short titles inside other words.

    No fuzzy matching or canonicalization: unmatched teacher titles remain visible
    as unaligned proposals for the reviewer to correct by selecting source text.
    """
    assert title.strip(), "Predicted title must be nonempty"
    pattern = re.escape(title)
    if re.match(r"\w", title[0]):
        pattern = r"(?<![^\W_])" + pattern
    if re.match(r"\w", title[-1]):
        pattern += r"(?![^\W_])"
    return [(m.start(), m.end()) for m in re.finditer(pattern, text, re.IGNORECASE)]


class EntityEdit(BaseModel):
    revision: int = Field(ge=0)
    action: Literal["add", "delete", "restore", "review", "unreview", "note"]
    note: str = Field(default="", max_length=20000)
    entity_id: int | None = None
    start: int | None = Field(default=None, ge=0)
    end: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def fields_for_action(self):
        if self.action == "add" and (
            self.start is None or self.end is None or self.end <= self.start
        ):
            raise ValueError("Select a nonempty title span in the comment")
        if self.action in ("delete", "restore") and self.entity_id is None:
            raise ValueError("Entity ID is required")
        return self


class StaleReview(ValueError):
    """The client must reload the latest item before editing again."""


class EntityAnnotationStore:
    """Reuse AnnotationStore transactions and rollout timestamps, not boolean labels.

    These tables share the corpus-bound annotation database. They intentionally
    do not insert extraction records into classifier_attempts: that ledger's
    acceptance and export contracts describe boolean classification.
    """

    def __init__(self, annotations):
        self.annotations = annotations
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS entity_batches(
                    id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL, snapshot_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS entity_items(
                    batch_id INTEGER NOT NULL REFERENCES entity_batches(id),
                    comment_id INTEGER NOT NULL, ordinal INTEGER NOT NULL,
                    text TEXT NOT NULL, split TEXT NOT NULL, source TEXT NOT NULL,
                    prediction_json TEXT NOT NULL, comparison_json TEXT NOT NULL,
                    reviewed INTEGER NOT NULL DEFAULT 0,
                    revision INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(batch_id,comment_id), UNIQUE(batch_id,ordinal));
                CREATE TABLE IF NOT EXISTS entity_labels(
                    id INTEGER PRIMARY KEY, batch_id INTEGER NOT NULL,
                    comment_id INTEGER NOT NULL, title TEXT NOT NULL,
                    author TEXT, start INTEGER, end INTEGER,
                    origin TEXT NOT NULL, deleted INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(batch_id,comment_id)
                        REFERENCES entity_items(batch_id,comment_id));
                CREATE TABLE IF NOT EXISTS entity_review_events(
                    id INTEGER PRIMARY KEY, batch_id INTEGER NOT NULL,
                    comment_id INTEGER NOT NULL, at TEXT NOT NULL,
                    action_json TEXT NOT NULL,
                    FOREIGN KEY(batch_id,comment_id)
                        REFERENCES entity_items(batch_id,comment_id));
                CREATE INDEX IF NOT EXISTS entity_labels_item
                    ON entity_labels(batch_id,comment_id);
            """)
            columns = {r[1] for r in db.execute("PRAGMA table_info(entity_items)")}
            if "note" not in columns:
                db.execute(
                    "ALTER TABLE entity_items ADD COLUMN note TEXT NOT NULL DEFAULT ''"
                )

    def connect(self):
        return self.annotations.connect()

    def batches(self):
        with self.connect() as db:
            return [
                dict(r)
                for r in db.execute("""
                SELECT b.id,b.name,b.created_at,count(i.comment_id) AS total,
                       coalesce(sum(i.reviewed),0) AS reviewed
                FROM entity_batches b LEFT JOIN entity_items i ON i.batch_id=b.id
                GROUP BY b.id ORDER BY b.id DESC""")
            ]

    def queue(self, batch_id):
        with self.connect() as db:
            return [
                dict(r)
                for r in db.execute(
                    """
                SELECT i.comment_id,i.ordinal,i.split,i.source,i.reviewed,
                    (length(trim(i.note))>0) AS has_notes,
                    (SELECT count(*) FROM entity_labels e WHERE
                     e.batch_id=i.batch_id AND e.comment_id=i.comment_id
                     AND e.deleted=0) AS entities
                FROM entity_items i WHERE batch_id=? ORDER BY ordinal""",
                    (batch_id,),
                )
            ]

    def item(self, batch_id, comment_id):
        with self.connect() as db:
            row = db.execute(
                "SELECT * FROM entity_items WHERE batch_id=? AND comment_id=?",
                (batch_id, comment_id),
            ).fetchone()
            if row is None:
                raise KeyError("Annotation comment not found")
            result = dict(row)
            result["prediction"] = json.loads(result.pop("prediction_json"))
            result["comparison"] = json.loads(result.pop("comparison_json"))
            result["entities"] = [
                dict(r)
                for r in db.execute(
                    "SELECT * FROM entity_labels WHERE batch_id=? AND comment_id=? ORDER BY id",
                    (batch_id, comment_id),
                )
            ]
            return result

    def edit(self, batch_id, comment_id, edit: EntityEdit):
        """Apply one atomic edit; entity changes invalidate the reviewed flag."""
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM entity_items WHERE batch_id=? AND comment_id=?",
                (batch_id, comment_id),
            ).fetchone()
            if row is None:
                raise KeyError("Annotation comment not found")
            if row["revision"] != edit.revision:
                raise StaleReview(
                    "This comment changed in another tab. Reload before editing."
                )
            if edit.action == "note":
                db.execute(
                    "UPDATE entity_items SET note=? WHERE batch_id=? AND comment_id=?",
                    (edit.note, batch_id, comment_id),
                )
            elif edit.action == "add":
                assert edit.start is not None and edit.end is not None
                if edit.end > len(row["text"]):
                    raise ValueError("Selection is outside the comment")
                title = row["text"][edit.start : edit.end]
                if not title.strip():
                    raise ValueError("Select a title, not whitespace")
                duplicate = db.execute(
                    """SELECT id FROM entity_labels WHERE
                    batch_id=? AND comment_id=? AND start=? AND end=? AND deleted=0""",
                    (batch_id, comment_id, edit.start, edit.end),
                ).fetchone()
                if duplicate:
                    raise ValueError("This span already has an active entity")
                db.execute(
                    """INSERT INTO entity_labels
                    (batch_id,comment_id,title,start,end,origin) VALUES (?,?,?,?,?,'manual')""",
                    (batch_id, comment_id, title, edit.start, edit.end),
                )
            elif edit.action in ("delete", "restore"):
                entity = db.execute(
                    """SELECT * FROM entity_labels WHERE id=?
                    AND batch_id=? AND comment_id=?""",
                    (edit.entity_id, batch_id, comment_id),
                ).fetchone()
                if entity is None:
                    raise KeyError("Entity does not belong to this comment")
                if edit.action == "restore" and entity["start"] is not None:
                    duplicate = db.execute(
                        """SELECT id FROM entity_labels WHERE
                        batch_id=? AND comment_id=? AND start=? AND end=?
                        AND deleted=0 AND id!=?""",
                        (
                            batch_id,
                            comment_id,
                            entity["start"],
                            entity["end"],
                            entity["id"],
                        ),
                    ).fetchone()
                    if duplicate:
                        raise ValueError("An active entity already occupies this span")
                db.execute(
                    "UPDATE entity_labels SET deleted=? WHERE id=?",
                    (edit.action == "delete", edit.entity_id),
                )
            db.execute(
                """UPDATE entity_items SET reviewed=?,revision=revision+1
                WHERE batch_id=? AND comment_id=?""",
                (
                    row["reviewed"]
                    if edit.action == "note"
                    else edit.action == "review",
                    batch_id,
                    comment_id,
                ),
            )
            db.execute(
                """INSERT INTO entity_review_events
                (batch_id,comment_id,at,action_json) VALUES (?,?,?,?)""",
                (batch_id, comment_id, now(), edit.model_dump_json()),
            )
        return self.item(batch_id, comment_id)

    def import_batch(self, name, snapshot, items):
        """Freeze inputs and proposals once; repeated imports never overwrite edits."""
        assert items and len({r["comment_id"] for r in items}) == len(items)
        with self.connect() as db:
            batch_id = db.execute(
                """INSERT INTO entity_batches
                (name,created_at,snapshot_json) VALUES (?,?,?)""",
                (name, now(), json.dumps(snapshot)),
            ).lastrowid
            for ordinal, row in enumerate(items, 1):
                db.execute(
                    """INSERT INTO entity_items
                    (batch_id,comment_id,ordinal,text,split,source,prediction_json,comparison_json)
                    VALUES (?,?,?,?,?,?,?,?)""",
                    (
                        batch_id,
                        row["comment_id"],
                        ordinal,
                        row["text"],
                        row["split"],
                        row["source"],
                        json.dumps(row["prediction"]),
                        json.dumps(row["comparison"]),
                    ),
                )
                for entity in row["entities"]:
                    start, end = entity["start"], entity["end"]
                    assert (start is None) == (end is None)
                    if start is not None:
                        assert 0 <= start < end <= len(row["text"])
                        assert (
                            row["text"][start:end].casefold()
                            == entity["title"].casefold()
                        )
                    db.execute(
                        """INSERT INTO entity_labels
                        (batch_id,comment_id,title,author,start,end,origin)
                        VALUES (?,?,?,?,?,?,'predicted')""",
                        (
                            batch_id,
                            row["comment_id"],
                            entity["title"],
                            entity["author"],
                            start,
                            end,
                        ),
                    )
        return batch_id
