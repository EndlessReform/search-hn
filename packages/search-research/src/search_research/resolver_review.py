"""Read-only inspection of a frozen resolver checkpoint, with no model calls.

Only compact navigation fields are cached. Full comments, receipts and the fifty
candidate records are fetched for the selected reference. Immutable SQLite mode
is appropriate for a portable checkpoint, never for a live WAL run database.
"""

import json
import sqlite3
from collections import Counter
from contextlib import closing
from pathlib import Path

MODELS = {
    "luna-original": "Luna · original",
    "deepseek-native": "DS V4.1 · native",
    "deepseek": "DS V4 · OpenRouter",
}


def selected_ids(receipt):
    """Read both legacy single selections and source groups with several works."""
    selection = receipt["selection"]
    if "work_ids" in selection:
        return selection["work_ids"]
    return [selection["work_id"]] if selection["work_id"] is not None else []


def selection_key(payload):
    return tuple(sorted(selected_ids(json.loads(payload)))) or None


def metadata_key(doc):
    """Compare exact title and author strings, ignoring author ordering only.

    This flags indistinguishable selector inputs; it does not establish that
    two catalog records represent the same literary work or edition.
    """
    return doc["title"], tuple(sorted(doc["authors"]))


class ResolverReview:
    """Keep the original retrieval and ranking order intact for inspection."""

    def __init__(self, path: Path, live=False):
        self.live = live
        self.path = path.resolve(strict=True)
        self.rows = []
        with closing(self.connect()) as db:
            self.has_retrieval_history = bool(
                db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='history'"
                ).fetchone()
            )
            labels = {}
            for ident, model, payload in db.execute(
                "SELECT id,model,payload FROM selections"
            ):
                labels.setdefault(ident, {})[model] = selection_key(payload)
            shortlists = {}
            for ident, payload in db.execute("""
                SELECT r.id,d.payload FROM rankings r,
                json_each(r.payload,'$.top3') c JOIN documents d ON d.id=c.value
                ORDER BY r.id, CAST(c.key AS INTEGER)
            """):
                shortlists.setdefault(ident, []).append(json.loads(payload))
            for ident, title, comment in db.execute("""
                SELECT id,json_extract(payload,'$.title'),
                json_extract(payload,'$.comment_id') FROM refs ORDER BY ordinal
            """):
                docs = shortlists.get(ident, [])
                self.rows.append(
                    {
                        "id": ident,
                        "title": title,
                        "comment_id": comment,
                        "labels": labels.get(ident, {}),
                        "shortlist": docs,
                        "duplicates": len(docs) - len({metadata_key(d) for d in docs}),
                    }
                )
        self.by_id = {r["id"]: r for r in self.rows}

    def connect(self):
        return sqlite3.connect(
            self.path.as_uri() + ("?mode=ro" if self.live else "?immutable=1"), uri=True
        )

    def refresh(self):
        """Refresh committed selector results while the rerun is active."""
        if self.live:
            with closing(self.connect()) as db:
                for ident, model, payload in db.execute(
                    "SELECT id,model,payload FROM selections"
                ):
                    self.by_id[ident]["labels"][model] = selection_key(payload)

    @staticmethod
    def category(row, model):
        labels = row["labels"]
        if "luna" not in labels or model not in labels:
            return "missing"
        a, b = labels["luna"], labels[model]
        if a == b:
            return "both_abstain" if a is None else "same_work"
        if a is None:
            return "ds_only"
        if b is None:
            return "luna_only"
        return "different_work"

    def filtered(self, model, category, query, duplicates):
        """Search mentions/reference IDs; keep frozen run order and full coverage."""
        query = query.casefold().strip()
        return [
            r
            for r in self.rows
            if (not query or query in r["title"].casefold() or query in r["id"])
            and (not duplicates or r["duplicates"] > 0)
            and (
                category == "all"
                or self.category(r, model) == category
                or category == "disagree"
                and self.category(r, model)
                in {"different_work", "luna_only", "ds_only"}
            )
        ]

    def summary(self, model):
        counts = Counter(self.category(r, model) for r in self.rows)
        return dict(counts) | {
            "total": len(self.rows),
            "duplicate_shortlists": sum(r["duplicates"] > 0 for r in self.rows),
            "one_group_shortlists": sum(
                len(r["shortlist"]) == 3 and r["duplicates"] == 2 for r in self.rows
            ),
        }

    def detail(self, ident):
        """Join by work ID, never by position: score arrays follow retrieval order."""
        with closing(self.connect()) as db:
            row = db.execute("SELECT payload FROM refs WHERE id=?", (ident,)).fetchone()
            if row is None:
                raise KeyError(ident)
            ref = json.loads(row[0])
            ranking = db.execute(
                "SELECT payload FROM rankings WHERE id=?", (ident,)
            ).fetchone()
            assert ranking is not None, f"Missing saved ranking for {ident}"
            ranking = json.loads(ranking[0])
            scores = dict(zip(ranking["ids"], ranking["scores"], strict=True))
            ranked = sorted(scores, key=lambda k: -scores[k])
            ranks = {key: i + 1 for i, key in enumerate(ranked)}
            docs = {
                key: json.loads(payload)
                for key, payload in db.execute(
                    """
                SELECT d.id,d.payload FROM refs r,
                json_each(r.payload,'$.candidates') c
                JOIN documents d ON d.id=json_extract(c.value,'$.id') WHERE r.id=?
            """,
                    (ident,),
                )
            }
            groups = Counter(metadata_key(d) for d in docs.values())
            candidates = []
            for rank, candidate in enumerate(ref["candidates"], 1):
                key = candidate["id"]
                doc = docs[key]
                candidates.append(
                    doc
                    | {
                        "bm25": candidate["bm25"],
                        "author_match": candidate.get("author_match", False),
                        "retrieval_score": candidate.get(
                            "retrieval_score", candidate["bm25"]
                        ),
                        "search_rank": rank,
                        "rerank_score": scores[key],
                        "original_score": dict(
                            zip(
                                ranking["ids"],
                                ranking.get("original_scores", ranking["scores"]),
                                strict=True,
                            )
                        )[key],
                        "rerank_rank": ranks[key],
                        "shortlisted": key in ranking["top3"],
                        "metadata_copies": groups[metadata_key(doc)],
                    }
                )
            selections = {
                model: json.loads(payload)
                for model, payload in db.execute(
                    "SELECT model,payload FROM selections WHERE id=?", (ident,)
                )
            }
            selected_docs = {}
            for receipt in selections.values():
                for key in selected_ids(receipt):
                    if key != "special:bible":
                        record = db.execute(
                            "SELECT payload FROM documents WHERE id=?", (key,)
                        ).fetchone()
                        assert record is not None, f"Missing selected record: {key}"
                        selected_docs[key] = json.loads(record[0])
            history = None
            if db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='history'"
            ).fetchone():
                record = db.execute(
                    "SELECT payload FROM history WHERE id=?", (ident,)
                ).fetchone()
                history = json.loads(record[0]) if record else None
        return {
            "selected_documents": selected_docs,
            "retrieval_history": history,
            "reference": ref,
            "ranking": ranking,
            "candidates": candidates,
            "selections": selections,
        }
