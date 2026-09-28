"""Optional baseline reuse and reference-sized reranker submissions."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path


def baseline_scores(path):
    """Only reuse an explicitly requested baseline; new years start empty."""
    scores = {}
    if path:
        uri = Path(path).resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True)) as db:
            for ident, payload in db.execute("SELECT id,payload FROM rankings"):
                row = json.loads(payload)
                for key, score in zip(
                    row["ids"], row.get("original_scores", row["scores"]), strict=True
                ):
                    # Published heal checkpoints distinguish boosted and raw scores.
                    scores[(ident, key)] = score
    return scores


def pending_batches(cases, scores, references_per_batch=128):
    """Keep all pending pairs for each 128-reference window in one submission.

    Resume can shorten a window; it must never turn the window back into a
    fixed pair-count batch or split a single reference across submissions.
    """
    for start in range(0, len(cases), references_per_batch):
        batch = [
            (case, doc)
            for case in cases[start : start + references_per_batch]
            for doc in case["candidates"]
            if (case["id"], doc["id"]) not in scores
        ]
        if batch:
            yield batch
