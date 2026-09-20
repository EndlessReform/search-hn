"""Build the first 1,300 reviews from matched, already-paid teacher rollouts.

Reserve 300 uniform random candidates before enrichment. Training includes all
remaining title-set disagreements and shared positives, then enough random
shared negatives to reach 1,000. Keep full predictions and exact run hashes.
No API requests are made and no classifier labels are changed.
"""

import argparse
import hashlib
import json
import random
import sqlite3
from collections import Counter
from pathlib import Path

from comment_book_llm import encode_record
from search_research.comment_annotations import AnnotationStore
from search_research.comment_entity_annotations import (
    EntityAnnotationStore,
    locate_title,
)


def read_predictions(path):
    rows = {}
    for line in path.open():
        row = json.loads(line)
        assert not row.get("error"), row["comment_id"]
        rows[row["comment_id"]] = row["extraction"]
    return rows


def build(root):
    """Create a reproducible, disjoint review packet with source-aligned spans."""
    paths = {
        "text": root / "sustain4096.jsonl",
        "primary": root / "deepseek-low-sustain-c64/responses.jsonl",
        "comparison": root / "luna-sustain-c64/responses.jsonl",
    }
    texts = {
        r["comment_id"]: r["text"]
        for line in paths["text"].open()
        if (r := json.loads(line))
    }
    a, b = read_predictions(paths["primary"]), read_predictions(paths["comparison"])
    assert a.keys() == b.keys() == texts.keys()
    rng = random.Random(20260920)
    ids = sorted(texts)
    rng.shuffle(ids)
    test = ids[:300]

    def titles(prediction):
        return {" ".join(e["title"].casefold().split()) for e in prediction["books"]}

    def source(cid):
        if titles(a[cid]) != titles(b[cid]):
            return "disagreement"
        return "both_positive" if a[cid]["books"] else "both_negative"

    enriched = [cid for cid in ids[300:] if source(cid) != "both_negative"]
    assert len(enriched) <= 1000
    negatives = [cid for cid in ids[300:] if source(cid) == "both_negative"]
    train = enriched + negatives[: 1000 - len(enriched)]
    rng.shuffle(train)
    items = []
    for split, group in [("train", train), ("test", test)]:
        for cid in group:
            entities, seen = [], set()
            for book in a[cid]["books"]:
                spans = locate_title(texts[cid], book["title"]) or [(None, None)]
                for start, end in spans:
                    key = (start, end, book["title"].casefold())
                    if key in seen:
                        continue
                    seen.add(key)
                    entities.append(book | {"start": start, "end": end})
            items.append(
                {
                    "comment_id": cid,
                    "text": texts[cid],
                    "split": split,
                    "source": source(cid),
                    "prediction": a[cid],
                    "comparison": b[cid],
                    "entities": entities,
                }
            )
    snapshot = {
        "seed": 20260920,
        "primary": "DeepSeek V4.1 Flash / low",
        "comparison": "GPT-5.6 Luna / medium",
        "candidate_count": len(ids),
        "train": len(train),
        "test": len(test),
        "selection": "300 uniform random held aside first; remaining shared positives and disagreements, then random shared negatives to 1000 train",
        "source_hashes": {
            k: hashlib.sha256(p.read_bytes()).hexdigest() for k, p in paths.items()
        },
        "counts": dict(Counter(f"{r['split']}:{r['source']}" for r in items)),
    }
    return snapshot, items


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source", type=Path, default=Path("data/probes/books-api-bakeoff-v1")
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--annotations-db", type=Path)
    args = parser.parse_args()
    snapshot, items = build(args.source)
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "snapshot.json").write_text(encode_record(snapshot))
    with (args.output / "items.jsonl").open("w") as stream:
        for item in items:
            stream.write(encode_record(item) + "\n")
    if args.annotations_db:
        # Reuse the corpus binding already checked by the explorer. Take an online
        # SQLite backup before adding annotation tables or inserting the batch.
        with sqlite3.connect(args.annotations_db) as db:
            corpus_id = db.execute("SELECT identity FROM corpus").fetchone()[0]
            with sqlite3.connect(args.output / "annotations-before.sqlite") as backup:
                db.backup(backup)
        ledger = EntityAnnotationStore(AnnotationStore(args.annotations_db, corpus_id))
        batch_id = ledger.import_batch("Books · first 1,300", snapshot, items)
        print(json.dumps({"batch_id": batch_id, **snapshot}))
    else:
        print(json.dumps(snapshot))


if __name__ == "__main__":
    main()
