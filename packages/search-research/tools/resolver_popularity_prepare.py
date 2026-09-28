"""Prepare a small, reproducible duplicate-grouping pilot without model calls.

Popularity breaks ties inside a metadata group; it never changes group order.
Missing popularity counts remain null and are never silently converted to zero.
"""

import argparse
import json
import re
import sqlite3
import unicodedata
from datetime import UTC, datetime
from pathlib import Path

FIELDS = (
    "edition_count",
    "readinglog_count",
    "ratings_count",
    "want_to_read_count",
    "currently_reading_count",
    "already_read_count",
)
CASE_IDS = [
    "42772738:49:68",
    "42659645:80:98",
    "42569899:2262:2273",
    "46026524:69:87",
    "44823110:110:144",
    "43492303:7:15",
    "44129528:74:85",
    "43791296:2044:2108",
    "45385485:0:28",
]


def normalized(value):
    """Case-fold and remove punctuation while retaining Unicode letters/digits."""
    return " ".join(
        re.findall(r"[^\W_]+", unicodedata.normalize("NFKC", value).casefold())
    )


def group_key(doc, rule):
    """Keep each author separate; token sorting handles inverted author names."""
    if rule == "exact":
        return doc["title"], tuple(sorted(doc["authors"]))
    authors = [normalized(a) for a in doc["authors"]]
    if rule == "author_tokens":
        authors = [" ".join(sorted(a.split())) for a in authors]
    return normalized(doc["title"]), tuple(sorted(authors))


def representative_key(doc):
    """Prefer observed reading logs, then their count, editions, and stable ID.

    A missing reading-log count sorts below every observed count, including zero.
    This is a declared policy, not a claim that missing activity is actually zero.
    """
    p = doc["popularity"]
    logs, editions = p["readinglog_count"], p["edition_count"]
    return (
        logs is None,
        -logs if logs is not None else 0,
        editions is None,
        -editions if editions is not None else 0,
        doc["id"],
    )


def algorithms(candidates):
    """Group all retrieved candidates before taking three ranked groups.

    Group order follows the best original rerank position. Scores are preserved,
    never boosted by popularity; popularity chooses only the group representative.
    """
    output = {}
    for rule in ("exact", "normalized", "author_tokens"):
        grouped = {}
        for doc in sorted(candidates, key=lambda d: d["rerank_rank"]):
            grouped.setdefault(group_key(doc, rule), []).append(doc)
        groups = [
            {
                "key": key,
                "member_ids": [d["id"] for d in members],
                "representative_id": min(members, key=representative_key)["id"],
                "max_rerank_score": max(d["rerank_score"] for d in members),
            }
            for key, members in grouped.items()
        ]
        output[rule] = {
            "groups": groups,
            "top3": [g["representative_id"] for g in groups[:3]],
        }
    return output


def cached_editions(ids, cache):
    """Reuse already-fetched edition counts; leave activity for the bulk join.

    This function performs no network requests. Activity fields remain unknown
    until the consistent reading-log snapshot is joined by the pilot runner.
    """
    found = {}
    for path in sorted(cache.glob("*.json")):
        payload = json.loads(path.read_text())["payload"]
        found.update({d["key"]: d for d in payload["docs"]})
    return {
        ident: {
            "found": ident in found,
            **{
                field: found.get(ident, {}).get(field)
                if field == "edition_count"
                else None
                for field in FIELDS
            },
        }
        for ident in ids
    }


def prepare(checkpoint, output):
    """Join the frozen saved inputs, then attach current public counts separately."""
    db = sqlite3.connect(checkpoint.resolve().as_uri() + "?immutable=1", uri=True)
    disney = db.execute(
        "SELECT id FROM refs WHERE json_extract(payload,'$.title')='Walt Disney' ORDER BY ordinal LIMIT 1"
    ).fetchone()
    assert disney is not None, "Walt Disney reference missing"
    cases = []
    for ident in CASE_IDS + [disney[0]]:
        ref_row = db.execute("SELECT payload FROM refs WHERE id=?", (ident,)).fetchone()
        assert ref_row is not None, f"Reference missing: {ident}"
        ref = json.loads(ref_row[0])
        ranking = json.loads(
            db.execute("SELECT payload FROM rankings WHERE id=?", (ident,)).fetchone()[
                0
            ]
        )
        scores = dict(zip(ranking["ids"], ranking["scores"], strict=True))
        ranks = {
            key: n + 1 for n, key in enumerate(sorted(scores, key=lambda k: -scores[k]))
        }
        candidates = []
        for n, hit in enumerate(ref["candidates"], 1):
            doc = json.loads(
                db.execute(
                    "SELECT payload FROM documents WHERE id=?", (hit["id"],)
                ).fetchone()[0]
            )
            candidates.append(
                {
                    **doc,
                    "bm25": hit["bm25"],
                    "search_rank": n,
                    "rerank_rank": ranks[doc["id"]],
                    "rerank_score": scores[doc["id"]],
                }
            )
        selections = {
            model: json.loads(payload)["selection"]["work_id"]
            for model, payload in db.execute(
                "SELECT model,payload FROM selections WHERE id=?", (ident,)
            )
        }
        cases.append(
            {
                "id": ident,
                "reference": ref,
                "ranking": ranking,
                "candidates": candidates,
                "original_selections": selections,
            }
        )
    db.close()
    stats = cached_editions(
        sorted({d["id"] for c in cases for d in c["candidates"]}),
        output.parent / "popularity_batches",
    )
    for case in cases:
        for doc in case["candidates"]:
            doc["popularity"] = stats[doc["id"]]
        case["algorithms"] = algorithms(case["candidates"])
    result = {
        "schema_version": 1,
        "created_at": datetime.now(UTC).isoformat(),
        "checkpoint": str(checkpoint),
        "popularity_source": "pending bulk activity snapshot join; edition counts from partial saved search API cache",
        "grouping_rules": {
            "exact": "exact title and sorted authors",
            "normalized": "normalized title and sorted normalized author strings",
            "author_tokens": "normalized title and sorted author-token strings",
        },
        "representative_policy": "observed readinglog_count before missing; descending count; observed edition_count before missing; descending count; ascending ID",
        "sample_method": "First four pairs from seed-20260926 25-pair trial, plus six specified diagnostic cases; purposive sample, not representative",
        "cases": cases,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n")
    print(f"Saved {len(cases)} cases / {len(stats)} unique works to {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=Path("data/research/books-resolver-2025-v1/checkpoint.sqlite"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/research/books-resolver-popularity-pilot-v1/cases.json"),
    )
    args = parser.parse_args()
    prepare(args.checkpoint, args.output)
