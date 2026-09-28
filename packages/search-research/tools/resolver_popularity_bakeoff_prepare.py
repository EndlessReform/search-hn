"""Freeze a held-out, fixed-weight boost versus duplicate-substitution bakeoff.

The population is every saved reference, with no restriction to earlier model
agreement. Sample only shortlist-set differences, excluding all earlier reviewed
references. No API or model calls occur; popularity comes from one local snapshot.
"""

import json
import math
import random
import re
import sqlite3
import unicodedata
from collections import Counter
from pathlib import Path

import duckdb

CHECKPOINT = Path("data/research/books-resolver-2025-v1/checkpoint.sqlite")
COUNTS = Path("/tmp/resolver-reading-log-counts.parquet")
PILOT = Path("data/research/books-resolver-popularity-pilot-v1/cases.json")
REVIEW = Path("data/research/books-resolver-popularity-full-v1/review-cases.json")
OUTPUT = Path("data/research/books-resolver-popularity-bakeoff-v1/cases.json")
SEED = 20260928
WEIGHT = 0.1
SAMPLE_SIZE = 300


def normalized(value):
    """Retain Unicode letters and numbers, folding case and punctuation."""
    return " ".join(
        re.findall(r"[^\W_]+", unicodedata.normalize("NFKC", value).casefold())
    )


def group_key(doc):
    """Normalize title and names without mixing tokens between distinct people."""
    authors = tuple(
        sorted(" ".join(sorted(normalized(a).split())) for a in doc["authors"])
    )
    return normalized(doc["title"]), authors


def shortlist_pair(ranking, docs, counts):
    """Compare the fixed soft boost with hard group representative substitution.

    Boost ties retain saved retrieval order. Group order follows best original
    rerank score, ties retaining saved retrieval order. Each group representative
    maximizes snapshot reading-log count, breaking count ties by ascending ID.
    Edition counts are deliberately absent from both algorithms.
    """
    ids, scores = ranking["ids"], ranking["scores"]
    assert len(ids) == len(scores), "Saved ranking IDs and scores disagree"
    boosted = sorted(
        range(len(ids)),
        key=lambda i: -(scores[i] + WEIGHT * math.log2(1 + counts.get(ids[i], 0))),
    )
    groups = {}
    for i in sorted(range(len(ids)), key=lambda i: -scores[i]):
        groups.setdefault(group_key(docs[ids[i]]), []).append(ids[i])
    representatives = [
        min(members, key=lambda key: (-counts.get(key, 0), key))
        for members in groups.values()
    ]
    return {"boost": [ids[i] for i in boosted[:3]], "substitution": representatives[:3]}


def main():
    """Enumerate the complete population before taking the seeded holdout sample."""
    pilot = json.loads(PILOT.read_text())
    reviews = json.loads(REVIEW.read_text())
    excluded = {c["id"] for c in pilot["cases"]} | {c["id"] for c in reviews}
    counts = dict(
        duckdb.sql(f"SELECT work_id,readinglog_count FROM '{COUNTS}'").fetchall()
    )
    db = sqlite3.connect(CHECKPOINT.resolve().as_uri() + "?immutable=1", uri=True)
    docs = {
        key: json.loads(payload)
        for key, payload in db.execute("SELECT id,payload FROM documents")
    }
    population = Counter()
    differing = {}
    for ident, payload in db.execute("SELECT id,payload FROM rankings ORDER BY id"):
        ranking = json.loads(payload)
        population["all_references"] += 1
        population["no_candidates"] += not ranking["ids"]
        shortlists = shortlist_pair(ranking, docs, counts)
        changed = set(shortlists["boost"]) != set(shortlists["substitution"])
        population["differing_shortlist_sets_all"] += changed
        if ident in excluded:
            population["excluded_references"] += 1
            population["excluded_differing_shortlist_sets"] += changed
        else:
            population["eligible_references"] += 1
            population["eligible_differing_shortlist_sets"] += changed
            if changed:
                differing[ident] = shortlists
    assert (
        population["all_references"]
        == db.execute("SELECT count(*) FROM refs").fetchone()[0]
    ), "References without saved rankings"
    assert len(differing) >= SAMPLE_SIZE, (
        "Insufficient differing cases for requested sample"
    )
    selected = random.Random(SEED).sample(sorted(differing), SAMPLE_SIZE)
    cases = []
    for ident in selected:
        reference = json.loads(
            db.execute("SELECT payload FROM refs WHERE id=?", (ident,)).fetchone()[0]
        )
        candidates = [
            {**docs[c["id"]], "readinglog_count": counts.get(c["id"], 0)}
            for c in reference["candidates"]
        ]
        cases.append(
            {
                "id": ident,
                "reference": reference,
                "candidates": candidates,
                "shortlists": differing[ident],
            }
        )
    db.close()
    result = {
        "manifest": {
            "checkpoint": str(CHECKPOINT),
            "count_file": str(COUNTS),
            "readinglog_source": pilot["readinglog_source"],
            "seed": SEED,
            "sample_size": SAMPLE_SIZE,
            "population": dict(population),
            "sampling": "random.Random(seed).sample(sorted(eligible differing shortlist-set IDs), 300)",
            "exclusion_ids": sorted(excluded),
            "exclusion_sources": [str(PILOT), str(REVIEW)],
            "boost_formula": "rerank_score + 0.1 * log2(1 + readinglog_count)",
            "substitution": "Across all retrieved candidates, normalized title and per-author sorted normalized tokens; groups ordered by maximum original rerank score; representative descending readinglog_count then ascending stable ID; first three groups",
            "tie_policy": "boost score ties and group score ties preserve saved retrieval order",
            "missing_count_policy": "Snapshot absent work IDs have zero reading-log rows",
            "edition_count_policy": "Not used; null if supplied to either selector prompt",
            "scope": "All saved references eligible regardless of previous model decision or agreement",
        },
        "cases": cases,
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result["manifest"], indent=2))


if __name__ == "__main__":
    main()
