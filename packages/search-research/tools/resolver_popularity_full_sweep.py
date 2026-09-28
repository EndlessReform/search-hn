"""Full frozen-corpus soft-boost sweep and reproducible held-out case review.

No model calls, duplicate grouping, or source writes. Model-choice retention is
an agreement diagnostic, never accuracy. Review samples are drawn before review.
"""

import json
import math
import random
import sqlite3
from collections import Counter
from pathlib import Path

import duckdb

OUT = Path("data/research/books-resolver-popularity-full-v1")
WEIGHTS = [0, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 4]


def main():
    OUT.mkdir(exist_ok=True)
    db = sqlite3.connect(
        "file:data/research/books-resolver-2025-v1/checkpoint.sqlite?immutable=1",
        uri=True,
    )
    counts = dict(
        duckdb.sql(
            "SELECT work_id,readinglog_count FROM '/tmp/resolver-reading-log-counts.parquet'"
        ).fetchall()
    )
    labels = {}
    for ident, model, work in db.execute(
        "SELECT id,model,json_extract(payload,'$.selection.work_id') FROM selections WHERE model IN ('luna','deepseek-native')"
    ):
        labels.setdefault(ident, {})[model] = work
    stats = {w: Counter() for w in WEIGHTS}
    changed = {w: [] for w in [0.05, 0.25, 0.5]}
    pilot = {
        c["id"]
        for c in json.loads(
            Path(
                "data/research/books-resolver-popularity-pilot-v1/cases.json"
            ).read_text()
        )["cases"]
    }
    for ident, payload in db.execute("SELECT id,payload FROM rankings ORDER BY id"):
        r = json.loads(payload)
        ids, scores = r["ids"], r["scores"]
        base = r["top3"]
        if ids:
            assert sorted(range(len(ids)), key=lambda i: -scores[i])[:3] == [
                ids.index(k) for k in base
            ]
        y = labels.get(ident, {})
        stratum = (
            "paired_disagreement"
            if "luna" in y
            and "deepseek-native" in y
            and y["luna"] != y["deepseek-native"]
            else "other"
        )
        terms = [math.log2(1 + counts.get(k, 0)) for k in ids]
        for w in WEIGHTS:
            s = stats[w]
            s["references"] += 1
            s[stratum + "_references"] += 1
            if not ids:
                s["no_candidates"] += 1
                continue
            top = [
                ids[i]
                for i in sorted(
                    range(len(ids)), key=lambda i: -(scores[i] + w * terms[i])
                )[:3]
            ]
            first = top[0] != base[0]
            members = set(top) != set(base)
            s["changed_first"] += first
            s["changed_top3_set"] += members
            s[stratum + "_changed_first"] += first
            s[stratum + "_changed_top3_set"] += members
            for model in ["luna", "deepseek-native"]:
                if y.get(model) is not None:
                    s[model + "_selected"] += 1
                    s[model + "_choice_in_top3"] += y[model] in top
            if w in changed and ident not in pilot and members:
                changed[w].append(ident)
    rng = random.Random(20260927)
    review = {}
    for w, pool in changed.items():
        review[str(w)] = rng.sample(pool, min(20, len(pool)))
    requested = {k for values in review.values() for k in values}
    docs = {k: json.loads(p) for k, p in db.execute("SELECT id,payload FROM documents")}
    rows = []
    for ident in sorted(requested):
        ref = json.loads(
            db.execute("SELECT payload FROM refs WHERE id=?", (ident,)).fetchone()[0]
        )
        ranking = json.loads(
            db.execute("SELECT payload FROM rankings WHERE id=?", (ident,)).fetchone()[
                0
            ]
        )
        candidates = []
        for i, (key, score) in enumerate(
            zip(ranking["ids"], ranking["scores"], strict=True)
        ):
            candidates.append(
                docs[key]
                | {"score": score, "readers": counts.get(key, 0), "search_rank": i + 1}
            )
        tops = {
            str(w): sorted(
                candidates,
                key=lambda d: -(d["score"] + w * math.log2(1 + d["readers"])),
            )[:3]
            for w in [0, 0.05, 0.25, 0.5]
        }
        rows.append(
            {
                "id": ident,
                "reference": {
                    k: ref[k]
                    for k in ["title", "context", "start", "end", "comment_id"]
                },
                "selected_for_weights": [
                    w for w, values in review.items() if ident in values
                ],
                "top3": tops,
                "original_labels": labels.get(ident, {}),
            }
        )
    source = json.loads(
        Path("data/research/books-resolver-popularity-pilot-v1/cases.json").read_text()
    )["readinglog_source"]
    result = {
        "formula": "rerank_score + weight * log2(1 + readinglog_count)",
        "source": source,
        "seed": 20260927,
        "review_method": "20 random top3-membership-changed references per weight .05/.25/.5; exclude original ten pilot cases; no outcome-based filtering",
        "review_ids": review,
        "sweep": [{"weight": w, **dict(s)} for w, s in stats.items()],
    }
    (OUT / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    (OUT / "review-cases.json").write_text(json.dumps(rows, indent=2) + "\n")
    for w in review:
        (OUT / f"review-{w}.json").write_text(
            json.dumps([r for r in rows if w in r["selected_for_weights"]], indent=2)
            + "\n"
        )
    print(json.dumps(result["sweep"], indent=2))


if __name__ == "__main__":
    main()
