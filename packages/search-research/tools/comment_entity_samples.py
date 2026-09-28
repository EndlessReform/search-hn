"""Freeze GLiNER samples and count the chosen filter across the full corpus.

Run on melchior with uv run --locked --package search-research --extra ner
--with xgboost-cpu python packages/search-research/tools/comment_entity_samples.py.
Only the new experiment directory is written. Source SQLite files stay read-only.
"""

import json
import sqlite3
from pathlib import Path
from time import perf_counter

import numpy as np
from search_research.quick_filter import infer

ROOT = Path("data/comment-2025")
OUT = Path("data/probes/books-gliner-v1")
CUTOFF = -0.48585514643850203
SEED = 20260923


def readonly(path):
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)


def write_rows(path, rows):
    with path.open("w") as stream:
        for row in rows:
            stream.write(json.dumps(row) + "\n")


def main():
    OUT.mkdir(parents=True, exist_ok=False)
    start = perf_counter()
    comment_ids, scores, recipe, anchor = infer(ROOT)
    assert recipe.cutoff == CUTOFF
    with readonly(ROOT / "index.sqlite") as db:
        passed = scores >= CUTOFF
        passed_ids = comment_ids[passed]
        write_rows(
            OUT / "filter_passes.jsonl",
            (
                {"comment_id": int(cid), "score": float(score)}
                for cid, score in zip(passed_ids, scores[passed], strict=True)
            ),
        )
        rng = np.random.default_rng(SEED)
        sample_ids = rng.choice(
            passed_ids, size=min(512, len(passed_ids)), replace=False
        )

        def full_row(cid):
            author, text = db.execute(
                "SELECT author,text FROM comments WHERE comment_id=?", (int(cid),)
            ).fetchone()
            return {
                "comment_id": int(cid),
                "author": author,
                "text": text,
                "filter_score": float(scores[np.searchsorted(comment_ids, cid)]),
            }

        write_rows(OUT / "benchmark.jsonl", [full_row(cid) for cid in sample_ids])
        labels_path = Path("data/probes/books-mlp-v1/labels.jsonl")
        with labels_path.open() as stream:
            labels = [json.loads(line) for line in stream]
        test_ids = set(
            json.loads(Path("data/probes/books-mlp-v1/split_ids.json").read_text())[
                "test"
            ]
        )
        passed_set = set(map(int, passed_ids))
        labeled = [
            r
            for r in labels
            if r["comment_id"] in test_ids and r["comment_id"] in passed_set
        ]
        counts = {
            str(truth): sum(r["is_positive"] == truth for r in labeled)
            for truth in [True, False]
        }
        # These counts reproduce the frozen operating point before sampling.
        assert counts == {"True": 393, "False": 313}, counts
        reviews = []
        for truth in [True, False]:
            candidates = [r for r in labeled if r["is_positive"] == truth]
            chosen = rng.choice(len(candidates), size=32, replace=False)
            for offset, index in enumerate(chosen):
                r = candidates[index]
                assert r["model"] == "openai/gpt-5.6-luna"
                reviews.append(
                    full_row(r["comment_id"])
                    | {
                        "luna_is_positive": truth,
                        "luna_taxonomy": r["taxonomy"],
                        "review_group": f"{'positive' if truth else 'negative'}_{offset // 16 + 1}",
                    }
                )
        write_rows(OUT / "review-inputs.jsonl", reviews)
        # Check the earlier 10k fixture pass identities, independently of its stored scores.
        with Path("data/probes/books-ensemble-v1/wild_scores.jsonl").open() as stream:
            wild = [json.loads(line) for line in stream]
        expected = {
            r["comment_id"] for r in wild if r["scores"]["blend_cosine_0.25"] >= CUTOFF
        }
        actual = {r["comment_id"] for r in wild if r["comment_id"] in passed_set}
        assert expected == actual and len(actual) == 310, (len(expected), len(actual))
        summary = {
            "seed": SEED,
            "corpus_comments": len(comment_ids),
            "passed": int(passed.sum()),
            "pass_fraction": float(passed.mean()),
            "filter_seconds": perf_counter() - start,
            "cutoff": CUTOFF,
            "heldout_passed_by_luna_label": counts,
            "benchmark_comments": len(sample_ids),
            "review_comments": len(reviews),
            "wild_pass_matches": len(actual),
            "pool_id": 1,
            "anchor_positive_ids": anchor["positive_ids"],
        }
        (OUT / "sample-summary.json").write_text(json.dumps(summary, indent=2))
        print(json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
