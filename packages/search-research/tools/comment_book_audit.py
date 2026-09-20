"""Compare structured LLM book extraction against the accumulated GLiNER audits.

Preserve each sample's selection separately: only random192 estimates population
precision. Matching ignores case/whitespace and a leading article. Semantic alias
adjudications can be supplied separately and are retained as auditable decisions.
"""

import argparse
import json
import re
from pathlib import Path


def read(path):
    return [json.loads(s) for s in path.read_text().splitlines()]


def normalize(text):
    value = re.sub(r"\s+", " ", text).strip().strip(" \"'_*.,:;!?()[]").casefold()
    return value.removeprefix("the ")


def load_gold(root):
    """Assemble existing annotations, preserving exclusions and corrections."""
    survey = root / "books-gliner-survey-v1"
    prep = root / "books-gliner-prep-v1"
    old = root / "books-gliner-v1"
    gold = {}
    for i in range(3):
        for row in read(survey / f"gold{i}.jsonl"):
            gold[row["comment_id"]] = row | {"sample": "random192"}
    for correction in json.loads((survey / "corrections.json").read_text()):
        gold[correction["comment_id"]].update(
            titles=correction["titles"], aliases=correction["aliases"]
        )
    for group in ["p1", "p2", "n1", "n2"]:
        for row in read(prep / f"{group}_gold.jsonl"):
            gold[row["comment_id"]] = row | {"sample": "title_rich63"}
    for correction in json.loads((prep / "corrections.json").read_text()):
        cid = correction["comment_id"]
        if correction.get("exclude"):
            del gold[cid]
            continue
        gold[cid]["titles"].extend(correction["add"])
        gold[cid]["aliases"].update(correction.get("aliases", {}))
    for group in ["positive_1", "positive_2", "negative_1", "negative_2"]:
        for row in read(old / f"{group}_review.jsonl"):
            gold[row["comment_id"]] = {
                "sample": "earlier64",
                "comment_id": row["comment_id"],
                "titles": row["explicit_titles"],
                "aliases": {},
            }
    return gold


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--aliases", type=Path)
    args = parser.parse_args()
    gold = load_gold(Path("data/probes"))
    if args.aliases:
        for decision in read(args.aliases):
            row = gold[decision["comment_id"]]
            row["aliases"].setdefault(decision["title"], []).extend(decision["aliases"])
    sources = {
        r["comment_id"]: r["text"]
        for r in read(Path("data/probes/books-gemma4-v1/evaluation.jsonl"))
    }
    cases = []
    counts = {}
    for prediction in read(args.predictions):
        cid = prediction["comment_id"]
        if cid not in gold:
            continue
        g = gold[cid]
        c = counts.setdefault(
            g["sample"],
            {
                "comments": 0,
                "positive_comments": 0,
                "tp": 0,
                "fp": 0,
                "fn": 0,
                "tn": 0,
                "titles": 0,
                "found_titles": 0,
                "emitted_titles": 0,
                "valid_emitted": 0,
                "valid_comments": 0,
                "complete": 0,
                "clean_complete": 0,
                "errors": 0,
            },
        )
        c["comments"] += 1
        c["positive_comments"] += bool(g["titles"])
        c["titles"] += len(g["titles"])
        if "error" in prediction:
            c["errors"] += 1
            continue
        result = prediction["extraction"]
        aliases = {
            t: {normalize(t), *(normalize(a) for a in g["aliases"].get(t, []))}
            for t in g["titles"]
        }
        emitted = {normalize(b["title"]): b for b in result["books"]}
        found = [t for t, forms in aliases.items() if forms & emitted.keys()]
        bad = [
            b
            for name, b in emitted.items()
            if not any(name in a for a in aliases.values())
        ]
        truth, gate = bool(g["titles"]), result["has_any_book"]
        c["tp" if truth and gate else "fn" if truth else "fp" if gate else "tn"] += 1
        c["found_titles"] += len(found)
        c["emitted_titles"] += len(emitted)
        c["valid_emitted"] += len(emitted) - len(bad)
        c["valid_comments"] += bool(found)
        c["complete"] += truth and len(found) == len(aliases)
        c["clean_complete"] += truth and len(found) == len(aliases) and not bad
        cases.append(
            {
                "comment_id": cid,
                "sample": g["sample"],
                "text": sources[cid],
                "gold": g["titles"],
                "extraction": result,
                "found": found,
                "missed": [t for t in aliases if t not in found],
                "extra": bad,
            }
        )
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "metrics.json").write_text(json.dumps(counts, indent=2))
    (args.out / "cases.jsonl").write_text("".join(json.dumps(r) + "\n" for r in cases))
    print(json.dumps(counts, indent=2))


if __name__ == "__main__":
    main()
