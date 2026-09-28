"""Freeze the filter recipe once, or apply it to a complete embedded slice."""

import argparse
import json
from pathlib import Path
from time import perf_counter

from search_research.quick_filter import DEFAULT_RECIPE, freeze, infer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    setup = commands.add_parser("freeze")
    setup.add_argument("--annotations", type=Path, required=True)
    setup.add_argument("--model", type=Path, required=True)
    setup.add_argument("--sample-summary", type=Path, required=True)
    setup.add_argument("--output", type=Path, required=True)
    run = commands.add_parser("run")
    run.add_argument("--slice", type=Path, required=True)
    run.add_argument("--recipe", type=Path, default=DEFAULT_RECIPE)
    run.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "freeze":
        expected = json.loads(args.sample_summary.read_text())["anchor_positive_ids"]
        freeze(args.annotations, args.model, args.output, expected)
        return
    assert not args.output.exists(), "Filter output already exists"
    started = perf_counter()
    ids, scores, recipe, _ = infer(args.slice, args.recipe)
    passed = scores >= recipe.cutoff
    temporary = args.output.with_suffix(".jsonl.tmp")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with temporary.open("w") as output:
        for cid, score in zip(ids[passed], scores[passed], strict=True):
            output.write(
                json.dumps({"comment_id": int(cid), "score": float(score)}) + "\n"
            )
    temporary.replace(args.output)
    print(
        json.dumps(
            {
                "comments": len(ids),
                "passed": int(passed.sum()),
                "seconds": perf_counter() - started,
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
