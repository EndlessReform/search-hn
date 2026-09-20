"""Prepare, embed, inspect, or verify reusable NPY + SQLite comment slices."""

import argparse
import json
from pathlib import Path

from search_research.comment_index import connect_index, metadata
from search_research.comment_slice_embed import EmbeddingSettings, embed, verify
from search_research.comment_slice_export import CommentSlice, prepare, selection_query


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action", choices=["prepare", "embed", "run", "status", "verify", "sql"]
    )
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "--slice", choices=["top-comments", "year"], default="top-comments"
    )
    parser.add_argument("--score-gt", type=int, default=100)
    parser.add_argument("--top-k", type=int, default=3)
    parser.add_argument("--year", type=int)
    parser.add_argument(
        "--dsn",
        default="host=searchhn-pg dbname=searchhn_test user=readonly_hn_agent connect_timeout=5",
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:18080")
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--checkpoint-rows", type=int, default=131072)
    args = parser.parse_args()
    if args.action in {"prepare", "run", "sql"}:
        selection = CommentSlice(
            kind=args.slice, score_gt=args.score_gt, top_k=args.top_k, year=args.year
        )
        if args.action == "sql":
            query, params = selection_query(selection)
            print(query)
            print(params)
            return
        prepare(args.output, selection, args.dsn)
    if args.action in {"embed", "run"}:
        embed(
            args.output,
            EmbeddingSettings(
                base_url=args.base_url,
                batch_size=args.batch_size,
                concurrency=args.concurrency,
                checkpoint_rows=args.checkpoint_rows,
            ),
        )
    if args.action == "verify":
        verify(args.output)
    if args.action == "status":
        index = connect_index(args.output / "index.sqlite", readonly=True)
        try:
            total, completed = index.execute(
                "SELECT total_rows,completed_rows FROM progress"
            ).fetchone()
            print(
                json.dumps(
                    {
                        "total": total,
                        "completed": completed,
                        "metadata": metadata(index),
                        "checkpoints": index.execute(
                            "SELECT count(*) FROM checkpoints"
                        ).fetchone()[0],
                    },
                    indent=2,
                )
            )
        finally:
            index.close()


if __name__ == "__main__":
    main()
