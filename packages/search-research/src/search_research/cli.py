"""Laptop-owned experiment driver; local-model inference runs on melchior."""

import argparse
import asyncio
import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from search_agent.data_access import HNStorySearchRepository


def main():
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=[
            "sample",
            "generate",
            "curate",
            "rewrite",
            "snapshot",
            "run",
            "suite",
            "report",
        ],
    )
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--as-of", type=date.fromisoformat, default=date(2026, 9, 4))
    parser.add_argument("--model", default="gpt-5.6-luna")
    parser.add_argument("--base-url", default="https://api.openai.com/v1")
    parser.add_argument(
        "--local-base-url",
        default="http://melchior-1:5000/v1",
        help="Remote Qwen/Gemma endpoint used by suite and snapshot; no local inference.",
    )
    parser.add_argument("--api", choices=["responses", "chat"], default="responses")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--exclusions", type=Path)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--question-edits", type=Path)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--max-turns", type=int, default=10)
    parser.add_argument("--max-tokens", type=int, default=4096)
    parser.add_argument("--timeout", type=float, default=600)
    args = parser.parse_args()
    args.root.mkdir(parents=True, exist_ok=True)
    if args.command in ("sample", "generate", "snapshot"):
        from search_research.dataset import generate, sample_candidates

        repo = HNStorySearchRepository.from_database_url(
            os.environ["HN_QUERY_DATABASE_URL"]
        )
        try:
            if args.command == "snapshot":
                from search_research.manifest import capture

                capture(args.root, repo, args.local_base_url)
            elif args.command == "sample":
                print(len(sample_candidates(repo, args.root, args.as_of)))
            else:
                asyncio.run(
                    generate(
                        args.root,
                        repo,
                        as_of=args.as_of,
                        model=args.model,
                        base_url=args.base_url,
                        count=args.count,
                    )
                )
        finally:
            repo.dispose()
    elif args.command == "curate":
        from search_research.curate import curate

        asyncio.run(curate(args.root, args.model, args.base_url, args.exclusions))
    elif args.command == "rewrite":
        from search_research.rewrite import rewrite

        assert args.source_root is not None, "rewrite requires --source-root"
        asyncio.run(
            rewrite(
                args.root,
                args.source_root,
                args.model,
                args.base_url,
                args.question_edits,
            )
        )
    elif args.command == "suite":
        from search_research.suite import suite

        asyncio.run(suite(args))
    elif args.command == "run":
        from search_research.rollouts import run

        asyncio.run(run(args))
    else:
        from search_research.report import report

        report(args.root)


if __name__ == "__main__":
    main()
