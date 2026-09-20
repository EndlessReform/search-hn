#!/usr/bin/env python3
"""Start a comment lab with a frozen corpus and separate editable positive sets."""

import argparse
from pathlib import Path

import uvicorn
from dotenv import load_dotenv
from search_research.comment_explorer import CommentExplorer
from search_research.comment_explorer_web import create_app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slice-dir", type=Path, required=True)
    parser.add_argument("--base-url", default="http://127.0.0.1:18080")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18081)
    parser.add_argument("--threads", type=int, default=16)
    parser.add_argument("--dtype", choices=["int8", "f32"], default="int8")
    parser.add_argument(
        "--annotations-db", type=Path, help="Defaults to SLICE/annotations.sqlite"
    )
    args = parser.parse_args()
    load_dotenv(Path(__file__).resolve().parents[3] / ".env", override=False)
    assert args.threads > 0
    explorer = CommentExplorer(
        args.slice_dir, args.base_url, threads=args.threads, dtype=args.dtype
    )
    print(
        f"Loaded {explorer.index.ntotal:,} vectors in {explorer.load_seconds:.3f}s",
        flush=True,
    )
    uvicorn.run(
        create_app(explorer, args.annotations_db), host=args.host, port=args.port
    )


if __name__ == "__main__":
    main()
