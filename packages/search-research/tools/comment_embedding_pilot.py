"""Export and embed a fixed comment slice; resume without re-reading live rows.

Example: uv run --package search-research python packages/search-research/tools/comment_embedding_pilot.py OUTPUT --base-url RAW_VLLM_ORIGIN
The raw origin ends at /vllm/embeddings, before /v1. No production writes occur.
"""

import argparse
import fcntl
import json
from pathlib import Path

import httpx
import polars as pl
from search_agent.journal import Journal
from search_research.comment_corpus import chunk_comments, extract_comments
from search_research.embedding_backfill import (
    BatchSettings,
    check_manifest,
    file_hash,
    run_shards,
)
from search_research.tei_embeddings import EmbeddingRecipe
from tokenizers import Tokenizer


def atomic_parquet(frame: pl.DataFrame, path: Path) -> None:
    """Publish complete metadata before embedding its positional shards."""
    temporary = path.with_suffix(".partial")
    frame.write_parquet(temporary)
    temporary.replace(path)


def run(args):
    """Lock one output directory and pin dataset, tokenizer, and vector recipe."""
    assert args.export_only or args.base_url, "Provide --base-url or --export-only"
    root = args.output
    root.mkdir(parents=True, exist_ok=True)
    with (root / "run.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        settings = BatchSettings(
            batch_size=args.batch_size, pause_seconds=args.pause, priority=1
        )
        recipe = EmbeddingRecipe(compute_dtype="bfloat16")
        check_manifest(
            root / "selection.json",
            {
                "count": args.count,
                "seed": args.seed,
                "score_gt": 100,
                "selection": "hash-story-order/top-3-usable-top-level/display-order/v1",
                "text": "decoded-html/paragraph-breaks/no-prefix/v1",
                "max_tokens": 2048,
            },
        )
        comments_path = root / "comments.parquet"
        if not comments_path.exists():
            atomic_parquet(
                extract_comments(args.dsn, args.count, args.seed), comments_path
            )
        comments = pl.read_parquet(comments_path)
        tokenizer_path = root / "tokenizer.json"
        if not tokenizer_path.exists():
            url = f"https://huggingface.co/{recipe.model_id}/resolve/{recipe.revision}/tokenizer.json"
            response = httpx.get(url, follow_redirects=True, timeout=60)
            response.raise_for_status()
            temporary = tokenizer_path.with_suffix(".partial")
            temporary.write_bytes(response.content)
            temporary.replace(tokenizer_path)
        inputs_path = root / "inputs.parquet"
        if not inputs_path.exists():
            atomic_parquet(
                chunk_comments(comments, Tokenizer.from_file(str(tokenizer_path))),
                inputs_path,
            )
        inputs = pl.read_parquet(inputs_path)
        if args.export_only:
            print(
                json.dumps(
                    {
                        "comments": comments.height,
                        "vectors": inputs.height,
                        "tokens": inputs["tokens"].sum(),
                    }
                ),
                flush=True,
            )
            return
        check_manifest(
            root / "manifest.json",
            {
                "recipe": recipe.model_dump(),
                "embedding_recipe": "pplx-0.6b-2c4d510dd4a7-vllm0.28.0-bf16-flash-mean-2048-tanh127-rne-v1",
                "comments_sha256": file_hash(comments_path),
                "inputs_sha256": file_hash(inputs_path),
                "tokenizer_sha256": file_hash(tokenizer_path),
                "batch_size": settings.batch_size,
            },
        )
        (root / "backfill-config.json").write_text(
            json.dumps(settings.model_dump(), indent=2)
        )
        print(
            json.dumps(
                {
                    "comments": comments.height,
                    "vectors": inputs.height,
                    "tokens": inputs["tokens"].sum(),
                    "split_comments": inputs.filter(pl.col("chunk") == 1).height,
                }
            ),
            flush=True,
        )
        journal = Journal(root / "batches.jsonl")
        try:
            with httpx.Client(
                base_url=args.base_url.rstrip("/"), timeout=180
            ) as client:
                result = run_shards(
                    client,
                    inputs["input"].to_list(),
                    root / "documents",
                    journal=journal,
                    kind="comments",
                    settings=settings,
                )
                print(json.dumps(result), flush=True)
        finally:
            journal.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--base-url")
    parser.add_argument(
        "--export-only",
        action="store_true",
        help="Freeze Parquet inputs without contacting inference",
    )
    parser.add_argument(
        "--dsn",
        default="host=searchhn-pg dbname=searchhn_test user=readonly_hn_agent connect_timeout=5",
    )
    parser.add_argument("--count", type=int, default=2000)
    parser.add_argument("--seed", default="comment-pilot-20260918")
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--pause", type=float, default=0.5)
    run(parser.parse_args())
