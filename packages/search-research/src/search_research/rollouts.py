"""Bounded remote rollouts with independent contexts, resume and durable traces."""

import asyncio
import fcntl
import hashlib
import json
import os
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path

from openai import APIConnectionError, APIStatusError
from search_agent.headless import run_headless
from search_agent.runtime import SearchRuntime

from search_research.dataset import read_jsonl


def source_hash():
    """Fingerprint all Python source in both packages, including uncommitted edits."""
    packages = Path(__file__).resolve().parents[3]
    digest = hashlib.sha256()
    for folder in ("search-agent", "search-research"):
        for file in sorted((packages / folder / "src").rglob("*.py")):
            digest.update(str(file.relative_to(packages)).encode())
            digest.update(file.read_bytes())
    return digest.hexdigest()


async def run(args):
    """At most one local inference request trajectory; low bounded API concurrency.

    Resume skips terminal runs (including errors, which remain in the denominator).
    An interrupted journal is preserved and a fresh attempt is written. A per-model
    advisory file lock prevents two drivers from evaluating the same cases at once.
    """
    assert args.concurrency >= 1
    if "api.openai.com" not in args.base_url:
        assert args.concurrency == 1, "Local inference is intentionally serialized"
    slug = re.sub(r"[^A-Za-z0-9_.-]", "_", args.model)
    directory = args.root / "trajectories" / slug
    directory.mkdir(parents=True, exist_ok=True)
    lock = (directory / "driver.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    dataset = read_jsonl(args.root / "eval.jsonl")
    if args.limit:
        dataset = dataset[: args.limit]
    fingerprint = hashlib.sha256((args.root / "eval.jsonl").read_bytes()).hexdigest()
    code_hash = source_hash()
    semaphore = asyncio.Semaphore(args.concurrency)
    errors = 0
    finished = 0
    total = len(dataset) * 2

    async def one(source, style):
        nonlocal errors, finished
        case = f"{source['id']}-{style}"
        for previous in sorted(directory.glob(f"{case}.*.jsonl")):
            events = read_jsonl(previous)
            if events and events[-1]["event"] in ("complete", "error"):
                assert events[0]["metadata"]["dataset_sha256"] == fingerprint
                finished += 1
                return
        async with semaphore:
            if errors >= 3:
                raise RuntimeError(
                    "Three consecutive failed trajectories; inspect journals before resuming"
                )
            runtime = SearchRuntime(
                model=args.model,
                base_url=args.base_url,
                database_url=os.environ["HN_QUERY_DATABASE_URL"],
                current_date=args.as_of,
                max_turns=args.max_turns,
                max_tokens=args.max_tokens,
                api=args.api,
            )
            path = (
                directory
                / f"{case}.{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}.jsonl"
            )
            try:
                await run_headless(
                    runtime,
                    source["questions"][style + "_question"],
                    path,
                    timeout=args.timeout,
                    metadata={
                        "case": case,
                        "target_id": source["id"],
                        "style": style,
                        "cohort": source["cohort"],
                        "dataset_sha256": fingerprint,
                        "source_sha256": code_hash,
                        "api": args.api,
                    },
                )
                errors = 0
                print(json.dumps({"complete": case, "model": args.model}), flush=True)
            except Exception as exc:  # noqa: BLE001 - persist failed cases, never silently drop them
                # Retrieval/tool-budget failures are outcomes, not an unhealthy server.
                errors = (
                    errors + 1
                    if isinstance(exc, (APIConnectionError, APIStatusError))
                    else 0
                )
                print(
                    json.dumps(
                        {"failed": case, "model": args.model, "error": str(exc)}
                    ),
                    flush=True,
                )
            finally:
                await runtime.close()
                finished += 1
                print(
                    json.dumps(
                        {
                            "progress": args.model,
                            "terminal": finished,
                            "expected": total,
                        }
                    ),
                    flush=True,
                )
                await asyncio.sleep(1)

    try:
        await asyncio.gather(
            *(one(row, style) for row in dataset for style in ("entity", "paraphrase"))
        )
    finally:
        lock.close()
