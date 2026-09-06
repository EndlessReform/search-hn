"""Resumable three-arm Luna E2E run, with streaming and an $8 default guard.

Each case owns its runtime and durable trace. Completed and max-turn cases are
terminal; infrastructure failures and budget pauses are preserved for a later
invocation. No automatic model retry can incur an unreserved second charge.
"""

import argparse
import asyncio
import fcntl
import hashlib
import json
import os
import time
import uuid
from collections import Counter
from datetime import date
from pathlib import Path

from agents import MaxTurnsExceeded, ModelSettings
from search_agent.headless import run_headless
from search_agent.journal import Journal
from search_agent.runtime import SearchRuntime
from search_agent.semantic_search import SemanticStoryRepository

from search_research.dataset import read_jsonl
from search_research.engine_data import TRACES
from search_research.rollout_budget import (
    BilledProvider,
    Budget,
    BudgetPaused,
    MeteredHooks,
)
from search_research.rollouts import source_hash
from search_research.semantic_rollouts import LIVE, SCRATCH
from search_research.sovereign_repository import ARMS, LocalQueries, prepare
from search_research.sovereign_run import HASHES

ROOT = Path("data/sovereign-e2e-20260905")
MODEL = "openai/gpt-5.6-luna"
ROUTING = {
    "service_tier": "default",
    "provider": {
        "only": ["openai"],
        "allow_fallbacks": False,
        "require_parameters": True,
    },
}


def manifest(root):
    """Freeze scientific settings; operational concurrency/budget may change on resume."""
    value = {
        "model": MODEL,
        "base_url": "https://openrouter.ai/api/v1",
        "routing": ROUTING,
        "max_turns": 10,
        "max_output_tokens": 4096,
        "system_date": "2026-09-04",
        "retrieval": "dense",
        "hashes": HASHES,
        "source_sha256": source_hash(),
        "eval_sha256": hashlib.sha256((TRACES / "eval.jsonl").read_bytes()).hexdigest(),
        "arms": {k: v[2].model_dump() for k, v in ARMS.items()},
    }
    path = root / "manifest.json"
    if path.exists():
        assert json.loads(path.read_text()) == value, (
            "Manifest changed; use a new run root"
        )
    else:
        path.write_text(json.dumps(value, indent=2))
    return value


def cases(dataset, smoke):
    """Eight preselected questions: both styles, two stories from each age cohort."""
    if smoke:
        cohorts = sorted({r["cohort"] for r in dataset})
        assert len(cohorts) == 2
        dataset = [
            r
            for c in cohorts
            for r in sorted(
                [r for r in dataset if r["cohort"] == c], key=lambda r: r["id"]
            )[:2]
        ]
    return [
        (r, style, arm)
        for r in dataset
        for style in ("entity", "paraphrase")
        for arm in ARMS
    ]


async def run(args):
    """Bound concurrency across whole conversations and drain safely on a pause."""
    root = args.root
    root.mkdir(parents=True, exist_ok=True)
    lock = (root / "run.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    prepare(root)
    frozen = manifest(root)
    dataset = read_jsonl(TRACES / "eval.jsonl")
    assert len(dataset) == 98
    selected = cases(dataset, args.smoke)
    outcomes = Journal(root / "outcomes.jsonl")
    previous = {r["key"]: r for r in read_jsonl(root / "outcomes.jsonl")}
    budget = Budget(root / "budget.jsonl", args.budget_usd)
    semaphore = asyncio.Semaphore(args.concurrency)
    counts = Counter()
    started = time.monotonic()
    for arm in ARMS:
        armroot = root / arm
        armroot.mkdir(exist_ok=True)
        (armroot / "eval.jsonl").write_bytes((TRACES / "eval.jsonl").read_bytes())

    async def one(source, style, arm):
        case = f"{source['id']}-{style}"
        key = f"{arm}/{case}"
        if key in previous and previous[key]["status"] in ("complete", "max_turns"):
            counts["skipped_terminal"] += 1
            return
        async with semaphore:
            if budget.paused:
                counts["not_dispatched"] += 1
                return
            armroot = root / arm
            directory = armroot / "trajectories" / "gpt-5.6-luna"
            directory.mkdir(parents=True, exist_ok=True)
            # Recover a killed process even if its terminal trace preceded outcome fsync.
            for old in directory.glob(f"{case}.*.jsonl"):
                repaired = Journal(old)
                repaired.close()
                events = read_jsonl(old)
                if events and (
                    events[-1]["event"] == "complete"
                    or events[-1].get("error_type") == "MaxTurnsExceeded"
                ):
                    status = (
                        "complete" if events[-1]["event"] == "complete" else "max_turns"
                    )
                    outcomes.write(
                        "outcome",
                        key=key,
                        status=status,
                        trajectory=str(old),
                        recovered=True,
                    )
                    counts["skipped_terminal"] += 1
                    return
                archive = armroot / "infrastructure-attempts"
                archive.mkdir(exist_ok=True)
                old.rename(archive / old.name)
            path = directory / f"{case}.{uuid.uuid4().hex[:12]}.jsonl"
            runtime = None
            repository = None
            status = "infrastructure_error"
            error = None
            try:
                repository = SemanticStoryRepository(
                    SCRATCH,
                    LIVE,
                    mode="dense",
                    embedding_provider=LocalQueries(arm),
                    vector_table="sovereign_vectors_" + arm,
                )
                runtime = SearchRuntime(
                    model=MODEL,
                    base_url=frozen["base_url"],
                    api_key=os.environ["OPENROUTER_API_KEY"],
                    repository=repository,
                    current_date=date(2026, 9, 4),
                    max_turns=10,
                    max_tokens=4096,
                )
                runtime.client.max_retries = 0
                runtime.settings = ModelSettings(
                    max_tokens=4096, store=False, extra_body=ROUTING
                )

                def hooks_factory(journal):
                    hooks = MeteredHooks(journal, budget, key)
                    runtime.config.model_provider = BilledProvider(
                        runtime.config.model_provider, hooks
                    )
                    return hooks

                await run_headless(
                    runtime,
                    source["questions"][style + "_question"],
                    path,
                    timeout=900,
                    metadata={
                        "case": case,
                        "target_id": source["id"],
                        "style": style,
                        "cohort": source["cohort"],
                        "embedding_arm": arm,
                        "retrieval": "dense",
                        "dataset_sha256": frozen["eval_sha256"],
                        "source_sha256": frozen["source_sha256"],
                        "api": "responses",
                    },
                    hooks_factory=hooks_factory,
                )
                status = "complete"
            except MaxTurnsExceeded as exc:
                status, error = "max_turns", str(exc)
            except BudgetPaused as exc:
                status, error = "paused", str(exc)
            except Exception as exc:  # noqa: BLE001 — isolate and journal each failed session
                error = f"{type(exc).__name__}: {exc}"
            finally:
                try:
                    if runtime is not None:
                        await runtime.close()
                    elif repository is not None:
                        repository.dispose()
                except Exception as exc:  # noqa: BLE001 — cleanup must not cancel sibling sessions
                    error = f"{error or ''}; cleanup: {type(exc).__name__}: {exc}"
                    # Cleanup cannot undo a terminal scientific observation.
                    # Preserve its trace/status so resume never repeats paid work.
            if status not in ("complete", "max_turns") and path.exists():
                archive = armroot / "infrastructure-attempts"
                archive.mkdir(exist_ok=True)
                path = path.rename(archive / path.name)
            outcomes.write(
                "outcome",
                key=key,
                status=status,
                error=error,
                trajectory=str(path),
                retrieval=repository.stats if repository else None,
            )
            counts[status] += 1
            print(
                json.dumps(
                    {
                        "case": key,
                        "status": status,
                        "error": error,
                        "counts": dict(counts),
                        **budget.summary(),
                    }
                ),
                flush=True,
            )

    try:
        await asyncio.gather(*(one(*case) for case in selected))
    finally:
        summary = {
            "counts": dict(counts),
            "seconds": time.monotonic() - started,
            "selected": len(selected),
            "concurrency": args.concurrency,
            **budget.summary(),
        }
        outcomes.write("invocation", key="__invocation__", status="finished", **summary)
        (root / "last-invocation.json").write_text(json.dumps(summary, indent=2))
        print(json.dumps(summary), flush=True)
        outcomes.close()
        budget.journal.close()
        lock.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--budget-usd", type=float, default=8.0)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    assert 1 <= args.concurrency <= 16
    asyncio.run(run(args))
