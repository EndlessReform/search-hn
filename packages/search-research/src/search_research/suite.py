"""Laptop-owned orchestration: Luna API alongside serialized melchior inference."""

import asyncio
from copy import copy

from search_agent.journal import Journal

from search_research.report import report
from search_research.rollouts import run


async def suite(args):
    """One GPU trajectory and at most two Luna trajectories at a time."""
    journal = Journal(args.root / "suite.jsonl")
    journal.write("suite_start", limit=args.limit)

    async def local():
        for model in ("qwen-3.6-27b", "gemma-4-31b-speculative"):
            config = copy(args)
            config.model = model
            config.base_url = args.local_base_url
            config.concurrency = 1
            await run(config)
            journal.write("model_finished", model=model)
            report(args.root)

    config = copy(args)
    config.model = "gpt-5.6-luna"
    config.base_url = "https://api.openai.com/v1"
    config.concurrency = 2
    try:
        results = await asyncio.gather(run(config), local(), return_exceptions=True)
        report(args.root)
        failures = [str(r) for r in results if isinstance(r, BaseException)]
        journal.write("suite_end", failures=failures)
        assert not failures, failures
    finally:
        journal.close()
