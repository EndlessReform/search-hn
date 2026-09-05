"""Fresh Luna trajectories, unchanged system prompt, two retrieval treatments."""

import argparse
import asyncio
import hashlib
import json
import re
import time
import uuid
from datetime import date

from aiolimiter import AsyncLimiter
from search_agent.headless import run_headless
from search_agent.journal import Journal, JournalHooks
from search_agent.runtime import SearchRuntime
from search_agent.semantic_search import SemanticStoryRepository

from search_research.dataset import read_jsonl
from search_research.engine_data import TRACES
from search_research.rate_retry import RateRetryProvider, is_rate_limit
from search_research.report import report
from search_research.rollouts import source_hash
from search_research.semantic_snapshot import ROOT

SCRATCH = "postgresql://postgres@127.0.0.1:55432/search_bakeoff"
LIVE = "postgresql://readonly_hn_agent@searchhn-pg:5432/searchhn_test"


class Budget:
    """Reserve an upper input-byte bound plus max output before each request.

    Completion replaces its reservation with uncached-price usage (conservative).
    Unknown failed requests keep their full reservation, including across resumes.
    The $4.80 model ceiling leaves $0.20 for new and query embeddings.
    """

    def __init__(self):
        path = ROOT / "budget.jsonl"
        self.charges = {}
        for row in read_jsonl(path) if path.exists() else []:
            if row["event"] == "charge":
                self.charges[row["id"]] = row["usd"]
        self.journal = Journal(path)
        self.limiter = AsyncLimiter(1, 0.25)  # <=240 RPM, no guessed TPM queue
        self.pause_until = 0.0

    async def feedback(self, response):
        """Use server headers for token exhaustion; SDK honors Retry-After retries."""
        headers = {
            k: v
            for k, v in response.headers.items()
            if k.startswith("x-ratelimit") or k == "retry-after"
        }
        self.journal.write(
            "rate_feedback", status=response.status_code, headers=headers
        )
        if (
            response.status_code == 429
            or int(headers.get("x-ratelimit-remaining-tokens", "500000")) < 10000
        ):
            raw = headers.get(
                "retry-after", headers.get("x-ratelimit-reset-tokens", "1s")
            )
            seconds = (
                float(raw)
                if raw.replace(".", "", 1).isdigit()
                else sum(
                    float(n) * {"ms": 0.001, "s": 1, "m": 60, "h": 3600}[u]
                    for n, u in re.findall(r"([\d.]+)(ms|s|m|h)", raw)
                )
            )
            self.pause_until = max(self.pause_until, time.monotonic() + seconds)

    def set(self, id, usd):
        self.charges[id] = usd
        self.journal.write("charge", id=id, usd=usd)


class MeteredHooks(JournalHooks):
    """Meter/throttle without changing any model-visible instruction."""

    def __init__(self, journal, budget):
        super().__init__(journal)
        self.budget = budget
        self.id = None

    async def on_llm_start(self, context, agent, system_prompt, input_items):
        # UTF-8 byte length is a deliberately conservative token upper bound.
        bound = (
            len(json.dumps([system_prompt, input_items], ensure_ascii=False).encode())
            + 8192
        )
        await self.budget.limiter.acquire()
        await asyncio.sleep(max(0, self.budget.pause_until - time.monotonic()))
        reserve = (bound * 0.20 + 4096 * 1.20) / 1e6
        assert sum(self.budget.charges.values()) + reserve < 4.80, (
            "Budget ceiling reached"
        )
        self.id = uuid.uuid4().hex
        self.budget.set(self.id, reserve)
        await super().on_llm_start(context, agent, system_prompt, input_items)

    async def on_llm_end(self, context, agent, response):
        self.budget.set(
            self.id,
            (
                (
                    response.usage.input_tokens
                    - response.usage.input_tokens_details.cached_tokens
                )
                * 0.20
                + response.usage.input_tokens_details.cached_tokens * 0.02
                + response.usage.input_tokens_details.cache_write_tokens * 0.05
                + response.usage.output_tokens * 1.20
            )
            / 1e6,
        )
        await super().on_llm_end(context, agent, response)


async def run(limit, concurrency):
    """Interleave treatments under one limiter; each question owns fresh state."""
    dataset = read_jsonl(TRACES / "eval.jsonl")
    fingerprint = hashlib.sha256((TRACES / "eval.jsonl").read_bytes()).hexdigest()
    code = source_hash()
    budget = Budget()
    semaphore = asyncio.Semaphore(concurrency)
    counters = {"dense": 0, "hybrid": 0}
    for mode in counters:
        root = ROOT / mode
        root.mkdir(exist_ok=True)
        (root / "eval.jsonl").write_bytes((TRACES / "eval.jsonl").read_bytes())

    async def one(source, style, mode):
        case = f"{source['id']}-{style}"
        root = ROOT / mode
        directory = root / "trajectories" / "gpt-5.6-luna"
        directory.mkdir(parents=True, exist_ok=True)
        for previous in directory.glob(f"{case}.*.jsonl"):
            terminal = read_jsonl(previous)[-1]
            infrastructure = terminal.get(
                "error"
            ) == "Unexpected context size: inspect before spending" or terminal.get(
                "error_type"
            ) in ("CancelledError", "KeyboardInterrupt", "RateLimitError")
            infrastructure |= is_rate_limit(terminal.get("error_type"), terminal.get("error", ""))
            if infrastructure or terminal["event"] not in ("complete", "error"):
                archive = root / "infrastructure-attempts"
                archive.mkdir(exist_ok=True)
                previous.rename(archive / previous.name)
            else:
                counters[mode] += 1
                return
        async with semaphore:
            repository = SemanticStoryRepository(SCRATCH, LIVE, mode=mode)
            runtime = SearchRuntime(
                model="gpt-5.6-luna",
                base_url="https://api.openai.com/v1",
                repository=repository,
                current_date=date(2026, 9, 4),
                max_turns=10,
                max_tokens=4096,
            )
            runtime.client.max_retries = 4
            runtime.client._client.event_hooks["response"].append(budget.feedback)
            runtime.config.model_provider = RateRetryProvider(
                runtime.config.model_provider, budget
            )
            try:
                await run_headless(
                    runtime,
                    source["questions"][style + "_question"],
                    directory / f"{case}.{uuid.uuid4().hex[:10]}.jsonl",
                    timeout=900,
                    metadata={
                        "case": case,
                        "target_id": source["id"],
                        "style": style,
                        "cohort": source["cohort"],
                        "dataset_sha256": fingerprint,
                        "source_sha256": code,
                        "api": "responses",
                        "retrieval": mode,
                    },
                    hooks_factory=lambda journal: MeteredHooks(journal, budget),
                )
            except Exception as exc:
                print(
                    json.dumps({"case": case, "mode": mode, "error": str(exc)}),
                    flush=True,
                )
                if "Budget ceiling" in str(exc):
                    raise
            finally:
                with JournalContext(root / "retrieval.jsonl") as journal:
                    journal.write("session", case=case, **repository.stats)
                await runtime.close()
            counters[mode] += 1
            print(
                json.dumps(
                    {"terminal": counters, "budget_usd": sum(budget.charges.values())}
                ),
                flush=True,
            )

    try:
        async with asyncio.TaskGroup() as tasks:
            for source in dataset[: limit or len(dataset)]:
                for style in ("entity", "paraphrase"):
                    for mode in counters:
                        tasks.create_task(one(source, style, mode))
    finally:
        budget.journal.close()
    for mode in counters:
        report(ROOT / mode)


class JournalContext:
    """Short-lived append journal for session statistics."""

    def __init__(self, path):
        self.journal = Journal(path)

    def __enter__(self):
        return self.journal

    def __exit__(self, *exc):
        self.journal.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Story count; two questions per story per treatment",
    )
    parser.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()
    asyncio.run(run(args.limit, args.concurrency))
