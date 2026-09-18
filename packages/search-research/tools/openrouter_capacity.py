"""Bounded streaming capacity probe using saved Luna requests, without tool execution.

Each request is used at most once per invocation. Closed-loop workers ramp only
between stages; the first error stops new work while in-flight requests drain.
Reservations bound spend even when a failed request has unknown billed usage.
This measures model serving, not agent success or full E2E runtime.
"""

import argparse
import asyncio
import hashlib
import json
import os
import random
import time
from pathlib import Path

import httpx
import numpy as np
from agents.models.openai_responses import Converter
from search_agent.tools import (
    fetch_stories,
    fetch_top_comments,
    fetch_top_stories_for_date,
)

MODEL = "openai/gpt-5.6-luna"
URL = "https://openrouter.ai/api/v1/responses"


def requests():
    """Balance short/long contexts in 32-request blocks without duplicating inputs."""
    rows = []
    for path in sorted(
        Path("data/luna-semantic-20260904").glob("*/trajectories/gpt-5.6-luna/*.jsonl")
    ):
        inputs = {}
        for line in path.open():
            row = json.loads(line)
            if row["event"] == "model_input":
                inputs[row["request"]] = row
            elif row["event"] == "model_output":
                rows.append(
                    {
                        "source": str(path),
                        "request": row["request"],
                        "tokens": row["usage"]["input_tokens"],
                        "input": inputs[row["request"]],
                    }
                )
    assert len(rows) == 1538
    rows.sort(key=lambda r: r["tokens"])
    rng = random.Random(20260905)
    bins = [list(x) for x in np.array_split(np.arange(len(rows)), 32)]
    for bucket in bins:
        rng.shuffle(bucket)
    order = []
    while any(bins):
        block = [int(b.pop()) for b in bins if b]
        rng.shuffle(block)
        order.extend(block)
    return [rows[i] for i in order]


class Probe:
    """One shared accounting ledger and bounded HTTP connection pool."""

    def __init__(self, root, pool, dollar_cap, request_cap):
        root.mkdir(parents=True, exist_ok=True)
        assert not (root / "requests.jsonl").exists(), "Use a fresh probe directory"
        self.root, self.pool = root, pool
        self.cursor, self.charge = 0, 0.0
        self.dollar_cap, self.request_cap = dollar_cap, request_cap
        self.tools = Converter.convert_tools(
            [fetch_stories, fetch_top_stories_for_date, fetch_top_comments], []
        ).tools
        self.client = httpx.AsyncClient(
            headers={"Authorization": "Bearer " + os.environ["OPENROUTER_API_KEY"]},
            timeout=httpx.Timeout(90, connect=15),
            limits=httpx.Limits(max_connections=32, max_keepalive_connections=32),
        )
        self.log = (root / "requests.jsonl").open("a", buffering=1)
        self.summaries = []

    async def call(self, row, concurrency, index):
        """Consume all SSE events; retain actual usage and response/provider identity."""
        payload = {
            "model": MODEL,
            "instructions": row["input"]["system_prompt"],
            "input": row["input"]["items"],
            "tools": self.tools,
            "max_output_tokens": 4096,
            "store": False,
            "stream": True,
            "service_tier": "default",
            "provider": {
                "only": ["openai"],
                "allow_fallbacks": False,
                "require_parameters": True,
            },
        }
        started = time.perf_counter()
        receipt = {
            "index": index,
            "concurrency": concurrency,
            "source": row["source"],
            "request": row["request"],
            "original_input_tokens": row["tokens"],
            "payload_sha256": hashlib.sha256(
                json.dumps(payload, sort_keys=True).encode()
            ).hexdigest(),
        }
        final = None
        try:
            async with self.client.stream("POST", URL, json=payload) as response:
                receipt["http_status"] = response.status_code
                receipt["retry_after"] = response.headers.get("retry-after")
                if response.status_code != 200:
                    raise RuntimeError((await response.aread()).decode()[:1200])
                async for line in response.aiter_lines():
                    if not line.startswith("data:") or line[5:].strip() == "[DONE]":
                        continue
                    event = json.loads(line[5:])
                    if "first_event_seconds" not in receipt:
                        receipt["first_event_seconds"] = time.perf_counter() - started
                    if event.get("error") or event.get("type") in (
                        "error",
                        "response.failed",
                    ):
                        raise RuntimeError(json.dumps(event)[:1200])
                    if event.get("type") in (
                        "response.completed",
                        "response.incomplete",
                    ):
                        final = event["response"]
            assert final is not None, "Stream ended without final response"
            receipt.update(
                ok=True,
                usage=final["usage"],
                response_id=final["id"],
                model=final["model"],
                provider=final.get("provider"),
                service_tier=final.get("service_tier"),
                status=final["status"],
            )
            assert final["status"] == "completed", final.get("incomplete_details")
            (self.root / f"response-{index:04d}.json").write_text(json.dumps(final))
        except (
            httpx.HTTPError,
            RuntimeError,
            AssertionError,
            json.JSONDecodeError,
        ) as exc:
            receipt.update(ok=False, error_type=type(exc).__name__, error=str(exc))
        receipt["seconds"] = time.perf_counter() - started
        return receipt

    async def stage(self, concurrency, duration, max_requests):
        """Drain on any error, deadline, request cap or accounting limit; never retry."""
        started, count = time.perf_counter(), 0
        results, stop_reason = [], None

        async def worker():
            nonlocal count, stop_reason
            while (
                not stop_reason
                and time.perf_counter() - started < duration
                and count < max_requests
            ):
                if self.cursor >= min(len(self.pool), self.request_cap):
                    stop_reason = "request_cap"
                    break
                row = self.pool[self.cursor]
                reserve = ((row["tokens"] * 1.5 + 8192) * 0.25 + 4096 * 1.2) / 1e6
                if self.charge + reserve > self.dollar_cap:
                    stop_reason = "dollar_cap"
                    break
                self.charge += reserve
                index = self.cursor
                self.cursor += 1
                count += 1
                receipt = await self.call(row, concurrency, index)
                if receipt["ok"]:
                    usage = receipt["usage"]
                    estimated = (
                        usage["input_tokens"] * 0.25 + usage["output_tokens"] * 1.2
                    ) / 1e6
                    charged = usage.get("cost", estimated)
                    if charged is None:
                        charged = estimated
                    self.charge += charged - reserve
                    receipt["charge_or_upper_estimate_usd"] = charged
                else:
                    stop_reason = "request_error"
                    receipt["unknown_charge_reservation_usd"] = reserve
                self.log.write(json.dumps(receipt) + "\n")
                results.append(receipt)

        await asyncio.gather(*(worker() for _ in range(concurrency)))
        elapsed = time.perf_counter() - started
        good = [r for r in results if r["ok"]]
        latencies = [r["seconds"] for r in good]
        input_tokens = sum(r["usage"]["input_tokens"] for r in good)
        output_tokens = sum(r["usage"]["output_tokens"] for r in good)
        summary = {
            "concurrency": concurrency,
            "seconds": elapsed,
            "requests": len(results),
            "successes": len(good),
            "errors": len(results) - len(good),
            "stop_reason": stop_reason,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "input_tpm": input_tokens * 60 / elapsed,
            "rpm": len(good) * 60 / elapsed,
            "p50_seconds": float(np.median(latencies)) if good else None,
            "p95_seconds": float(np.quantile(latencies, 0.95)) if good else None,
            "ledger_usd": self.charge,
        }
        self.summaries.append(summary)
        (self.root / "summary.json").write_text(json.dumps(self.summaries, indent=2))
        print(json.dumps(summary), flush=True)
        if results and not good:
            print(json.dumps(results[0]), flush=True)
        return summary


async def main(args):
    pool = requests()[args.skip :]
    probe = Probe(args.root, pool, args.dollars, args.max_requests)
    (args.root / "manifest.json").write_text(
        json.dumps(
            {
                "model": MODEL,
                "url": URL,
                "service_tier": "default",
                "provider": "openai",
                "concurrency": args.concurrency,
                "seconds_per_stage": args.seconds,
                "dollar_cap": args.dollars,
                "request_cap": args.max_requests,
                "skip_source_requests": args.skip,
                "streaming": True,
                "max_output_tokens": 4096,
                "tools_sha256": hashlib.sha256(
                    json.dumps(probe.tools, sort_keys=True).encode()
                ).hexdigest(),
                "source_sha256": hashlib.sha256(
                    Path(__file__).read_bytes()
                ).hexdigest(),
            },
            indent=2,
        )
    )
    try:
        for concurrency in args.concurrency:
            summary = await probe.stage(concurrency, args.seconds, args.stage_requests)
            if summary["stop_reason"]:
                break
            if len(probe.summaries) >= 2:
                previous = probe.summaries[-2]
                if (
                    summary["input_tpm"] < previous["input_tpm"] * 1.1
                    and summary["p95_seconds"] > previous["p95_seconds"] * 1.5
                ):
                    print(
                        "Stopping ramp: <10% throughput gain and >50% p95 latency increase.",
                        flush=True,
                    )
                    break
    finally:
        probe.log.close()
        await probe.client.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument(
        "--concurrency", type=int, nargs="+", default=[2, 4, 8, 16, 24, 32]
    )
    parser.add_argument("--seconds", type=float, default=35)
    parser.add_argument("--stage-requests", type=int, default=1200)
    parser.add_argument("--max-requests", type=int, default=1200)
    parser.add_argument("--dollars", type=float, default=10)
    parser.add_argument("--skip", type=int, default=0)
    args = parser.parse_args()
    assert all(1 <= c <= 32 for c in args.concurrency)
    asyncio.run(main(args))
