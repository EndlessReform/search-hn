"""Measure managed book extraction, retaining per-request usage and cache counts.

Run with uv run --locked --package search-research python this_file.py.
Each invocation is a bounded, explicitly selected packet. No automatic retries or
warmup replays: use disjoint packets for new-comment throughput and a separately
named invocation for cache replay. API keys are read from the environment/.env
and are never copied into artifacts.
"""

import argparse
import asyncio
import hashlib
import json
import os
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter

import httpx
from comment_book_llm import Extraction, encode_record
from dotenv import load_dotenv


def request_body(provider, prompt, text, max_tokens, effort):
    """Use native provider controls with the same extraction contract."""
    body = {
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": text},
        ],
        "max_tokens": max_tokens,
        "stream": False,
    }
    if provider == "deepseek":
        return body | {
            "model": "deepseek-flash",
            "thinking": {"type": "enabled"},
            "reasoning_effort": effort,
            "response_format": {"type": "json_object"},
        }
    assert provider == "luna"
    return body | {
        "model": "openai/gpt-5.6-luna",
        "reasoning": {"effort": effort},
        "provider": {
            "only": ["openai"],
            "allow_fallbacks": False,
            "require_parameters": True,
        },
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "book_extraction",
                "strict": True,
                "schema": Extraction.model_json_schema(),
            },
        },
    }


def accounting(provider, usage, started):
    """Separate cache reads, fresh input and all generated tokens before pricing.

    DeepSeek publishes time-dependent rates; record both schedules and the one
    applying when the request started. OpenRouter's reported charge is retained
    separately from token-based reconstruction, including any cache writes.
    """
    inputs, outputs = usage["prompt_tokens"], usage["completion_tokens"]
    details = usage.get("completion_tokens_details") or {}
    reasoning = details.get("reasoning_tokens")
    if provider == "deepseek":
        cached = usage["prompt_cache_hit_tokens"]
        uncached = usage["prompt_cache_miss_tokens"]
        assert cached + uncached == inputs, usage
        offpeak = (uncached * 0.15 + cached * 0.003 + outputs * 0.60) / 1e6
        peak = started.weekday() < 5 and (
            1 <= started.hour < 4 or 6 <= started.hour < 10
        )
        return {
            "input_tokens": inputs,
            "cached_tokens": cached,
            "uncached_tokens": uncached,
            "output_tokens": outputs,
            "reasoning_tokens": reasoning,
            "calculated_cost": offpeak * (2 if peak else 1),
            "offpeak_cost": offpeak,
            "peak_cost": offpeak * 2,
            "uncached_cost": (inputs * 0.15 + outputs * 0.60) / 1e6,
            "reported_cost": usage.get("cost"),
        }
    prompt_details = usage["prompt_tokens_details"]
    cached = prompt_details["cached_tokens"]
    writes = prompt_details.get("cache_write_tokens", 0)
    assert cached + writes <= inputs, usage
    return {
        "input_tokens": inputs,
        "cached_tokens": cached,
        "uncached_tokens": inputs - cached,
        "cache_write_tokens": writes,
        "output_tokens": outputs,
        "reasoning_tokens": reasoning,
        "calculated_cost": (
            (inputs - cached - writes) * 0.20
            + cached * 0.02
            + writes * 0.25
            + outputs * 1.20
        )
        / 1e6,
        "uncached_cost": ((inputs - writes) * 0.20 + writes * 0.25 + outputs * 1.20)
        / 1e6,
        "reported_cost": usage["cost"],
    }


async def run(args):
    """Dispatch a bounded queue and flush each receipt as soon as it finishes."""
    load_dotenv()
    key_name = (
        "DEEPSEEK_API_KEY" if args.provider == "deepseek" else "OPENROUTER_API_KEY"
    )
    key = os.environ[key_name]
    base = (
        "https://api.deepseek.com"
        if args.provider == "deepseek"
        else "https://openrouter.ai/api/v1"
    )
    prompt = args.prompt_file.read_text()
    rows = [json.loads(s) for s in args.input.read_text().splitlines()]
    if args.limit:
        rows = rows[: args.limit]
    assert rows and len({r["comment_id"] for r in rows}) == len(rows)
    args.out.mkdir(parents=True, exist_ok=False)
    manifest = {
        "provider": args.provider,
        "base_url": base,
        "input": str(args.input),
        "input_sha256": hashlib.sha256(args.input.read_bytes()).hexdigest(),
        "comments": len(rows),
        "concurrency": args.concurrency,
        "request": request_body(
            args.provider, prompt, "<complete comment>", args.max_tokens, args.effort
        ),
        "started_at": datetime.now(UTC).isoformat(),
    }
    (args.out / "request.json").write_text(encode_record(manifest))
    semaphore = asyncio.Semaphore(args.concurrency)
    results = []
    started = perf_counter()
    async with httpx.AsyncClient(
        base_url=base,
        headers={"Authorization": f"Bearer {key}"},
        timeout=300,
        limits=httpx.Limits(max_connections=args.concurrency),
    ) as client:
        with (args.out / "responses.jsonl").open("w") as stream:

            async def one(row):
                async with semaphore:
                    timestamp, begin = datetime.now(UTC), perf_counter()
                    result = {
                        "comment_id": row["comment_id"],
                        "started_at": timestamp.isoformat(),
                    }
                    try:
                        response = await client.post(
                            "/chat/completions",
                            json=request_body(
                                args.provider,
                                prompt,
                                row["text"],
                                args.max_tokens,
                                args.effort,
                            ),
                        )
                        result.update(
                            status=response.status_code, seconds=perf_counter() - begin
                        )
                        if response.status_code != 200:
                            result["error"] = response.text
                        else:
                            data = response.json()
                            result["response"] = data
                            result["usage"] = data["usage"]
                            choice = data["choices"][0]
                            result["finish_reason"] = choice["finish_reason"]
                            if choice["finish_reason"] != "stop":
                                result["error"] = "Incomplete generation"
                            else:
                                try:
                                    result["extraction"] = (
                                        Extraction.model_validate_json(
                                            choice["message"]["content"]
                                        ).model_dump()
                                    )
                                except (ValueError, TypeError) as error:
                                    result["error"] = str(error)
                            try:
                                result["accounting"] = accounting(
                                    args.provider, data["usage"], timestamp
                                )
                            except (KeyError, AssertionError, TypeError) as error:
                                result["accounting_error"] = repr(error)
                    except httpx.TransportError as error:
                        result.update(
                            seconds=perf_counter() - begin, error=repr(error), status=0
                        )
                    result["finished_at"] = datetime.now(UTC).isoformat()
                    stream.write(encode_record(result) + "\n")
                    stream.flush()
                    results.append(result)
                    if len(results) % 128 == 0:
                        print(f"Completed {len(results)}/{len(rows)}", flush=True)

            await asyncio.gather(*(one(row) for row in rows))
    seconds = perf_counter() - started
    valid = sum("extraction" in r for r in results)
    latency = sorted(r["seconds"] for r in results)
    bills = [r["accounting"] for r in results if "accounting" in r]
    totals = {
        k: (
            sum(b[k] for b in bills if b.get(k) is not None)
            if any(b.get(k) is not None for b in bills)
            else None
        )
        for k in [
            "input_tokens",
            "cached_tokens",
            "cache_write_tokens",
            "uncached_tokens",
            "output_tokens",
            "reasoning_tokens",
            "calculated_cost",
            "reported_cost",
            "offpeak_cost",
            "peak_cost",
            "uncached_cost",
        ]
    }
    summary = manifest | {
        "seconds": seconds,
        "valid": valid,
        "errors": len(rows) - valid,
        "accounted_requests": len(bills),
        "statuses": dict(Counter(r["status"] for r in results)),
        "comments_per_second": len(rows) / seconds,
        "valid_per_second": valid / seconds,
        "p50_seconds": latency[len(latency) // 2],
        "p95_seconds": latency[int(len(latency) * 0.95)],
        "totals": totals,
    }
    (args.out / "summary.json").write_text(encode_record(summary))
    print(json.dumps({k: v for k, v in summary.items() if k != "request"}), flush=True)
    assert not any("accounting_error" in r for r in results), (
        "Inspect accounting_error receipts"
    )
    assert valid == len(rows), (
        "Inspect failed responses; no automatic retries were issued"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=["deepseek", "luna"], required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--prompt-file", type=Path, required=True)
    parser.add_argument("--concurrency", type=int, required=True)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--max-tokens", type=int, default=8192)
    parser.add_argument(
        "--effort", choices=["low", "medium", "high", "max"], required=True
    )
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
