"""Run resumable multi-result decisions; preserve every request and failed attempt."""

import argparse
import asyncio
import fcntl
import hashlib
import json
import os
import random
import time
from pathlib import Path

import httpx
from dotenv import load_dotenv
from journal import Journal, fingerprint
from schema import PROMPT, Decision
from selector_metrics import RunMetrics

ROOT = Path(os.environ.get("RESOLVER_RUN_ROOT", "data/research/books-resolver-heal-v1"))


def request(case, round_number, config):
    r = case["reference"]
    text = r["context"]
    a, b = r["start"], r["end"]
    assert text[a:b] == r["title"]
    docs = {d["id"]: d for d in case["candidates"]}
    ids = sorted(
        case["top3"],
        key=lambda k: hashlib.sha256((case["id"] + ":" + k).encode()).hexdigest(),
    )
    content = {
        "original_mention": r["title"],
        "comment": text[:a] + "<mention>" + text[a:b] + "</mention>" + text[b:],
        "person_names": [s["text"] for s in case["person_spans"]],
        "candidates": [
            {k: docs[i][k] for k in ("id", "title", "authors", "readinglog_count")}
            for i in ids
        ],
    }
    if round_number:
        content["search_target"] = {
            k: case[k] for k in ("query_title", "query_author", "target_reason")
        }
    prompt = PROMPT
    if round_number >= 2:
        prompt += "\nThis is the last retrieval round. Select the most useful matching supplied work or abstain if none fits; do not request another search."
    return config | {
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps(content)},
        ]
    }


async def main(args):
    load_dotenv(".env")
    key = os.environ["OPENROUTER_API_KEY"]
    cases = json.loads((ROOT / f"round{args.round}-ready.json").read_text())["cases"]
    if args.limit is not None:
        cases = cases[: args.limit]
    config = json.loads((ROOT / "luna-config.json").read_text())
    config["max_tokens"] = 4096
    config["response_format"] = {
        "type": "json_schema",
        "json_schema": {
            "name": "resolutions",
            "strict": True,
            "schema": Decision.model_json_schema(),
        },
    }
    path = ROOT / f"round{args.round}-decisions.jsonl"
    journal = Journal(ROOT)
    prior = journal.records(args.round)
    if not prior and path.exists():
        for line in path.open():
            journal.save(json.loads(line))
        prior = journal.records(args.round)
    bodies = {c["id"]: request(c, args.round, config) for c in cases}
    for record in prior:
        if record["id"] in bodies:
            assert fingerprint(record["request"]) == fingerprint(
                bodies[record["id"]]
            ), "Saved request differs; use a new run directory"
    done = {r["id"] for r in prior if "decision" in r}
    attempts = {}
    for r in prior:
        attempts[r["id"]] = max(attempts.get(r["id"], 0), r["attempt"])
    pending = [c for c in cases if c["id"] not in done]
    if args.order_seed is not None:
        random.Random(args.order_seed).shuffle(pending)
    assert args.take is None or args.take > 0, "Pending-case limit must be positive"
    if args.take is not None:
        pending = pending[: args.take]
    assert args.concurrency > 0
    label = args.run_label or f"round{args.round}-{time.time_ns()}"
    assert label.replace("-", "").replace("_", "").isalnum(), "Invalid metrics label"
    assert not (ROOT / f"selector-metrics-{label}.json").exists(), (
        "Metrics label already used"
    )
    gate = asyncio.Semaphore(args.concurrency)
    stopped = asyncio.Event()
    stop_reason = None
    metrics = RunMetrics(
        ROOT, args.round, args.concurrency, label, [c["id"] for c in pending]
    )
    async with httpx.AsyncClient(
        timeout=120,
        limits=httpx.Limits(
            max_connections=args.concurrency, max_keepalive_connections=args.concurrency
        ),
    ) as client:

        async def one(c):
            nonlocal stop_reason
            if c["id"] in done:
                return
            async with gate:
                if stopped.is_set():
                    return
                body = bodies[c["id"]]
                for attempt in range(attempts.get(c["id"], 0) + 1, 4):
                    if stopped.is_set():
                        return
                    if journal.cost() >= args.budget:
                        stop_reason = "budget"
                        stopped.set()
                        return
                    record = {
                        "id": c["id"],
                        "attempt": attempt,
                        "request": body,
                        "round": args.round,
                        "run_label": label,
                        "concurrency": args.concurrency,
                    }
                    started = time.monotonic()
                    started_unix = time.time()
                    try:
                        response = await client.post(
                            "https://openrouter.ai/api/v1/chat/completions",
                            headers={"Authorization": "Bearer " + key},
                            json=body,
                        )
                        record["response"] = response.json()
                        record["http_status"] = response.status_code
                        if "retry-after" in response.headers:
                            record["retry_after"] = response.headers["retry-after"]
                        if response.status_code in (401, 402, 403, 429):
                            stop_reason = f"http_{response.status_code}"
                            stopped.set()
                        response.raise_for_status()
                        choice = record["response"]["choices"][0]
                        assert choice["finish_reason"] == "stop", choice[
                            "finish_reason"
                        ]
                        decision = Decision.model_validate_json(
                            choice["message"]["content"]
                        )
                        for r in decision.results:
                            assert (
                                r.work_id is None
                                or r.work_id in c["top3"]
                                or r.work_id == "special:bible"
                            ), "Unknown selected work"
                            assert args.round < 2 or r.action != "search", (
                                "Retrieval budget exhausted"
                            )
                        record["decision"] = decision.model_dump()
                    except (
                        httpx.HTTPError,
                        ValueError,
                        KeyError,
                        AssertionError,
                    ) as exc:
                        record["error"] = str(exc)
                        record["error_type"] = type(exc).__name__
                        record["error_detail"] = repr(exc)
                    record["timing"] = {
                        "started_unix": started_unix,
                        "service_seconds": time.monotonic() - started,
                    }
                    commit_started = time.monotonic()
                    journal.save(record)
                    metrics.add(record, time.monotonic() - commit_started)
                    if "decision" in record:
                        done.add(c["id"])
                        if len(done) % 100 == 0:
                            print(
                                f"Round {args.round}: {len(done)}/{len(cases)}",
                                flush=True,
                            )
                        return
                print(
                    f"Exhausted attempts for {c['id']}; other cases continue",
                    flush=True,
                )

        # Drain every active request even when one task fails. Each response is
        # committed before a task returns; a process restart skips saved successes.
        results = await asyncio.gather(
            *(one(c) for c in pending), return_exceptions=True
        )
    metrics.finish(done, stop_reason)
    journal.export(args.round)
    errors = [r for r in results if isinstance(r, BaseException)]
    assert not errors, f"Worker failures after draining active calls: {errors!r}"
    assert {c["id"] for c in pending} <= done, (
        "Incomplete round; receipts saved for resume"
    )
    print(
        json.dumps({"round": args.round, "completed": len(done), "total": len(cases)}),
        flush=True,
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--round", type=int, choices=[0, 1, 2], default=0)
    p.add_argument(
        "--limit", type=int, help="Smoke test prefix; successful calls are reused"
    )
    p.add_argument("--concurrency", type=int, default=16)
    p.add_argument(
        "--take",
        type=int,
        help="Process this many pending cases; successful work is retained",
    )
    p.add_argument(
        "--order-seed",
        type=int,
        help="Shuffle pending cases reproducibly for useful-work comparisons",
    )
    p.add_argument(
        "--run-label", help="Unique label for per-attempt timings and the batch summary"
    )
    p.add_argument(
        "--budget",
        type=float,
        default=12.0,
        help="Total recorded dollars across all rounds; active calls drain",
    )
    with (ROOT / "selector.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        asyncio.run(main(p.parse_args()))
