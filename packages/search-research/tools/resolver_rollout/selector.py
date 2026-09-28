"""Continuously label completed shortlists, retaining exact requests and receipts.

One process per provider with configurable concurrency. Restart skips successes;
individual errors are retried and retained without stopping the remaining queue.
"""

import argparse
import asyncio
import fcntl
import json
import os
import random
import signal
import time
from pathlib import Path

import httpx
from common import PROMPT, RUN, connect, marked, status
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict


class Pick(BaseModel):
    model_config = ConfigDict(extra="forbid")
    work_id: str | None


async def main(args):
    lock = (RUN / f"{args.arm}.lock").open("w")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    load_dotenv(Path.cwd() / ".env")
    native = args.native
    assert not native or args.arm == "deepseek-native"
    key = os.environ["DEEPSEEK_API_KEY" if native else "OPENROUTER_API_KEY"]
    endpoint = (
        "https://api.deepseek.com/chat/completions"
        if native
        else "https://openrouter.ai/api/v1/chat/completions"
    )
    db = connect()
    docs = {k: json.loads(p) for k, p in db.execute("SELECT id,payload FROM documents")}
    model = "openai/gpt-6-luna" if args.arm == "luna" else "deepseek/deepseek-v4-flash"
    config = {
        "model": model,
        "reasoning": {"effort": "medium" if args.arm == "luna" else "low"},
        "max_tokens": 2048 if args.arm == "luna" else 8192,
        "provider": {"allow_fallbacks": False, "require_parameters": True},
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "pick",
                "strict": True,
                "schema": Pick.model_json_schema(),
            },
        },
    }
    if args.arm == "luna":
        config["provider"]["only"] = ["openai"]
    if native:
        model = "deepseek-flash"
        config.pop("provider")
        config.pop("reasoning")
        config.update(
            model=model,
            thinking={"type": "enabled"},
            reasoning_effort="low",
            response_format={"type": "json_object"},
        )
    config_path = RUN / f"{args.arm}-config.json"
    if config_path.exists():
        assert json.loads(config_path.read_text()) == config, (
            "Do not resume with different request settings."
        )
    else:
        config_path.write_text(json.dumps(config, indent=2))
    # A separate history preserves failed paid attempts even after recovery.
    db.execute(
        "CREATE TABLE IF NOT EXISTS attempts(id TEXT, model TEXT, attempt INTEGER, payload TEXT NOT NULL, PRIMARY KEY(id,model,attempt))"
    )
    db.execute("""INSERT OR IGNORE INTO attempts
        SELECT f.id,f.stage,0,f.payload FROM failures f
        WHERE NOT EXISTS (SELECT 1 FROM attempts a WHERE a.id=f.id AND a.model=f.stage)
    """)
    db.commit()
    total = db.execute("SELECT count(*) FROM refs").fetchone()[0]
    ranked = db.execute("SELECT count(*) FROM rankings").fetchone()[0]
    assert total == ranked, "Resume labelers after reranking is complete"
    rows = db.execute(
        "SELECT r.payload,k.payload FROM refs r JOIN rankings k ON k.id=r.id LEFT JOIN selections s ON s.id=r.id AND s.model=? WHERE s.id IS NULL ORDER BY r.ordinal",
        (args.arm,),
    ).fetchall()
    if args.limit:
        rows = rows[: args.limit]
    queue = asyncio.Queue()
    for row in rows:
        queue.put_nowait(row)
    draining = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, draining.set)
    loop.add_signal_handler(signal.SIGINT, draining.set)
    start = time.monotonic()
    completed = 0
    failed = 0
    policy = {
        "concurrency": args.concurrency,
        "attempts_per_resume": 5,
        "length_retry": "double output allowance, maximum 65536",
        "transport_retry": "exponential backoff; preserve each attempt",
    }
    (RUN / f"{args.arm}-recovery-policy.json").write_text(json.dumps(policy, indent=2))
    async with httpx.AsyncClient(
        headers={"Authorization": "Bearer " + key},
        timeout=180,
        limits=httpx.Limits(max_connections=args.concurrency),
    ) as client:

        async def one(row, ranking):
            ids = ranking["top3"].copy()
            random.Random("20260926:" + row["id"]).shuffle(ids)
            payload = {
                "mention": row["title"],
                "comment": marked(row),
                "candidates": [docs[k] for k in ids],
            }
            body = config | {
                "messages": [
                    {
                        "role": "system",
                        "content": PROMPT
                        + (
                            "\nReturn JSON with exactly one key, work_id (string or null)."
                            if native
                            else ""
                        ),
                    },
                    {"role": "user", "content": json.dumps(payload)},
                ]
            }
            previous = db.execute(
                "SELECT attempt,payload FROM attempts WHERE id=? AND model=? ORDER BY attempt DESC LIMIT 1",
                (row["id"], args.arm),
            ).fetchone()
            attempt = previous[0] + 1 if previous else 1
            if previous:
                old = json.loads(previous[1])
                if (
                    old.get("response", {}).get("choices", [{}])[0].get("finish_reason")
                    == "length"
                ):
                    body["max_tokens"] = min(65536, old["request"]["max_tokens"] * 2)
            for retry in range(5):
                receipt = {
                    "id": row["id"],
                    "requested_model": model,
                    "endpoint": endpoint,
                    "request": body.copy(),
                    "attempt": attempt,
                    "created_unix": time.time(),
                }
                t = time.monotonic()
                failure = None
                try:
                    response = await client.post(endpoint, json=body)
                    receipt["http_status"] = response.status_code
                    try:
                        receipt["response"] = response.json()
                    except ValueError:
                        receipt["response_text"] = response.text
                        raise ValueError("Provider returned non-JSON response")
                    response.raise_for_status()
                    pick = validate_pick(receipt["response"], ids)
                except (
                    httpx.HTTPError,
                    ValueError,
                    KeyError,
                    IndexError,
                    TypeError,
                ) as exc:
                    failure = exc
                    receipt["error"] = f"{type(exc).__name__}: {str(exc)[:500]}"
                receipt["seconds"] = time.monotonic() - t
                db.execute(
                    "INSERT INTO attempts VALUES (?,?,?,?)",
                    (row["id"], args.arm, attempt, json.dumps(receipt)),
                )
                if failure is None:
                    receipt["selection"] = pick.model_dump()
                    db.execute(
                        "INSERT INTO selections VALUES (?,?,?)",
                        (row["id"], args.arm, json.dumps(receipt)),
                    )
                    db.execute(
                        "DELETE FROM failures WHERE id=? AND stage=?",
                        (row["id"], args.arm),
                    )
                    db.commit()
                    return True
                db.execute(
                    "INSERT OR REPLACE INTO failures VALUES (?,?,?)",
                    (row["id"], args.arm, json.dumps(receipt)),
                )
                db.commit()
                if isinstance(failure, OutputLimit):
                    body["max_tokens"] = min(65536, body["max_tokens"] * 2)
                if receipt.get("http_status") in (401, 402, 403):
                    # Account-wide failures require intervention, not thousands of retries.
                    draining.set()
                    raise RuntimeError(receipt["error"])
                attempt += 1
                if retry < 4:
                    await asyncio.sleep(min(30, 2**retry))
            return False

        async def worker():
            nonlocal completed, failed
            while not queue.empty() and not draining.is_set():
                r, k = queue.get_nowait()
                ok = await one(json.loads(r), json.loads(k))
                completed += int(ok)
                failed += int(not ok)
                progress = {
                    "state": "running",
                    "newly_completed": completed,
                    "unresolved": failed,
                    "queued": queue.qsize(),
                    "seconds": time.monotonic() - start,
                }
                status(db, args.arm, progress)
                if (completed + failed) % 100 == 0:
                    print(json.dumps(progress), flush=True)

        status(db, args.arm, {"state": "running", "model": model, "policy": policy})
        tasks = [asyncio.create_task(worker()) for _ in range(args.concurrency)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        errors = [r for r in results if isinstance(r, BaseException)]
        if errors:
            raise errors[0]
    done = db.execute(
        "SELECT count(*) FROM selections WHERE model=?", (args.arm,)
    ).fetchone()[0]
    state = (
        "complete"
        if done == total
        else "slice_complete"
        if args.limit
        else "incomplete"
    )
    status(
        db,
        args.arm,
        {
            "state": state,
            "references": done,
            "total": total,
            "unresolved": failed,
            "seconds": time.monotonic() - start,
        },
    )
    print(json.dumps({"state": state, "references": done, "total": total}), flush=True)
    return 0 if state in ("complete", "slice_complete") else 2


class OutputLimit(ValueError):
    """A paid response ran out of output tokens without returning a final label."""


def validate_pick(data, ids):
    """Reject incomplete output and foreign IDs; neither becomes an abstention."""
    choice = data["choices"][0]
    if choice["finish_reason"] == "length":
        raise OutputLimit("Output token allowance exhausted")
    if choice["finish_reason"] != "stop":
        raise ValueError(f"Unexpected finish reason: {choice['finish_reason']}")
    pick = Pick.model_validate_json(choice["message"]["content"])
    if pick.work_id is not None and pick.work_id not in ids:
        raise ValueError(f"Selected ID outside supplied candidates: {pick.work_id}")
    return pick


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("arm", choices=["luna", "deepseek", "deepseek-native"])
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--native", action="store_true")
    args = parser.parse_args()
    try:
        raise SystemExit(asyncio.run(main(args)))
    except BlockingIOError:
        print(f"{args.arm} already has an active worker; no requests sent.", flush=True)
        raise SystemExit(3)
    except Exception as exc:
        status(connect(), args.arm, {"state": "failed", "error": repr(exc)})
        raise
