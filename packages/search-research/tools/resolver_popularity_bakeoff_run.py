"""Run the fixed, paired Luna comparison and retain every request receipt.

Only public HN comment text and catalog metadata are sent. Both methods share
model configuration, instructions, and deterministic candidate presentation order.
A resume skips successful requests and permits at most three total attempts per
case/method. Provider account errors stop dispatch, never become abstentions.
"""

import argparse
import asyncio
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path

import httpx
from dotenv import load_dotenv
from resolver_popularity_select import POPULARITY, PROMPT, Pick
from search_research.resolver_bible import BIBLE_ID, BIBLE_PROMPT, bible_candidate

ROOT = Path("data/research/books-resolver-popularity-bakeoff-v1")


def request(case, method, config, bible_rule=False):
    docs = {d["id"]: d for d in case["candidates"]}
    ids = sorted(
        case["shortlists"][method],
        key=lambda k: hashlib.sha256((case["id"] + ":" + k).encode()).hexdigest(),
    )
    candidates = [
        {k: docs[key][k] for k in ("id", "title", "authors")}
        | {
            "popularity": {
                "readinglog_count": docs[key]["readinglog_count"],
                "edition_count": None,
            }
        }
        for key in ids
    ]
    r = case["reference"]
    text, a, b = r["context"], r["start"], r["end"]
    assert text[a:b] == r["title"]
    content = {
        "mention": r["title"],
        "comment": text[:a] + "<mention>" + text[a:b] + "</mention>" + text[b:],
        "candidates": candidates,
    }
    special = bible_rule and bible_candidate(r["title"])
    if special:
        ids.append(BIBLE_ID)
    return config | {
        "messages": [
            {
                "role": "system",
                "content": PROMPT + POPULARITY + (BIBLE_PROMPT if special else ""),
            },
            {"role": "user", "content": json.dumps(content)},
        ]
    }, ids


async def main(
    root=ROOT, methods=("boost", "substitution"), checkpoint=None, bible_rule=False
):
    load_dotenv(".env")
    key = os.environ["OPENROUTER_API_KEY"]
    data = json.loads((root / "cases.json").read_text())
    config = json.loads(
        Path("data/research/books-resolver-2025-v1/luna-config.json").read_text()
    )
    path = root / "receipts.jsonl"
    db = sqlite3.connect(checkpoint) if checkpoint else None
    prior = (
        [json.loads(l) for l in path.read_text().splitlines()] if path.exists() else []
    )
    done = {(r["id"], r["method"]) for r in prior if "selection" in r}
    attempts = {}
    for r in prior:
        k = (r["id"], r["method"])
        attempts[k] = max(attempts.get(k, 0), r["attempt"])
    stopped = asyncio.Event()
    gate = asyncio.Semaphore(16)
    completed = len(done)
    async with httpx.AsyncClient(timeout=120) as client:

        async def one(case, method):
            nonlocal completed
            k = (case["id"], method)
            if k in done:
                return
            async with gate:
                body, ids = request(case, method, config, bible_rule)
                for attempt in range(attempts.get(k, 0) + 1, 4):
                    if stopped.is_set():
                        return
                    record = {
                        "id": case["id"],
                        "method": method,
                        "attempt": attempt,
                        "request": body,
                        "created_unix": time.time(),
                    }
                    start = time.monotonic()
                    try:
                        response = await client.post(
                            "https://openrouter.ai/api/v1/chat/completions",
                            headers={"Authorization": "Bearer " + key},
                            json=body,
                        )
                        record["http_status"] = response.status_code
                        record["response"] = response.json()
                        if response.status_code in (401, 402, 403, 429):
                            stopped.set()
                        response.raise_for_status()
                        choice = record["response"]["choices"][0]
                        assert choice["finish_reason"] == "stop", choice[
                            "finish_reason"
                        ]
                        pick = Pick.model_validate_json(choice["message"]["content"])
                        assert pick.work_id is None or pick.work_id in ids
                        record["selection"] = pick.model_dump()
                    except (
                        httpx.HTTPError,
                        ValueError,
                        KeyError,
                        AssertionError,
                    ) as exc:
                        record["error"] = str(exc)
                    record["seconds"] = time.monotonic() - start
                    with path.open("a") as stream:
                        stream.write(json.dumps(record) + "\n")
                    if "selection" in record:
                        if db is not None:
                            db.execute(
                                "INSERT OR REPLACE INTO selections(id,model,payload) VALUES (?,?,?)",
                                (case["id"], "luna", json.dumps(record)),
                            )
                            db.commit()
                        done.add(k)
                        completed += 1
                        if completed % 25 == 0:
                            print(
                                json.dumps(
                                    {
                                        "completed": completed,
                                        "planned": len(data["cases"]) * len(methods),
                                    }
                                ),
                                flush=True,
                            )
                        return
                    print(
                        json.dumps(
                            {
                                k: v
                                for k, v in record.items()
                                if k not in ("request", "response")
                            }
                        ),
                        flush=True,
                    )
                    if stopped.is_set():
                        return
                    await asyncio.sleep(attempt)

        # One successful probe precedes bulk dispatch.
        await one(data["cases"][0], "boost")
        if not stopped.is_set():
            await asyncio.gather(*(one(c, m) for c in data["cases"] for m in methods))
    print(
        json.dumps(
            {
                "successful": len(done),
                "planned": len(data["cases"]) * len(methods),
                "provider_stopped": stopped.is_set(),
            }
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--boost-only", action="store_true")
    parser.add_argument("--checkpoint", type=Path)
    parser.add_argument("--bible-rule", action="store_true")
    args = parser.parse_args()
    asyncio.run(
        main(
            args.root,
            ("boost",) if args.boost_only else ("boost", "substitution"),
            args.checkpoint,
            args.bible_rule,
        )
    )
