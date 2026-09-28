"""Exercise useful-work concurrency trials against a durable local receipt journal."""

import argparse
import asyncio
import importlib.util
import json
import sqlite3
from pathlib import Path

import httpx
import pytest


@pytest.fixture
def selector(tmp_path, monkeypatch):
    tools = Path(__file__).parents[1] / "tools/resolver_heal"
    monkeypatch.syspath_prepend(str(tools))
    spec = importlib.util.spec_from_file_location(
        "heal_selector_under_test", tools / "select.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OPENROUTER_API_KEY", "local-test-key")
    root = tmp_path / "run"
    root.mkdir()
    config = root / "luna-config.json"
    config.write_text(
        json.dumps({"model": "openai/gpt-6-luna", "reasoning": {"effort": "medium"}})
    )
    cases = [
        {
            "id": str(i),
            "reference": {"title": "Book", "context": "Book", "start": 0, "end": 4},
            "person_spans": [],
            "query_title": "Book",
            "query_author": None,
            "target_reason": "Find the named work",
            "top3": ["work"],
            "candidates": [
                {
                    "id": "work",
                    "title": "Book",
                    "authors": ["Author"],
                    "readinglog_count": 1,
                }
            ],
        }
        for i in range(8)
    ]
    (root / "round1-ready.json").write_text(json.dumps({"cases": cases}))
    monkeypatch.setattr(module, "ROOT", root)
    return module, root, cases


def arguments(label, **changes):
    values = {
        "round": 1,
        "limit": None,
        "take": 4,
        "concurrency": 2,
        "order_seed": 42,
        "run_label": label,
        "budget": 12.0,
    }
    return argparse.Namespace(**(values | changes))


def fake_client(module, monkeypatch, statuses):
    serialize = json.dumps
    state = {"active": 0, "peak": 0, "calls": 0, "bodies": [], "limits": []}

    class Client:
        def __init__(self, *, timeout, limits):
            state["limits"].append(limits.max_connections)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, *, headers, json):
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
            index = state["calls"]
            state["calls"] += 1
            state["bodies"].append(json)
            await asyncio.sleep(0.002)
            state["active"] -= 1
            status = statuses[index] if index < len(statuses) else 200
            if status != 200:
                return httpx.Response(
                    status,
                    json={"error": "rate limit"},
                    headers={"retry-after": "1"},
                    request=httpx.Request("POST", url),
                )
            decision = {
                "results": [
                    {
                        "title": "Book",
                        "author": "Author",
                        "action": "select",
                        "work_id": "work",
                        "reason": "Matching work",
                    }
                ]
            }
            return httpx.Response(
                200,
                json={
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {"content": serialize(decision)},
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 1400,
                        "completion_tokens": 100,
                        "cost": 0.001,
                        "completion_tokens_details": {"reasoning_tokens": 30},
                    },
                },
                request=httpx.Request("POST", url),
            )

    monkeypatch.setattr(module.httpx, "AsyncClient", Client)
    return state


def test_batches_resume_without_duplicate_calls_or_request_changes(
    selector, monkeypatch
):
    module, root, _ = selector
    state = fake_client(module, monkeypatch, [])
    asyncio.run(module.main(arguments("first")))
    assert state["calls"] == 4 and state["peak"] == 2
    asyncio.run(module.main(arguments("second", concurrency=4)))
    assert state["calls"] == 8 and state["limits"] == [2, 4]
    assert state["bodies"][0] == state["bodies"][-1]
    assert state["bodies"][0]["reasoning"] == {"effort": "medium"}
    assert state["bodies"][0]["max_tokens"] == 4096
    db = sqlite3.connect(root / "receipts.sqlite")
    assert db.execute(
        "SELECT count(*),count(DISTINCT id) FROM receipts"
    ).fetchone() == (8, 8)
    metrics = json.loads((root / "selector-metrics-second.json").read_text())
    assert metrics["completed"] == 4 and metrics["failed_attempts"] == 0
    assert metrics["tokens"]["input_tokens"]["mean"] == 1400
    assert all(
        r["timing"]["service_seconds"] > 0
        for (payload,) in db.execute("SELECT payload FROM receipts")
        if (r := json.loads(payload))
    )


def test_rate_limit_stops_dispatch_and_remains_resumable(selector, monkeypatch):
    module, root, _ = selector
    state = fake_client(module, monkeypatch, [429])
    with pytest.raises(AssertionError, match="Incomplete round"):
        asyncio.run(module.main(arguments("limited", concurrency=1)))
    assert state["calls"] == 1
    metrics = json.loads((root / "selector-metrics-limited.json").read_text())
    assert metrics["stop_reason"] == "http_429"
    assert metrics["requests"][0]["retry_after"] == "1"
    asyncio.run(module.main(arguments("resumed", take=8)))
    assert state["calls"] == 9
    db = sqlite3.connect(root / "receipts.sqlite")
    assert (
        db.execute("SELECT count(*) FROM receipts WHERE attempt=2").fetchone()[0] == 1
    )


def test_original_budget_still_stops_new_dispatch(selector, monkeypatch):
    module, root, _ = selector
    state = fake_client(module, monkeypatch, [])
    with pytest.raises(AssertionError, match="Incomplete round"):
        asyncio.run(module.main(arguments("budget", concurrency=1, budget=0.0005)))
    assert state["calls"] == 1
    metrics = json.loads((root / "selector-metrics-budget.json").read_text())
    assert metrics["stop_reason"] == "budget" and metrics["completed"] == 1
