"""Exercise queue continuation, retry receipts, and resumption with fake HTTP."""

import argparse
import asyncio
import json
import sqlite3

import httpx
import pytest
import selector


def response(reason="stop", work_id="w1"):
    return {
        "choices": [
            {
                "finish_reason": reason,
                "message": {"content": json.dumps({"work_id": work_id})},
            }
        ]
    }


def test_validation():
    assert selector.validate_pick(response(work_id=None), []).work_id is None
    with pytest.raises(selector.OutputLimit):
        selector.validate_pick(response("length"), ["w1"])
    with pytest.raises(ValueError):
        selector.validate_pick(response(work_id="foreign"), ["w1"])


def test_queue_recovers_and_resumes(tmp_path, monkeypatch):
    db = sqlite3.connect(tmp_path / "run.sqlite")
    db.executescript("""
    CREATE TABLE refs(id TEXT PRIMARY KEY, ordinal INTEGER, payload TEXT);
    CREATE TABLE rankings(id TEXT PRIMARY KEY, payload TEXT);
    CREATE TABLE documents(id TEXT PRIMARY KEY, payload TEXT);
    CREATE TABLE selections(id TEXT, model TEXT, payload TEXT, PRIMARY KEY(id,model));
    CREATE TABLE failures(id TEXT, stage TEXT, payload TEXT, PRIMARY KEY(id,stage));
    CREATE TABLE status(stage TEXT PRIMARY KEY, payload TEXT);
    """)
    db.execute(
        "INSERT INTO documents VALUES (?,?)",
        ("w1", json.dumps({"id": "w1", "title": "Book", "authors": []})),
    )
    for i in range(3):
        row = {"id": str(i), "title": str(i), "context": str(i), "start": 0, "end": 1}
        db.execute("INSERT INTO refs VALUES (?,?,?)", (str(i), i, json.dumps(row)))
        db.execute(
            "INSERT INTO rankings VALUES (?,?)", (str(i), json.dumps({"top3": ["w1"]}))
        )
    db.commit()
    monkeypatch.setattr(selector, "RUN", tmp_path)
    monkeypatch.setattr(selector, "connect", lambda: db)
    monkeypatch.setenv("OPENROUTER_API_KEY", "fake-test-key")
    calls = []
    recover = False

    def handler(request):
        body = json.loads(request.content)
        mention = json.loads(body["messages"][1]["content"])["mention"]
        calls.append((mention, body["max_tokens"]))
        if mention == "0" and not recover:
            return httpx.Response(200, json=response("length"))
        if mention == "1" and sum(m == "1" for m, _ in calls) == 1:
            raise httpx.ReadTimeout("test transport failure")
        return httpx.Response(200, json=response())

    original_client = httpx.AsyncClient
    monkeypatch.setattr(
        selector.httpx,
        "AsyncClient",
        lambda **kw: original_client(**kw, transport=httpx.MockTransport(handler)),
    )

    async def no_sleep(_):
        pass

    monkeypatch.setattr(selector.asyncio, "sleep", no_sleep)
    args = argparse.Namespace(arm="luna", limit=0, concurrency=1, native=False)
    asyncio.run(selector.main(args))
    assert db.execute("SELECT count(*) FROM selections").fetchone()[0] == 2
    assert db.execute("SELECT count(*) FROM failures").fetchone()[0] == 1
    assert [tokens for mention, tokens in calls if mention == "0"] == [
        2048,
        4096,
        8192,
        16384,
        32768,
    ]
    assert db.execute("SELECT count(*) FROM attempts").fetchone()[0] == 8
    assert (
        json.loads(
            db.execute("SELECT payload FROM status WHERE stage='luna'").fetchone()[0]
        )["state"]
        == "incomplete"
    )
    recover = True
    calls.clear()
    asyncio.run(selector.main(args))
    assert calls == [("0", 65536)]
    assert db.execute("SELECT count(*) FROM selections").fetchone()[0] == 3
    assert db.execute("SELECT count(*) FROM failures").fetchone()[0] == 0
    assert (
        json.loads(
            db.execute("SELECT payload FROM status WHERE stage='luna'").fetchone()[0]
        )["state"]
        == "complete"
    )
