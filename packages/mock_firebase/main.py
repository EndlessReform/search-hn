"""Fixture-driven Firebase mock used by catchup_worker integration tests.

This server implements just enough of the HN Firebase API surface for v1 catchup:
- `GET /v0/maxitem.json`
- `GET /v0/item/{id}.json`

A JSON fixture controls:
- the max item id,
- item payloads (or `null` for missing items),
- optional scripted HTTP status responses per item request attempt.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn


@dataclass
class FixtureData:
    maxitem: int
    items: dict[int, Any | None] = field(default_factory=dict)
    scripted_statuses: dict[int, list[int]] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> "FixtureData":
        payload = json.loads(path.read_text(encoding="utf-8"))
        maxitem = int(payload["maxitem"])

        raw_items = payload.get("items", {})
        items: dict[int, Any | None] = {int(key): value for key, value in raw_items.items()}

        raw_scripts = payload.get("scripts", {})
        scripted_statuses: dict[int, list[int]] = {}
        for key, values in raw_scripts.items():
            scripted_statuses[int(key)] = [int(code) for code in values]

        return cls(maxitem=maxitem, items=items, scripted_statuses=scripted_statuses)


@dataclass
class FixtureState:
    fixture: FixtureData
    attempts_by_item: dict[int, int] = field(default_factory=dict)
    lock: Lock = field(default_factory=Lock)

    def next_status_override(self, item_id: int) -> int | None:
        with self.lock:
            attempt_idx = self.attempts_by_item.get(item_id, 0)
            self.attempts_by_item[item_id] = attempt_idx + 1

            scripted = self.fixture.scripted_statuses.get(item_id, [])
            if attempt_idx < len(scripted):
                return scripted[attempt_idx]
            return None


def build_app(state: FixtureState) -> FastAPI:
    app = FastAPI(title="mock_firebase")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/v0/maxitem.json")
    async def maxitem() -> int:
        return state.fixture.maxitem

    @app.get("/v0/item/{item_id}.json")
    async def item(item_id: int) -> JSONResponse:
        override = state.next_status_override(item_id)
        if override is not None and override != 200:
            detail = {"error": f"scripted status {override} for item {item_id}"}
            return JSONResponse(status_code=override, content=detail)

        value = state.fixture.items.get(item_id)
        if value is None:
            return JSONResponse(status_code=200, content=None)

        if not isinstance(value, dict):
            raise HTTPException(status_code=500, detail=f"fixture item {item_id} is not an object")

        # Keep fixture ergonomics simple: default ID if omitted.
        body = dict(value)
        body.setdefault("id", item_id)
        return JSONResponse(status_code=200, content=body)

    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a fixture-driven Firebase mock server.")
    parser.add_argument(
        "--fixture",
        required=True,
        help="Path to JSON fixture file describing items and scripted responses.",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fixture = FixtureData.load(Path(args.fixture))
    state = FixtureState(fixture=fixture)
    app = build_app(state)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
