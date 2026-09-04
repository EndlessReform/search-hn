"""HTTP wrapper tests for the shared search repository boundary."""

from __future__ import annotations

import inspect
from datetime import date
from unittest.mock import patch

from fastapi.testclient import TestClient
from search_agent.app import create_app
from search_agent.data_access import StorySearchHit
from search_agent.runtime_context import SearchAgentContext


class FakeRepository:
    """Small repository double that records the HTTP request contract."""

    def __init__(self) -> None:
        self.calls: list[tuple[str | None, int]] = []
        self.disposed = False

    def search_stories(self, query: str | None, *, limit: int, **_filters):
        self.calls.append((query, limit))
        return [
            StorySearchHit(
                id=42,
                title="A useful result",
                url="https://example.com/result",
                score=123,
                by="alice",
                time=1_700_000_000,
                day=date(2023, 11, 14),
            )
        ]

    def dispose(self) -> None:
        self.disposed = True


def test_search_endpoint_uses_sync_worker_route_and_shared_repository() -> None:
    """The sync route keeps psycopg2 work off FastAPI's event loop."""

    repository = FakeRepository()
    context = SearchAgentContext(repository=repository)  # type: ignore[arg-type]
    app = create_app()
    search_route = next(route for route in app.routes if route.path == "/search")

    assert not inspect.iscoroutinefunction(search_route.endpoint)

    with (
        patch("search_agent.app.build_search_agent_context", return_value=context),
        TestClient(app) as client,
    ):
        response = client.get("/search", params={"q": "postgres", "limit": 3})

    assert response.status_code == 200
    assert repository.calls == [("postgres", 3)]
    assert repository.disposed
    assert response.json() == [
        {
            "id": 42,
            "type": "story",
            "score": 123,
            "title": "A useful result",
            "by": "alice",
            "time": 1_700_000_000,
        }
    ]


def test_search_endpoint_validates_limit_before_database_access() -> None:
    repository = FakeRepository()
    context = SearchAgentContext(repository=repository)  # type: ignore[arg-type]
    app = create_app()

    with (
        patch("search_agent.app.build_search_agent_context", return_value=context),
        TestClient(app) as client,
    ):
        response = client.get("/search", params={"q": "postgres", "limit": 101})

    assert response.status_code == 422
    assert repository.calls == []
