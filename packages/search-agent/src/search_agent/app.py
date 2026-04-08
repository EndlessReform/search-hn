"""FastAPI application for HN search agent."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request
from pydantic import BaseModel

from search_agent.data_access import MAX_SEARCH_LIMIT
from search_agent.runtime_context import (
    SearchAgentContext,
    build_search_agent_context,
    dispose_search_agent_context,
)


class SearchResult(BaseModel):
    """A single search result item."""

    id: int
    type: str
    score: int | None
    title: str | None
    by: str | None
    time: int | None


def create_app() -> FastAPI:
    """Create and configure the FastAPI application.

    The app and the Agents tool share the same repository/context builder so we
    have one canonical data-access path across wrappers.

    Returns:
        Configured FastAPI instance with search endpoints.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        context = build_search_agent_context()
        app.state.search_context = context
        try:
            yield
        finally:
            dispose_search_agent_context(context)

    app = FastAPI(title="HN Search Agent", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        """Health check endpoint."""
        return {"status": "ok"}

    @app.get("/search", response_model=list[SearchResult])
    async def search(
        request: Request,
        q: str = Query(..., description="Search query string"),
        limit: int = Query(
            20,
            ge=1,
            le=MAX_SEARCH_LIMIT,
            description="Maximum results to return",
        ),
    ) -> list[SearchResult]:
        """Search Hacker News stories.

        Args:
            q: Search query string
            limit: Maximum number of results

        Returns:
            List of matching items with id, type, score, title, by, time
        """
        context: SearchAgentContext = request.app.state.search_context
        hits = context.repository.search_stories(query=q, limit=limit)
        return [
            SearchResult(
                id=hit.id,
                type="story",
                score=hit.score,
                title=hit.title,
                by=hit.by,
                time=hit.time,
            )
            for hit in hits
        ]

    return app


app = create_app()
