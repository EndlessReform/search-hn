"""Agent tool for reading another chunk from a cached webpage."""

from __future__ import annotations

import json
from typing import Annotated

from agents import RunContextWrapper, function_tool
from pydantic import Field

from search_agent.runtime_context import SearchAgentContext


def build_read_webpage_payload(
    context: SearchAgentContext,
    *,
    page_id: str,
    cursor: str,
) -> dict[str, object]:
    """Read cached content through the shared service without an SDK run."""

    if context.web_service is None:
        return {
            "status": "extractor_unavailable",
            "reason": "webpage extraction was not enabled for this application context",
            "recommended_action": "Use fetch_top_comments instead.",
            "story_id": None,
        }
    return context.web_service.read(page_id=page_id, cursor=cursor)


@function_tool
def read_webpage(
    ctx: RunContextWrapper[SearchAgentContext],
    page_id: Annotated[
        str,
        Field(min_length=6, description="Exact page_id returned by open_webpage."),
    ],
    cursor: Annotated[
        str,
        Field(
            min_length=6,
            description="Exact next_cursor or read_cursor returned for this page.",
        ),
    ],
) -> str:
    """Read one more bounded chunk from an already-cached webpage."""

    return json.dumps(
        build_read_webpage_payload(ctx.context, page_id=page_id, cursor=cursor),
        ensure_ascii=False,
    )
