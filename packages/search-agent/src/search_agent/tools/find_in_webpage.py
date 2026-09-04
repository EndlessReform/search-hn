"""Agent tool for literal search over a cached webpage."""

from __future__ import annotations

import json
from typing import Annotated

from agents import RunContextWrapper, function_tool
from pydantic import Field

from search_agent.runtime_context import SearchAgentContext
from search_agent.web.inspection import MAX_FIND_TERM_CHARACTERS


def build_find_in_webpage_payload(
    context: SearchAgentContext,
    *,
    page_id: str,
    term: str,
    cursor: str | None = None,
) -> dict[str, object]:
    """Search cached content through the shared service without an SDK run."""

    if context.web_service is None:
        return {
            "status": "extractor_unavailable",
            "reason": "webpage extraction was not enabled for this application context",
            "recommended_action": "Use fetch_top_comments instead.",
            "story_id": None,
        }
    normalized_cursor = cursor
    if cursor is not None and cursor.strip().lower() in {"", "null", "none"}:
        normalized_cursor = None
    return context.web_service.find(
        page_id=page_id,
        term=term,
        cursor=normalized_cursor,
    )


@function_tool(strict_mode=False)
def find_in_webpage(
    ctx: RunContextWrapper[SearchAgentContext],
    page_id: Annotated[
        str,
        Field(min_length=6, description="Exact page_id returned by open_webpage."),
    ],
    term: Annotated[
        str,
        Field(
            min_length=1,
            max_length=MAX_FIND_TERM_CHARACTERS,
            description="Literal case-insensitive term to find in cached page text.",
        ),
    ],
    cursor: Annotated[
        str | None,
        Field(
            default=None,
            description="Optional next_cursor from an earlier find for this page and term.",
        ),
    ] = None,
) -> str:
    """Find a term in cached content and return snippets plus read cursors."""

    return json.dumps(
        build_find_in_webpage_payload(
            ctx.context,
            page_id=page_id,
            term=term,
            cursor=cursor,
        ),
        ensure_ascii=False,
    )
