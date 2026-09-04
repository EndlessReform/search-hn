"""Agent tool for authorized webpage extraction and preview."""

from __future__ import annotations

import json
from typing import Annotated

from agents import RunContextWrapper, function_tool
from pydantic import Field

from search_agent.runtime_context import SearchAgentContext


def build_open_webpage_payload(
    context: SearchAgentContext, *, url: str
) -> dict[str, object]:
    """Open one page through the context service without requiring an SDK run."""

    if context.web_service is None:
        return {
            "status": "extractor_unavailable",
            "reason": "webpage extraction was not enabled for this application context",
            "recommended_action": "Use fetch_top_comments instead.",
            "story_id": None,
        }
    return context.web_service.open(url)


@function_tool
def open_webpage(
    ctx: RunContextWrapper[SearchAgentContext],
    url: Annotated[
        str,
        Field(
            min_length=8,
            description=(
                "An exact HTTP(S) URL previously returned by fetch_stories, "
                "fetch_top_stories_for_date, or a top-level comment."
            ),
        ),
    ],
) -> str:
    """Extract and preview an authorized HTML page without bypass attempts."""

    return json.dumps(
        build_open_webpage_payload(ctx.context, url=url),
        ensure_ascii=False,
    )
