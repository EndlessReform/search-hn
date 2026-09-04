"""Agent tool for authorized webpage extraction and preview."""

from __future__ import annotations

import json
from typing import Annotated

from agents import RunContextWrapper, function_tool
from pydantic import Field

from search_agent.runtime_context import SearchAgentContext
from search_agent.web.security import WebAddressError


async def _comment_link_needs_approval(
    ctx: RunContextWrapper[SearchAgentContext],
    tool_parameters: dict[str, object],
    _call_id: str,
) -> bool:
    """Require consent only for URLs whose best provenance is an HN comment."""

    url = tool_parameters.get("url")
    if not isinstance(url, str):
        return False
    try:
        authorization = ctx.context.web_state.authorization_for(url)
    except WebAddressError:
        return False
    return authorization is not None and authorization.source == "top-level-comment"


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


@function_tool(needs_approval=_comment_link_needs_approval)
def open_webpage(
    ctx: RunContextWrapper[SearchAgentContext],
    url: Annotated[
        str,
        Field(
            min_length=8,
            description=(
                "An exact HTTP(S) URL previously returned by fetch_stories, "
                "fetch_top_stories_for_date, or a top-level comment. URLs found "
                "only in comments pause for explicit user approval because "
                "comments are user-authored; submission URLs do not."
            ),
        ),
    ],
) -> str:
    """Extract and preview an authorized HTML page without bypass attempts."""

    return json.dumps(
        build_open_webpage_payload(ctx.context, url=url),
        ensure_ascii=False,
    )
