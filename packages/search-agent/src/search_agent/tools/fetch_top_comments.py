"""Top-level comment tool and payload builder."""

from __future__ import annotations

import json
from typing import Annotated

from agents import RunContextWrapper, function_tool
from pydantic import Field

from search_agent.citations import build_story_cursor
from search_agent.data_access import HNStorySearchRepository
from search_agent.runtime_context import SearchAgentContext
from search_agent.tools.utils import (
    StoryIdInput,
    normalize_story_id_batch,
    top_comment_to_payload,
)


def build_fetch_top_comments_payload(
    repository: HNStorySearchRepository,
    *,
    story_id: int | list[int],
    limit: int = 5,
    skip: int = 0,
) -> dict[str, object]:
    """Build the JSON payload for ``fetch_top_comments``.

    The top-level tool can now batch a handful of story IDs. This is useful
    after a search step where the model wants to inspect comments for several
    nearby candidate stories without spending multiple tool turns.

    As with ``fetch_stories``, we keep the original single-story top-level keys
    when the request contains exactly one ID.
    """

    story_ids = normalize_story_id_batch(story_id)
    story_payloads: list[dict[str, object]] = []

    for current_story_id in story_ids:
        total, comments = repository.fetch_top_level_comments(
            story_id=current_story_id,
            limit=limit,
            skip=skip,
        )
        comment_payloads = [top_comment_to_payload(comment) for comment in comments]
        story_payloads.append(
            {
                "story_id": current_story_id,
                "story_cursor": build_story_cursor(current_story_id),
                "total_top_level_comments": total,
                "returned": len(comment_payloads),
                "remaining_after_page": max(total - (skip + len(comment_payloads)), 0),
                "comments": comment_payloads,
            }
        )

    if len(story_payloads) == 1:
        single_payload = story_payloads[0]
        return {
            "story_id": single_payload["story_id"],
            "story_cursor": single_payload["story_cursor"],
            "total_top_level_comments": single_payload["total_top_level_comments"],
            "returned": single_payload["returned"],
            "remaining_after_page": single_payload["remaining_after_page"],
            "comments": single_payload["comments"],
            "stories": story_payloads,
        }

    return {
        "story_count": len(story_payloads),
        "stories": story_payloads,
    }


@function_tool
def fetch_top_comments(
    ctx: RunContextWrapper[SearchAgentContext],
    story_id: StoryIdInput,
    limit: Annotated[
        int,
        Field(
            ge=1,
            le=20,
            description="Maximum number of top-level comments to return per story (1-20).",
        ),
    ] = 5,
    skip: Annotated[
        int,
        Field(
            ge=0,
            le=1000,
            description="Number of top-level comments to skip for pagination.",
        ),
    ] = 0,
) -> str:
    """Fetch top-level comments for one or more stories.

    Query semantics intentionally mirror the ``hn_query`` skill's comments mode:
    - Validate each target ID exists and is ``type='story'``.
    - Read only top-level comments confirmed by both the ``kids`` edge and the
      comment's current ``items.parent`` value.
    - Order by ``kids.display_order`` then ``kid``.
    """

    payload = build_fetch_top_comments_payload(
        ctx.context.repository,
        story_id=story_id,
        limit=limit,
        skip=skip,
    )
    ctx.context.web_state.register_comment_payload(payload)
    return json.dumps(payload, ensure_ascii=False)
