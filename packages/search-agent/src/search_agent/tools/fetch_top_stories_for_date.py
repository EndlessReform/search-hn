"""Top-stories-by-date tool and payload builder."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import Annotated

from agents import RunContextWrapper, function_tool
from pydantic import Field

from search_agent.data_access import HNStorySearchRepository
from search_agent.runtime_context import SearchAgentContext
from search_agent.tools.utils import (
    parse_optional_iso_date,
    story_hit_to_payload,
)


def build_top_stories_for_date_payload(
    repository: HNStorySearchRepository,
    *,
    target_date: str | None = None,
    today: date | None = None,
    limit: int = 10,
) -> dict[str, object]:
    """Build the JSON payload for ``fetch_top_stories_for_date``.

    ``today`` lets the CLI inject a fake "current date" for prompt experiments
    while keeping the tool's default-date behavior aligned with that fiction.
    """

    parsed = parse_optional_iso_date(target_date)
    resolved_date = (
        parsed
        if parsed is not None
        else (today or datetime.now(UTC).astimezone().date())
    )
    hits = repository.top_stories_for_date(
        target_date=resolved_date,
        limit=limit,
    )
    return {
        "date": resolved_date.isoformat(),
        "results": [story_hit_to_payload(hit) for hit in hits],
    }


@function_tool(strict_mode=False)
def fetch_top_stories_for_date(
    ctx: RunContextWrapper[SearchAgentContext],
    target_date: Annotated[
        str | None,
        Field(
            default=None,
            description=(
                "Calendar date in ISO format (YYYY-MM-DD). "
                "Defaults to today if omitted. Use for questions like "
                "'what was popular last Monday' or 'top stories on 2025-12-01'."
            ),
        ),
    ] = None,
    limit: Annotated[
        int,
        Field(
            ge=1,
            le=20,
            description="Maximum number of stories to return (1-20).",
        ),
    ] = 10,
) -> str:
    """Return the highest-scored stories for a single calendar date.

    Unlike ``fetch_stories`` this does not require a text query; it simply
    ranks all stories on the chosen day by score.
    """

    payload = build_top_stories_for_date_payload(
        ctx.context.repository,
        target_date=target_date,
        today=ctx.context.current_date,
        limit=limit,
    )
    return json.dumps(payload, ensure_ascii=False)
