"""Function tools exposed to the OpenAI Agents runtime.

Key upstream methods worth reading while learning the SDK internals:
- `agents.tool.function_tool`:
  Decorator that builds schema + runtime wrapper around Python callables.
- `agents.function_schema.function_schema`:
  Signature/docstring parser that turns function params into JSON schema.
- `agents.run_context.RunContextWrapper`:
  Dependency container passed by `Runner.run(..., context=...)`.
- `agents.run_internal.tool_execution.execute_function_tool_calls`:
  Tool executor that may run function tools concurrently in the same turn.
"""

from __future__ import annotations

import json
from datetime import date
from typing import Annotated

from agents import RunContextWrapper, function_tool
from pydantic import Field

from search_agent.runtime_context import SearchAgentContext


@function_tool(strict_mode=False)
def fetch_stories(
    ctx: RunContextWrapper[SearchAgentContext],
    query: Annotated[
        str,
        Field(
            min_length=2,
            description=(
                "Keyword query matched against the mirrored HN full-text index "
                "(`items.search_tsv`, built from title + URL)."
            ),
        ),
    ],
    limit: Annotated[
        int,
        Field(
            ge=1,
            le=20,
            description="Maximum number of stories to return (1-20).",
        ),
    ] = 8,
    min_score: Annotated[
        int | None,
        Field(
            default=None,
            description=(
                "Minimum story score filter. Omit for no minimum. "
                "Useful for surfacing only notable/popular stories."
            ),
        ),
    ] = None,
    min_date: Annotated[
        str | None,
        Field(
            default=None,
            description=(
                "Earliest story date (ISO format YYYY-MM-DD, inclusive). "
                "Use for time-bound topics like breaking news or recent events."
            ),
        ),
    ] = None,
    max_date: Annotated[
        str | None,
        Field(
            default=None,
            description=(
                "Latest story date (ISO format YYYY-MM-DD, inclusive). "
                "Combine with min_date to target a specific time window."
            ),
        ),
    ] = None,
    include_domains: Annotated[
        list[str] | None,
        Field(
            default=None,
            description=(
                "Only include stories from these domains (e.g. ['arxiv.org', 'github.com']). "
                "Plain domain names, no protocol. Leading 'www.' is stripped automatically."
            ),
        ),
    ] = None,
    exclude_domains: Annotated[
        list[str] | None,
        Field(
            default=None,
            description=(
                "Exclude stories from these domains (e.g. ['reddit.com']). "
                "Plain domain names, no protocol. Leading 'www.' is stripped automatically."
            ),
        ),
    ] = None,
) -> str:
    """Search HN stories with optional score, date, and domain filters.

    Why this function is intentionally *sync*:
    - In the SDK, sync `@function_tool` handlers run via `asyncio.to_thread(...)`.
    - That keeps blocking DB calls off the event loop without extra async DB
      plumbing in this hello-world implementation.

    Dependency flow:
    1. CLI (or another wrapper) builds `SearchAgentContext`.
    2. Wrapper passes it into `Runner.run(..., context=...)` or `run_demo_loop`.
    3. SDK injects `RunContextWrapper[SearchAgentContext]` as first arg here.
    4. Handler pulls only what it needs (`ctx.context.repository`) and executes.
    """

    parsed_min = date.fromisoformat(min_date) if min_date else None
    parsed_max = date.fromisoformat(max_date) if max_date else None

    # Normalize domains: lowercase and strip leading www.
    def _normalize_domains(domains: list[str] | None) -> list[str] | None:
        if not domains:
            return None
        out: list[str] = []
        for d in domains:
            d = d.strip().lower()
            if d.startswith("www."):
                d = d[4:]
            if d:
                out.append(d)
        return out or None

    hits = ctx.context.repository.search_stories(
        query=query,
        limit=limit,
        min_score=min_score,
        min_date=parsed_min,
        max_date=parsed_max,
        include_domains=_normalize_domains(include_domains),
        exclude_domains=_normalize_domains(exclude_domains),
    )
    payload = [
        {
            "id": hit.id,
            "title": hit.title,
            "url": hit.url,
            "score": hit.score,
            "author": hit.by,
            "unix_time": hit.time,
            "date": hit.day.isoformat() if hit.day is not None else None,
        }
        for hit in hits
    ]
    return json.dumps({"query": query, "results": payload}, ensure_ascii=False)


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

    Unlike ``fetch_stories`` this does **not** require a text query — it
    simply returns the top stories by score for the given day.
    """

    resolved = date.fromisoformat(target_date) if target_date else date.today()

    hits = ctx.context.repository.top_stories_for_date(
        target_date=resolved,
        limit=limit,
    )
    payload = [
        {
            "id": hit.id,
            "title": hit.title,
            "url": hit.url,
            "score": hit.score,
            "author": hit.by,
            "date": hit.day.isoformat() if hit.day is not None else None,
        }
        for hit in hits
    ]
    return json.dumps(
        {"date": resolved.isoformat(), "results": payload},
        ensure_ascii=False,
    )


@function_tool
def fetch_top_comments(
    ctx: RunContextWrapper[SearchAgentContext],
    story_id: Annotated[
        int,
        Field(
            gt=0,
            description="Hacker News story ID whose top-level comments should be fetched.",
        ),
    ],
    limit: Annotated[
        int,
        Field(
            ge=1,
            le=20,
            description="Maximum number of top-level comments to return (1-20).",
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
    """Fetch top-level comments for a story using the shared repository context.

    Query semantics intentionally mirror the `hn_query` skill's comments mode:
    - Validate `story_id` exists and is `type='story'`.
    - Read only top-level comments via `kids` join.
    - Order by `kids.display_order` then `kid`.
    """

    total, comments = ctx.context.repository.fetch_top_level_comments(
        story_id=story_id,
        limit=limit,
        skip=skip,
    )
    payload = [
        {
            "id": comment.id,
            "author": comment.author,
            "comment": comment.comment,
        }
        for comment in comments
    ]
    remaining = max(total - (skip + len(payload)), 0)
    return json.dumps(
        {
            "story_id": story_id,
            "total_top_level_comments": total,
            "returned": len(payload),
            "remaining_after_page": remaining,
            "comments": payload,
        },
        ensure_ascii=False,
    )
