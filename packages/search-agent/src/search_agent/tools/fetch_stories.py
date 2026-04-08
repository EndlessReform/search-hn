"""Story-search tool and payload builder."""

from __future__ import annotations

import json
from typing import Annotated

from agents import RunContextWrapper, function_tool
from pydantic import Field

from search_agent.data_access import HNStorySearchRepository
from search_agent.runtime_context import SearchAgentContext
from search_agent.tools.utils import (
    StoryQueryInput,
    normalize_domains,
    normalize_query_batch,
    parse_optional_iso_date,
    story_hit_to_payload,
)


def build_fetch_stories_payload(
    repository: HNStorySearchRepository,
    *,
    query: str | list[str],
    limit: int = 8,
    min_score: int | None = None,
    min_date: str | None = None,
    max_date: str | None = None,
    include_domains: list[str] | None = None,
    exclude_domains: list[str] | None = None,
) -> dict[str, object]:
    """Build the JSON payload for ``fetch_stories``.

    The decorated tool itself is intentionally tiny. This helper carries the
    real behavior so it can be tested without needing the Agents runtime.

    A single query remains the normal case, but the model may batch up to five
    alternate phrasings in one call. We preserve the historical top-level
    ``query`` / ``results`` keys when exactly one query is requested so the TUI
    and any other lightweight consumers keep working unchanged.
    """

    queries = normalize_query_batch(query)
    parsed_min = parse_optional_iso_date(min_date)
    parsed_max = parse_optional_iso_date(max_date)
    normalized_include = normalize_domains(include_domains)
    normalized_exclude = normalize_domains(exclude_domains)

    query_payloads: list[dict[str, object]] = []
    for current_query in queries:
        hits = repository.search_stories(
            query=current_query,
            limit=limit,
            min_score=min_score,
            min_date=parsed_min,
            max_date=parsed_max,
            include_domains=normalized_include,
            exclude_domains=normalized_exclude,
        )
        query_payloads.append(
            {
                "query": current_query,
                "results": [story_hit_to_payload(hit) for hit in hits],
            }
        )

    if len(query_payloads) == 1:
        single_payload = query_payloads[0]
        return {
            "query": single_payload["query"],
            "results": single_payload["results"],
            "queries": query_payloads,
        }

    return {
        "query_count": len(query_payloads),
        "queries": query_payloads,
    }


@function_tool(strict_mode=False)
def fetch_stories(
    ctx: RunContextWrapper[SearchAgentContext],
    query: StoryQueryInput,
    limit: Annotated[
        int,
        Field(
            ge=1,
            le=20,
            description="Maximum number of stories to return per query (1-20).",
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

    Why this function is intentionally sync:
    - In the SDK, sync ``@function_tool`` handlers run via ``asyncio.to_thread``.
    - That keeps blocking DB calls off the event loop without adding async DB
      plumbing to the TUI.

    Batching behavior:
    - One query string is still the standard call shape.
    - The model may pass up to five queries as a list when it wants to compare
      nearby phrasings in a single turn.
    """

    payload = build_fetch_stories_payload(
        ctx.context.repository,
        query=query,
        limit=limit,
        min_score=min_score,
        min_date=min_date,
        max_date=max_date,
        include_domains=include_domains,
        exclude_domains=exclude_domains,
    )
    return json.dumps(payload, ensure_ascii=False)
