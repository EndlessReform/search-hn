"""Story-search tool and payload builder."""

from __future__ import annotations

import json
from datetime import date
from typing import Annotated, Literal

from agents import RunContextWrapper, function_tool
from pydantic import Field

from search_agent.data_access import HNStorySearchRepository
from search_agent.production_search import SearchResults
from search_agent.runtime_context import SearchAgentContext
from search_agent.tools.utils import (
    StoryQueryInput,
    normalize_domains,
    normalize_query_batch,
    parse_optional_iso_date,
    story_hit_to_payload,
)
from search_agent.web.policy import PublisherPolicy

_NO_RESULTS_GUIDANCE = (
    "No matches found. Try a clearer or broader description of the topic, or a named entity "
    "if appropriate. Relax filters that are not essential to the user's request. "
    "The production corpus only includes eligible stories with 25+ points."
)
"""One-time-per-turn nudge shown after a fully empty story search."""

_MISSING_STORY_SEARCH_CONSTRAINTS = (
    "fetch_stories requires either a `query`, or at least one domain filter "
    "(`include_domains`/`exclude_domains`) or date filter (`min_date`/`max_date`). "
    "Omit `query` only when you intentionally want top stories constrained by "
    "domain and/or date."
)
"""Validation message for accidentally unconstrained story searches."""


def _resolve_story_queries(
    *,
    query: str | list[str] | None,
    parsed_min: date | None,
    parsed_max: date | None,
    normalized_include: list[str] | None,
    normalized_exclude: list[str] | None,
) -> list[str | None]:
    """Normalize the request into one concrete search spec or a query batch.

    ``fetch_stories`` primarily serves topic search, but we also support a
    narrower "filter-only" mode for prompts like "top GitHub stories in March".
    In that mode the repository receives ``query=None`` and ranks the filtered
    stories by score.
    """

    if query is not None:
        return normalize_query_batch(query)

    has_domain_filter = bool(normalized_include or normalized_exclude)
    has_date_filter = parsed_min is not None or parsed_max is not None
    assert has_domain_filter or has_date_filter, _MISSING_STORY_SEARCH_CONSTRAINTS
    return [None]


def build_fetch_stories_payload(
    repository: HNStorySearchRepository,
    *,
    query: str | list[str] | None = None,
    limit: int = 20,
    min_score: int | None = None,
    min_date: str | None = None,
    max_date: str | None = None,
    include_domains: list[str] | None = None,
    exclude_domains: list[str] | None = None,
    include_no_results_guidance: bool = False,
    page: int = 1,
    sort: Literal["relevance", "score", "date"] = "relevance",
    publisher_policy: PublisherPolicy | None = None,
) -> dict[str, object]:
    """Build the JSON payload for ``fetch_stories``.

    The decorated tool itself is intentionally tiny. This helper carries the
    real behavior so it can be tested without needing the Agents runtime.

    A single query remains the normal case, but the model may batch up to five
    alternate phrasings in one call. We also allow a single filter-only search
    when callers omit ``query`` but provide domain/date constraints. We
    preserve the historical top-level ``query`` / ``results`` keys when
    exactly one search specification is requested so the TUI and any other
    lightweight consumers keep working unchanged.
    """

    assert 1 <= page <= 3, "page must be in [1, 3]"
    assert 1 <= limit <= 20, "limit must be in [1, 20]"
    parsed_min = parse_optional_iso_date(min_date)
    parsed_max = parse_optional_iso_date(max_date)
    normalized_include = normalize_domains(include_domains)
    normalized_exclude = normalize_domains(exclude_domains)
    queries = _resolve_story_queries(
        query=query,
        parsed_min=parsed_min,
        parsed_max=parsed_max,
        normalized_include=normalized_include,
        normalized_exclude=normalized_exclude,
    )

    query_payloads: list[dict[str, object]] = []
    for current_query in queries:
        hits = repository.search_stories(
            query=current_query,
            limit=limit + 1,
            min_score=min_score,
            min_date=parsed_min,
            max_date=parsed_max,
            include_domains=normalized_include,
            exclude_domains=normalized_exclude,
            skip=(page - 1) * limit,
            sort=sort,
        )
        visible = (
            hits.for_page(limit) if isinstance(hits, SearchResults) else hits[:limit]
        )
        has_more = (
            hits.remaining > limit
            if isinstance(hits, SearchResults)
            else len(hits) > limit
        )
        metadata = (
            {"retrieval_mode": hits.mode} if isinstance(hits, SearchResults) else {}
        )
        query_payloads.append(
            {
                "query": current_query,
                "results": [
                    {
                        **story_hit_to_payload(hit, publisher_policy=publisher_policy),
                        **(
                            hits.ranks[hit.id]
                            if isinstance(hits, SearchResults)
                            else {}
                        ),
                    }
                    for hit in visible
                ],
                **metadata,
                "page": page,
                "next_page": page + 1 if has_more and page < 3 else None,
            }
        )

    if len(query_payloads) == 1:
        single_payload = query_payloads[0]
        payload = {
            "query": single_payload["query"],
            "results": single_payload["results"],
            "queries": query_payloads,
            "page": page,
            "next_page": single_payload["next_page"],
        }
        if "retrieval_mode" in single_payload:
            payload["retrieval_mode"] = single_payload["retrieval_mode"]
        if include_no_results_guidance and _all_story_batches_empty(query_payloads):
            payload["search_guidance"] = _NO_RESULTS_GUIDANCE
        return payload

    payload = {
        "query_count": len(query_payloads),
        "queries": query_payloads,
    }
    if include_no_results_guidance and _all_story_batches_empty(query_payloads):
        payload["search_guidance"] = _NO_RESULTS_GUIDANCE
    return payload


def _all_story_batches_empty(query_payloads: list[dict[str, object]]) -> bool:
    """Return whether every requested query in the batch came back empty."""

    return all(not batch.get("results") for batch in query_payloads)


@function_tool(strict_mode=False)
def fetch_stories(
    ctx: RunContextWrapper[SearchAgentContext],
    query: Annotated[
        StoryQueryInput | None,
        Field(
            default=None,
            description=(
                "Search query, or a list of 1-5 alternate queries. For production hybrid search, "
                "prefer one natural-language sentence or question describing the topic, "
                "preserving useful entity names. Start without filters unless the request "
                "requires them; unfiltered hybrid search uses the fast path. "
                "You may omit `query` entirely when `include_domains`/`exclude_domains` "
                "and/or `min_date`/`max_date` are present, in which case this tool "
                "returns the top-scoring stories matching those filters."
            ),
        ),
    ] = None,
    limit: Annotated[
        int,
        Field(
            ge=1,
            le=20,
            description="Maximum number of stories to return per query (1-20).",
        ),
    ] = 20,
    page: Annotated[
        int,
        Field(
            ge=1,
            le=3,
            description="Result page (1-3). Keep query, filters, sort and limit unchanged when requesting the next page.",
        ),
    ] = 1,
    sort: Annotated[
        Literal["relevance", "score", "date"],
        Field(
            description="Optional ordering; relevance by default, or descending score/date."
        ),
    ] = "relevance",
    min_score: Annotated[
        int | None,
        Field(
            default=None,
            description=(
                "Minimum story score filter. Production always has a 25-point floor. "
                "Omit unless the user requests a popularity threshold; do not add it as a quality heuristic."
            ),
        ),
    ] = None,
    min_date: Annotated[
        str | None,
        Field(
            default=None,
            description=(
                "Earliest story date (ISO format YYYY-MM-DD, inclusive). "
                "Use when the requested time period requires it; otherwise omit."
            ),
        ),
    ] = None,
    max_date: Annotated[
        str | None,
        Field(
            default=None,
            description=(
                "Latest story date (ISO format YYYY-MM-DD, inclusive). "
                "Use for a required time window; otherwise omit."
            ),
        ),
    ] = None,
    include_domains: Annotated[
        list[str] | None,
        Field(
            default=None,
            description=(
                "Use only for required source restrictions or an observed retrieval problem. Only include stories from these domains (e.g. ['arxiv.org', 'github.com']). "
                "Plain domain names, no protocol. Leading 'www.' is stripped automatically."
            ),
        ),
    ] = None,
    exclude_domains: Annotated[
        list[str] | None,
        Field(
            default=None,
            description=(
                "Use only for required source restrictions or an observed retrieval problem. Exclude stories from these domains (e.g. ['reddit.com']). "
                "Plain domain names, no protocol. Leading 'www.' is stripped automatically."
            ),
        ),
    ] = None,
) -> str:
    """Search HN stories with optional text, score, date, and domain filters.

    Why this function is intentionally sync:
    - In the SDK, sync ``@function_tool`` handlers run via ``asyncio.to_thread``.
    - That keeps blocking DB calls off the event loop without adding async DB
      plumbing to the TUI.

    Batching behavior:
    - One query string is still the standard call shape.
    - The model may pass up to five queries as a list when it wants to compare
      nearby phrasings in a single turn.

    Query-optional behavior:
    - ``query`` may be omitted only when domain filters and/or a date window
      narrow the search.
    - In that mode, results are simply the highest-scored stories that satisfy
      those filters.
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
        include_no_results_guidance=not ctx.context.turn_state.no_results_guidance_emitted,
        page=page,
        sort=sort,
        publisher_policy=(
            ctx.context.web_service.policy
            if ctx.context.web_service is not None
            else None
        ),
    )
    if payload.get("search_guidance"):
        ctx.context.turn_state.no_results_guidance_emitted = True
    ctx.context.web_state.register_story_payload(payload)
    ctx.context.web_state.reset_inspection_budget()
    return json.dumps(payload, ensure_ascii=False)
