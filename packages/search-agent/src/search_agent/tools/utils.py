"""Shared helpers for the search-agent tool package.

The individual tool modules focus on repository interactions and tool-specific
payload shapes. This module centralizes the small pieces they have in common:

- batching rules for tool inputs,
- date/domain normalization,
- and JSON-friendly row serialization.
"""

from __future__ import annotations

from datetime import date
from typing import Annotated

from pydantic import Field

from search_agent.citations import (
    build_comment_cursor,
    build_story_cursor,
)
from search_agent.data_access import TopLevelCommentHit, StorySearchHit

MAX_BATCH_TOOL_ITEMS = 5
"""Maximum number of items a batched tool call may include."""

SingleStoryQuery = Annotated[
    str,
    Field(min_length=2),
]
"""One full-text story query string."""

StoryQueryBatch = Annotated[
    list[SingleStoryQuery],
    Field(
        min_length=1,
        max_length=MAX_BATCH_TOOL_ITEMS,
    ),
]
"""A small batch of story query strings."""

StoryQueryInput = Annotated[
    SingleStoryQuery | StoryQueryBatch,
    Field(
        description=(
            "One query string is standard. You may also pass a list of 1-5 query "
            "strings when you want to compare alternate phrasings in a single tool call."
        ),
    ),
]
"""Tool-schema alias for one-or-many story queries."""

SingleStoryId = Annotated[int, Field(gt=0)]
"""One positive Hacker News story ID."""

StoryIdBatch = Annotated[
    list[SingleStoryId],
    Field(
        min_length=1,
        max_length=MAX_BATCH_TOOL_ITEMS,
    ),
]
"""A small batch of positive Hacker News story IDs."""

StoryIdInput = Annotated[
    SingleStoryId | StoryIdBatch,
    Field(
        description=(
            "One story ID is standard. You may also pass a list of 1-5 story IDs "
            "when you want comments for several candidate stories in a single tool call."
        ),
    ),
]
"""Tool-schema alias for one-or-many story IDs."""


def parse_optional_iso_date(raw_value: str | None) -> date | None:
    """Parse an optional ISO date string.

    Strips surrounding whitespace and common quote characters (single, double)
    that the model may include when passing date literals. We intentionally let
    ``date.fromisoformat`` raise on truly invalid values so the model sees a
    clear validation-style error instead of silently guessing.
    """

    if raw_value is None:
        return None
    cleaned = raw_value.strip().strip("\"'")
    return date.fromisoformat(cleaned) if cleaned else None


def normalize_domains(domains: list[str] | None) -> list[str] | None:
    """Normalize domain filters to match repository/query expectations.

    The repository SQL already applies the same normalization as a safety net,
    but normalizing here keeps payloads and debugging behavior predictable.
    """

    if not domains:
        return None

    normalized: list[str] = []
    for domain in domains:
        clean = domain.strip().lower()
        if clean.startswith("www."):
            clean = clean[4:]
        if clean:
            normalized.append(clean)
    return normalized or None


def normalize_query_batch(query: str | list[str]) -> list[str]:
    """Normalize a one-or-many query input into a concrete batch list."""

    raw_queries = [query] if isinstance(query, str) else list(query)
    assert 1 <= len(raw_queries) <= MAX_BATCH_TOOL_ITEMS, (
        f"query must contain between 1 and {MAX_BATCH_TOOL_ITEMS} entries, "
        f"got {len(raw_queries)}"
    )

    normalized: list[str] = []
    for index, raw_query in enumerate(raw_queries, start=1):
        clean = raw_query.strip()
        assert len(clean) >= 2, f"query[{index}] must be at least 2 characters after trimming"
        normalized.append(clean)
    return normalized


def normalize_story_id_batch(story_id: int | list[int]) -> list[int]:
    """Normalize a one-or-many story-ID input into a concrete batch list."""

    raw_story_ids = [story_id] if isinstance(story_id, int) else list(story_id)
    assert 1 <= len(raw_story_ids) <= MAX_BATCH_TOOL_ITEMS, (
        f"story_id must contain between 1 and {MAX_BATCH_TOOL_ITEMS} entries, "
        f"got {len(raw_story_ids)}"
    )

    for index, current_story_id in enumerate(raw_story_ids, start=1):
        assert current_story_id > 0, f"story_id[{index}] must be a positive integer"
    return raw_story_ids


def story_hit_to_payload(hit: StorySearchHit) -> dict[str, object | None]:
    """Serialize a story search hit into the JSON shape returned by tools.

    The lightweight ``cursor`` field is the only citation-specific bit exposed
    to the model. The richer citation registry is maintained application-side.
    """

    return {
        "id": hit.id,
        "cursor": build_story_cursor(hit.id),
        "title": hit.title,
        "url": hit.url,
        "score": hit.score,
        "author": hit.by,
        "unix_time": hit.time,
        "date": hit.day.isoformat() if hit.day is not None else None,
    }


def top_comment_to_payload(comment: TopLevelCommentHit) -> dict[str, object | None]:
    """Serialize a top-level comment row into the JSON shape returned by tools.

    Comments expose only the cursor needed for inline model citations. The app
    resolves that cursor into richer metadata later.
    """

    return {
        "id": comment.id,
        "cursor": build_comment_cursor(comment.id),
        "author": comment.author,
        "comment": comment.comment,
    }
