"""Memory-only pagination and literal search over extracted webpages."""

from __future__ import annotations

import re
from itertools import islice

from search_agent.web.state import InspectionCall, WebConversationState
from search_agent.web.text import remaining_chunk_count, slice_extraction_tokens

MAX_FIND_MATCHES = 10
MAX_FIND_TERM_CHARACTERS = 128
SNIPPET_CONTEXT_CHARACTERS = 140

_INSPECTION_WARNING = (
    "Webpage inspection budget is now exhausted. Stop reading/searching pages "
    "and use fetch_top_comments for the associated story."
)


def inspection_budget_failure(*, story_id: int | None = None) -> dict[str, object]:
    """Return the stable refusal used after consecutive page calls run out."""

    action = "Use fetch_top_comments and stop inspecting webpages."
    if story_id is not None:
        action = f"Use fetch_top_comments for story {story_id}."
    return {
        "status": "inspection_budget_exhausted",
        "reason": "consecutive webpage-tool call limit reached",
        "recommended_action": action,
        "story_id": story_id,
    }


def finish_inspection_call(
    payload: dict[str, object],
    call: InspectionCall,
) -> dict[str, object]:
    """Attach the final-call warning without changing the base tool contract."""

    if call.warn_after_response:
        payload["inspection_warning"] = _INSPECTION_WARNING
    return payload


def read_cached_page(
    state: WebConversationState,
    *,
    page_id: str,
    cursor: str,
) -> dict[str, object]:
    """Read one bounded chunk using a page-bound opaque cursor."""

    call = state.begin_inspection_call()
    page = state.cached_for_id(page_id)
    if not call.allowed:
        return inspection_budget_failure(
            story_id=page.story_id if page is not None else None
        )
    if page is None:
        return finish_inspection_call(
            _failure("page_not_found", f"cached page {page_id!r} is unavailable"),
            call,
        )

    resolved = state.resolve_read_cursor(page_id, cursor)
    if resolved is None:
        return finish_inspection_call(
            _failure(
                "invalid_cursor",
                "read cursor is unknown, expired, or belongs to another page",
                story_id=page.story_id,
            ),
            call,
        )

    chunk = slice_extraction_tokens(page.markdown, start=resolved.offset)
    next_cursor = (
        state.issue_read_cursor(page_id, chunk.next_offset)
        if chunk.next_offset is not None
        else None
    )
    return finish_inspection_call(
        {
            "status": "ok",
            "page_id": page.page_id,
            "cursor": cursor,
            "untrusted_page_content": chunk.text,
            "chunk_token_count": chunk.token_count,
            "next_cursor": next_cursor,
            "remaining_chunks": remaining_chunk_count(
                page.markdown,
                start=chunk.next_offset,
            ),
        },
        call,
    )


def find_in_cached_page(
    state: WebConversationState,
    *,
    page_id: str,
    term: str,
    cursor: str | None = None,
) -> dict[str, object]:
    """Find up to ten literal, case-insensitive matches in cached content."""

    call = state.begin_inspection_call()
    page = state.cached_for_id(page_id)
    if not call.allowed:
        return inspection_budget_failure(
            story_id=page.story_id if page is not None else None
        )
    if page is None:
        return finish_inspection_call(
            _failure("page_not_found", f"cached page {page_id!r} is unavailable"),
            call,
        )

    clean_term = term.strip()
    if not clean_term or len(clean_term) > MAX_FIND_TERM_CHARACTERS:
        return finish_inspection_call(
            _failure(
                "invalid_request",
                f"term must contain 1-{MAX_FIND_TERM_CHARACTERS} characters",
                story_id=page.story_id,
            ),
            call,
        )

    bound_term = clean_term.casefold()
    start = 0
    if cursor is not None:
        resolved = state.resolve_find_cursor(page_id, bound_term, cursor)
        if resolved is None:
            return finish_inspection_call(
                _failure(
                    "invalid_cursor",
                    "find cursor is unknown, expired, or bound to another page/term",
                    story_id=page.story_id,
                ),
                call,
            )
        start = resolved.offset

    pattern = re.compile(re.escape(clean_term), re.IGNORECASE)
    found = list(islice(pattern.finditer(page.markdown, start), MAX_FIND_MATCHES + 1))
    visible = found[:MAX_FIND_MATCHES]
    matches = []
    for match in visible:
        read_offset = max(0, match.start() - SNIPPET_CONTEXT_CHARACTERS)
        matches.append(
            {
                "snippet": _match_snippet(page.markdown, match.start(), match.end()),
                "read_cursor": state.issue_read_cursor(page_id, read_offset),
            }
        )

    next_cursor = None
    if len(found) > MAX_FIND_MATCHES:
        next_cursor = state.issue_find_cursor(
            page_id,
            bound_term,
            found[MAX_FIND_MATCHES].start(),
        )

    return finish_inspection_call(
        {
            "status": "ok",
            "page_id": page.page_id,
            "term": clean_term,
            "returned": len(matches),
            "matches": matches,
            "next_cursor": next_cursor,
        },
        call,
    )


def _match_snippet(text: str, start: int, end: int) -> str:
    """Return compact context around one match without interpreting markup."""

    left = max(0, start - SNIPPET_CONTEXT_CHARACTERS)
    right = min(len(text), end + SNIPPET_CONTEXT_CHARACTERS)
    snippet = re.sub(r"\s+", " ", text[left:right]).strip()
    if left > 0:
        snippet = f"…{snippet}"
    if right < len(text):
        snippet = f"{snippet}…"
    return snippet


def _failure(
    status: str,
    reason: str,
    *,
    story_id: int | None = None,
) -> dict[str, object]:
    """Return a structured cached-inspection failure."""

    return {
        "status": status,
        "reason": reason,
        "recommended_action": "Use a cursor returned for this cached page.",
        "story_id": story_id,
    }
