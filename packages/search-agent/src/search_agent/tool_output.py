"""Tool output parsing and formatting utilities.

These helpers turn raw JSON tool results into compact status-log lines and
extract structured data from streamed SDK run items. All functions here are
pure (no I/O, no Textual dependency) so they can be reused by non-TUI
consumers.
"""

from __future__ import annotations

import json
from typing import Any

from rich.markup import escape

_MAX_CONSECUTIVE_TOOL_FAILURES = 3


# ── story / comment summarisers ───────────────────────────────────────────


def _summarize_story_batches(story_batches: list[dict]) -> str:
    """Summarize one or more story-search batches for the status log."""

    if not story_batches:
        return "no story searches returned"

    if len(story_batches) == 1:
        batch = story_batches[0]
        results = batch.get("results", [])
        query = escape(str(batch.get("query", "?")))
        if not results:
            return f'no stories for "{query}"'
        titles = ", ".join(
            escape(str(result.get("title", "?"))[:60]) for result in results[:4]
        )
        suffix = f" (+{len(results) - 4} more)" if len(results) > 4 else ""
        return f'{len(results)} stories for "{query}": {titles}{suffix}'

    parts = []
    for batch in story_batches[:3]:
        query = escape(str(batch.get("query", "?")))
        result_count = len(batch.get("results", []))
        parts.append(f'"{query}" ({result_count})')
    suffix = (
        f", +{len(story_batches) - 3} more queries" if len(story_batches) > 3 else ""
    )
    return f"{len(story_batches)} story searches: {', '.join(parts)}{suffix}"


def _summarize_comment_batches(comment_batches: list[dict]) -> str:
    """Summarize one or more comment fetch batches for the status log."""

    if not comment_batches:
        return "no comment lookups returned"

    if len(comment_batches) == 1:
        batch = comment_batches[0]
        total = batch.get("total_top_level_comments", 0)
        returned = batch.get("returned", 0)
        story_id = batch.get("story_id", "?")
        return f"{returned}/{total} comments for story {story_id}"

    parts = []
    for batch in comment_batches[:3]:
        story_id = batch.get("story_id", "?")
        returned = batch.get("returned", 0)
        total = batch.get("total_top_level_comments", 0)
        parts.append(f"{story_id} ({returned}/{total})")
    suffix = (
        f", +{len(comment_batches) - 3} more stories"
        if len(comment_batches) > 3
        else ""
    )
    return f"{len(comment_batches)} comment lookups: {', '.join(parts)}{suffix}"


def _summarize_tool_result(tool_name: str, raw: str) -> str:
    """Turn raw tool JSON into a compact one-liner for the status log."""

    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return escape(raw[:200])

    if tool_name == "fetch_stories":
        story_batches = data.get("queries")
        if isinstance(story_batches, list):
            return _summarize_story_batches(story_batches)
        return _summarize_story_batches([data])

    if tool_name == "fetch_top_stories_for_date":
        results = data.get("results", [])
        d = escape(data.get("date", "?"))
        if not results:
            return f"no stories for {d}"
        titles = ", ".join(escape(r.get("title", "?")[:60]) for r in results[:4])
        suffix = f" (+{len(results) - 4} more)" if len(results) > 4 else ""
        return f"{len(results)} top stories for {d}: {titles}{suffix}"

    if tool_name == "fetch_top_comments":
        comment_batches = data.get("stories")
        if isinstance(comment_batches, list):
            return _summarize_comment_batches(comment_batches)
        return _summarize_comment_batches([data])

    if tool_name in {"open_webpage", "read_webpage", "find_in_webpage"}:
        status = data.get("status", "unknown")
        if status != "ok":
            return f"{escape(str(status))}: {escape(str(data.get('reason', 'failed'))[:140])}"
        warning = (
            " — inspection limit reached; move to comments"
            if data.get("inspection_warning")
            else ""
        )
        if tool_name == "read_webpage":
            remaining = data.get("remaining_chunks", 0)
            return (
                f"read {escape(str(data.get('page_id', 'page')))} "
                f"({remaining} chunks remain){warning}"
            )
        if tool_name == "find_in_webpage":
            returned = data.get("returned", 0)
            term = escape(str(data.get("term", "?")))
            return f'{returned} matches for "{term[:80]}"{warning}'
        title = escape(str(data.get("title") or data.get("url") or "page"))
        cache_note = " (cached)" if data.get("cache_hit") else ""
        return f"opened {title[:100]}{cache_note}{warning}"

    return escape(raw[:200])


# ── tool-call preview ─────────────────────────────────────────────────────


def _format_tool_call_preview(tool_name: str, arguments: str | None) -> str | None:
    """Return a concise verbose preview of an imminent tool call.

    Search calls expose their query and webpage opens expose their exact target.
    The formatter stays independent of Textual so other clients can reuse the
    same safe JSON parsing and compact labels.
    """

    if not arguments:
        return None

    try:
        payload = json.loads(arguments)
    except (json.JSONDecodeError, TypeError):
        return None

    if tool_name == "open_webpage":
        url = payload.get("url")
        if not isinstance(url, str) or not url.strip():
            return None
        return f"webpage: {url.strip()}"

    if tool_name != "fetch_stories":
        return None

    raw_query = payload.get("query")
    if isinstance(raw_query, str):
        queries = [raw_query.strip()]
    elif isinstance(raw_query, list):
        queries = [
            candidate.strip()
            for candidate in raw_query
            if isinstance(candidate, str) and candidate.strip()
        ]
    else:
        return None

    if not queries:
        return None

    preview = ", ".join(f'"{query}"' for query in queries[:5])
    if len(queries) == 1:
        return f"search query: {preview}"
    return f"search queries ({len(queries)}): {preview}"


# ── SDK item introspection ────────────────────────────────────────────────


def _safe_getattr(value: object, attr_name: str) -> Any | None:
    """Return ``getattr`` if present, but tolerate SDK items without the field."""

    try:
        return getattr(value, attr_name)
    except AttributeError:
        return None


def _extract_tool_name_from_raw_item(raw_item: object) -> str | None:
    """Best-effort extraction of the tool name from a raw SDK item."""

    candidate: object | None = None
    if isinstance(raw_item, dict):
        candidate = raw_item.get("name") or raw_item.get("tool_name")
    else:
        candidate = _safe_getattr(raw_item, "name") or _safe_getattr(
            raw_item, "tool_name"
        )

    return candidate if isinstance(candidate, str) and candidate else None


def _extract_tool_arguments_from_raw_item(raw_item: object) -> str | None:
    """Best-effort extraction of tool arguments from a raw SDK item."""

    candidate: object | None = None
    if isinstance(raw_item, dict):
        candidate = raw_item.get("arguments")
        if candidate is None:
            candidate = raw_item.get("params") or raw_item.get("input")
    else:
        candidate = _safe_getattr(raw_item, "arguments")
        if candidate is None:
            candidate = _safe_getattr(raw_item, "params") or _safe_getattr(
                raw_item, "input"
            )

    if candidate is None:
        return None
    if isinstance(candidate, str):
        return candidate
    try:
        return json.dumps(candidate)
    except (TypeError, ValueError):
        return str(candidate)


def _extract_tool_call_name_and_arguments(
    item: object,
) -> tuple[str | None, str | None]:
    """Extract a tool-call name and argument string from a streamed run item.

    The Agents SDK has helper properties on ``ToolCallItem``, but we keep this
    logic tolerant of minor SDK shape differences by reading the dataclass field
    and underlying raw item directly.
    """

    raw_item = _safe_getattr(item, "raw_item")
    tool_name = _safe_getattr(item, "tool_name")
    if not isinstance(tool_name, str) or not tool_name:
        tool_name = _extract_tool_name_from_raw_item(raw_item)

    arguments = _extract_tool_arguments_from_raw_item(raw_item)
    return tool_name, arguments
