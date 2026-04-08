"""Shared citation helpers for model output, TUI rendering, and future APIs.

This module deliberately separates three concerns that are easy to tangle:

- tool payloads expose lightweight cursor strings such as ``story:123``,
- the application maintains a richer in-memory registry keyed by those cursors,
- and renderers turn inline model markers into UI-specific annotations.

The current Textual TUI is the first consumer, but the same registry and
resolution logic can back an HTTP response format later without depending on
Textual or Rich.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Literal

CitationKind = Literal["story", "comment"]
"""Supported citation kinds emitted by the current HN toolset."""

_CITATION_MARKER_RE = re.compile(r"【([^】]+)】")
_EXACT_CURSOR_RE = re.compile(r"^(story|comment):(\d+)$")
_BARE_ITEM_ID_RE = re.compile(r"^\d+$")


def build_story_cursor(story_id: int) -> str:
    """Return the canonical cursor string for one HN story."""

    assert story_id > 0, f"story_id must be positive, got {story_id}"
    return f"story:{story_id}"


def build_comment_cursor(comment_id: int) -> str:
    """Return the canonical cursor string for one HN comment."""

    assert comment_id > 0, f"comment_id must be positive, got {comment_id}"
    return f"comment:{comment_id}"


def build_hn_item_url(item_id: int) -> str:
    """Return the public Hacker News permalink for a story or comment item."""

    assert item_id > 0, f"item_id must be positive, got {item_id}"
    return f"https://news.ycombinator.com/item?id={item_id}"


@dataclass(frozen=True)
class CitationEntry:
    """One citeable item known to the application.

    ``story`` entries may carry article metadata such as title and source URL.
    ``comment`` entries usually carry author and parent story information.
    """

    cursor: str
    kind: CitationKind
    item_id: int
    hn_url: str
    title: str | None = None
    source_url: str | None = None
    author: str | None = None
    story_id: int | None = None

    def merged_with(self, newer: CitationEntry) -> CitationEntry:
        """Merge two entries for the same cursor, preferring richer new data.

        The registry often sees the same story twice:
        - once as a lightweight stub from ``fetch_top_comments``,
        - then later as a richer search result carrying title and URL.

        This method keeps the stable identity fields and fills in any missing
        descriptive metadata from the newer observation.
        """

        assert self.cursor == newer.cursor, (
            f"cannot merge different cursors: {self.cursor!r} vs {newer.cursor!r}"
        )
        assert self.kind == newer.kind, (
            f"cannot merge different citation kinds for {self.cursor!r}: "
            f"{self.kind!r} vs {newer.kind!r}"
        )
        assert self.item_id == newer.item_id, (
            f"cannot merge different item IDs for {self.cursor!r}: "
            f"{self.item_id} vs {newer.item_id}"
        )
        if self.story_id is not None and newer.story_id is not None:
            assert self.story_id == newer.story_id, (
                f"conflicting parent story IDs for {self.cursor!r}: "
                f"{self.story_id} vs {newer.story_id}"
            )

        return CitationEntry(
            cursor=self.cursor,
            kind=self.kind,
            item_id=self.item_id,
            hn_url=newer.hn_url or self.hn_url,
            title=newer.title or self.title,
            source_url=newer.source_url or self.source_url,
            author=newer.author or self.author,
            story_id=newer.story_id if newer.story_id is not None else self.story_id,
        )


@dataclass(frozen=True)
class CitationReference:
    """One numbered reference produced when rendering model citation markers."""

    number: int
    entry: CitationEntry


@dataclass(frozen=True)
class CitationRenderResult:
    """A renderer-neutral view of cited model text.

    ``text`` contains the assistant message with inline markers rewritten into
    compact numbered references such as ``[1]``. ``references`` carries the
    structured metadata needed by a TUI, web frontend, or JSON API to show the
    underlying targets.
    """

    text: str
    references: list[CitationReference]


class CitationRegistry:
    """Application-owned registry of citeable stories and comments.

    The model never needs the full registry. It only sees lightweight cursor
    strings in tool outputs and copies them into its answer. The application
    resolves those copied cursors against this registry after the run.
    """

    def __init__(self) -> None:
        self._entries: dict[str, CitationEntry] = {}
        self._item_id_to_cursor: dict[int, str] = {}

    def clear(self) -> None:
        """Drop all known citation entries, typically on conversation reset."""

        self._entries.clear()
        self._item_id_to_cursor.clear()

    def register(self, entry: CitationEntry) -> None:
        """Register or enrich a citation entry."""

        existing = self._entries.get(entry.cursor)
        if existing is None:
            self._entries[entry.cursor] = entry
        else:
            self._entries[entry.cursor] = existing.merged_with(entry)

        indexed_cursor = self._item_id_to_cursor.get(entry.item_id)
        if indexed_cursor is None:
            self._item_id_to_cursor[entry.item_id] = entry.cursor
        else:
            assert indexed_cursor == entry.cursor, (
                f"item ID {entry.item_id} already mapped to {indexed_cursor!r}, "
                f"cannot also map it to {entry.cursor!r}"
            )

    def resolve(self, raw_cursor: str) -> CitationEntry | None:
        """Resolve an inline model marker token into a known citation entry.

        The prompt contract asks the model to copy exact cursors such as
        ``story:123`` and ``comment:456``. We also accept a bare numeric item ID
        as a forgiving fallback, provided the registry has already seen it.
        """

        token = raw_cursor.strip()
        exact_match = _EXACT_CURSOR_RE.fullmatch(token)
        if exact_match:
            return self._entries.get(token)

        if _BARE_ITEM_ID_RE.fullmatch(token):
            cursor = self._item_id_to_cursor.get(int(token))
            if cursor is not None:
                return self._entries.get(cursor)

        return None

    def ingest_tool_result(self, tool_name: str, raw_output: str) -> None:
        """Update the registry from one successful tool result payload.

        Tool handlers still return plain JSON strings because that is the most
        interoperable shape for the Agents SDK and for future HTTP reuse. This
        method is the one place where the application interprets those payloads
        as citeable stories/comments.
        """

        try:
            payload = json.loads(raw_output)
        except (json.JSONDecodeError, TypeError):
            return

        if tool_name == "fetch_stories":
            query_batches = payload.get("queries")
            if isinstance(query_batches, list):
                for batch in query_batches:
                    if isinstance(batch, dict):
                        self._ingest_story_list(batch.get("results"))
            else:
                self._ingest_story_list(payload.get("results"))
            return

        if tool_name == "fetch_top_stories_for_date":
            self._ingest_story_list(payload.get("results"))
            return

        if tool_name == "fetch_top_comments":
            story_batches = payload.get("stories")
            if isinstance(story_batches, list):
                for batch in story_batches:
                    if isinstance(batch, dict):
                        self._ingest_comment_story_batch(batch)
            elif isinstance(payload, dict):
                self._ingest_comment_story_batch(payload)

    def render_text(self, text: str) -> CitationRenderResult:
        """Resolve inline model citation markers into numbered references.

        The returned object is intentionally renderer-neutral. Consumers can
        display the text and attach references however they want.
        """

        ordered_references: dict[str, CitationReference] = {}

        def replace(match: re.Match[str]) -> str:
            token = match.group(1)
            resolved = self.resolve(token)
            if resolved is None:
                return match.group(0)

            reference = ordered_references.get(resolved.cursor)
            if reference is None:
                reference = CitationReference(
                    number=len(ordered_references) + 1,
                    entry=resolved,
                )
                ordered_references[resolved.cursor] = reference
            return f"[{reference.number}]"

        rendered_text = _CITATION_MARKER_RE.sub(replace, text)
        return CitationRenderResult(
            text=rendered_text,
            references=list(ordered_references.values()),
        )

    def _ingest_story_list(self, raw_results: object) -> None:
        """Register a list of story payload rows if present."""

        if not isinstance(raw_results, list):
            return

        for raw_result in raw_results:
            if not isinstance(raw_result, dict):
                continue
            story_id = raw_result.get("id")
            if not isinstance(story_id, int) or story_id <= 0:
                continue

            cursor = raw_result.get("cursor")
            if not isinstance(cursor, str) or not cursor:
                cursor = build_story_cursor(story_id)

            self.register(
                CitationEntry(
                    cursor=cursor,
                    kind="story",
                    item_id=story_id,
                    hn_url=build_hn_item_url(story_id),
                    title=_string_or_none(raw_result.get("title")),
                    source_url=_string_or_none(raw_result.get("url")),
                )
            )

    def _ingest_comment_story_batch(self, raw_story_payload: dict[str, object]) -> None:
        """Register one comment-tool story batch and its child comments."""

        story_id = raw_story_payload.get("story_id")
        if not isinstance(story_id, int) or story_id <= 0:
            return

        raw_story_cursor = raw_story_payload.get("story_cursor")
        story_cursor = (
            raw_story_cursor
            if isinstance(raw_story_cursor, str) and raw_story_cursor
            else build_story_cursor(story_id)
        )
        self.register(
            CitationEntry(
                cursor=story_cursor,
                kind="story",
                item_id=story_id,
                hn_url=build_hn_item_url(story_id),
            )
        )

        raw_comments = raw_story_payload.get("comments")
        if not isinstance(raw_comments, list):
            return

        for raw_comment in raw_comments:
            if not isinstance(raw_comment, dict):
                continue
            comment_id = raw_comment.get("id")
            if not isinstance(comment_id, int) or comment_id <= 0:
                continue

            raw_comment_cursor = raw_comment.get("cursor")
            comment_cursor = (
                raw_comment_cursor
                if isinstance(raw_comment_cursor, str) and raw_comment_cursor
                else build_comment_cursor(comment_id)
            )
            self.register(
                CitationEntry(
                    cursor=comment_cursor,
                    kind="comment",
                    item_id=comment_id,
                    hn_url=build_hn_item_url(comment_id),
                    author=_string_or_none(raw_comment.get("author")),
                    story_id=story_id,
                )
            )


def _string_or_none(value: object) -> str | None:
    """Return the string value if present and non-empty, else ``None``."""

    if isinstance(value, str) and value:
        return value
    return None
