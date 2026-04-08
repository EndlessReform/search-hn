"""Shared database access layer used by both the API and agent tool wrappers.

This module intentionally centralizes SQL and row-shaping logic so we can expose
multiple interfaces (HTTP, function tool, skill-like CLI wrappers) without
duplicating query behavior.

Design notes:
- We keep a *persistent* SQLAlchemy `Engine` in process for connection pooling.
- Each query acquires its own short-lived DB connection with `engine.connect()`.
  This is the safe default because OpenAI Agents can execute multiple function
  tools concurrently in the same turn.
- We target the `items.search_tsv` generated column added in
  `crates/hn_core/migrations/2026-02-16-000005_add_items_search_tsvector/up.sql`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

DEFAULT_SEARCH_LIMIT = 8
"""Default result count when wrappers do not specify an explicit limit."""

MAX_SEARCH_LIMIT = 100
"""Hard upper bound accepted by this repository for a single search call."""

DEFAULT_COMMENT_LIMIT = 5
"""Default comment count for top-level comment lookups."""


_STORY_SEARCH_SQL = text(
    """
    SELECT
        i.id,
        i.title,
        i.url,
        i.score,
        i.by,
        i.time,
        i.day
    FROM items AS i
    WHERE i.type = 'story'
      AND i.search_tsv @@ plainto_tsquery('simple', :query)
    ORDER BY i.score DESC NULLS LAST, i.day DESC NULLS LAST, i.id DESC
    LIMIT :limit
    """
)

_ITEM_TYPE_SQL = text(
    """
    SELECT i.type
    FROM items AS i
    WHERE i.id = :item_id
    """
)

_TOP_LEVEL_COMMENT_COUNT_SQL = text(
    """
    SELECT COUNT(*) AS total
    FROM kids AS k
    JOIN items AS c ON c.id = k.kid
    WHERE k.item = :story_id
      AND c.type = 'comment'
    """
)

_TOP_LEVEL_COMMENT_FETCH_SQL = text(
    """
    SELECT
        c.id,
        c.by AS author,
        c.text AS comment
    FROM kids AS k
    JOIN items AS c ON c.id = k.kid
    WHERE k.item = :story_id
      AND c.type = 'comment'
    ORDER BY k.display_order ASC NULLS LAST, k.kid ASC
    LIMIT :limit
    OFFSET :skip
    """
)


@dataclass(frozen=True)
class StorySearchHit:
    """Typed result row for story search.

    Keeping this shape explicit makes the handoff boundary obvious for wrappers:
    they receive Python values, and each wrapper decides how to render them.
    """

    id: int
    title: str | None
    url: str | None
    score: int | None
    by: str | None
    time: int | None
    day: date | None


@dataclass(frozen=True)
class TopLevelCommentHit:
    """Typed row for one top-level comment under a story."""

    id: int
    author: str | None
    comment: str | None


def create_db_engine(database_url: str) -> Engine:
    """Create the shared SQLAlchemy engine for read queries.

    A single engine is designed to live for the process lifetime. It owns the
    connection pool and is thread-safe for concurrent reads.
    """

    return create_engine(
        database_url,
        future=True,
        connect_args={"client_encoding": "utf8"},
    )


class HNStorySearchRepository:
    """Read-only repository over the mirrored HN Postgres schema.

    This class intentionally carries both story-search and top-level-comment
    read paths so wrappers can share one dependency object in context.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    @classmethod
    def from_database_url(cls, database_url: str) -> HNStorySearchRepository:
        """Factory used by context builders to create a pooled repository."""

        return cls(engine=create_db_engine(database_url))

    def dispose(self) -> None:
        """Release pooled DB resources.

        Call this during process shutdown (or CLI teardown) so long-lived
        wrappers do not leak connections.
        """

        self._engine.dispose()

    def search_stories(self, query: str, *, limit: int = DEFAULT_SEARCH_LIMIT) -> list[StorySearchHit]:
        """Search stories by full-text query over title+URL tokens.

        Contract:
        - `query` must be non-empty text.
        - `limit` must be between 1 and `MAX_SEARCH_LIMIT`.
        - Returns only `items.type='story'` rows.
        """

        normalized_query = query.strip()
        assert normalized_query, "query must be non-empty"
        assert 1 <= limit <= MAX_SEARCH_LIMIT, (
            f"limit must be in [1, {MAX_SEARCH_LIMIT}], got {limit}"
        )

        with self._engine.connect() as conn:
            rows = conn.execute(
                _STORY_SEARCH_SQL,
                {"query": normalized_query, "limit": limit},
            ).all()

        return [
            StorySearchHit(
                id=int(row.id),
                title=row.title,
                url=row.url,
                score=row.score,
                by=row.by,
                time=row.time,
                day=row.day,
            )
            for row in rows
        ]

    def fetch_top_level_comments(
        self,
        story_id: int,
        *,
        limit: int = DEFAULT_COMMENT_LIMIT,
        skip: int = 0,
    ) -> tuple[int, list[TopLevelCommentHit]]:
        """Fetch paged top-level comments for a known story ID.

        This mirrors the `hn_query` skill's comment behavior:
        - Validate the target row exists and is `type='story'`.
        - Count total top-level comments in `kids`.
        - Fetch a page ordered by `kids.display_order` then `kid`.
        """

        assert story_id > 0, "story_id must be a positive integer"
        assert 1 <= limit <= MAX_SEARCH_LIMIT, (
            f"limit must be in [1, {MAX_SEARCH_LIMIT}], got {limit}"
        )
        assert skip >= 0, f"skip must be >= 0, got {skip}"

        with self._engine.connect() as conn:
            item_row = conn.execute(_ITEM_TYPE_SQL, {"item_id": story_id}).one_or_none()
            assert item_row is not None, f"story_id {story_id} does not exist in items"
            assert item_row.type == "story", (
                f"ID {story_id} exists but is not a story (type={item_row.type!r})"
            )

            total = int(
                conn.execute(
                    _TOP_LEVEL_COMMENT_COUNT_SQL,
                    {"story_id": story_id},
                ).scalar_one()
            )
            if total == 0 or skip >= total:
                return total, []

            rows = conn.execute(
                _TOP_LEVEL_COMMENT_FETCH_SQL,
                {"story_id": story_id, "limit": limit, "skip": skip},
            ).all()

        return total, [
            TopLevelCommentHit(
                id=int(row.id),
                author=row.author,
                comment=row.comment,
            )
            for row in rows
        ]
