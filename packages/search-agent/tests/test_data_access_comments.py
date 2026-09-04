"""Regression tests for top-level comment relationship handling."""

from __future__ import annotations

import json

from sqlalchemy import create_engine, text

from search_agent.citations import CitationRegistry
from search_agent.data_access import HNStorySearchRepository
from search_agent.tools.fetch_top_comments import build_fetch_top_comments_payload


def test_fetch_comments_ignores_stale_kids_edge_after_thread_move() -> None:
    """Treat ``items.parent`` as authoritative when a discussion is moved.

    HN can leave the same comment in the ``kids`` lists of both the original
    and destination stories after a moderator moves a thread. This fixture
    reproduces that shape and ensures the old relationship is ignored.
    """

    engine = create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE items (
                    id INTEGER PRIMARY KEY,
                    type TEXT NOT NULL,
                    parent INTEGER,
                    by TEXT,
                    text TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE kids (
                    item INTEGER NOT NULL,
                    kid INTEGER NOT NULL,
                    display_order INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO items (id, type, parent, by, text) VALUES
                    (100, 'story', NULL, 'submitter', NULL),
                    (200, 'story', NULL, 'submitter', NULL),
                    (300, 'comment', 200, 'moved', 'moved comment'),
                    (301, 'comment', 100, 'original', 'original comment')
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO kids (item, kid, display_order) VALUES
                    (100, 300, 0),
                    (100, 301, 1),
                    (200, 300, 0)
                """
            )
        )

    repository = HNStorySearchRepository(engine)
    try:
        old_total, old_comments = repository.fetch_top_level_comments(100, limit=10)
        new_total, new_comments = repository.fetch_top_level_comments(200, limit=10)
        batched_payload = build_fetch_top_comments_payload(
            repository,
            story_id=[100, 200],
            limit=10,
        )
    finally:
        repository.dispose()

    assert old_total == 1
    assert [comment.id for comment in old_comments] == [301]
    assert new_total == 1
    assert [comment.id for comment in new_comments] == [300]

    registry = CitationRegistry()
    registry.ingest_tool_result("fetch_top_comments", json.dumps(batched_payload))
    moved_entry = registry.resolve("comment:300")
    assert moved_entry is not None
    assert moved_entry.story_id == 200
