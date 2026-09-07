"""Opt-in PostgreSQL integration checks for the search repository."""

from __future__ import annotations

import json
import os

import pytest
from search_agent.citations import CitationRegistry
from search_agent.data_access import HNStorySearchRepository
from search_agent.tools.fetch_top_comments import build_fetch_top_comments_payload


@pytest.mark.integration
def test_story_search_against_postgres_schema() -> None:
    """Exercise the generated tsvector and row mapping against a real mirror.

    Set ``TEST_DATABASE_URL`` to opt in. Keeping this separate from
    ``DATABASE_URL`` prevents an ordinary unit-test run from connecting to a
    developer's configured database unexpectedly.
    """

    database_url = os.getenv("TEST_DATABASE_URL")
    if database_url is None:
        pytest.skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")

    repository = HNStorySearchRepository.from_database_url(database_url)
    try:
        results = repository.search_stories("postgres", limit=3)
    finally:
        repository.dispose()

    assert len(results) <= 3
    assert all(result.id > 0 for result in results)


@pytest.mark.integration
def test_moved_comment_belongs_only_to_its_current_parent() -> None:
    """Regress the real HN move that formerly crashed citation ingestion.

    Comment 49555558 remains in both stories' mirrored ``kids`` relationships,
    but its current ``items.parent`` is story 49554643. The repository must not
    return it for the obsolete parent, story 49554273.
    """

    database_url = os.getenv("TEST_DATABASE_URL")
    if database_url is None:
        pytest.skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")

    repository = HNStorySearchRepository.from_database_url(database_url)
    try:
        payload = build_fetch_top_comments_payload(
            repository,
            story_id=[49554273, 49554643],
            limit=100,
        )
    finally:
        repository.dispose()

    story_batches = payload["stories"]
    assert isinstance(story_batches, list)
    comments_by_story = {
        batch["story_id"]: {comment["id"] for comment in batch["comments"]}
        for batch in story_batches
    }
    assert 49555558 not in comments_by_story[49554273]
    assert 49555558 in comments_by_story[49554643]

    registry = CitationRegistry()
    registry.ingest_tool_result("fetch_top_comments", json.dumps(payload))
    moved_entry = registry.resolve("comment:49555558")
    assert moved_entry is not None
    assert moved_entry.story_id == 49554643
