"""Opt-in PostgreSQL integration checks for the search repository."""

from __future__ import annotations

import os

import pytest
from search_agent.data_access import HNStorySearchRepository


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
