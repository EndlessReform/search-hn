"""Unit tests for the search-agent tool payload builders."""

from __future__ import annotations

from datetime import date
import unittest

from search_agent.data_access import TopLevelCommentHit, StorySearchHit
from search_agent.tools.fetch_stories import build_fetch_stories_payload
from search_agent.tools.fetch_top_comments import build_fetch_top_comments_payload


class FakeRepository:
    """Tiny fake repository for payload-builder tests."""

    def __init__(self) -> None:
        self.search_calls: list[dict[str, object]] = []
        self.comment_calls: list[dict[str, object]] = []

    def search_stories(
        self,
        query: str,
        *,
        limit: int,
        min_score: int | None,
        min_date: date | None,
        max_date: date | None,
        include_domains: list[str] | None,
        exclude_domains: list[str] | None,
    ) -> list[StorySearchHit]:
        self.search_calls.append(
            {
                "query": query,
                "limit": limit,
                "min_score": min_score,
                "min_date": min_date,
                "max_date": max_date,
                "include_domains": include_domains,
                "exclude_domains": exclude_domains,
            }
        )
        return [
            StorySearchHit(
                id=len(self.search_calls),
                title=f"result for {query}",
                url="https://example.com",
                score=42,
                by="pg",
                time=1_700_000_000,
                day=date(2025, 1, 2),
            )
        ]

    def fetch_top_level_comments(
        self,
        story_id: int,
        *,
        limit: int,
        skip: int,
    ) -> tuple[int, list[TopLevelCommentHit]]:
        self.comment_calls.append(
            {
                "story_id": story_id,
                "limit": limit,
                "skip": skip,
            }
        )
        return (
            3,
            [
                TopLevelCommentHit(
                    id=story_id * 10,
                    author="dang",
                    comment=f"comment for {story_id}",
                )
            ],
        )


class BuildFetchStoriesPayloadTests(unittest.TestCase):
    """Behavioral tests for story-search payload shaping."""

    def test_single_query_preserves_original_top_level_shape(self) -> None:
        repository = FakeRepository()

        payload = build_fetch_stories_payload(
            repository,
            query="  rust agents  ",
            limit=4,
            min_score=10,
            min_date="2025-01-01",
            max_date="2025-01-31",
            include_domains=["WWW.GitHub.com", "  arxiv.org "],
            exclude_domains=["reddit.com"],
        )

        self.assertEqual(payload["query"], "rust agents")
        self.assertEqual(len(payload["results"]), 1)
        self.assertEqual(len(payload["queries"]), 1)
        self.assertEqual(
            repository.search_calls,
            [
                {
                    "query": "rust agents",
                    "limit": 4,
                    "min_score": 10,
                    "min_date": date(2025, 1, 1),
                    "max_date": date(2025, 1, 31),
                    "include_domains": ["github.com", "arxiv.org"],
                    "exclude_domains": ["reddit.com"],
                }
            ],
        )

    def test_multi_query_returns_query_batch_payload(self) -> None:
        repository = FakeRepository()

        payload = build_fetch_stories_payload(
            repository,
            query=["rust tui", "textual agent"],
            limit=2,
        )

        self.assertEqual(payload["query_count"], 2)
        self.assertEqual(
            [entry["query"] for entry in payload["queries"]],
            ["rust tui", "textual agent"],
        )
        self.assertEqual(
            [call["query"] for call in repository.search_calls],
            ["rust tui", "textual agent"],
        )

    def test_query_batch_rejects_more_than_five_items(self) -> None:
        repository = FakeRepository()

        with self.assertRaisesRegex(AssertionError, "between 1 and 5"):
            build_fetch_stories_payload(
                repository,
                query=["a1", "a2", "a3", "a4", "a5", "a6"],
            )


class BuildFetchTopCommentsPayloadTests(unittest.TestCase):
    """Behavioral tests for top-comment payload shaping."""

    def test_single_story_preserves_original_top_level_shape(self) -> None:
        repository = FakeRepository()

        payload = build_fetch_top_comments_payload(
            repository,
            story_id=123,
            limit=2,
            skip=1,
        )

        self.assertEqual(payload["story_id"], 123)
        self.assertEqual(payload["returned"], 1)
        self.assertEqual(payload["remaining_after_page"], 1)
        self.assertEqual(len(payload["stories"]), 1)
        self.assertEqual(
            repository.comment_calls,
            [{"story_id": 123, "limit": 2, "skip": 1}],
        )

    def test_multi_story_returns_story_batch_payload(self) -> None:
        repository = FakeRepository()

        payload = build_fetch_top_comments_payload(
            repository,
            story_id=[123, 456],
        )

        self.assertEqual(payload["story_count"], 2)
        self.assertEqual(
            [entry["story_id"] for entry in payload["stories"]],
            [123, 456],
        )
        self.assertEqual(
            [call["story_id"] for call in repository.comment_calls],
            [123, 456],
        )


if __name__ == "__main__":
    unittest.main()
