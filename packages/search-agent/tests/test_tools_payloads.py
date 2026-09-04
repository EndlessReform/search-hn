"""Unit tests for the search-agent tool payload builders."""

from __future__ import annotations

import unittest
from datetime import date

from search_agent.data_access import StorySearchHit, TopLevelCommentHit
from search_agent.tools.fetch_stories import build_fetch_stories_payload
from search_agent.tools.fetch_top_comments import build_fetch_top_comments_payload
from search_agent.tools.fetch_top_stories_for_date import (
    build_top_stories_for_date_payload,
)
from search_agent.web.policy import PublisherPolicy


class FakeRepository:
    """Tiny fake repository for payload-builder tests."""

    def __init__(self) -> None:
        self.search_calls: list[dict[str, object]] = []
        self.comment_calls: list[dict[str, object]] = []
        self.top_story_calls: list[dict[str, object]] = []
        self.empty_story_queries: set[str] = set()

    def search_stories(
        self,
        query: str | None,
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
        if query in self.empty_story_queries:
            return []
        query_label = query if query is not None else "<filtered top stories>"
        return [
            StorySearchHit(
                id=len(self.search_calls),
                title=f"result for {query_label}",
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

    def top_stories_for_date(
        self,
        target_date: date,
        *,
        limit: int,
    ) -> list[StorySearchHit]:
        self.top_story_calls.append(
            {
                "target_date": target_date,
                "limit": limit,
            }
        )
        return [
            StorySearchHit(
                id=999,
                title=f"top stories for {target_date.isoformat()}",
                url="https://example.com/top",
                score=99,
                by="pg",
                time=1_700_000_000,
                day=target_date,
            )
        ]


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
        self.assertEqual(payload["results"][0]["cursor"], "story:1")
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

    def test_policy_affected_story_is_marked_comments_only_before_opening(self) -> None:
        repository = FakeRepository()
        policy = PublisherPolicy(
            hard_blacklist=frozenset(),
            comment_only_blacklist=frozenset({"example.com"}),
        )

        payload = build_fetch_stories_payload(
            repository,
            query="publisher policy",
            publisher_policy=policy,
        )

        self.assertEqual(payload["results"][0]["web"], "comments_only")
        self.assertEqual(payload["results"][0]["domain"], "example.com")
        self.assertNotIn("url", payload["results"][0])

        story_id = payload["results"][0]["id"]
        comments = build_fetch_top_comments_payload(
            repository,
            story_id=story_id,
        )
        self.assertEqual(comments["story_id"], story_id)
        self.assertEqual(repository.comment_calls[-1]["story_id"], story_id)

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

    def test_filter_only_story_search_is_allowed_with_domain_or_date_filters(
        self,
    ) -> None:
        repository = FakeRepository()

        payload = build_fetch_stories_payload(
            repository,
            query=None,
            min_date="2025-01-01",
            max_date="2025-01-31",
            include_domains=["WWW.GitHub.com"],
            limit=3,
        )

        self.assertIsNone(payload["query"])
        self.assertEqual(len(payload["results"]), 1)
        self.assertEqual(
            payload["results"][0]["title"], "result for <filtered top stories>"
        )
        self.assertEqual(
            repository.search_calls,
            [
                {
                    "query": None,
                    "limit": 3,
                    "min_score": None,
                    "min_date": date(2025, 1, 1),
                    "max_date": date(2025, 1, 31),
                    "include_domains": ["github.com"],
                    "exclude_domains": None,
                }
            ],
        )

    def test_missing_query_and_filter_constraints_raises_informative_error(
        self,
    ) -> None:
        repository = FakeRepository()

        with self.assertRaisesRegex(
            AssertionError,
            "requires either a `query`, or at least one domain filter",
        ):
            build_fetch_stories_payload(
                repository,
                query=None,
            )

    def test_empty_story_search_can_include_one_time_guidance(self) -> None:
        repository = FakeRepository()
        repository.empty_story_queries.add("hhkb")

        payload = build_fetch_stories_payload(
            repository,
            query="hhkb",
            include_no_results_guidance=True,
        )

        self.assertEqual(payload["query"], "hhkb")
        self.assertEqual(payload["results"], [])
        self.assertIn("search_guidance", payload)

    def test_guidance_is_omitted_when_results_exist(self) -> None:
        repository = FakeRepository()

        payload = build_fetch_stories_payload(
            repository,
            query="hhkb",
            include_no_results_guidance=True,
        )

        self.assertNotIn("search_guidance", payload)

    def test_guidance_is_omitted_when_not_requested(self) -> None:
        repository = FakeRepository()
        repository.empty_story_queries.add("hhkb")

        payload = build_fetch_stories_payload(
            repository,
            query="hhkb",
            include_no_results_guidance=False,
        )

        self.assertNotIn("search_guidance", payload)


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
        self.assertEqual(payload["story_cursor"], "story:123")
        self.assertEqual(payload["returned"], 1)
        self.assertEqual(payload["remaining_after_page"], 1)
        self.assertEqual(len(payload["stories"]), 1)
        self.assertEqual(payload["comments"][0]["cursor"], "comment:1230")
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
            [entry["story_cursor"] for entry in payload["stories"]],
            ["story:123", "story:456"],
        )
        self.assertEqual(
            [call["story_id"] for call in repository.comment_calls],
            [123, 456],
        )


class BuildTopStoriesForDatePayloadTests(unittest.TestCase):
    """Behavioral tests for top-stories-by-date payload shaping."""

    def test_uses_injected_today_when_target_date_is_omitted(self) -> None:
        repository = FakeRepository()

        payload = build_top_stories_for_date_payload(
            repository,
            today=date(1862, 1, 1),
            limit=4,
        )

        self.assertEqual(payload["date"], "1862-01-01")
        self.assertEqual(payload["results"][0]["title"], "top stories for 1862-01-01")
        self.assertEqual(
            repository.top_story_calls,
            [{"target_date": date(1862, 1, 1), "limit": 4}],
        )


if __name__ == "__main__":
    unittest.main()
