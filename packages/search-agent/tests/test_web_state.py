"""Unit tests for conversation URL provenance and page caching."""

from __future__ import annotations

from search_agent.web.state import WebConversationState


def test_story_payload_authorizes_only_valid_source_urls() -> None:
    state = WebConversationState()
    count = state.register_story_payload(
        {
            "queries": [
                {
                    "results": [
                        {"id": 12, "url": "https://example.com/article#part"},
                        {"id": 13, "url": "file:///tmp/nope"},
                        {"id": 14, "url": None},
                    ]
                }
            ]
        }
    )

    assert count == 1
    authorization = state.authorization_for("https://example.com/article")
    assert authorization is not None
    assert authorization.depth == 0
    assert authorization.story_id == 12
    assert authorization.source == "story"


def test_comment_payload_authorizes_only_anchor_links() -> None:
    state = WebConversationState()
    count = state.register_comment_payload(
        {
            "stories": [
                {
                    "story_id": 88,
                    "comments": [
                        {
                            "comment": (
                                '<p>See <a href="https://notes.example.org/a#x">notes</a> '
                                'and <a href="javascript:alert(1)">nope</a>.</p>'
                            )
                        }
                    ],
                }
            ]
        }
    )

    assert count == 1
    authorization = state.authorization_for("https://notes.example.org/a")
    assert authorization is not None
    assert authorization.story_id == 88
    assert authorization.source == "top-level-comment"


def test_cache_is_conversation_scoped_and_lru_bounded() -> None:
    state = WebConversationState(max_cached_pages=1)
    first_auth = state.authorize(
        "https://example.com/one",
        depth=0,
        story_id=1,
        source="story",
    )
    second_auth = state.authorize(
        "https://example.com/two",
        depth=0,
        story_id=2,
        source="story",
    )
    first = state.cache_page(
        requested_url=first_auth.url,
        final_url=first_auth.url,
        title="One",
        author=None,
        published=None,
        markdown="one " * 30,
        extractor="fixture",
        authorization=first_auth,
    )
    second = state.cache_page(
        requested_url=second_auth.url,
        final_url=second_auth.url,
        title="Two",
        author=None,
        published=None,
        markdown="two " * 30,
        extractor="fixture",
        authorization=second_auth,
    )

    assert first.page_id == "page:1"
    assert second.page_id == "page:2"
    assert state.cached_for_url(first_auth.url) is None
    assert state.cached_for_url(second_auth.url) == second

    state.clear()
    assert not state.is_authorized(first_auth.url)
    assert state.cached_for_url(second_auth.url) is None
