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


def test_comment_payload_authorizes_anchor_plaintext_and_nested_links() -> None:
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
                                'and <a href="javascript:alert(1)">nope</a>. '
                                "Plain https://plain.example/post. "
                                '<a href="https://archive.example/2020/'
                                'https://original.example/report">archived</a></p>'
                            )
                        }
                    ],
                }
            ]
        }
    )

    assert count == 4
    authorization = state.authorization_for("https://notes.example.org/a")
    assert authorization is not None
    assert authorization.story_id == 88
    assert authorization.source == "top-level-comment"
    assert state.is_authorized("https://plain.example/post")
    assert state.is_authorized(
        "https://archive.example/2020/https://original.example/report"
    )
    assert state.is_authorized("https://original.example/report")


def test_submission_provenance_wins_when_comment_exposed_same_url_first() -> None:
    """Do not require comment approval for a URL also returned as a submission."""

    state = WebConversationState()
    state.authorize(
        "https://example.com/shared",
        depth=0,
        story_id=10,
        source="top-level-comment",
    )
    state.authorize(
        "https://example.com/shared",
        depth=0,
        story_id=20,
        source="story",
    )

    authorization = state.authorization_for("https://example.com/shared")
    assert authorization is not None
    assert authorization.source == "story"
    assert authorization.story_id == 20


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


def test_discovered_links_are_authorized_only_through_depth_three() -> None:
    state = WebConversationState()
    root = state.authorize(
        "https://example.com/root",
        depth=0,
        story_id=42,
        source="story",
    )

    assert (
        state.authorize_page_links(
            '<a href="/one">one</a>',
            base_url=root.url,
            parent=root,
        )
        == 1
    )
    one = state.authorization_for("https://example.com/one")
    assert one is not None
    assert one.depth == 1
    assert one.story_id == 42

    state.authorize_page_links('<a href="/two">two</a>', base_url=one.url, parent=one)
    two = state.authorization_for("https://example.com/two")
    assert two is not None and two.depth == 2

    state.authorize_page_links(
        '<a href="/three">three</a>', base_url=two.url, parent=two
    )
    three = state.authorization_for("https://example.com/three")
    assert three is not None and three.depth == 3

    assert (
        state.authorize_page_links(
            '<a href="/four">four</a>', base_url=three.url, parent=three
        )
        == 0
    )
    assert not state.is_authorized("https://example.com/four")


def test_cursors_are_page_bound_and_expire_with_eviction() -> None:
    state = WebConversationState(max_cached_pages=1)
    first_auth = state.authorize(
        "https://example.com/one", depth=0, story_id=1, source="story"
    )
    first = state.cache_page(
        requested_url=first_auth.url,
        final_url=first_auth.url,
        title=None,
        author=None,
        published=None,
        markdown="first page content",
        extractor="fixture",
        authorization=first_auth,
    )
    read_cursor = state.issue_read_cursor(first.page_id, 3)
    find_cursor = state.issue_find_cursor(first.page_id, "term", 4)

    assert state.resolve_read_cursor(first.page_id, read_cursor) is not None
    assert state.resolve_read_cursor("page:999", read_cursor) is None
    assert state.resolve_find_cursor(first.page_id, "other", find_cursor) is None

    second_auth = state.authorize(
        "https://example.com/two", depth=0, story_id=2, source="story"
    )
    state.cache_page(
        requested_url=second_auth.url,
        final_url=second_auth.url,
        title=None,
        author=None,
        published=None,
        markdown="second page content",
        extractor="fixture",
        authorization=second_auth,
    )

    assert state.resolve_read_cursor(first.page_id, read_cursor) is None
    assert state.resolve_find_cursor(first.page_id, "term", find_cursor) is None


def test_inspection_budget_warns_then_refuses_until_reset() -> None:
    state = WebConversationState(inspection_call_limit=3)

    first = state.begin_inspection_call()
    second = state.begin_inspection_call()
    third = state.begin_inspection_call()
    fourth = state.begin_inspection_call()

    assert first.allowed and not first.warn_after_response
    assert second.allowed and not second.warn_after_response
    assert third.allowed and third.warn_after_response
    assert not fourth.allowed

    state.reset_inspection_budget()
    assert state.begin_inspection_call().number == 1
