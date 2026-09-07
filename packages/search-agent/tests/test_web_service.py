"""Behavioral tests for the standalone webpage application service."""

from __future__ import annotations

from dataclasses import dataclass

from search_agent.web.extractor import ExtractedDocument
from search_agent.web.fetcher import FetchedPage
from search_agent.web.policy import PublisherPolicy
from search_agent.web.service import PREVIEW_TOKENS, WebPageService, _preview
from search_agent.web.state import WebConversationState


@dataclass
class FakeFetcher:
    """Return one deterministic HTML page and count network-equivalent calls."""

    calls: int = 0

    def fetch(self, url: str, *, authorization, policy) -> FetchedPage:
        assert authorization.is_authorized(url)
        assert policy.evaluate(url) is None
        self.calls += 1
        return FetchedPage(
            final_url=url,
            content_type="text/html; charset=utf-8",
            body=(
                b"<html><main><h1>Useful</h1><p>fixture body</p></main>"
                b'<script src="/cdn-cgi/challenge-platform/scripts/jsd/main.js"></script>'
                b"</html>"
            ),
        )


@dataclass
class FakeExtractor:
    """Return enough Markdown to satisfy the service usefulness threshold."""

    name: str = "fixture-extractor"
    calls: int = 0

    def extract(self, html: str) -> ExtractedDocument:
        assert "fixture body" in html
        self.calls += 1
        return ExtractedDocument(
            markdown="# Useful\n\n" + "A substantial extracted sentence. " * 40,
            title="Useful",
            author="Ada",
            published="2026-08-01",
        )


def _service(*, state: WebConversationState | None = None):
    resolved_state = state or WebConversationState()
    fetcher = FakeFetcher()
    extractor = FakeExtractor()
    service = WebPageService(
        state=resolved_state,
        policy=PublisherPolicy(
            frozenset({"paywall.example"}), frozenset({"news.example"})
        ),
        fetcher=fetcher,  # type: ignore[arg-type]
        extractor=extractor,  # type: ignore[arg-type]
    )
    return service, fetcher, extractor


def test_open_requires_authorization_before_fetching() -> None:
    service, fetcher, _extractor = _service()

    payload = service.open("https://example.com/article")

    assert payload["status"] == "not_authorized"
    assert fetcher.calls == 0


def test_policy_short_circuits_before_fetching() -> None:
    state = WebConversationState()
    state.authorize(
        "https://sub.paywall.example/article",
        depth=0,
        story_id=45,
        source="story",
    )
    service, fetcher, _extractor = _service(state=state)

    payload = service.open("https://sub.paywall.example/article")

    assert payload["status"] == "blocked_domain"
    assert payload["story_id"] == 45
    assert fetcher.calls == 0


def test_success_returns_preview_and_second_open_is_cache_hit() -> None:
    state = WebConversationState()
    state.authorize(
        "https://example.com/article",
        depth=0,
        story_id=77,
        source="story",
    )
    service, fetcher, extractor = _service(state=state)

    first = service.open("https://example.com/article#ignored")
    second = service.open("https://example.com/article")

    assert first["status"] == "ok"
    assert first["page_id"] == "page:1"
    assert first["cache_hit"] is False
    assert first["title"] == "Useful"
    assert "substantial extracted sentence" in first["untrusted_page_content"]
    assert second["page_id"] == "page:1"
    assert second["cache_hit"] is True
    assert fetcher.calls == 1
    assert extractor.calls == 1


def test_subscription_preview_is_rejected_before_extraction() -> None:
    state = WebConversationState()
    authorization = state.authorize(
        "https://nature.example/paper",
        depth=0,
        story_id=91,
        source="story",
    )
    service, _fetcher, extractor = _service(state=state)

    class PreviewFetcher:
        def fetch(self, url: str, *, authorization, policy) -> FetchedPage:
            return FetchedPage(
                final_url=url,
                content_type="text/html",
                body=b"<p>This is a preview of subscription content.</p>",
            )

    service = WebPageService(
        state=state,
        policy=service.policy,
        fetcher=PreviewFetcher(),  # type: ignore[arg-type]
        extractor=extractor,  # type: ignore[arg-type]
    )
    assert authorization.story_id == 91

    payload = service.open(authorization.url)

    assert payload["status"] == "paywall_detected"
    assert extractor.calls == 0


def test_preview_uses_a_bounded_model_independent_token_estimate() -> None:
    preview, truncated, token_count = _preview("word " * (PREVIEW_TOKENS + 50))

    assert truncated
    assert token_count == PREVIEW_TOKENS
    assert len(preview.split()) == PREVIEW_TOKENS


def test_full_article_with_subscription_footer_is_not_rejected() -> None:
    state = WebConversationState()
    authorization = state.authorize(
        "https://example.com/full",
        depth=0,
        story_id=92,
        source="story",
    )
    service, fetcher, extractor = _service(state=state)
    extractor.extract = lambda _html: ExtractedDocument(  # type: ignore[method-assign]
        markdown=("Substantive reporting sentence. " * 200)
        + "Subscribe to continue reading.",
        title="Full article",
        author=None,
        published=None,
    )

    payload = service.open(authorization.url)

    assert payload["status"] == "ok"
    assert fetcher.calls == 1


def test_open_and_read_page_chunks_without_another_fetch() -> None:
    state = WebConversationState()
    authorization = state.authorize(
        "https://example.com/long",
        depth=0,
        story_id=93,
        source="story",
    )
    service, fetcher, extractor = _service(state=state)
    extractor.extract = lambda _html: ExtractedDocument(  # type: ignore[method-assign]
        markdown="word " * 1_600,
        title="Long page",
        author=None,
        published=None,
    )

    opened = service.open(authorization.url)
    assert opened["status"] == "ok"
    assert opened["next_cursor"] is not None
    assert opened["remaining_chunks"] >= 1

    read = service.read(
        page_id=opened["page_id"],
        cursor=opened["next_cursor"],
    )
    assert read["status"] == "ok"
    assert read["chunk_token_count"] > 0
    assert fetcher.calls == 1


def test_find_returns_bounded_snippets_read_cursors_and_match_pagination() -> None:
    state = WebConversationState(inspection_call_limit=5)
    authorization = state.authorize(
        "https://example.com/find",
        depth=0,
        story_id=94,
        source="story",
    )
    service, fetcher, extractor = _service(state=state)
    extractor.extract = lambda _html: ExtractedDocument(  # type: ignore[method-assign]
        markdown="prefix Needle suffix. " * 12,
        title="Searchable page",
        author=None,
        published=None,
    )

    opened = service.open(authorization.url)
    first = service.find(page_id=opened["page_id"], term="needle")

    assert first["status"] == "ok"
    assert first["returned"] == 10
    assert first["next_cursor"] is not None
    assert all("Needle" in match["snippet"] for match in first["matches"])
    assert all(match["read_cursor"] for match in first["matches"])

    second = service.find(
        page_id=opened["page_id"],
        term="NEEDLE",
        cursor=first["next_cursor"],
    )
    assert second["status"] == "ok"
    assert second["returned"] == 2
    assert second["next_cursor"] is None
    assert fetcher.calls == 1


def test_find_cursor_is_bound_to_its_original_term() -> None:
    state = WebConversationState()
    authorization = state.authorize(
        "https://example.com/cursor",
        depth=0,
        story_id=95,
        source="story",
    )
    service, _fetcher, extractor = _service(state=state)
    extractor.extract = lambda _html: ExtractedDocument(  # type: ignore[method-assign]
        markdown="needle hay " * 20,
        title=None,
        author=None,
        published=None,
    )

    opened = service.open(authorization.url)
    found = service.find(page_id=opened["page_id"], term="needle")
    mismatch = service.find(
        page_id=opened["page_id"],
        term="hay",
        cursor=found["next_cursor"],
    )

    assert mismatch["status"] == "invalid_cursor"


def test_successful_html_authorizes_discovered_relative_links() -> None:
    state = WebConversationState()
    authorization = state.authorize(
        "https://example.com/root",
        depth=0,
        story_id=96,
        source="story",
    )
    service, _fetcher, _extractor = _service(state=state)
    linked_extractor = FakeExtractor()
    linked_extractor.extract = lambda _html: ExtractedDocument(  # type: ignore[method-assign]
        markdown="A useful page with a discovered child link. " * 10,
        title=None,
        author=None,
        published=None,
    )

    class LinkedFetcher:
        def fetch(self, url: str, *, authorization, policy) -> FetchedPage:
            return FetchedPage(
                final_url=url,
                content_type="text/html",
                body=b'<html><a href="/child#part">child</a></html>',
            )

    service = WebPageService(
        state=state,
        policy=service.policy,
        fetcher=LinkedFetcher(),  # type: ignore[arg-type]
        extractor=linked_extractor,  # type: ignore[arg-type]
    )
    opened = service.open(authorization.url)

    assert opened["status"] == "ok"
    child = state.authorization_for("https://example.com/child")
    assert child is not None
    assert child.depth == 1
    assert child.story_id == 96


def test_fourth_page_call_warns_and_fifth_refuses_until_reset() -> None:
    state = WebConversationState(inspection_call_limit=4)
    authorization = state.authorize(
        "https://example.com/budget",
        depth=0,
        story_id=97,
        source="story",
    )
    service, _fetcher, extractor = _service(state=state)
    extractor.extract = lambda _html: ExtractedDocument(  # type: ignore[method-assign]
        markdown="word needle " * 2_000,
        title=None,
        author=None,
        published=None,
    )

    opened = service.open(authorization.url)
    first_read = service.read(page_id=opened["page_id"], cursor=opened["next_cursor"])
    found = service.find(page_id=opened["page_id"], term="needle")
    fourth = service.read(page_id=opened["page_id"], cursor=first_read["next_cursor"])
    fifth = service.find(
        page_id=opened["page_id"],
        term="needle",
        cursor=found["next_cursor"],
    )

    assert "inspection_warning" in fourth
    assert fifth["status"] == "inspection_budget_exhausted"
    assert fifth["story_id"] == 97

    state.reset_inspection_budget()
    resumed = service.find(page_id=opened["page_id"], term="needle")
    assert resumed["status"] == "ok"
