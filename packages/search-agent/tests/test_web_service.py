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
