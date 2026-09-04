"""HTTP-boundary tests for redirects and structured fetch failures."""

from __future__ import annotations

from unittest.mock import patch

import httpx
import pytest

from search_agent.web.fetcher import FetchFailure, WebPageFetcher
from search_agent.web.policy import PublisherPolicy
from search_agent.web.state import WebConversationState


def _empty_policy() -> PublisherPolicy:
    return PublisherPolicy(frozenset(), frozenset())


def test_cross_origin_redirect_is_a_structured_failure() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            302,
            headers={"location": "https://other.example/article"},
            request=request,
        )

    state = WebConversationState()
    state.authorize(
        "https://source.example/article",
        depth=0,
        story_id=1,
        source="story",
    )
    fetcher = WebPageFetcher(transport=httpx.MockTransport(handler))

    with (
        patch("search_agent.web.fetcher.validate_public_destination"),
        pytest.raises(FetchFailure, match="cross-origin redirect"),
    ):
        fetcher.fetch(
            "https://source.example/article",
            authorization=state,
            policy=_empty_policy(),
        )


def test_pdf_magic_is_rejected_even_with_html_content_type() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            content=b"%PDF-1.7 pretend",
            request=request,
        )

    state = WebConversationState()
    state.authorize(
        "https://source.example/download",
        depth=0,
        story_id=1,
        source="story",
    )
    fetcher = WebPageFetcher(transport=httpx.MockTransport(handler))

    with (
        patch("search_agent.web.fetcher.validate_public_destination"),
        pytest.raises(FetchFailure, match="PDF resources"),
    ):
        fetcher.fetch(
            "https://source.example/download",
            authorization=state,
            policy=_empty_policy(),
        )


def test_cloudflare_challenge_header_is_rejected() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/html", "cf-mitigated": "challenge"},
            content=b"<html><p>challenge</p></html>",
            request=request,
        )

    state = WebConversationState()
    state.authorize(
        "https://source.example/article",
        depth=0,
        story_id=1,
        source="story",
    )
    fetcher = WebPageFetcher(transport=httpx.MockTransport(handler))

    with (
        patch("search_agent.web.fetcher.validate_public_destination"),
        pytest.raises(FetchFailure, match="Cloudflare challenge"),
    ):
        fetcher.fetch(
            "https://source.example/article",
            authorization=state,
            policy=_empty_policy(),
        )


def test_authorized_same_origin_redirect_is_followed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/old":
            return httpx.Response(301, headers={"location": "/new"}, request=request)
        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            content=b"<html><p>A sufficiently useful response body for the fetcher.</p></html>",
            request=request,
        )

    state = WebConversationState()
    state.authorize(
        "https://source.example/old",
        depth=0,
        story_id=1,
        source="story",
    )
    fetcher = WebPageFetcher(transport=httpx.MockTransport(handler))

    with patch("search_agent.web.fetcher.validate_public_destination"):
        result = fetcher.fetch(
            "https://source.example/old",
            authorization=state,
            policy=_empty_policy(),
        )

    assert result.final_url == "https://source.example/new"


def test_policy_is_rechecked_before_authorized_cross_origin_redirect() -> None:
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        return httpx.Response(
            302,
            headers={"location": "https://paywall.example/article"},
            request=request,
        )

    state = WebConversationState()
    state.authorize(
        "https://source.example/article",
        depth=0,
        story_id=1,
        source="story",
    )
    state.authorize(
        "https://paywall.example/article",
        depth=0,
        story_id=2,
        source="story",
    )
    policy = PublisherPolicy(frozenset({"paywall.example"}), frozenset())
    fetcher = WebPageFetcher(transport=httpx.MockTransport(handler))

    with (
        patch("search_agent.web.fetcher.validate_public_destination"),
        pytest.raises(FetchFailure, match="access/paywall blacklist"),
    ):
        fetcher.fetch(
            "https://source.example/article",
            authorization=state,
            policy=policy,
        )

    assert requests == ["https://source.example/article"]
