"""Unit tests for URL safety and publisher-policy decisions."""

from __future__ import annotations

import socket

import pytest

from search_agent.web.policy import PublisherPolicy
from search_agent.web.security import (
    WebAddressError,
    normalize_web_url,
    same_origin,
    validate_public_destination,
)


def test_normalize_web_url_removes_fragment_and_default_port() -> None:
    assert (
        normalize_web_url(" HTTPS://WWW.Example.com:443/a?q=2&q=1#heading ")
        == "https://www.example.com/a?q=2&q=1"
    )


@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "https://alice:secret@example.com/",
        "https:///missing-host",
    ],
)
def test_normalize_web_url_rejects_unsafe_shapes(url: str) -> None:
    with pytest.raises(WebAddressError):
        normalize_web_url(url)


def test_same_origin_uses_effective_default_ports() -> None:
    assert same_origin("https://example.com/a", "https://example.com:443/b")
    assert not same_origin("https://example.com/a", "http://example.com/a")


def test_validate_public_destination_rejects_any_private_answer() -> None:
    def mixed_resolver(_host: str, _port: int, *, type: int):
        assert type == socket.SOCK_STREAM
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443)),
        ]

    with pytest.raises(WebAddressError, match="non-public"):
        validate_public_destination(
            "https://example.com/",
            resolver=mixed_resolver,
        )


def test_validate_public_destination_accepts_public_answers() -> None:
    def public_resolver(_host: str, _port: int, *, type: int):
        assert type == socket.SOCK_STREAM
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]

    assert validate_public_destination(
        "https://example.com/",
        resolver=public_resolver,
    ) == ("93.184.216.34",)


def test_reviewed_policy_files_load_without_overlap() -> None:
    policy = PublisherPolicy.load()

    assert (
        policy.evaluate("https://subdomain.nytimes.com/article").status
        == "blocked_domain"
    )
    assert policy.evaluate("https://www.zdnet.com/article").status == "news_skipped"
    assert policy.evaluate("https://arstechnica.com/article") is None
