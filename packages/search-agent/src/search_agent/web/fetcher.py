"""Bounded HTTP fetching with redirect and content-type controls."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urljoin

import httpx

from search_agent.web.security import (
    WebAddressError,
    looks_like_pdf_url,
    normalize_web_url,
    same_origin,
    validate_public_destination,
)
from search_agent.web.policy import PublisherPolicy

MAX_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_REDIRECTS = 3
USER_AGENT = "search-hn-web-reader/0.1 (+conversation-scoped research tool)"


class AuthorizationLookup(Protocol):
    """The fetcher only needs exact-ledger membership for redirects."""

    def is_authorized(self, raw_url: str) -> bool: ...


@dataclass
class FetchFailure(RuntimeError):
    """An expected network/content failure with a stable tool status."""

    status: str
    reason: str

    def __str__(self) -> str:
        return self.reason


@dataclass(frozen=True)
class FetchedPage:
    """A successful bounded HTTP response."""

    final_url: str
    content_type: str
    body: bytes

    def decoded_text(self) -> str:
        """Decode HTML/text using the declared charset, then UTF-8 fallback."""

        charset = "utf-8"
        for parameter in self.content_type.split(";")[1:]:
            name, separator, value = parameter.strip().partition("=")
            if separator and name.lower() == "charset":
                charset = value.strip().strip("\"'") or "utf-8"
                break
        try:
            return self.body.decode(charset, errors="replace")
        except LookupError:
            return self.body.decode("utf-8", errors="replace")


class WebPageFetcher:
    """Fetch one authorized URL without automatic redirects or unbounded reads."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 12.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._timeout_seconds = timeout_seconds
        self._transport = transport

    def fetch(
        self,
        url: str,
        *,
        authorization: AuthorizationLookup,
        policy: PublisherPolicy,
    ) -> FetchedPage:
        """Fetch a page while checking every redirect before network access."""

        current = normalize_web_url(url)
        with httpx.Client(
            follow_redirects=False,
            timeout=httpx.Timeout(self._timeout_seconds),
            transport=self._transport,
            trust_env=False,
            headers={"User-Agent": USER_AGENT, "Accept": "text/html, text/plain;q=0.9"},
        ) as client:
            for redirect_number in range(MAX_REDIRECTS + 1):
                if looks_like_pdf_url(current):
                    raise FetchFailure(
                        "pdf_rejected", "PDF resources are not supported"
                    )
                policy_decision = policy.evaluate(current)
                if policy_decision is not None:
                    raise FetchFailure(policy_decision.status, policy_decision.reason)
                try:
                    validate_public_destination(current)
                except WebAddressError as exc:
                    raise FetchFailure(exc.status, exc.reason) from exc

                try:
                    with client.stream("GET", current) as response:
                        if (
                            response.headers.get("cf-mitigated", "").lower()
                            == "challenge"
                        ):
                            raise FetchFailure(
                                "access_challenge",
                                "publisher returned a Cloudflare challenge",
                            )
                        if response.is_redirect:
                            location = response.headers.get("location")
                            if not location:
                                raise FetchFailure(
                                    "http_error", "redirect omitted Location"
                                )
                            target = normalize_web_url(urljoin(current, location))
                            if redirect_number >= MAX_REDIRECTS:
                                raise FetchFailure(
                                    "redirect_rejected", "too many redirects"
                                )
                            if not same_origin(
                                current, target
                            ) and not authorization.is_authorized(target):
                                raise FetchFailure(
                                    "redirect_rejected",
                                    "cross-origin redirect target was not authorized",
                                )
                            current = target
                            continue

                        content_length = response.headers.get("content-length")
                        if content_length:
                            try:
                                declared_length = int(content_length)
                            except ValueError:
                                declared_length = 0
                            if declared_length > MAX_RESPONSE_BYTES:
                                raise FetchFailure(
                                    "response_too_large", "response exceeds 2 MiB"
                                )

                        body = bytearray()
                        for chunk in response.iter_bytes():
                            body.extend(chunk)
                            if len(body) > MAX_RESPONSE_BYTES:
                                raise FetchFailure(
                                    "response_too_large", "response exceeds 2 MiB"
                                )

                        content_type = response.headers.get("content-type", "").lower()
                        body_bytes = bytes(body)
                        if response.status_code in {401, 403, 429}:
                            raise FetchFailure(
                                "access_challenge",
                                f"publisher returned HTTP {response.status_code}",
                            )
                        if response.status_code >= 400:
                            raise FetchFailure(
                                "http_error",
                                f"publisher returned HTTP {response.status_code}",
                            )
                        if (
                            "application/pdf" in content_type
                            or body_bytes.lstrip().startswith(b"%PDF-")
                        ):
                            raise FetchFailure(
                                "pdf_rejected", "PDF resources are not supported"
                            )
                        media_type = content_type.partition(";")[0].strip()
                        if media_type not in {
                            "text/html",
                            "application/xhtml+xml",
                            "text/plain",
                        }:
                            raise FetchFailure(
                                "unsupported_content_type",
                                f"unsupported Content-Type: {media_type or 'missing'}",
                            )
                        return FetchedPage(current, content_type, body_bytes)
                except httpx.TimeoutException as exc:
                    raise FetchFailure(
                        "timeout", "publisher request timed out"
                    ) from exc
                except httpx.HTTPError as exc:
                    raise FetchFailure(
                        "http_error", f"publisher request failed: {exc}"
                    ) from exc

        raise AssertionError("redirect loop exited without a result")
