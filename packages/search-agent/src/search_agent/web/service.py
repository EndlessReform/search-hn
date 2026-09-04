"""Application service implementing the first-preview webpage workflow."""

from __future__ import annotations

import re
from math import ceil
from dataclasses import dataclass

from search_agent.web.extractor import (
    ExtractedDocument,
    ExtractionError,
    LocalDefuddleExtractor,
)
from search_agent.web.fetcher import FetchFailure, WebPageFetcher
from search_agent.web.policy import PublisherPolicy
from search_agent.web.security import WebAddressError, normalize_web_url
from search_agent.web.state import CachedPage, WebConversationState

MAX_EXTRACTED_CHARACTERS = 1024 * 1024
PREVIEW_TOKENS = 768
MIN_USEFUL_CHARACTERS = 80
_TOKEN_PIECE = re.compile(r"\s+|[A-Za-z0-9_]+|[^\s]", re.UNICODE)

_CHALLENGE_MARKERS = (
    "cf-mitigated",
    "cf-chl-",
    "<title>just a moment...</title>",
    'id="challenge-running"',
    "enable javascript and cookies to continue",
    "unusual traffic from your computer network",
)
_PAYWALL_MARKERS = (
    "subscribe to continue reading",
    "subscribe to read the full article",
    "this article is for subscribers only",
)
_SUBSCRIPTION_PREVIEW_MARKER = "preview of subscription content"
_MAX_GENERIC_PAYWALL_PREVIEW_CHARACTERS = 4_000


@dataclass(frozen=True)
class WebPageService:
    """Authorize, fetch, extract, cache, and preview one webpage."""

    state: WebConversationState
    policy: PublisherPolicy
    fetcher: WebPageFetcher
    extractor: LocalDefuddleExtractor | None
    extractor_error: str | None = None

    def open(self, raw_url: str) -> dict[str, object]:
        """Return a stable JSON-ready result for an attempted page open."""

        try:
            normalized = normalize_web_url(raw_url)
        except WebAddressError as exc:
            return _failure(exc.status, exc.reason)

        authorization = self.state.authorization_for(normalized)
        if authorization is None:
            return _failure(
                "not_authorized",
                "URL was not exposed by an HN tool in this conversation",
            )

        policy_decision = self.policy.evaluate(normalized)
        if policy_decision is not None:
            return _failure(
                policy_decision.status,
                policy_decision.reason,
                story_id=authorization.story_id,
            )

        cached = self.state.cached_for_url(normalized)
        if cached is not None:
            return _success(cached, cache_hit=True)
        if self.extractor is None:
            return _failure(
                "extractor_unavailable",
                self.extractor_error or "local Defuddle runtime is unavailable",
                story_id=authorization.story_id,
            )

        try:
            fetched = self.fetcher.fetch(
                normalized,
                authorization=self.state,
                policy=self.policy,
            )
        except FetchFailure as exc:
            return _failure(exc.status, exc.reason, story_id=authorization.story_id)

        source_text = fetched.decoded_text()
        lower_source = source_text.lower()
        if any(marker in lower_source for marker in _CHALLENGE_MARKERS):
            return _failure(
                "access_challenge",
                "publisher returned an access-challenge page",
                story_id=authorization.story_id,
            )
        if _SUBSCRIPTION_PREVIEW_MARKER in lower_source:
            return _failure(
                "paywall_detected",
                "publisher returned a subscription preview",
                story_id=authorization.story_id,
            )

        if fetched.content_type.startswith("text/plain"):
            extracted = ExtractedDocument(source_text.strip(), None, None, None)
            extractor_name = "plain-text"
        else:
            try:
                extracted = self.extractor.extract(source_text)
            except ExtractionError as exc:
                return _failure(
                    "extraction_empty",
                    str(exc),
                    story_id=authorization.story_id,
                )
            extractor_name = self.extractor.name

        markdown = extracted.markdown[:MAX_EXTRACTED_CHARACTERS].strip()
        if len(markdown) < MIN_USEFUL_CHARACTERS:
            return _failure(
                "extraction_empty",
                "extracted content was too short to be useful",
                story_id=authorization.story_id,
            )
        lower_markdown = markdown.lower()
        generic_paywall_preview = len(
            markdown
        ) <= _MAX_GENERIC_PAYWALL_PREVIEW_CHARACTERS and any(
            marker in lower_markdown for marker in _PAYWALL_MARKERS
        )
        if _SUBSCRIPTION_PREVIEW_MARKER in lower_markdown or generic_paywall_preview:
            return _failure(
                "paywall_detected",
                "extraction contains only a subscription preview",
                story_id=authorization.story_id,
            )

        page = self.state.cache_page(
            requested_url=normalized,
            final_url=fetched.final_url,
            title=extracted.title,
            author=extracted.author,
            published=extracted.published,
            markdown=markdown,
            extractor=extractor_name,
            authorization=authorization,
        )
        return _success(page, cache_hit=False)


def _preview(markdown: str) -> tuple[str, bool, int]:
    """Cut at a conservative, tokenizer-independent approximation of 768 tokens.

    ASCII word runs cost roughly one unit per four characters; punctuation and
    non-ASCII characters cost one each.  This intentionally errs toward a
    shorter preview for code, Markdown, and CJK text without coupling the tool
    to whichever model happens to be selected in the TUI.
    """

    used = 0
    for piece in _TOKEN_PIECE.finditer(markdown):
        value = piece.group()
        if value.isspace():
            continue
        ascii_word = value.isascii() and value.replace("_", "a").isalnum()
        units = ceil(len(value) / 4) if ascii_word else 1
        if used + units > PREVIEW_TOKENS:
            if ascii_word:
                allowed_characters = (PREVIEW_TOKENS - used) * 4
                if allowed_characters > 0:
                    end = piece.start() + allowed_characters
                    return markdown[:end].rstrip(), True, PREVIEW_TOKENS
            return markdown[: piece.start()].rstrip(), True, used
        used += units
    return markdown, False, used


def _success(page: CachedPage, *, cache_hit: bool) -> dict[str, object]:
    """Serialize a cached page using the tool's success contract."""

    preview, truncated, token_count = _preview(page.markdown)
    return {
        "status": "ok",
        "page_id": page.page_id,
        "url": page.final_url,
        "title": page.title,
        "author": page.author,
        "published": page.published,
        "extractor": page.extractor,
        "cache_hit": cache_hit,
        "untrusted_page_content": preview,
        "preview_token_count": token_count,
        "preview_truncated": truncated,
        "follow_up_tools_available": False,
    }


def _failure(
    status: str,
    reason: str,
    *,
    story_id: int | None = None,
) -> dict[str, object]:
    """Build a failure that consistently directs the agent back to comments."""

    action = "Use fetch_top_comments and do not attempt access workarounds."
    if story_id is not None:
        action = (
            f"Use fetch_top_comments for story {story_id} and do not attempt "
            "access workarounds."
        )
    return {
        "status": status,
        "reason": reason,
        "recommended_action": action,
        "story_id": story_id,
    }
