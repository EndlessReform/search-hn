"""Application service implementing the first-preview webpage workflow."""

from __future__ import annotations

from dataclasses import dataclass

from search_agent.web.extractor import (
    ExtractedDocument,
    ExtractionError,
    LocalDefuddleExtractor,
)
from search_agent.web.fetcher import FetchFailure, WebPageFetcher
from search_agent.web.inspection import (
    find_in_cached_page,
    finish_inspection_call,
    inspection_budget_failure,
    read_cached_page,
)
from search_agent.web.policy import PublisherPolicy
from search_agent.web.security import WebAddressError, normalize_web_url
from search_agent.web.state import CachedPage, WebConversationState
from search_agent.web.text import (
    DEFAULT_CHUNK_TOKENS,
    remaining_chunk_count,
    slice_extraction_tokens,
)

MAX_EXTRACTED_CHARACTERS = 1024 * 1024
PREVIEW_TOKENS = DEFAULT_CHUNK_TOKENS
MIN_USEFUL_CHARACTERS = 80

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

        call = self.state.begin_inspection_call()
        if not call.allowed:
            try:
                authorization = self.state.authorization_for(raw_url)
            except WebAddressError:
                authorization = None
            return inspection_budget_failure(
                story_id=(authorization.story_id if authorization is not None else None)
            )
        return finish_inspection_call(self._open(raw_url), call)

    def _open(self, raw_url: str) -> dict[str, object]:
        """Execute one open after the public method reserves its call budget."""

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
            return _success(self.state, cached, cache_hit=True)
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

        if not fetched.content_type.startswith("text/plain"):
            self.state.authorize_page_links(
                source_text,
                base_url=fetched.final_url,
                parent=authorization,
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
        return _success(self.state, page, cache_hit=False)

    def read(self, *, page_id: str, cursor: str) -> dict[str, object]:
        """Read another chunk from an already-cached page without network I/O."""

        return read_cached_page(self.state, page_id=page_id, cursor=cursor)

    def find(
        self,
        *,
        page_id: str,
        term: str,
        cursor: str | None = None,
    ) -> dict[str, object]:
        """Search an already-cached page without network I/O."""

        return find_in_cached_page(
            self.state,
            page_id=page_id,
            term=term,
            cursor=cursor,
        )


def _preview(markdown: str) -> tuple[str, bool, int]:
    """Cut at a conservative, tokenizer-independent approximation of 768 tokens.

    ASCII word runs cost roughly one unit per four characters; punctuation and
    non-ASCII characters cost one each.  This intentionally errs toward a
    shorter preview for code, Markdown, and CJK text without coupling the tool
    to whichever model happens to be selected in the TUI.
    """

    chunk = slice_extraction_tokens(markdown)
    return chunk.text, chunk.next_offset is not None, chunk.token_count


def _success(
    state: WebConversationState,
    page: CachedPage,
    *,
    cache_hit: bool,
) -> dict[str, object]:
    """Serialize a cached page using the tool's success contract."""

    chunk = slice_extraction_tokens(page.markdown)
    next_cursor = (
        state.issue_read_cursor(page.page_id, chunk.next_offset)
        if chunk.next_offset is not None
        else None
    )
    return {
        "status": "ok",
        "page_id": page.page_id,
        "url": page.final_url,
        "title": page.title,
        "author": page.author,
        "published": page.published,
        "extractor": page.extractor,
        "cache_hit": cache_hit,
        "untrusted_page_content": chunk.text,
        "preview_token_count": chunk.token_count,
        "preview_truncated": chunk.next_offset is not None,
        "next_cursor": next_cursor,
        "remaining_chunks": remaining_chunk_count(
            page.markdown,
            start=chunk.next_offset,
        ),
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
