"""Conversation-scoped URL authorization and extracted-page state."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from html.parser import HTMLParser
from threading import RLock
from urllib.parse import urljoin

from search_agent.web.security import WebAddressError, normalize_web_url

MAX_LINK_DEPTH = 3
MAX_CACHED_PAGES = 16


@dataclass(frozen=True)
class AuthorizedUrl:
    """Why a normalized URL may be requested by the model."""

    url: str
    depth: int
    story_id: int | None
    source: str


@dataclass(frozen=True)
class CachedPage:
    """One successful extraction retained for the current conversation."""

    page_id: str
    requested_url: str
    final_url: str
    title: str | None
    author: str | None
    published: str | None
    markdown: str
    extractor: str
    depth: int
    story_id: int | None


class _LinkParser(HTMLParser):
    """Collect ordinary anchor destinations from trusted container markup."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for name, value in attrs:
            if name.lower() == "href" and value:
                self.links.append(value)
                return


def links_from_html(markup: str, *, base_url: str | None = None) -> list[str]:
    """Extract and normalize HTTP(S) links, ignoring malformed destinations."""

    parser = _LinkParser()
    parser.feed(markup)
    normalized: list[str] = []
    for raw_link in parser.links:
        candidate = urljoin(base_url, raw_link) if base_url else raw_link
        try:
            normalized.append(normalize_web_url(candidate))
        except WebAddressError:
            continue
    return list(dict.fromkeys(normalized))


class WebConversationState:
    """Mutable webpage state whose lifetime matches one TUI conversation.

    All compound operations take one re-entrant lock.  SDK sync tools normally
    run in worker threads and may be scheduled concurrently, so callers should
    not need to add their own locking around authorization/cache operations.
    """

    def __init__(self, *, max_cached_pages: int = MAX_CACHED_PAGES) -> None:
        assert max_cached_pages > 0, "max_cached_pages must be positive"
        self._lock = RLock()
        self._authorized: dict[str, AuthorizedUrl] = {}
        self._pages_by_url: OrderedDict[str, CachedPage] = OrderedDict()
        self._pages_by_id: dict[str, CachedPage] = {}
        self._next_page_number = 1
        self._max_cached_pages = max_cached_pages

    def clear(self) -> None:
        """Discard every authorization and extraction from the conversation."""

        with self._lock:
            self._authorized.clear()
            self._pages_by_url.clear()
            self._pages_by_id.clear()
            self._next_page_number = 1

    def authorize(
        self,
        raw_url: str,
        *,
        depth: int,
        story_id: int | None,
        source: str,
    ) -> AuthorizedUrl:
        """Record a model-independent authorization, keeping minimum depth."""

        assert 0 <= depth <= MAX_LINK_DEPTH, f"depth must be 0-{MAX_LINK_DEPTH}"
        normalized = normalize_web_url(raw_url)
        candidate = AuthorizedUrl(normalized, depth, story_id, source)
        with self._lock:
            existing = self._authorized.get(normalized)
            if existing is None or candidate.depth < existing.depth:
                self._authorized[normalized] = candidate
                return candidate
            return existing

    def authorization_for(self, raw_url: str) -> AuthorizedUrl | None:
        """Return authorization for an exact normalized URL."""

        normalized = normalize_web_url(raw_url)
        with self._lock:
            return self._authorized.get(normalized)

    def is_authorized(self, raw_url: str) -> bool:
        """Return whether the exact normalized URL is in the ledger."""

        try:
            return self.authorization_for(raw_url) is not None
        except WebAddressError:
            return False

    def register_story_payload(self, payload: dict[str, object]) -> int:
        """Authorize source URLs emitted by a story-search tool payload."""

        raw_batches = payload.get("queries")
        if isinstance(raw_batches, list):
            batches = raw_batches
        else:
            batches = [payload]

        registered = 0
        for batch in batches:
            if not isinstance(batch, dict):
                continue
            results = batch.get("results")
            if not isinstance(results, list):
                continue
            for result in results:
                if not isinstance(result, dict):
                    continue
                url = result.get("url")
                story_id = result.get("id")
                if not isinstance(url, str) or not isinstance(story_id, int):
                    continue
                try:
                    self.authorize(url, depth=0, story_id=story_id, source="story")
                except WebAddressError:
                    continue
                registered += 1
        return registered

    def register_comment_payload(self, payload: dict[str, object]) -> int:
        """Authorize absolute HTTP(S) links found in returned top-level comments."""

        raw_stories = payload.get("stories")
        stories = raw_stories if isinstance(raw_stories, list) else [payload]
        registered = 0
        for story in stories:
            if not isinstance(story, dict):
                continue
            story_id = story.get("story_id")
            comments = story.get("comments")
            if not isinstance(story_id, int) or not isinstance(comments, list):
                continue
            for comment in comments:
                if not isinstance(comment, dict):
                    continue
                markup = comment.get("comment")
                if not isinstance(markup, str):
                    continue
                for url in links_from_html(markup):
                    self.authorize(
                        url,
                        depth=0,
                        story_id=story_id,
                        source="top-level-comment",
                    )
                    registered += 1
        return registered

    def cached_for_url(self, raw_url: str) -> CachedPage | None:
        """Return a cached page and mark it most recently used."""

        normalized = normalize_web_url(raw_url)
        with self._lock:
            page = self._pages_by_url.get(normalized)
            if page is not None:
                self._pages_by_url.move_to_end(normalized)
            return page

    def cache_page(
        self,
        *,
        requested_url: str,
        final_url: str,
        title: str | None,
        author: str | None,
        published: str | None,
        markdown: str,
        extractor: str,
        authorization: AuthorizedUrl,
    ) -> CachedPage:
        """Insert an extraction and evict the least-recently-used page."""

        with self._lock:
            page = CachedPage(
                page_id=f"page:{self._next_page_number}",
                requested_url=requested_url,
                final_url=final_url,
                title=title,
                author=author,
                published=published,
                markdown=markdown,
                extractor=extractor,
                depth=authorization.depth,
                story_id=authorization.story_id,
            )
            self._next_page_number += 1
            self._pages_by_url[requested_url] = page
            self._pages_by_url.move_to_end(requested_url)
            self._pages_by_id[page.page_id] = page
            while len(self._pages_by_url) > self._max_cached_pages:
                _, evicted = self._pages_by_url.popitem(last=False)
                self._pages_by_id.pop(evicted.page_id, None)
            return page
