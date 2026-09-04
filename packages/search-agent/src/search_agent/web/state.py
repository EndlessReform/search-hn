"""Conversation-scoped URL authorization and extracted-page state."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from html.parser import HTMLParser
import re
from threading import RLock
from urllib.parse import urljoin

from search_agent.web.security import WebAddressError, normalize_web_url

MAX_LINK_DEPTH = 3
MAX_CACHED_PAGES = 16
DEFAULT_INSPECTION_CALL_LIMIT = 4
_ABSOLUTE_URL_START = re.compile(r"https?://", re.IGNORECASE)
_URL_END_CHARS = frozenset(" \t\r\n<>\"'")
_TRAILING_PROSE_PUNCTUATION = ".,;:!?)]}"


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


@dataclass(frozen=True)
class ReadCursor:
    """An opaque cursor resolved only within its originating conversation."""

    page_id: str
    offset: int


@dataclass(frozen=True)
class FindCursor:
    """A page-and-term-bound cursor for another batch of matches."""

    page_id: str
    term: str
    offset: int


@dataclass(frozen=True)
class InspectionCall:
    """The outcome of reserving one consecutive webpage-tool call."""

    number: int
    allowed: bool
    warn_after_response: bool


class _LinkParser(HTMLParser):
    """Collect ordinary anchor destinations from trusted container markup."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []
        self.text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for name, value in attrs:
            if name.lower() == "href" and value:
                self.links.append(value)
                return

    def handle_data(self, data: str) -> None:
        """Retain visible comment text for plain absolute-URL discovery."""

        self.text_parts.append(data)


def _embedded_absolute_urls(text: str) -> list[str]:
    """Return every HTTP(S) substring, including URLs nested inside URLs."""

    candidates: list[str] = []
    for match in _ABSOLUTE_URL_START.finditer(text):
        end = match.start()
        while end < len(text) and text[end] not in _URL_END_CHARS:
            end += 1
        candidate = text[match.start() : end].rstrip(_TRAILING_PROSE_PUNCTUATION)
        try:
            candidates.append(normalize_web_url(candidate))
        except WebAddressError:
            continue
    return candidates


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


def links_from_comment_html(markup: str) -> list[str]:
    """Extract explicit, visible, and nested HTTP(S) destinations from a comment.

    HN comments sometimes link through an archive URL whose path contains the
    original destination.  Both strings were exposed by the comment, so both
    become approval-gated candidates.  This broader parsing is intentionally
    limited to user-authored comments; fetched pages still authorize only
    ordinary anchors via :func:`links_from_html`.
    """

    parser = _LinkParser()
    parser.feed(markup)
    discovered: list[str] = []
    for candidate_text in [*parser.links, *parser.text_parts]:
        discovered.extend(_embedded_absolute_urls(candidate_text))
    return list(dict.fromkeys(discovered))


class WebConversationState:
    """Mutable webpage state whose lifetime matches one TUI conversation.

    All compound operations take one re-entrant lock.  SDK sync tools normally
    run in worker threads and may be scheduled concurrently, so callers should
    not need to add their own locking around authorization/cache operations.
    """

    def __init__(
        self,
        *,
        max_cached_pages: int = MAX_CACHED_PAGES,
        inspection_call_limit: int = DEFAULT_INSPECTION_CALL_LIMIT,
    ) -> None:
        assert max_cached_pages > 0, "max_cached_pages must be positive"
        assert 3 <= inspection_call_limit <= 5, (
            "inspection_call_limit must be between 3 and 5"
        )
        self._lock = RLock()
        self._authorized: dict[str, AuthorizedUrl] = {}
        self._pages_by_url: OrderedDict[str, CachedPage] = OrderedDict()
        self._pages_by_id: dict[str, CachedPage] = {}
        self._next_page_number = 1
        self._read_cursors: dict[str, ReadCursor] = {}
        self._find_cursors: dict[str, FindCursor] = {}
        self._next_cursor_number = 1
        self._max_cached_pages = max_cached_pages
        self._inspection_call_limit = inspection_call_limit
        self._consecutive_inspection_calls = 0

    def clear(self) -> None:
        """Discard every authorization and extraction from the conversation."""

        with self._lock:
            self._authorized.clear()
            self._pages_by_url.clear()
            self._pages_by_id.clear()
            self._next_page_number = 1
            self._read_cursors.clear()
            self._find_cursors.clear()
            self._next_cursor_number = 1
            self._consecutive_inspection_calls = 0

    def begin_inspection_call(self) -> InspectionCall:
        """Atomically reserve one call from the consecutive inspection budget."""

        with self._lock:
            self._consecutive_inspection_calls += 1
            number = self._consecutive_inspection_calls
            return InspectionCall(
                number=number,
                allowed=number <= self._inspection_call_limit,
                warn_after_response=number == self._inspection_call_limit,
            )

    def reset_inspection_budget(self) -> None:
        """Allow a fresh run after user input or an HN data-tool call."""

        with self._lock:
            self._consecutive_inspection_calls = 0

    def authorize(
        self,
        raw_url: str,
        *,
        depth: int,
        story_id: int | None,
        source: str,
    ) -> AuthorizedUrl:
        """Record an authorization, preferring shallow and submission provenance.

        A URL may be encountered through more than one path.  Lower link depth
        wins normally; at equal depth, a top-level story replaces comment
        provenance so the same submitted URL never asks for comment approval
        merely because the comment tool happened to expose it first.
        """

        assert 0 <= depth <= MAX_LINK_DEPTH, f"depth must be 0-{MAX_LINK_DEPTH}"
        normalized = normalize_web_url(raw_url)
        candidate = AuthorizedUrl(normalized, depth, story_id, source)
        with self._lock:
            existing = self._authorized.get(normalized)
            story_supersedes_comment = (
                existing is not None
                and candidate.depth == existing.depth
                and candidate.source == "story"
                and existing.source != "story"
            )
            if (
                existing is None
                or candidate.depth < existing.depth
                or story_supersedes_comment
            ):
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
                for url in links_from_comment_html(markup):
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

    def cached_for_id(self, page_id: str) -> CachedPage | None:
        """Return a cached page by model-visible ID and refresh its LRU age."""

        with self._lock:
            page = self._pages_by_id.get(page_id)
            if page is not None:
                self._pages_by_url.move_to_end(page.requested_url)
            return page

    def issue_read_cursor(self, page_id: str, offset: int) -> str:
        """Create an opaque cursor bound to one cached page and offset."""

        with self._lock:
            page = self._pages_by_id.get(page_id)
            assert page is not None, f"cannot issue cursor for unknown page {page_id!r}"
            assert 0 <= offset < len(page.markdown), "read cursor offset outside page"
            token = f"read:{self._next_cursor_number}"
            self._next_cursor_number += 1
            self._read_cursors[token] = ReadCursor(page_id, offset)
            return token

    def resolve_read_cursor(self, page_id: str, token: str) -> ReadCursor | None:
        """Resolve a read cursor only when it belongs to the requested page."""

        with self._lock:
            cursor = self._read_cursors.get(token)
            if cursor is None or cursor.page_id != page_id:
                return None
            if page_id not in self._pages_by_id:
                return None
            return cursor

    def issue_find_cursor(self, page_id: str, term: str, offset: int) -> str:
        """Create an opaque match-pagination cursor bound to page and term."""

        with self._lock:
            assert page_id in self._pages_by_id, (
                f"cannot issue cursor for unknown page {page_id!r}"
            )
            token = f"find:{self._next_cursor_number}"
            self._next_cursor_number += 1
            self._find_cursors[token] = FindCursor(page_id, term, offset)
            return token

    def resolve_find_cursor(
        self,
        page_id: str,
        term: str,
        token: str,
    ) -> FindCursor | None:
        """Resolve a match cursor only for its exact page and search term."""

        with self._lock:
            cursor = self._find_cursors.get(token)
            if cursor is None or cursor.page_id != page_id or cursor.term != term:
                return None
            if page_id not in self._pages_by_id:
                return None
            return cursor

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
                self._discard_page_cursors(evicted.page_id)
            return page

    def _discard_page_cursors(self, page_id: str) -> None:
        """Remove cursor records made unusable by page eviction."""

        self._read_cursors = {
            token: cursor
            for token, cursor in self._read_cursors.items()
            if cursor.page_id != page_id
        }
        self._find_cursors = {
            token: cursor
            for token, cursor in self._find_cursors.items()
            if cursor.page_id != page_id
        }

    def authorize_page_links(
        self,
        markup: str,
        *,
        base_url: str,
        parent: AuthorizedUrl,
    ) -> int:
        """Authorize links discovered in a fetched page through depth three."""

        child_depth = parent.depth + 1
        if child_depth > MAX_LINK_DEPTH:
            return 0

        registered = 0
        for link in links_from_html(markup, base_url=base_url):
            self.authorize(
                link,
                depth=child_depth,
                story_id=parent.story_id,
                source=f"page:{parent.depth}",
            )
            registered += 1
        return registered
