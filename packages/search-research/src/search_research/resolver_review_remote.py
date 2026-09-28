"""On-demand public source records, kept separate from frozen model inputs.

Live HN and Open Library responses are cached in memory for this review process.
Edition pages are explicit and complete per page; the work JSON is never reduced.
Network failures remain visible and retryable rather than implying absent data.
"""

import json
from functools import lru_cache
from html import escape
from html.parser import HTMLParser

import httpx
from fastapi import Query
from fastapi.responses import HTMLResponse


@lru_cache(maxsize=2048)
def fetch(url):
    response = httpx.get(url, follow_redirects=True, timeout=20)
    response.raise_for_status()
    return response.json()


class CommentText(HTMLParser):
    """Render HN's HTML as escaped text while retaining paragraph boundaries."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag in {"p", "br", "pre"}:
            self.parts.append("\n\n")

    def handle_data(self, data):
        self.parts.append(data)


def raw(value, label):
    return f"<details><summary>{escape(label)}</summary><pre>{escape(json.dumps(value, indent=2, ensure_ascii=False))}</pre></details>"


def hn_item(item):
    parser = CommentText()
    parser.feed(item.get("text", item.get("title", "[deleted or unavailable text]")))
    return (
        f'<p><a href="https://news.ycombinator.com/item?id={item["id"]}" target="_blank">'
        f"HN {item['id']}</a> · {escape(item.get('by', 'unknown'))}</p>"
        f'<div class="comment-text">{escape("".join(parser.parts))}</div>'
    )


def install_remote(app):
    """Install bounded public lookups; no writes or model calls are performed."""

    @app.get("/source/hn/{ident}", response_class=HTMLResponse)
    def source(ident: int):
        try:
            item = fetch(f"https://hacker-news.firebaseio.com/v0/item/{ident}.json")
            if item is None:
                return "<p>HN item unavailable.</p>"
            result = ""
            if "parent" in item:
                parent = fetch(
                    f"https://hacker-news.firebaseio.com/v0/item/{item['parent']}.json"
                )
                result += "<h3>Parent context · not supplied to the selectors</h3>"
                if parent is not None:
                    result += hn_item(parent)
                    if "parent" in parent:
                        result += (
                            f'<button hx-get="/source/hn/{parent["id"]}" hx-target="next .ancestor">'
                            'Show context above this parent</button><div class="ancestor"></div>'
                        )
                else:
                    result += "<p>Parent unavailable.</p>"
            return result or "<p>No parent context available.</p>"
        except httpx.HTTPError as exc:
            return f'<p>HN lookup failed: {escape(str(exc))}</p><button hx-get="/source/hn/{ident}" hx-target="closest .live-source">Retry</button>'

    @app.get("/source/work/{key}", response_class=HTMLResponse)
    def work(key: str, offset: int = Query(0, ge=0)):
        import re

        if not re.fullmatch(r"OL[0-9]+W", key):
            return HTMLResponse("Invalid work ID", status_code=400)
        try:
            record = fetch(f"https://openlibrary.org/works/{key}.json")
            editions = fetch(
                f"https://openlibrary.org/works/{key}/editions.json?limit=10&offset={offset}"
            )
            result = (
                "<p><b>Live Open Library metadata</b> · may differ from the frozen catalog. "
                "The selectors saw only work ID, title and author names.</p>"
            )
            result += raw(record, "Full work record JSON (all fields)")
            result += f"<h4>Linked editions · {editions['size']} total · offset {offset} · up to 10 per page</h4>"
            for edition in editions["entries"]:
                result += f'<article class="edition"><b>{escape(edition["title"])}</b>'
                for field in (
                    "subtitle",
                    "publish_date",
                    "publishers",
                    "publish_places",
                    "languages",
                    "physical_format",
                    "number_of_pages",
                    "pagination",
                    "series",
                    "isbn_13",
                    "isbn_10",
                    "subjects",
                    "notes",
                    "source_records",
                ):
                    if field in edition:
                        result += f"<div><b>{field}:</b> {escape(json.dumps(edition[field], ensure_ascii=False))}</div>"
                result += (
                    raw(edition, f"Full edition JSON · {edition['key']}") + "</article>"
                )
            for page, label in [
                (offset - 10, "Previous editions"),
                (offset + 10, "Next editions"),
            ]:
                if 0 <= page < editions["size"]:
                    result += f'<button hx-get="/source/work/{key}?offset={page}" hx-target="closest .catalog-record">{label}</button>'
            return result
        except httpx.HTTPError as exc:
            return f'<p>Catalog lookup failed: {escape(str(exc))}</p><button hx-get="/source/work/{key}" hx-target="closest .catalog-record">Retry</button>'
