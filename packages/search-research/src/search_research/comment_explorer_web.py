"""Small HTMX explorer: plain phrase, cosine cutoff, and full comment pages."""

import json
import sqlite3
from contextlib import asynccontextmanager, closing
from html import escape
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from search_research.comment_annotations import AnnotationStore
from search_research.comment_classifier import install_classifier
from search_research.comment_entities import install_entities
from search_research.comment_entity_annotation_web import install_entity_annotations
from search_research.comment_entity_worker import EntityWorker
from search_research.comment_experiments import install_experiments
from search_research.comment_explorer import CommentExplorer
from search_research.comment_rollouts import install_rollouts


def results_html(data):
    """Escape frozen text rather than trusting historical HN HTML."""
    parts = [
        (
            f"<p>{data['total']:,} comments · {escape(data['engine'])} · "
            f"query embedding {data['embedding_seconds']:.3f}s · "
            f"ranking {data['ranking_seconds']:.3f}s · "
            f"this request {data['request_seconds']:.3f}s · cached: {data['cached']}</p>"
        )
    ]
    pager = []
    for page, label in [(data["page"] - 1, "Previous"), (data["page"] + 1, "Next")]:
        if page > 0 and (page - 1) * data["page_size"] < data["total"]:
            url = "/?" + urlencode(
                {k: data[k] for k in ("q", "page_size", "min_score")} | {"page": page}
            )
            pager.append(
                f'<a href="{escape(url, quote=True)}" hx-get="{escape(url, quote=True)}" '
                f'hx-target="#results" hx-push-url="true">{label}</a>'
            )
    navigation = f"<nav>Page {data['page']} · " + " · ".join(pager) + "</nav>"
    parts.append(navigation)
    for offset, row in enumerate(
        data["results"], (data["page"] - 1) * data["page_size"] + 1
    ):
        title = escape(row["story_title"] or "(story title absent in this slice)")
        story = (
            f'<a href="https://news.ycombinator.com/item?id={row["story_id"]}">{title}</a>'
            if row["story_id"]
            else title
        )
        parts.append(
            f"<article><h2>{offset}. {row['score']:.5f} · {story}</h2>"
            f'<p><a href="https://news.ycombinator.com/item?id={row["comment_id"]}">'
            f"comment {row['comment_id']}</a> · {escape(row['author'] or 'unknown')} · "
            f"{escape(row['comment_day'] or '')} · best chunk {row['chunk']}</p>"
            f'<div class="comment">{escape(row["text"])}</div></article>'
        )
    parts.append(navigation)
    return "".join(parts)


def create_app(
    explorer: CommentExplorer, annotations_path: Path | None = None, *, ner_device="auto"
):
    """Serve a frozen corpus with a separate writable annotation store."""
    entity_worker = EntityWorker(ner_device)

    @asynccontextmanager
    async def lifespan(app):
        yield
        entity_worker.close()

    app = FastAPI(title="Frozen comment explorer", lifespan=lifespan)
    assets = Path(__file__).with_name("explorer_assets")
    app.mount("/assets", StaticFiles(directory=assets), name="assets")
    if (
        annotations_path
        and annotations_path.resolve() == explorer.root / "index.sqlite"
    ):
        raise ValueError(
            "Annotations must use a separate database from the frozen index"
        )
    store = AnnotationStore(
        annotations_path or explorer.root / "annotations.sqlite", explorer.corpus_id
    )
    install_experiments(app, explorer, store)
    install_classifier(app, explorer, store)
    install_rollouts(app, explorer, store)
    install_entity_annotations(app, store)
    install_entities(app, explorer, entity_worker)
    asset = (
        Path(__file__).resolve().parents[4]
        / "crates/hn_app/assets/vendor/htmx-2.0.8.min.js"
    )

    @app.get("/htmx.js")
    def htmx():
        return FileResponse(asset, media_type="application/javascript")

    @app.get("/classifier", response_class=HTMLResponse)
    def classifier_page():
        return (assets / "classifier.html").read_text()

    @app.get("/rollouts", response_class=HTMLResponse)
    def rollout_page():
        return (assets / "rollouts.html").read_text()

    @app.get("/health")
    def health():
        return {
            "vectors": explorer.index.ntotal,
            "dtype": explorer.dtype,
            "load_seconds": explorer.load_seconds,
            "recipe": explorer.manifest["recipe"],
        }

    @app.get("/api/comments/{comment_id}/parent")
    def parent_context(comment_id: int):
        """Read immediate context with two primary-key lookups, never rank it.

        The frozen source metadata retains parent IDs, but parent text may be
        outside the slice. Top-level comments point to a story instead.
        """
        with closing(explorer.connect()) as db:
            db.row_factory = sqlite3.Row
            child = db.execute(
                "SELECT story_id,source_json FROM comments WHERE comment_id=?",
                (comment_id,),
            ).fetchone()
            if child is None:
                raise KeyError("Comment is absent from this corpus")
            parent_id = json.loads(child["source_json"]).get("parent")
            if parent_id is None:
                return {"parent_id": None, "status": "unknown"}
            if parent_id == child["story_id"]:
                return {"parent_id": parent_id, "status": "story"}
            parent = db.execute(
                "SELECT comment_id,author,text FROM comments WHERE comment_id=?",
                (parent_id,),
            ).fetchone()
            return {"parent_id": parent_id,
                    "status": "available" if parent else "outside_slice",
                    "parent": dict(parent) if parent else None}

    @app.get("/api/search")
    def search(
        q: str = Query(min_length=1, max_length=1000),
        page: int = Query(1, ge=1),
        page_size: int = Query(250, ge=1, le=1000),
        min_score: float = Query(-1, ge=-1, le=1),
    ):
        return explorer.search(q, page, page_size, min_score)

    @app.get("/", response_class=HTMLResponse)
    def home(
        request: Request,
        q: str = Query("", max_length=1000),
        page: int = Query(1, ge=1),
        page_size: int = Query(250, ge=1, le=1000),
        min_score: float = Query(-1, ge=-1, le=1),
    ):
        content = (
            results_html(explorer.search(q, page, page_size, min_score)) if q else ""
        )
        if request.headers.get("HX-Request") == "true":
            return HTMLResponse(content)
        shell = (assets / "index.html").read_text()
        if content:
            before, _, rest = shell.partition("<!--initial-results-->")
            _, _, after = rest.partition("<!--/initial-results-->")
            shell = before + content + after
        return HTMLResponse(shell)

    return app
