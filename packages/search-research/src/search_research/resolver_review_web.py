"""HTMX review server for frozen resolver runs.

Launch from the repository root with:
uv run --no-sync --package search-research python -m search_research.resolver_review_web
No catalog, model server, API key or corpus embedding index is needed.
"""

import argparse
from pathlib import Path
from typing import Literal

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from search_research.resolver_review import ResolverReview
from search_research.resolver_review_html import review_html
from search_research.resolver_review_remote import install_remote

DEFAULT_CHECKPOINT = Path("data/research/books-resolver-2025-v1/checkpoint.sqlite")


def create_app(checkpoint=DEFAULT_CHECKPOINT, live=False):
    """Expose only reads of an explicitly frozen SQLite checkpoint."""
    store = ResolverReview(Path(checkpoint), live=live)
    app = FastAPI(title="Resolver Lab")
    install_remote(app)
    assets = Path(__file__).with_name("explorer_assets")
    app.mount("/assets", StaticFiles(directory=assets), name="assets")

    @app.get("/htmx.js")
    def htmx():
        return FileResponse(
            Path(__file__).resolve().parents[4]
            / "crates/hn_app/assets/vendor/htmx-2.0.8.min.js",
            media_type="application/javascript",
        )

    @app.get("/health")
    def health():
        return {"checkpoint": str(store.path), "references": len(store.rows)}

    @app.get("/api/summary")
    def summary(
        model: Literal[
            "deepseek-native", "deepseek", "luna-original"
        ] = "deepseek-native",
    ):
        store.refresh()
        return store.summary(model)

    @app.get("/api/reference/{ident}")
    def reference(ident: str):
        if ident not in store.by_id:
            raise HTTPException(404, "Unknown reference")
        return store.detail(ident)

    @app.get("/", response_class=HTMLResponse)
    def home(
        request: Request,
        model: Literal[
            "deepseek-native", "deepseek", "luna-original"
        ] = "deepseek-native",
        category: Literal[
            "disagree",
            "different_work",
            "luna_only",
            "ds_only",
            "missing",
            "same_work",
            "both_abstain",
            "all",
        ] = "disagree",
        q: str = Query("", max_length=1000),
        duplicates: bool = False,
        page: int = Query(1, ge=1),
        id: str | None = None,
        case: int | None = Query(None, ge=1),
    ):
        if id is not None and id not in store.by_id:
            raise HTTPException(404, "Unknown reference")
        store.refresh()
        content = review_html(store, model, category, q, duplicates, page, id, case)
        if request.headers.get("HX-Request") == "true":
            return HTMLResponse(content, headers={"Vary": "HX-Request"})
        pipeline = (
            "Person NER → title search + author bonus → rerank + popularity → selector → repair searches. Each repair shows its own title, author, candidates, and decision below."
            if store.has_retrieval_history
            else "Popularity rerun uses logit + 0.1 × log2(1 + reader count). No duplicate substitution."
        )
        return HTMLResponse(
            """<!doctype html><html lang="en"><head><meta charset="utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1"><title>RESOLVER LAB // Review</title>
        <link rel="stylesheet" href="/assets/lab.css"><link rel="stylesheet" href="/assets/resolver.css">
        <script src="/htmx.js"></script></head><body>
        <header class="titlebar">RESOLVER LAB // Candidate inspection <small>Saved search + reranker · read only</small></header>
        <div class="menubar">Original span → title BM25 top 50 → ranking → top 3 → selector
        <br>"""
            + pipeline
            + """
        <span id="load-state" role="status"></span></div><div id="review">"""
            + content
            + """</div>
        <footer>All saved candidates shown · Model outputs are proposals, not human judgments</footer>
        <script>
        document.body.addEventListener('htmx:beforeRequest',()=>document.getElementById('load-state').textContent='Loading…');
        document.body.addEventListener('htmx:afterRequest',e=>document.getElementById('load-state').textContent=e.detail.successful?'':'Request failed — please retry or reload.');
        </script></body></html>""",
            headers={"Vary": "HX-Request"},
        )

    return app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=DEFAULT_CHECKPOINT,
        help="Frozen portable SQLite checkpoint; do not use a live WAL database",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8767)
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    uvicorn.run(
        create_app(args.checkpoint, live=args.live), host=args.host, port=args.port
    )


if __name__ == "__main__":
    main()
