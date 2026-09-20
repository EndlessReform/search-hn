"""HTTP contracts for editable example sets and centroid experiments."""

import sqlite3
import time
from contextlib import closing
from typing import Literal

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from search_research.comment_centroids import CommentCentroids


class SetName(BaseModel):
    name: str = Field(min_length=1, max_length=120)

    @field_validator("name")
    @classmethod
    def clean_name(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("Set name cannot be blank")
        return value


class NegativeLabel(BaseModel):
    note: str = ""


class Experiment(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)
    mode: Literal["text", "mean", "corrected"] = "text"
    q: str = Field(default="", max_length=1000)
    set_id: int | None = Field(default=None, ge=1)
    gamma: float = Field(default=1, ge=0)
    baseline_size: Literal[100, 1000, 10000] = 1000
    seed: int = Field(default=20260919, ge=0, le=2**32 - 1)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=1000)
    min_score: float = Field(default=-1, ge=-1, le=1)
    hide_positives: bool = True
    query_positive_ids: list[int] | None = None

    @model_validator(mode="after")
    def require_query(self):
        if self.mode == "text" and not self.q.strip():
            raise ValueError("Enter a search phrase")
        if self.mode != "text" and self.set_id is None:
            raise ValueError("Select a positive set")
        return self


def install_experiments(app, explorer, store):
    """Expose a small JSON API; writes have a browser same-origin boundary.

    This remains a single-user private-tailnet tool. Requiring JSON on mutations
    prevents cross-origin HTML form submissions; no permissive CORS is enabled.
    """
    centroids = CommentCentroids(explorer, store)

    @app.middleware("http")
    async def mutation_boundary(request: Request, call_next):
        if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            if (
                request.headers.get("content-type", "").split(";")[0]
                != "application/json"
            ):
                return JSONResponse(
                    {"detail": "Mutations require application/json"}, status_code=415
                )
            if request.headers.get("sec-fetch-site") == "cross-site":
                return JSONResponse(
                    {"detail": "Cross-site mutations are disabled"}, status_code=403
                )
        return await call_next(request)

    @app.exception_handler(ValueError)
    async def invalid(request, error):
        return JSONResponse({"detail": str(error)}, status_code=400)

    @app.exception_handler(KeyError)
    async def missing(request, error):
        return JSONResponse({"detail": str(error.args[0])}, status_code=404)

    @app.exception_handler(sqlite3.IntegrityError)
    async def conflict(request, error):
        return JSONResponse(
            {"detail": "A set with that name already exists"}, status_code=409
        )

    router = APIRouter(prefix="/api")

    @router.get("/sets")
    def sets():
        return {
            "sets": store.list_sets(),
            "corpus_id": explorer.corpus_id,
            "corpus_name": explorer.root.name,
            "comments": len(centroids.unique_ids),
        }

    @router.post("/sets", status_code=201)
    def create(body: SetName):
        return store.get(store.create(body.name))

    @router.patch("/sets/{set_id}")
    def rename(set_id: int, body: SetName):
        store.rename(set_id, body.name)
        return store.get(set_id)

    @router.delete("/sets/{set_id}")
    def delete(set_id: int):
        store.delete(set_id)
        return {"deleted": set_id}

    @router.get("/sets/{set_id}")
    def detail(set_id: int, page: int = 1):
        if page < 1:
            raise ValueError("Page must be positive")
        data = store.get(set_id)
        labels = ([{"comment_id": cid, "label": "positive", "note": ""}
                   for cid in data["comment_ids"]] +
                  [row | {"label": "negative"} for row in data["negatives"]])
        page_labels = labels[(page - 1) * 50 : page * 50]
        ids = [row["comment_id"] for row in page_labels]
        with closing(explorer.connect()) as db:
            db.row_factory = sqlite3.Row
            rows = (
                [
                    dict(r)
                    for r in db.execute(
                        "SELECT comment_id,story_id,author,text FROM comments WHERE comment_id IN ("
                        + ",".join("?" for _ in ids)
                        + ") ORDER BY comment_id",
                        ids,
                    )
                ]
                if ids
                else []
            )
        return data | {
            "results": [next(r for r in rows if r["comment_id"] == label["comment_id"]) | label
                        for label in page_labels],
            "page": page,
            "page_size": 50,
            "total": data["count"] + data["negative_count"],
        }

    @router.put("/sets/{set_id}/members/{comment_id}")
    def add(set_id: int, comment_id: int):
        with closing(explorer.connect()) as db:
            if (
                db.execute(
                    "SELECT 1 FROM comments WHERE comment_id=?", (comment_id,)
                ).fetchone()
                is None
            ):
                raise KeyError("Comment is absent from this corpus")
        store.member(set_id, comment_id, True)
        return store.get(set_id)

    @router.delete("/sets/{set_id}/members/{comment_id}")
    def remove(set_id: int, comment_id: int):
        store.member(set_id, comment_id, False)
        return store.get(set_id)

    @router.put("/sets/{set_id}/negatives/{comment_id}")
    def negative(set_id: int, comment_id: int, body: NegativeLabel):
        with closing(explorer.connect()) as db:
            if db.execute("SELECT 1 FROM comments WHERE comment_id=?",
                          (comment_id,)).fetchone() is None:
                raise KeyError("Comment is absent from this corpus")
        store.negative(set_id, comment_id, body.note)
        return store.get(set_id)

    @router.delete("/sets/{set_id}/negatives/{comment_id}")
    def remove_negative(set_id: int, comment_id: int):
        store.remove_negative(set_id, comment_id)
        return store.get(set_id)

    @router.post("/experiment")
    def search(body: Experiment):
        with explorer.lock:
            started = time.perf_counter()
            selected = store.get(body.set_id) if body.set_id is not None else None
            ids = selected["comment_ids"] if selected else []
            # Paging replays the applied membership snapshot. New labels must
            # not move page boundaries or change a centroid until Apply.
            query_ids = (
                ids
                if body.query_positive_ids is None
                else sorted(set(body.query_positive_ids))
            )
            diagnostics = {}
            if body.mode == "text":
                ranking, cached = explorer.rank(body.q)
            else:
                key = (
                    "centroid",
                    body.set_id,
                    tuple(query_ids),
                    body.mode,
                    body.gamma,
                    body.seed,
                    body.baseline_size,
                )
                # Centroid construction is cheap relative to ranking and returns
                # diagnostics even on cached pages. Background means are cached.
                query, diagnostics = centroids.query(
                    query_ids, body.mode, body.gamma, body.seed, body.baseline_size
                )
                ranking, cached = explorer.rank_vector(query, key)
            rows, scores, embed_seconds, rank_seconds = ranking
            data = explorer.page_results(
                rows,
                scores,
                body.q,
                body.page,
                body.page_size,
                body.min_score,
                embed_seconds,
                rank_seconds,
                cached,
                started,
                exclude=query_ids
                if body.hide_positives and body.mode != "text"
                else (),
            )
            return data | {
                "engine": (
                    "numpy-exact-int8-cosine"
                    if body.mode != "text" and explorer.dtype == "int8"
                    else data["engine"]
                ),
                "experiment": body.model_dump(),
                "diagnostics": diagnostics,
                "set_revision": selected["revision"] if selected else None,
                "positive_ids": ids,
                "negatives": selected["negatives"] if selected else [],
                "query_positive_ids": query_ids,
            }

    app.include_router(router)
