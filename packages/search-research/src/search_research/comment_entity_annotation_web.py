"""Separate annotation UI and JSON API on the existing comment-lab server."""

import json
from pathlib import Path

from fastapi import HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse

from search_research.comment_entity_annotations import (
    EntityAnnotationStore,
    EntityEdit,
    StaleReview,
)


def install_entity_annotations(app, annotations):
    ledger = EntityAnnotationStore(annotations)
    app.state.entity_annotation_store = ledger

    @app.get("/annotator")
    def page():
        return FileResponse(
            Path(__file__).with_name("explorer_assets") / "annotator.html"
        )

    @app.get("/api/entity-annotations")
    def batches():
        return ledger.batches()

    @app.get("/api/entity-annotations/{batch_id}")
    def queue(batch_id: int):
        return ledger.queue(batch_id)

    @app.get("/api/entity-annotations/{batch_id}/export")
    def export(batch_id: int, reviewed_only: bool = Query(True)):
        """Export effective titles plus original proposals, offsets and edit origins."""

        def records():
            for row in ledger.queue(batch_id):
                if reviewed_only and not row["reviewed"]:
                    continue
                item = ledger.item(batch_id, row["comment_id"])
                active = [e for e in item["entities"] if not e["deleted"]]
                item["extraction"] = {
                    "has_any_book": bool(active),
                    "books": [
                        {"title": e["title"], "author": e["author"]} for e in active
                    ],
                }
                yield json.dumps(item, ensure_ascii=False) + "\n"

        return StreamingResponse(
            records(),
            media_type="application/x-ndjson",
            headers={
                "Content-Disposition": f'attachment; filename="entity-review-{batch_id}.jsonl"'
            },
        )

    @app.get("/api/entity-annotations/{batch_id}/comments/{comment_id}")
    def item(batch_id: int, comment_id: int):
        try:
            return ledger.item(batch_id, comment_id)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.post("/api/entity-annotations/{batch_id}/comments/{comment_id}")
    def edit(batch_id: int, comment_id: int, body: EntityEdit):
        try:
            return ledger.edit(batch_id, comment_id, body)
        except StaleReview as exc:
            raise HTTPException(409, str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
