"""HTTP surface for sampled classifier runs and prediction review."""

import json
import threading
from contextlib import closing
from typing import Literal

from fastapi import Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from search_research.comment_rollout_store import RolloutStore, now
from search_research.comment_rollout_worker import MODELS, RolloutWorker
from search_research.comment_sampling import PoolSettings, Sampler, SamplingRule


class SampleRequest(BaseModel):
    more: bool = False


class RunRequest(BaseModel):
    model: str = MODELS[0]
    limit: int = Field(default=50, ge=1, le=100000)
    concurrency: int = Field(default=8, ge=1, le=32)


class ReviewRequest(BaseModel):
    action: Literal["accept", "reject", "retry"]


class InvalidateRequest(BaseModel):
    confirmation: str


def install_rollouts(app, explorer, annotations):
    ledger = RolloutStore(annotations)
    sampler = Sampler(explorer, ledger)
    worker = RolloutWorker(explorer, ledger)
    mutation_lock = threading.Lock()
    app.state.rollout_worker = worker

    def pool_for(set_id):
        pool = ledger.pool(set_id)
        if not pool:
            raise ValueError("Create a sampling pool first")
        return pool

    @app.get("/api/sets/{set_id}/rollouts")
    def summary(set_id: int):
        import os

        result = ledger.summary(set_id)
        eligible = excluded = 0
        if result["pool"]:
            with ledger.connect() as db:
                row = db.execute(
                    "SELECT draft_json FROM classifier_drafts WHERE set_id=?", (set_id,)
                ).fetchone()
                example_ids = (
                    {e["comment_id"] for e in json.loads(row[0])["examples"]}
                    if row
                    else set()
                )
                pending = [
                    r[0]
                    for r in db.execute(
                        "SELECT comment_id FROM rollout_picks WHERE pool_id=? AND status='pending'",
                        (result["pool"]["id"],),
                    )
                ]
                excluded = sum(cid in example_ids for cid in pending)
                eligible = len(pending) - excluded
        return result | {
            "models": MODELS,
            "credential_available": bool(os.environ.get("OPENROUTER_API_KEY")),
            "eligible_pending": eligible,
            "excluded_prompt_examples": excluded,
        }

    @app.post("/api/sets/{set_id}/rollouts/pool")
    def create_pool(set_id: int, body: PoolSettings):
        with mutation_lock, explorer.lock:
            pool = sampler.create(set_id, body)
            sampler.add_rule(
                pool, SamplingRule(kind="rank", start_rank=1, end_rank=1000, count=1000)
            )
            sampler.add_rule(pool, SamplingRule(kind="random", count=150))
        return ledger.summary(set_id)

    @app.post("/api/sets/{set_id}/rollouts/rules")
    def add_rule(set_id: int, body: SamplingRule):
        with mutation_lock:
            sampler.add_rule(pool_for(set_id), body)
        return ledger.summary(set_id)

    @app.patch("/api/sets/{set_id}/rollouts/rules/{rule_id}")
    def edit_rule(set_id: int, rule_id: int, body: SamplingRule):
        with mutation_lock:
            sampler.edit_rule(pool_for(set_id), rule_id, body)
        return ledger.summary(set_id)

    @app.delete("/api/sets/{set_id}/rollouts/rules/{rule_id}")
    def delete_rule(set_id: int, rule_id: int):
        with mutation_lock:
            sampler.edit_rule(pool_for(set_id), rule_id)
        return ledger.summary(set_id)

    @app.post("/api/sets/{set_id}/rollouts/rules/{rule_id}/sample")
    def sample(set_id: int, rule_id: int, body: SampleRequest):
        with mutation_lock, explorer.lock:
            result = sampler.sample(pool_for(set_id), rule_id, body.more)
        return result

    @app.post("/api/sets/{set_id}/rollouts/prune-pending")
    def prune_pending(set_id: int):
        with mutation_lock:
            return {"removed": sampler.prune_pending(pool_for(set_id))}

    @app.post("/api/sets/{set_id}/rollouts/invalidate")
    def invalidate(set_id: int, body: InvalidateRequest):
        if body.confirmation != "INVALIDATE ALL":
            raise ValueError("Type INVALIDATE ALL to confirm")
        with mutation_lock:
            ledger.invalidate(set_id)
        return {"invalidated": True}

    @app.post("/api/sets/{set_id}/rollouts/run")
    def start(set_id: int, body: RunRequest):
        with mutation_lock:
            run_id = worker.start(
                pool_for(set_id), body.model, body.limit, body.concurrency
            )
        return {"run_id": run_id}

    @app.post("/api/sets/{set_id}/rollouts/stop")
    def stop(set_id: int):
        pool = pool_for(set_id)
        with ledger.connect() as db:
            if db.execute(
                "SELECT 1 FROM classifier_runs WHERE pool_id=? AND status='running'",
                (pool["id"],),
            ).fetchone():
                worker.stop_event.set()
        return {"stopping": True}

    @app.post("/api/sets/{set_id}/rollouts/picks/{comment_id}")
    def review(set_id: int, comment_id: int, body: ReviewRequest):
        with mutation_lock, ledger.connect() as db:
            pool = pool_for(set_id)
            row = db.execute(
                """SELECT p.*,a.label_json FROM rollout_picks p
                LEFT JOIN classifier_attempts a ON a.id=p.latest_attempt
                WHERE p.pool_id=? AND p.comment_id=?""",
                (pool["id"], comment_id),
            ).fetchone()
            if row is None:
                raise KeyError("Candidate does not exist")
            if row["status"] in ("queued", "running"):
                raise ValueError("Wait for this prediction to finish")
            if body.action == "accept" and not row["label_json"]:
                raise ValueError("There is no valid label to accept")
            status = {"accept": "accepted", "reject": "rejected", "retry": "pending"}[
                body.action
            ]
            db.execute(
                "UPDATE rollout_picks SET status=? WHERE pool_id=? AND comment_id=?",
                (status, pool["id"], comment_id),
            )
            db.execute(
                "INSERT INTO rollout_reviews(pool_id,comment_id,action,at) VALUES (?,?,?,?)",
                (pool["id"], comment_id, body.action, now()),
            )
        return {"status": status}

    def rows_with_text(rows):
        with closing(explorer.connect()) as corpus:
            for row in rows:
                data = dict(row)
                text = corpus.execute(
                    "SELECT text,author FROM comments WHERE comment_id=?",
                    (data["comment_id"],),
                ).fetchone()
                data.update(text=text[0], author=text[1])
                label_json = data.pop("label_json")
                data["label"] = json.loads(label_json) if label_json else None
                yield data

    @app.get("/api/sets/{set_id}/rollouts/predictions")
    def predictions(
        set_id: int,
        page: int = Query(1, ge=1),
        status: str = "",
        label: Literal["", "positive", "negative"] = "",
    ):
        pool = pool_for(set_id)
        where = "p.pool_id=?"
        args = [pool["id"]]
        if status:
            where += " AND p.status=?"
            args.append(status)
        if label:
            where += " AND json_extract(a.label_json,'$.is_positive')=?"
            args.append(label == "positive")
        with ledger.connect() as db:
            join = " FROM rollout_picks p LEFT JOIN classifier_attempts a ON a.id=p.latest_attempt "
            total = db.execute(
                "SELECT count(*)" + join + "WHERE " + where, args
            ).fetchone()[0]
            rows = db.execute(
                """SELECT p.*,a.label_json,a.error,a.run_id,a.finished_at,
                r.model,a.response_json"""
                + join
                + "LEFT JOIN classifier_runs r ON r.id=a.run_id WHERE "
                + where
                + " ORDER BY p.rank,p.comment_id LIMIT 50 OFFSET ?",
                args + [(page - 1) * 50],
            ).fetchall()
            results = list(rows_with_text(rows))
            for row in results:
                response = json.loads(row.pop("response_json") or "{}")
                choices = response.get("choices") or []
                row["raw_output"] = (
                    choices[0]["message"].get("content") if choices else None
                )
                row["sources"] = [
                    r[0]
                    for r in db.execute(
                        "SELECT rule_id FROM rollout_sources WHERE pool_id=? AND comment_id=?",
                        (pool["id"], row["comment_id"]),
                    )
                ]
        return {"results": results, "total": total, "page": page, "page_size": 50}

    @app.get("/api/sets/{set_id}/rollouts/export")
    def export(set_id: int):
        pool = pool_for(set_id)

        def generate():
            with ledger.connect() as db:
                rows = db.execute(
                    """SELECT p.*,a.label_json,a.run_id,a.finished_at,
                    r.model,r.snapshot_json FROM rollout_picks p
                    JOIN classifier_attempts a ON a.id=p.latest_attempt
                    JOIN classifier_runs r ON r.id=a.run_id
                    WHERE p.pool_id=? AND p.status='accepted' ORDER BY p.comment_id""",
                    (pool["id"],),
                )
                for row in rows_with_text(rows):
                    row["provenance"] = json.loads(row.pop("snapshot_json"))
                    row["sampling_anchor"] = json.loads(pool["anchor_json"])
                    row["sources"] = [
                        dict(r)
                        for r in db.execute(
                            """
                        SELECT r.id,r.spec_json FROM rollout_sources s JOIN rollout_rules r ON r.id=s.rule_id
                        WHERE s.pool_id=? AND s.comment_id=?""",
                            (pool["id"], row["comment_id"]),
                        )
                    ]
                    yield json.dumps(row, ensure_ascii=False) + "\n"

        return StreamingResponse(
            generate(),
            media_type="application/x-ndjson",
            headers={
                "Content-Disposition": f'attachment; filename="classifier-pool-{pool["id"]}.jsonl"'
            },
        )
