"""Bounded single-call rollouts with durable attempts and explicit retries."""

import asyncio
import hashlib
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

from openai import APIStatusError, AsyncOpenAI
from pydantic import BaseModel, ConfigDict

from search_research.comment_classifier import (
    ClassifierDraft,
    compile_prompt,
    example_catalog,
)
from search_research.comment_rollout_store import now

MODELS = ["openai/gpt-5.6-luna", "openai/gpt-5.6-terra", "openai/gpt-5.6-sol"]
BASE_URL = "https://openrouter.ai/api/v1"


class Prediction(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    is_positive: bool
    taxonomy: str


class RolloutWorker:
    def __init__(self, explorer, ledger):
        self.explorer, self.ledger = explorer, ledger
        self.executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="classifier"
        )
        self.stop_event = threading.Event()
        self.future = None

    def start(self, pool, model, limit, concurrency):
        """Freeze the actual prompt, labels, schema and routing before dispatch."""
        if self.future is not None and not self.future.done():
            raise ValueError("A rollout is already running; finish or stop it first")
        if not os.environ.get("OPENROUTER_API_KEY"):
            raise ValueError("OPENROUTER_API_KEY is not configured for this service")
        if model not in MODELS:
            raise ValueError("Select a configured rollout model")
        with self.ledger.connect() as db:
            row = db.execute(
                "SELECT draft_json FROM classifier_drafts WHERE set_id=?",
                (pool["set_id"],),
            ).fetchone()
        if not row:
            raise ValueError("Save a classifier prompt first")
        draft = ClassifierDraft.model_validate_json(row[0])
        compiled = compile_prompt(
            draft,
            example_catalog(self.explorer, self.ledger.annotations, pool["set_id"]),
        )
        snapshot = compiled | {
            "draft": draft.model_dump(),
            "corpus_id": self.explorer.corpus_id,
            "base_url": BASE_URL,
            "concurrency": concurrency,
            "prompt_sha256": hashlib.sha256(compiled["prompt"].encode()).hexdigest(),
            "max_output_tokens": 4096,
            "routing": {
                "only": ["openai"],
                "allow_fallbacks": False,
                "require_parameters": True,
            },
        }
        excluded = {e.comment_id for e in draft.examples}
        with self.ledger.connect() as db:
            # Round-robin by source avoids a small batch containing only the first
            # added rule. Each comment remains a single candidate.
            picks = db.execute(
                """SELECT p.comment_id,
                (SELECT min(rule_id) FROM rollout_sources s WHERE s.pool_id=p.pool_id
                 AND s.comment_id=p.comment_id) AS source
                FROM rollout_picks p WHERE pool_id=? AND status='pending'
                ORDER BY picked_at,comment_id""",
                (pool["id"],),
            ).fetchall()
            groups = {}
            for row in picks:
                if row["comment_id"] not in excluded:
                    groups.setdefault(row["source"], []).append(row["comment_id"])
            selected = []
            while groups and len(selected) < limit:
                for source in list(groups):
                    if len(selected) >= limit:
                        break
                    selected.append(groups[source].pop(0))
                    if not groups[source]:
                        del groups[source]
            if not selected:
                raise ValueError(
                    "No pending candidates; sample more or mark failed/rejected rows for retry"
                )
            run_id = db.execute(
                """INSERT INTO classifier_runs
                (pool_id,created_at,status,model,snapshot_json,requested)
                VALUES (?,?,'running',?,?,?)""",
                (pool["id"], now(), model, json.dumps(snapshot), len(selected)),
            ).lastrowid
            for cid in selected:
                attempt = db.execute(
                    """INSERT INTO classifier_attempts(run_id,comment_id,status)
                    VALUES (?,?,'queued')""",
                    (run_id, cid),
                ).lastrowid
                db.execute(
                    """UPDATE rollout_picks SET status='queued',latest_attempt=?
                    WHERE pool_id=? AND comment_id=?""",
                    (attempt, pool["id"], cid),
                )
        self.stop_event.clear()
        self.future = self.executor.submit(
            self.execute, run_id, pool["id"], selected, model, snapshot
        )
        return run_id

    def execute(self, run_id, pool_id, selected, model, snapshot):
        try:
            asyncio.run(self.batch(run_id, pool_id, selected, model, snapshot))
            status, error = ("paused" if self.stop_event.is_set() else "complete"), None
        except Exception as exc:  # noqa: BLE001 -- persist terminal worker failures
            status, error = "error", f"{type(exc).__name__}: {exc}"
        with self.ledger.connect() as db:
            for unfinished in db.execute(
                "SELECT id,comment_id,status FROM classifier_attempts WHERE run_id=? AND status IN ('queued','running')",
                (run_id,),
            ).fetchall():
                state = "pending" if unfinished["status"] == "queued" else "interrupted"
                db.execute(
                    "UPDATE rollout_picks SET status=? WHERE pool_id=? AND comment_id=?",
                    (state, pool_id, unfinished["comment_id"]),
                )
                db.execute(
                    "UPDATE classifier_attempts SET status=?,finished_at=? WHERE id=?",
                    (
                        "not_dispatched" if state == "pending" else state,
                        now(),
                        unfinished["id"],
                    ),
                )
            db.execute(
                "UPDATE classifier_runs SET status=?,finished_at=?,error=? WHERE id=?",
                (status, now(), error, run_id),
            )

    async def batch(self, run_id, pool_id, selected, model, snapshot):
        semaphore = asyncio.Semaphore(snapshot["concurrency"])
        async with AsyncOpenAI(
            base_url=BASE_URL,
            api_key=os.environ["OPENROUTER_API_KEY"],
            timeout=180,
            max_retries=0,
        ) as client:

            async def one(cid):
                async with semaphore:
                    with self.ledger.connect() as db:
                        attempt = db.execute(
                            "SELECT id FROM classifier_attempts WHERE run_id=? AND comment_id=?",
                            (run_id, cid),
                        ).fetchone()[0]
                        if self.stop_event.is_set():
                            db.execute(
                                "UPDATE classifier_attempts SET status='not_dispatched' WHERE id=?",
                                (attempt,),
                            )
                            db.execute(
                                "UPDATE rollout_picks SET status='pending' WHERE pool_id=? AND comment_id=?",
                                (pool_id, cid),
                            )
                            return
                        db.execute(
                            "UPDATE classifier_attempts SET status='running',started_at=? WHERE id=?",
                            (now(), attempt),
                        )
                        db.execute(
                            "UPDATE rollout_picks SET status='running' WHERE pool_id=? AND comment_id=?",
                            (pool_id, cid),
                        )
                    response_data = label = None
                    error = None
                    try:
                        with closing(self.explorer.connect()) as db:
                            row = db.execute(
                                "SELECT text FROM comments WHERE comment_id=?", (cid,)
                            ).fetchone()
                            assert row is not None, (
                                "Sampled comment missing from frozen corpus"
                            )
                        response = await client.chat.completions.create(
                            model=model,
                            messages=[
                                {"role": "system", "content": snapshot["prompt"]},
                                {"role": "user", "content": row[0]},
                            ],
                            max_tokens=snapshot["max_output_tokens"],
                            response_format={
                                "type": "json_schema",
                                "json_schema": {
                                    "name": "comment_label",
                                    "strict": True,
                                    "schema": snapshot["schema"],
                                },
                            },
                            extra_body={"provider": snapshot["routing"]},
                        )
                        response_data = response.model_dump(mode="json")
                        choice = response.choices[0]
                        if choice.finish_reason != "stop" or choice.message.refusal:
                            raise ValueError(
                                f"Non-label completion: {choice.finish_reason}; {choice.message.refusal}"
                            )
                        prediction = Prediction.model_validate_json(
                            choice.message.content or ""
                        )
                        polarity = {
                            t["name"].strip(): t["is_positive"]
                            for t in snapshot["draft"]["taxonomy"]
                        }
                        if prediction.taxonomy not in polarity:
                            raise ValueError("Taxonomy is outside the run's schema")
                        if prediction.is_positive != polarity[prediction.taxonomy]:
                            raise ValueError(
                                "Boolean label disagrees with private taxonomy polarity"
                            )
                        label = prediction.model_dump()
                        status = "accepted"
                    except Exception as exc:  # noqa: BLE001 -- preserve every failed paid attempt
                        status = "error"
                        error = f"{type(exc).__name__}: {exc}"
                        # Authentication/credit failure should stop further dispatch.
                        if isinstance(exc, APIStatusError) and exc.status_code in (
                            401,
                            402,
                            403,
                        ):
                            self.stop_event.set()
                    with self.ledger.connect() as db:
                        db.execute(
                            """UPDATE classifier_attempts SET status=?,finished_at=?,
                            response_json=?,label_json=?,error=? WHERE id=?""",
                            (
                                status,
                                now(),
                                json.dumps(response_data) if response_data else None,
                                json.dumps(label) if label else None,
                                error,
                                attempt,
                            ),
                        )
                        db.execute(
                            "UPDATE rollout_picks SET status=? WHERE pool_id=? AND comment_id=?",
                            (status, pool_id, cid),
                        )

            await asyncio.gather(*(one(cid) for cid in selected))
