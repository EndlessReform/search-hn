"""Bounded concurrent inference feeding one durable NPY/SQLite writer.

Selection is already frozen in the index. No PostgreSQL connection is used here.
Requests may finish out of order, but a bounded deque publishes them in input row
order so a single contiguous progress marker is sufficient for exact resumption.
"""

import hashlib
import json
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
from pydantic import BaseModel, Field

from search_research.comment_index import metadata
from search_research.comment_vector_store import CommentVectorStore
from search_research.vllm_transport import encode


class EmbeddingSettings(BaseModel):
    base_url: str = "http://127.0.0.1:18080"
    batch_size: int = Field(default=128, ge=1)
    concurrency: int = Field(default=2, ge=1)
    checkpoint_rows: int = Field(default=131072, ge=1)
    timeout_seconds: float = Field(default=300, gt=0)


def batches(index, start, end, batch_size):
    """Read only bounded text batches from the frozen SQLite input table."""
    for position in range(start, end, batch_size):
        stop = min(position + batch_size, end)
        rows = index.execute(
            "SELECT vector_row,input FROM inputs WHERE vector_row>=? AND vector_row<? ORDER BY vector_row",
            (position, stop),
        ).fetchall()
        assert [row[0] for row in rows] == list(range(position, stop)), (
            "Missing frozen input rows"
        )
        yield position, [row[1] for row in rows]


def embed(root: Path, settings: EmbeddingSettings):
    """Resume committed vectors and checkpoint each completed block and final tail.

    HTTP errors propagate. Completed checkpoints remain valid; this deliberately
    adds no retries, fallback model, or automatic smaller-batch substitution.
    On restart the unfinished checkpoint is recomputed. Changing client scheduling
    settings is allowed, but the pinned model/tokenizer/output contract is not.
    """
    with CommentVectorStore(root) as store:
        cursor = store.index.execute(
            "INSERT INTO runs(settings_json,status) VALUES (?,'running')",
            (settings.model_dump_json(),),
        )
        run_id = cursor.lastrowid
        store.index.commit()
        started = last_log = time.perf_counter()
        initial = store.completed_rows
        print(
            json.dumps(
                {
                    "event": "embedding_start",
                    "completed": initial,
                    "total": store.total_rows,
                    "settings": settings.model_dump(),
                }
            ),
            flush=True,
        )
        try:
            with (
                httpx.Client(
                    base_url=settings.base_url.rstrip("/"),
                    timeout=settings.timeout_seconds,
                ) as client,
                ThreadPoolExecutor(max_workers=settings.concurrency) as workers,
            ):
                while store.completed_rows < store.total_rows:
                    end = min(
                        store.completed_rows + settings.checkpoint_rows,
                        store.total_rows,
                    )
                    source = iter(
                        batches(
                            store.index, store.completed_rows, end, settings.batch_size
                        )
                    )
                    pending = deque()

                    def submit(source=source, pending=pending):
                        item = next(source, None)
                        if item is not None:
                            start, texts = item
                            pending.append(
                                (
                                    start,
                                    workers.submit(encode, client, texts, priority=1),
                                )
                            )

                    for _ in range(settings.concurrency):
                        submit()
                    while pending:
                        start, future = pending.popleft()
                        vectors, _, _ = future.result()
                        store.write(start, vectors)
                        submit()
                        now = time.perf_counter()
                        if now - last_log >= 15:
                            print(
                                json.dumps(
                                    {
                                        "event": "embedding_progress",
                                        "written": store.written_rows,
                                        "committed": store.completed_rows,
                                        "total": store.total_rows,
                                        "vectors_per_second": (
                                            store.written_rows - initial
                                        )
                                        / (now - started),
                                    }
                                ),
                                flush=True,
                            )
                            last_log = now
                    tick = time.perf_counter()
                    store.checkpoint()
                    print(
                        json.dumps(
                            {
                                "event": "checkpoint",
                                "completed": store.completed_rows,
                                "total": store.total_rows,
                                "checkpoint_seconds": time.perf_counter() - tick,
                                "elapsed_seconds": time.perf_counter() - started,
                            }
                        ),
                        flush=True,
                    )
            status = "complete"
        except BaseException:
            with store.index:
                store.index.execute(
                    "UPDATE runs SET status='failed',finished_at=CURRENT_TIMESTAMP WHERE id=?",
                    (run_id,),
                )
            raise
        with store.index:
            store.index.execute(
                "UPDATE runs SET status=?,finished_at=CURRENT_TIMESTAMP WHERE id=?",
                (status, run_id),
            )
        print(
            json.dumps(
                {
                    "event": "embedding_complete",
                    "total": store.total_rows,
                    "new_vectors": store.completed_rows - initial,
                    "seconds": time.perf_counter() - started,
                }
            ),
            flush=True,
        )


def verify(root: Path):
    """Recheck vector checksums and the frozen input digest in bounded memory."""
    with CommentVectorStore(root) as store:
        digest = hashlib.sha256()
        count = 0
        for row in store.index.execute("SELECT * FROM inputs ORDER BY vector_row"):
            assert row[0] == count
            assert hashlib.sha256(row[6].encode()).hexdigest() == row[7], (
                "Input text hash mismatch"
            )
            digest.update(json.dumps(row, ensure_ascii=False).encode() + b"\n")
            count += 1
        saved = metadata(store.index)
        assert (
            count == store.total_rows and digest.hexdigest() == saved["inputs_sha256"]
        ), "Frozen input digest mismatch"
        assert store.index.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert not store.index.execute("PRAGMA foreign_key_check").fetchall()
        print(
            json.dumps(
                {
                    "event": "verified",
                    "total": count,
                    "completed": store.completed_rows,
                    "complete": store.completed_rows == count,
                }
            ),
            flush=True,
        )
