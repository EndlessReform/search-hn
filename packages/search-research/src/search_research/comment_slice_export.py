"""Freeze selected PostgreSQL comments into a reusable local SQLite input index.

Export uses a read-only repeatable-read snapshot and a server-side cursor. Only a
bounded block is decoded/tokenized at once. The final index is published only
after the whole export succeeds; interrupted preparation restarts its temporary
index rather than combining different source snapshots.
"""

import hashlib
import json
import time
from datetime import date
from itertools import islice
from pathlib import Path
from typing import Literal

import httpx
import polars as pl
import psycopg2
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field, model_validator
from tokenizers import Tokenizer

from search_research.comment_corpus import CommentText, chunk_comments
from search_research.comment_index import (
    FORMAT_VERSION,
    RECIPE,
    connect_index,
    create_index,
    directory_lock,
    metadata,
    put_metadata,
    sync_directory,
    sync_file,
)
from search_research.comment_selection_rows import usable_rows
from search_research.embedding_backfill import file_hash
from search_research.tei_embeddings import EmbeddingRecipe


class CommentSlice(BaseModel):
    """Selection semantics; dates refer to each comment's own posting date."""

    kind: Literal["top-comments", "year"] = "top-comments"
    score_gt: int = 100
    top_k: int = Field(default=3, ge=1)
    year: int | None = Field(default=None, ge=2006, le=9998)

    @model_validator(mode="after")
    def check_year(self):
        assert (self.kind == "year") == (self.year is not None), (
            "--year is required only for the year selector"
        )
        return self


def selection_query(selection: CommentSlice):
    """Force ordered child lookup before payload lookup for the top-k selector.

    OFFSET 0 keeps the parameterized child lookup from being flattened into the
    much more expensive plan that scans/sorts all replies before taking three.
    It does not remove rows. The secondary kid order resolves display-order ties.
    """
    if selection.kind == "year":
        return (
            """
            SELECT c.id AS comment_id,c.story_id,c.by AS author,c.text AS html,
                   c.day::text AS comment_day,c.parent
            FROM items c WHERE c.type='comment' AND c.day >= %s AND c.day < %s
              AND NOT coalesce(c.deleted,false) AND NOT coalesce(c.dead,false)
              AND nullif(btrim(c.text),'') IS NOT NULL
            ORDER BY c.id
        """,
            (date(selection.year, 1, 1), date(selection.year + 1, 1, 1)),
        )
    return (
        """
        SELECT s.id AS story_id,s.title AS story_title,s.score AS story_score,
               s.day::text AS story_day,c.*
        FROM items s CROSS JOIN LATERAL (
            SELECT payload.*,k.display_order FROM kids k CROSS JOIN LATERAL (
                SELECT c.id AS comment_id,c.by AS author,c.text AS html,
                       c.day::text AS comment_day,c.parent
                FROM items c WHERE c.id=k.kid AND c.parent=s.id AND c.type='comment'
                  AND NOT coalesce(c.deleted,false) AND NOT coalesce(c.dead,false)
                  AND nullif(btrim(c.text),'') IS NOT NULL OFFSET 0
            ) payload WHERE k.item=s.id
            ORDER BY k.display_order NULLS LAST,k.kid LIMIT %s
        ) c WHERE s.type='story' AND s.score>%s
          AND NOT coalesce(s.deleted,false) AND NOT coalesce(s.dead,false)
        ORDER BY s.id,c.display_order NULLS LAST,c.comment_id
    """,
        (selection.top_k, selection.score_gt),
    )


def append_comments(index, rows, tokenizer, vector_row, digest):
    """Decode a bounded block; store original text and exact chunk coordinates."""
    decoded = []
    for row in rows:
        parser = CommentText()
        parser.feed(row["html"])
        text = "".join(parser.parts).strip()
        assert text, f"Empty decoded comment {row['comment_id']}"
        index.execute(
            "INSERT INTO comments VALUES (?,?,?,?,?,?,?)",
            (
                row["comment_id"],
                row["story_id"],
                row["author"],
                row["html"],
                text,
                hashlib.sha256(text.encode()).hexdigest(),
                json.dumps(
                    {
                        key: value
                        for key, value in row.items()
                        if key not in {"comment_id", "story_id", "author", "html"}
                    },
                    sort_keys=True,
                ),
            ),
        )
        decoded.append(
            {"comment_id": row["comment_id"], "story_id": row["story_id"], "text": text}
        )
    inputs = chunk_comments(
        pl.DataFrame(
            decoded,
            schema={"comment_id": pl.Int64, "story_id": pl.Int64, "text": pl.String},
        ),
        tokenizer,
    )
    values = []
    for row in inputs.iter_rows(named=True):
        value = (
            vector_row,
            row["comment_id"],
            row["chunk"],
            row["char_start"],
            row["char_end"],
            row["tokens"],
            row["input"],
            row["input_sha256"],
        )
        digest.update(json.dumps(value, ensure_ascii=False).encode() + b"\n")
        values.append(value)
        vector_row += 1
    index.executemany("INSERT INTO inputs VALUES (?,?,?,?,?,?,?,?)", values)
    return vector_row


def prepare(root: Path, selection: CommentSlice, dsn: str):
    """Publish one frozen SQLite index; an existing matching slice is reused."""
    with directory_lock(root):
        target = root / "index.sqlite"
        if target.exists():
            index = connect_index(target, readonly=True)
            try:
                saved = metadata(index)
                assert saved["selection"] == selection.model_dump(), (
                    "Existing slice selection differs; use another directory"
                )
                assert (
                    saved["format_version"] == FORMAT_VERSION
                    and saved["recipe"] == RECIPE
                )
                assert saved["tokenizer_sha256"] == file_hash(root / "tokenizer.json")
            finally:
                index.close()
            return
        recipe = EmbeddingRecipe(compute_dtype="bfloat16")
        tokenizer_path = root / "tokenizer.json"
        if not tokenizer_path.exists():
            response = httpx.get(
                f"https://huggingface.co/{recipe.model_id}/resolve/{recipe.revision}/tokenizer.json",
                follow_redirects=True,
                timeout=120,
            )
            response.raise_for_status()
            temporary = tokenizer_path.with_suffix(".partial")
            temporary.write_bytes(response.content)
            sync_file(temporary)
            temporary.replace(tokenizer_path)
            sync_directory(root)
        tokenizer = Tokenizer.from_file(str(tokenizer_path))
        temporary = root / "index.partial.sqlite"
        temporary.unlink(missing_ok=True)
        Path(str(temporary) + "-journal").unlink(missing_ok=True)
        index = connect_index(temporary)
        started = time.perf_counter()
        try:
            create_index(index)
            digest = hashlib.sha256()
            vector_row = comments = 0
            with psycopg2.connect(dsn) as connection:
                connection.set_client_encoding("UTF8")
                connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL statement_timeout='15min'")
                    cursor.execute(
                        "SELECT transaction_timestamp()::text,txid_current_snapshot()::text"
                    )
                    snapshot = cursor.fetchone()
                with connection.cursor(
                    name="comment_export", cursor_factory=RealDictCursor
                ) as cursor:
                    cursor.itersize = 2048
                    cursor.execute(*selection_query(selection))
                    selected = usable_rows(cursor, connection, selection, index)
                    while rows := list(islice(selected, 2048)):
                        vector_row = append_comments(
                            index,
                            [dict(row) for row in rows],
                            tokenizer,
                            vector_row,
                            digest,
                        )
                        comments += len(rows)
                        if comments % 32768 == 0:
                            print(
                                json.dumps(
                                    {
                                        "event": "export_progress",
                                        "comments": comments,
                                        "vectors": vector_row,
                                        "seconds": time.perf_counter() - started,
                                    }
                                ),
                                flush=True,
                            )
            assert vector_row > 0, "Selection produced no embedding inputs"
            put_metadata(
                index,
                {
                    "format_version": FORMAT_VERSION,
                    "recipe": RECIPE,
                    "selection": selection.model_dump(),
                    "source_snapshot": snapshot,
                    "tokenizer_sha256": file_hash(tokenizer_path),
                    "inputs_sha256": digest.hexdigest(),
                    "comments": comments,
                },
            )
            index.execute("INSERT INTO progress VALUES (1,?,0)", (vector_row,))
            index.commit()
        finally:
            index.close()
        sync_file(temporary)
        temporary.replace(target)
        sync_directory(root)
        print(
            json.dumps(
                {
                    "event": "export_complete",
                    "comments": comments,
                    "vectors": vector_row,
                    "seconds": time.perf_counter() - started,
                }
            ),
            flush=True,
        )
