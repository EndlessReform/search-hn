"""Refresh live >=25-vote stories; reuse TE3 vectors only for byte-identical text."""

import asyncio
import hashlib
import io
import json
from pathlib import Path

import numpy as np
import polars as pl
import psycopg2
import tiktoken
from psycopg2.extras import execute_values

from search_research.embedding_api import embed
from search_research.engine_data import BASE, DSN, load_vectors

ROOT = Path("data/luna-semantic-20260904")
LIVE = "host=searchhn-pg port=5432 dbname=searchhn_test user=readonly_hn_agent"
SQL = """SELECT id,title,url,score,day,"by",time,domain FROM items
WHERE type='story' AND day >= DATE '2024-09-04' AND day < DATE '2026-09-05'
AND score>=25 AND NOT coalesce(dead,false) AND NOT coalesce(deleted,false)
ORDER BY id"""


def prepare():
    """Freeze metadata as Parquet and vector inputs before touching scratch PG."""
    ROOT.mkdir(exist_ok=True)
    path = ROOT / "corpus.parquet"
    if not path.exists():
        with (
            psycopg2.connect(LIVE, client_encoding="utf8") as conn,
            conn.cursor() as cur,
        ):
            cur.execute("SET statement_timeout='45s'")
            cur.execute(SQL)
            frame = pl.DataFrame(
                cur.fetchall(), schema=[d.name for d in cur.description], orient="row"
            )
        frame = frame.with_columns(
            (pl.col("title").fill_null("") + "\n" + pl.col("url").fill_null("")).alias(
                "input"
            )
        )
        enc = tiktoken.encoding_for_model("text-embedding-3-large")
        frame = frame.with_columns(
            pl.Series(
                "tokens",
                [len(enc.encode(s, disallowed_special=())) for s in frame["input"]],
            )
        )
        frame.write_parquet(path)
    frame = pl.read_parquet(path)
    old = pl.read_parquet(BASE / "corpus.parquet")
    old_index = {s: i for i, s in enumerate(old["input"])}
    missing = frame.filter(~pl.col("input").is_in(list(old_index))).unique(
        "input", maintain_order=True
    )
    missing.write_parquet(ROOT / "missing.parquet")
    if len(missing):
        asyncio.run(embed(ROOT, missing, missing.head(0)))
        fresh = load_vectors(ROOT, "documents", missing)
        fresh_index = {s: i for i, s in enumerate(missing["input"])}
    old_vectors = load_vectors(BASE, "documents", old)
    vectors = np.stack(
        [
            old_vectors[old_index[s]][:1536]
            if s in old_index
            else fresh[fresh_index[s]][:1536]
            for s in frame["input"]
        ]
    )
    vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
    np.save(ROOT / "vectors.npy", vectors)
    info = {
        "sql": SQL,
        "stories": len(frame),
        "new_texts": len(missing),
        "embedding_estimated_usd": missing["tokens"].sum() * 0.13 / 1e6,
        "corpus_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "model": "text-embedding-3-large",
        "dimensions": 1536,
    }
    (ROOT / "snapshot.json").write_text(json.dumps(info, indent=2))
    print(json.dumps(info), flush=True)
    with psycopg2.connect(DSN, client_encoding="utf8") as conn, conn.cursor() as cur:
        cur.execute(
            'CREATE TABLE IF NOT EXISTS semantic_stories (id bigint PRIMARY KEY,title text,url text,score int,day date,"by" text,time bigint,domain text)'
        )
        cur.execute("SELECT count(*) FROM semantic_stories")
        count = cur.fetchone()[0]
        assert count in (0, len(frame)), "Existing snapshot differs; do not overwrite"
        if count == 0:
            execute_values(
                cur,
                "INSERT INTO semantic_stories VALUES %s",
                frame.select(
                    "id", "title", "url", "score", "day", "by", "time", "domain"
                ).rows(),
                page_size=1000,
            )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS semantic_vectors (id bigint PRIMARY KEY,embedding vector(1536))"
        )
        cur.execute(
            "ALTER TABLE semantic_vectors ALTER COLUMN embedding SET STORAGE EXTERNAL"
        )
        cur.execute("SELECT count(*) FROM semantic_vectors")
        count = cur.fetchone()[0]
        assert count in (0, len(frame))
        if count == 0:
            import struct

            for start in range(0, len(frame), 512):
                buf = io.BytesIO(b"PGCOPY\n\xff\r\n\x00" + struct.pack("!ii", 0, 0))
                buf.seek(0, 2)
                for id, vector in zip(
                    frame["id"][start : start + 512],
                    vectors[start : start + 512],
                    strict=True,
                ):
                    payload = (
                        struct.pack("!hh", 1536, 0) + vector.astype(">f4").tobytes()
                    )
                    buf.write(struct.pack("!hiq i", 2, 8, id, len(payload)))
                    buf.write(payload)
                buf.write(struct.pack("!h", -1))
                buf.seek(0)
                cur.copy_expert("COPY semantic_vectors FROM STDIN WITH BINARY", buf)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS semantic_title_bm25 ON semantic_stories USING bm25(title) WITH (text_config='english')"
        )
        cur.execute("ANALYZE semantic_stories")
        cur.execute("ANALYZE semantic_vectors")
    print("Scratch snapshot ready", flush=True)


if __name__ == "__main__":
    prepare()
