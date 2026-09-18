"""Frozen inputs and bounded local storage for the PG/DuckDB bakeoff."""

import asyncio
import hashlib
import io
import json
import struct
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import polars as pl
import psycopg2
import tiktoken

from search_research.embedding_api import batches, embed
from search_research.embedding_baseline import shortened

BASE = Path("data/te3-large-baseline-20260904")
TRACES = Path("data/fts-baseline-20260904/plain-results")
OUT = Path("data/pg-duckdb-bakeoff-20260904")
DSN = "host=127.0.0.1 port=55432 dbname=search_bakeoff user=postgres"
DIMS = (256, 512, 768, 1024, 1536, 3072)


def load_vectors(root, kind, frame):
    """Load existing durable shards in frozen row order."""
    return np.concatenate(
        [np.load(root / kind / f"{a:07d}-{b:07d}.npy") for a, b in batches(frame)]
    )


def prepare_queries():
    """Embed unique literal trajectory strings only; retain malformed arrays."""
    OUT.mkdir(parents=True, exist_ok=True)
    frame = pl.read_parquet(TRACES / "queries.parquet")
    strings = sorted({q for q in frame["query"] if q and q.strip()})
    enc = tiktoken.encoding_for_model("text-embedding-3-large")
    queries = pl.DataFrame(
        {
            "input": strings,
            "tokens": [len(enc.encode(s, disallowed_special=())) for s in strings],
        }
    )
    path = OUT / "trajectory-inputs.parquet"
    if path.exists():
        assert pl.read_parquet(path).equals(queries), "Frozen query inputs changed"
    queries.write_parquet(path)
    cost = queries["tokens"].sum() * 0.13 / 1e6
    print(
        json.dumps(
            {
                "unique_queries": len(strings),
                "tokens": queries["tokens"].sum(),
                "estimated_usd": cost,
            }
        ),
        flush=True,
    )
    assert cost < 0.10, "Unexpected query embedding cost"
    shards = [
        (OUT / "queries" / f"{a:07d}-{b:07d}.npy", b - a) for a, b in batches(queries)
    ]
    if all(path.exists() for path, _ in shards):
        for path, count in shards:
            assert np.load(path, mmap_mode="r").shape == (count, 3072)
        print("All query embeddings already cached; no API client needed", flush=True)
        return
    asyncio.run(embed(OUT, queries.head(0), queries))


def load_pg():
    """COPY vectors in binary batches; only writes the dedicated local database."""
    OUT.mkdir(parents=True, exist_ok=True)
    corpus = pl.read_parquet(BASE / "corpus.parquet")
    manifest = json.loads((BASE / "manifest.json").read_text())
    assert (
        hashlib.sha256((BASE / "corpus.parquet").read_bytes()).hexdigest()
        == manifest["corpus_sha256"]
    )
    matrix = load_vectors(BASE, "documents", corpus)
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS documents (id bigint PRIMARY KEY, input text, score int, day date, domain text, tsv tsvector GENERATED ALWAYS AS (to_tsvector('english',input)) STORED)"
        )
        cur.execute("SELECT count(*) FROM documents")
        n = cur.fetchone()[0]
        assert n in (0, corpus.height), "Incomplete corpus load; inspect benchmark DB"
        if n == 0:
            from psycopg2.extras import execute_values

            values = [
                (
                    r["id"],
                    r["input"],
                    r["score"],
                    r["day"],
                    (urlparse(r["url"] or "").hostname or "")
                    .lower()
                    .removeprefix("www."),
                )
                for r in corpus.to_dicts()
            ]
            with conn:
                execute_values(
                    cur,
                    "INSERT INTO documents(id,input,score,day,domain) VALUES %s",
                    values,
                    page_size=1000,
                )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS documents_fts ON documents USING gin(tsv)"
        )
        cur.execute("ANALYZE documents")
        for dim in DIMS:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS vectors_{dim} (id bigint PRIMARY KEY, embedding vector({dim}))"
            )
            cur.execute(f"SELECT count(*) FROM vectors_{dim}")
            n = cur.fetchone()[0]
            assert n in (0, corpus.height), f"Incomplete vectors_{dim}"
            if n:
                continue
            cur.execute(
                f"ALTER TABLE vectors_{dim} ALTER COLUMN embedding SET STORAGE EXTERNAL"
            )
            normalized = shortened(matrix, dim)
            with conn:
                for start in range(0, corpus.height, 512):
                    buf = io.BytesIO(b"PGCOPY\n\xff\r\n\x00" + struct.pack("!ii", 0, 0))
                    buf.seek(0, 2)
                    for story_id, vector in zip(
                        corpus["id"][start : start + 512],
                        normalized[start : start + 512],
                    ):
                        payload = (
                            struct.pack("!hh", dim, 0) + vector.astype(">f4").tobytes()
                        )
                        buf.write(struct.pack("!hiq i", 2, 8, story_id, len(payload)))
                        buf.write(payload)
                    buf.write(struct.pack("!h", -1))
                    buf.seek(0)
                    cur.copy_expert(
                        f"COPY vectors_{dim} (id,embedding) FROM STDIN WITH BINARY", buf
                    )
            cur.execute(f"ANALYZE vectors_{dim}")
            print(
                json.dumps({"loaded_dimension": dim, "documents": corpus.height}),
                flush=True,
            )
        cur.execute(
            "SELECT version(), (SELECT extversion FROM pg_extension WHERE extname='vector')"
        )
        version = cur.fetchone()
        cur.execute(
            "SELECT name,setting,unit FROM pg_settings WHERE name IN ('shared_buffers','work_mem','maintenance_work_mem','effective_cache_size','max_parallel_workers_per_gather','jit','random_page_cost')"
        )
        settings = cur.fetchall()
    conn.close()
    (OUT / "pg-config.json").write_text(
        json.dumps(
            {
                "version": version,
                "settings": settings,
                "corpus_sha256": manifest["corpus_sha256"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["load", "embed"])
    if parser.parse_args().action == "load":
        load_pg()
    else:
        prepare_queries()
