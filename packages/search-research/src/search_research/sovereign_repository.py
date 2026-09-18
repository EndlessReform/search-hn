"""Pinned local query encoders and isolated exact-vector tables for E2E runs."""

import hashlib
import io
import json
import struct
from pathlib import Path

import polars as pl
import psycopg2
from psycopg2 import sql

from search_research.embedding_baseline import shortened
from search_research.engine_data import DSN
from search_research.sovereign_run import CORPUS, HASHES, QUESTIONS, ROOT
from search_research.sovereign_score import load_native
from search_research.tei_embeddings import (
    NEMOTRON_RECIPE,
    QWEN_RECIPE,
    EmbeddingRecipe,
    TeiEmbeddings,
)

ARMS = {
    "pplx": ("pplx-1024", 58081, EmbeddingRecipe()),
    "qwen": ("qwen-1024", 58082, QWEN_RECIPE),
    "nemotron": ("nemotron-2048", 58080, NEMOTRON_RECIPE),
}


class LocalQueries:
    """Apply the validated query recipe once; cosine uses float32 unit vectors."""

    def __init__(self, arm):
        _, port, recipe = ARMS[arm]
        self.transport = TeiEmbeddings(f"http://127.0.0.1:{port}", recipe)

    def query(self, text):
        vectors, _ = self.transport.encode([text], "query")
        return shortened(vectors, self.transport.recipe.dimensions)[0].tolist()

    def close(self):
        self.transport.close()


def prepare(root: Path):
    """Load frozen shards atomically into new tables; never overwrite TE3.

    Binary COPY avoids large decimal strings. A metadata digest identifies the
    exact matrix loaded, and corpus equality protects every filter and payload.
    Tables are heap-scanned: no ANN index or dimension-dependent index limit.
    """
    root.mkdir(parents=True, exist_ok=True)
    for name, path in (("corpus", CORPUS), ("questions", QUESTIONS)):
        assert hashlib.sha256(path.read_bytes()).hexdigest() == HASHES[name]
    corpus = pl.read_parquet(CORPUS)
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            'SELECT id,title,url,score,day,"by",time,domain FROM semantic_stories ORDER BY id'
        )
        assert (
            cur.fetchall()
            == corpus.select(
                "id", "title", "url", "score", "day", "by", "time", "domain"
            ).rows()
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS sovereign_vector_manifests (arm text PRIMARY KEY, manifest jsonb NOT NULL)"
        )
        for arm, (directory, port, recipe) in ARMS.items():
            transport = TeiEmbeddings(f"http://127.0.0.1:{port}", recipe)
            try:
                info = transport.info()
            finally:
                transport.close()
            (root / f"{arm}-server.json").write_text(json.dumps(info, indent=2))
            cached = json.loads((ROOT.parent / directory / "manifest.json").read_text())
            assert cached["recipe"] == recipe.model_dump()
            vectors = shortened(
                load_native(ROOT.parent / directory, "documents", len(corpus)),
                recipe.dimensions,
            )
            manifest = {
                "recipe": recipe.model_dump(),
                "corpus": HASHES["corpus"],
                "matrix_sha256": hashlib.sha256(vectors.tobytes()).hexdigest(),
            }
            table = sql.Identifier("sovereign_vectors_" + arm)
            cur.execute(
                "SELECT manifest FROM sovereign_vector_manifests WHERE arm=%s", (arm,)
            )
            previous = cur.fetchone()
            if previous:
                assert previous[0] == manifest, "Vector identity changed"
                cur.execute(sql.SQL("SELECT count(*) FROM {}").format(table))
                assert cur.fetchone()[0] == len(corpus)
                continue
            cur.execute(
                sql.SQL(
                    "CREATE TABLE {} (id bigint PRIMARY KEY, embedding vector({}))"
                ).format(table, sql.Literal(recipe.dimensions))
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {} ALTER COLUMN embedding SET STORAGE EXTERNAL"
                ).format(table)
            )
            for start in range(0, len(corpus), 512):
                buf = io.BytesIO()
                buf.write(b"PGCOPY\n\xff\r\n\x00" + struct.pack("!ii", 0, 0))
                for id, vector in zip(
                    corpus["id"][start : start + 512],
                    vectors[start : start + 512],
                    strict=True,
                ):
                    payload = (
                        struct.pack("!hh", recipe.dimensions, 0)
                        + vector.astype(">f4").tobytes()
                    )
                    buf.write(struct.pack("!hiq i", 2, 8, id, len(payload)))
                    buf.write(payload)
                buf.write(struct.pack("!h", -1))
                buf.seek(0)
                cur.copy_expert(
                    sql.SQL("COPY {} FROM STDIN WITH BINARY")
                    .format(table)
                    .as_string(conn),
                    buf,
                )
            cur.execute(sql.SQL("ANALYZE {}").format(table))
            cur.execute(
                "INSERT INTO sovereign_vector_manifests VALUES (%s,%s)",
                (arm, json.dumps(manifest)),
            )
            print(
                json.dumps({"loaded": arm, "rows": len(corpus), **manifest}), flush=True
            )
