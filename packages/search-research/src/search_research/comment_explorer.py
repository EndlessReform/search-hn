"""Exact phrase-to-comment ranking directly from a completed NPY/SQLite slice."""

import hashlib
import json
import sqlite3
import threading
import time
from collections import OrderedDict
from contextlib import closing
from pathlib import Path

import faiss
import httpx
import numpy as np

from search_research.comment_index import DIMENSIONS, FORMAT_VERSION, RECIPE, metadata
from search_research.vllm_transport import encode


class CommentExplorer:
    """Build a disposable index and preserve the frozen row-to-comment join.

    L2-normalized float32 copies of the native int8 coordinates make inner product
    equal cosine similarity. This does not recover pre-quantization model values.
    Exact ranking avoids confounding phrase quality with approximate-index recall.
    One lock serializes requests, protecting the index and a four-phrase LRU cache.
    """

    def __init__(
        self, root: Path, base_url: str, *, threads: int = 16, dtype: str = "int8"
    ):
        assert dtype in {"int8", "f32"}
        self.dtype = dtype
        self.root = root.resolve()
        self.base_url = base_url
        self.threads = threads
        self.lock = threading.Lock()
        self.cache = OrderedDict()
        self.index = (
            faiss.IndexFlatIP(DIMENSIONS)
            if dtype == "f32"
            else faiss.IndexScalarQuantizer(
                DIMENSIONS,
                faiss.ScalarQuantizer.QT_8bit_direct_signed,
                faiss.METRIC_INNER_PRODUCT,
            )
        )
        started = time.perf_counter()
        with closing(self.connect()) as db:
            saved = metadata(db)
            assert (
                saved["format_version"] == FORMAT_VERSION and saved["recipe"] == RECIPE
            )
            total, complete = db.execute(
                "SELECT total_rows,completed_rows FROM progress WHERE id=1"
            ).fetchone()
            assert total == complete and total > 0, (
                "Explorer requires a completed frozen slice"
            )
            vectors = np.load(self.root / "vectors.npy", mmap_mode="r")
            self.vectors = vectors
            assert vectors.shape == (total, DIMENSIONS) and vectors.dtype == np.int8
            end = 0
            for start, stop, digest in db.execute(
                "SELECT start_row,end_row,sha256 FROM checkpoints ORDER BY start_row"
            ):
                assert start == end and stop <= total
                assert hashlib.sha256(vectors[start:stop]).hexdigest() == digest, (
                    "Vector checksum mismatch"
                )
                end = stop
            assert end == total, "Checkpoint ledger is incomplete"
            self.corpus_id = hashlib.sha256(
                json.dumps(
                    {
                        "manifest": saved,
                        "checkpoints": db.execute(
                            "SELECT start_row,end_row,sha256 FROM checkpoints ORDER BY start_row"
                        ).fetchall(),
                    },
                    sort_keys=True,
                ).encode()
            ).hexdigest()
            # Only IDs and chunk coordinates enter RAM; full text stays in SQLite.
            mapping = np.asarray(
                db.execute(
                    "SELECT vector_row,comment_id,chunk FROM inputs ORDER BY vector_row"
                ).fetchall(),
                dtype=np.int64,
            )
            assert mapping.shape == (total, 3)
            assert np.array_equal(mapping[:, 0], np.arange(total))
            assert db.execute("PRAGMA foreign_key_check").fetchone() is None
            self.comment_ids = mapping[:, 1].copy()
            self.chunks = mapping[:, 2].copy()
            self.manifest = saved
        faiss.omp_set_num_threads(threads)
        self.norms = np.empty(total, dtype=np.float32)
        for start in range(0, total, 32768):
            block = vectors[start : start + 32768].astype(np.float32)
            assert np.any(block != 0, axis=1).all()
            self.norms[start : start + len(block)] = np.linalg.norm(block, axis=1)
            if dtype == "f32":
                faiss.normalize_L2(block)
            self.index.add(block)
        self.load_seconds = time.perf_counter() - started

    def connect(self):
        """Open metadata read-only, separately for each requesting thread."""
        return sqlite3.connect(
            (self.root / "index.sqlite").as_uri() + "?mode=ro", uri=True
        )

    def rank(self, phrase: str):
        """Cache complete rankings; keep the best chunk per comment, with stable ties.

        A raw phrase uses exactly the corpus quantization recipe. Ranking is not a
        probability or a learned classifier. A threshold is only a cosine cutoff.
        Caller holds the lock so concurrent pages share the same query embedding.
        """
        if phrase in self.cache:
            self.cache.move_to_end(phrase)
            return self.cache[phrase], True
        with httpx.Client(base_url=self.base_url, timeout=120) as client:
            query, embed_seconds, _ = encode(client, [phrase], priority=0)
        return self.rank_vector(query, phrase, embed_seconds, native_query=True)

    def rank_vector(self, query, key, embed_seconds=0.0, *, native_query=False):
        """Rank an arbitrary float query against unchanged native corpus codes.

        Centroid differences must not be requantized: fractional coordinates are
        meaningful. Callers hold the same lock used by phrase searches and pages.
        """
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key], True
        query = np.asarray(query, dtype=np.float32).reshape(1, DIMENSIONS).copy()
        query_norm = float(np.linalg.norm(query))
        if not np.isfinite(query).all() or query_norm < 1e-7:
            raise ValueError("Query direction is zero or nonfinite")
        if self.dtype == "f32":
            faiss.normalize_L2(query)
        faiss.omp_set_num_threads(self.threads)
        started = time.perf_counter()
        if self.dtype == "int8" and not native_query:
            # FAISS direct-signed SQ also casts QUERY coordinates to integers.
            # Streaming einsum converts coordinates during accumulation, keeping
            # fractional queries exact without a corpus-sized float32 copy.
            scores = np.einsum(
                "ij,j->i", self.vectors, query[0], dtype=np.float32, optimize=False
            )
            rows = np.arange(self.index.ntotal)
        else:
            distances, rows = self.index.search(query, self.index.ntotal)
            rows, scores = rows[0], distances[0]
        if self.dtype == "int8":
            # Direct signed SQ stores the existing integer coordinates losslessly.
            # Rank ALL dot products before cosine selection: top-k dot products
            # alone could exclude genuine cosine neighbors with smaller norms.
            scores = scores / (self.norms[rows] * query_norm)
        order = np.lexsort((rows, self.comment_ids[rows], -scores))
        rows, scores = rows[order], scores[order]
        _, first = np.unique(self.comment_ids[rows], return_index=True)
        first.sort()
        result = (
            rows[first],
            scores[first],
            embed_seconds,
            time.perf_counter() - started,
        )
        self.cache[key] = result
        if len(self.cache) > 4:
            self.cache.popitem(last=False)
        return result, False

    def search(
        self, phrase: str, page: int = 1, page_size: int = 250, min_score: float = -1
    ):
        """Page all qualifying comments; fetch full text only for the selected page."""
        with self.lock:
            started = time.perf_counter()
            (rows, scores, embed_seconds, rank_seconds), cached = self.rank(phrase)
            return self.page_results(
                rows,
                scores,
                phrase,
                page,
                page_size,
                min_score,
                embed_seconds,
                rank_seconds,
                cached,
                started,
            )

    def page_results(
        self,
        rows,
        scores,
        phrase,
        page,
        page_size,
        min_score,
        embed_seconds,
        rank_seconds,
        cached,
        started,
        exclude=(),
    ):
        """Share exact ranking pagination between text and example queries."""
        if len(exclude):
            keep = ~np.isin(self.comment_ids[rows], exclude)
            rows, scores = rows[keep], scores[keep]
        count = int(np.count_nonzero(scores >= min_score))
        start = (page - 1) * page_size
        selected = (
            rows[start : min(start + page_size, count)] if start < count else rows[:0]
        )
        ids = self.comment_ids[selected].tolist()
        with closing(self.connect()) as db:
            # Input row, comment, and story metadata remain tied to the frozen dump.
            db.row_factory = sqlite3.Row
            docs = (
                {
                    r["comment_id"]: dict(r)
                    for r in db.execute(
                        """SELECT c.comment_id,c.story_id,c.author,c.text,
                              json_extract(c.source_json,'$.story_title') AS story_title,
                              json_extract(c.source_json,'$.comment_day') AS comment_day,
                              i.chunk,i.char_start,i.char_end
                       FROM inputs i JOIN comments c USING(comment_id)
                       WHERE i.vector_row IN ("""
                        + ",".join("?" for _ in selected)
                        + ")",
                        selected.tolist(),
                    )
                }
                if len(selected)
                else {}
            )
        results = [
            dict(
                docs[cid],
                score=float(scores[start + i]),
                vector_row=int(selected[i]),
            )
            for i, cid in enumerate(ids)
        ]
        return {
            "q": phrase,
            "page": page,
            "page_size": page_size,
            "min_score": min_score,
            "total": count,
            "total_vectors": self.index.ntotal,
            "results": results,
            "engine": f"faiss-exact-{self.dtype}-cosine",
            "cached": cached,
            "embedding_seconds": embed_seconds,
            "ranking_seconds": rank_seconds,
            "request_seconds": time.perf_counter() - started,
        }
