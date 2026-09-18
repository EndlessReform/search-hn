"""Session-owned exact-vector / BM25 hybrid repository for a frozen PG snapshot.

Only retrieval changes: normal story payloads and live comment access remain
compatible with the agent. One repository belongs to one conversation, so its
query vectors and stable ranked lists cannot leak into another session. Closing
the runtime clears the cache. The lock coalesces concurrent identical requests.
"""

from __future__ import annotations

import re
import time
from datetime import date
from threading import RLock
from typing import Literal, Protocol

from openai import OpenAI
from sqlalchemy import text

from search_agent.data_access import (
    HNStorySearchRepository,
    StorySearchHit,
    create_db_engine,
)


class QueryEmbeddings(Protocol):
    """Session-owned query encoder; model-specific prompting belongs here."""

    def query(self, text: str) -> list[float]: ...

    def close(self) -> None: ...


class SemanticStoryRepository(HNStorySearchRepository):
    """Exact cosine with optional half-weight RRF over pg_textsearch top 100.

    No ANN approximation or reranker. Score/date sorts explicitly reorder the
    retrieved candidate union (or all matching rows for filter-only requests).
    The snapshot's >=25-vote floor cannot be undone with a lower min_score.
    """

    def __init__(
        self,
        database_url: str,
        comments_url: str,
        *,
        mode: Literal["dense", "hybrid"],
        embedding_provider: QueryEmbeddings | None = None,
        vector_table: str = "semantic_vectors",
    ):
        super().__init__(create_db_engine(database_url))
        assert mode in ("dense", "hybrid")
        assert re.fullmatch(r"[a-z][a-z0-9_]*", vector_table), "Invalid vector table"
        self.vector_table = vector_table
        self.embedding_provider = embedding_provider
        self.mode = mode
        self.comments = HNStorySearchRepository.from_database_url(comments_url)
        self.client = (
            None
            if embedding_provider is not None
            else OpenAI(base_url="https://api.openai.com/v1", max_retries=2)
        )
        self.embeddings: dict[str, list[float]] = {}
        self.rankings: dict[tuple, list[StorySearchHit]] = {}
        self.lock = RLock()
        self.stats = {
            "embedding_requests": 0,
            "embedding_tokens": 0,
            "embedding_cache_hits": 0,
            "ranking_cache_hits": 0,
            "searches": 0,
            "retrieval_ms": 0.0,
        }

    def _embedding(self, query):
        """Caller holds the session lock; reuse across pages, filters and turns."""
        if query in self.embeddings:
            self.stats["embedding_cache_hits"] += 1
            return self.embeddings[query]
        if self.embedding_provider is not None:
            vector = self.embedding_provider.query(query)
        else:
            assert self.client is not None
            response = self.client.embeddings.create(
                model="text-embedding-3-large", input=query, dimensions=1536
            )
            vector = response.data[0].embedding
            assert len(vector) == 1536
            self.stats["embedding_tokens"] += response.usage.total_tokens
        self.embeddings[query] = vector
        self.stats["embedding_requests"] += 1
        return vector

    def search_stories(
        self,
        query,
        *,
        limit=20,
        min_score=None,
        min_date=None,
        max_date=None,
        include_domains=None,
        exclude_domains=None,
        skip=0,
        sort="relevance",
    ):
        """Cache ordered candidates, then page without another embedding or DB read."""
        assert 1 <= limit <= 100 and skip >= 0
        assert sort in ("relevance", "score", "date")
        query = query.strip() if query else None
        assert query or min_date or max_date or include_domains or exclude_domains
        key = (
            query,
            max(25, min_score or 25),
            min_date,
            max_date,
            tuple(include_domains or ()),
            tuple(exclude_domains or ()),
            sort,
        )
        with self.lock:
            self.stats["searches"] += 1
            if key in self.rankings:
                self.stats["ranking_cache_hits"] += 1
                return self.rankings[key][skip : skip + limit]
            clauses = ["score >= :min_score"]
            params = {"min_score": key[1]}
            for column, value, operator, name in [
                ("day", min_date, ">=", "min_date"),
                ("day", max_date, "<=", "max_date"),
            ]:
                if value is not None:
                    clauses.append(f"{column} {operator} :{name}")
                    params[name] = value
            for name, values, negation in [
                ("include", include_domains, ""),
                ("exclude", exclude_domains, "NOT"),
            ]:
                if values:
                    clauses.append(
                        f"{negation} (regexp_replace(lower(coalesce(domain,'')), '^www\\.', '') = ANY(CAST(:{name} AS text[])))"
                    )
                    params[name] = values
            where = " AND ".join(clauses)
            if query:
                params["embedding"] = str(self._embedding(query))
                params["query"] = query
            start = time.perf_counter()
            with self._engine.connect() as conn:
                conn.execute(text("SET LOCAL statement_timeout='30s'"))
                if query:
                    dense = list(
                        conn.execute(
                            text(
                                f"SELECT s.id FROM {self.vector_table} v JOIN semantic_stories s USING(id) WHERE {where} ORDER BY (v.embedding <=> CAST(:embedding AS vector)) + 0, s.id LIMIT 100"
                            ),
                            params,
                        ).scalars()
                    )
                    scores = {id: 1 / (60 + i) for i, id in enumerate(dense, 1)}
                    if self.mode == "hybrid":
                        lexical = list(
                            conn.execute(
                                text(
                                    f"SELECT id FROM semantic_stories WHERE {where} AND title <@> to_bm25query(:query,'semantic_title_bm25') < 0 ORDER BY title <@> to_bm25query(:query,'semantic_title_bm25'),id LIMIT 100"
                                ),
                                params,
                            ).scalars()
                        )
                        for i, id in enumerate(lexical, 1):
                            scores[id] = scores.get(id, 0) + 0.5 / (60 + i)
                    ids = sorted(scores, key=lambda id: (-scores[id], id))
                    rows = (
                        conn.execute(
                            text(
                                'SELECT id,title,url,score,"by",time,day FROM semantic_stories WHERE id = ANY(CAST(:ids AS bigint[]))'
                            ),
                            {"ids": ids},
                        )
                        .mappings()
                        .all()
                        if ids
                        else []
                    )
                    hits = {r["id"]: StorySearchHit(**r) for r in rows}
                    ordered = [hits[id] for id in ids]
                    if sort != "relevance":
                        ordered.sort(
                            key=lambda hit: (
                                (hit.score or 0, hit.id)
                                if sort == "score"
                                else (hit.day or date.min, hit.id)
                            ),
                            reverse=True,
                        )
                else:
                    order = (
                        "day DESC NULLS LAST,id DESC"
                        if sort == "date"
                        else "score DESC NULLS LAST,id DESC"
                    )
                    rows = (
                        conn.execute(
                            text(
                                f'SELECT id,title,url,score,"by",time,day FROM semantic_stories WHERE {where} ORDER BY {order} LIMIT 100'
                            ),
                            params,
                        )
                        .mappings()
                        .all()
                    )
                    ordered = [StorySearchHit(**r) for r in rows]
            self.stats["retrieval_ms"] += (time.perf_counter() - start) * 1000
            self.rankings[key] = ordered
            return ordered[skip : skip + limit]

    def top_stories_for_date(self, target_date, *, limit=8):
        return self.search_stories(
            None, min_date=target_date, max_date=target_date, limit=limit, sort="score"
        )

    def fetch_top_level_comments(self, story_id, *, limit=5, skip=0):
        return self.comments.fetch_top_level_comments(story_id, limit=limit, skip=skip)

    def dispose(self):
        with self.lock:
            self.reset_session()
            if self.client is not None:
                self.client.close()
            if self.embedding_provider is not None:
                self.embedding_provider.close()
            self.comments.dispose()
            super().dispose()

    def reset_session(self):
        """Conversation reset releases cached vectors/rankings, not the DB pool."""
        with self.lock:
            self.embeddings.clear()
            self.rankings.clear()
