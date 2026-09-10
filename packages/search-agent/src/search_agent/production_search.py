"""Live production hybrid retrieval shared by the agent's Python interfaces.

Unfiltered queries use HNSW. Explicit filters use exact cosine over the eligible
filtered population: this avoids ANN starvation at the cost of more work for
broad filters. Both branches apply filters before their 100-candidate limits.
Only ranked IDs are cached; displayed fields and eligibility are read each page.
"""

from dataclasses import dataclass
from http.client import HTTPException
import logging
from threading import RLock
from time import monotonic
from urllib.error import URLError

from sqlalchemy import text

from search_agent.data_access import (
    HNStorySearchRepository,
    StorySearchHit,
    create_db_engine,
)
from search_agent.production_embeddings import embed_query


class SearchResults(list[StorySearchHit]):
    """List-compatible results with query-specific retrieval provenance."""

    def __init__(self, hits, mode, ranks, *, positions, remaining):
        super().__init__(hits)
        self.mode = mode
        self.ranks = ranks
        self.positions = positions
        self.remaining = remaining

    def for_page(self, size):
        """Never promote a lookahead row into a deleted story's page position."""
        return [hit for hit in self if self.positions[hit.id] < size]


@dataclass
class Ranking:
    created: float
    ids: list[int]
    mode: str
    ranks: dict[int, dict]


def fuse(dense: list[int], lexical: list[int]) -> dict[int, dict]:
    """Weighted reciprocal rank fusion; ranks start at one, absent branches add zero.

    These are ranking scores, not relevance probabilities or HN vote scores.
    Match the production SQL prototype: k=60, dense=1, title BM25=0.125.
    """
    ranks: dict[int, dict] = {}
    for name, weight, ids in (
        ("dense_rank", 1.0, dense),
        ("bm25_rank", 0.125, lexical),
    ):
        for rank, story_id in enumerate(ids, 1):
            entry = ranks.setdefault(
                story_id, {"dense_rank": None, "bm25_rank": None, "rrf": 0.0}
            )
            entry[name] = rank
            entry["rrf"] += weight / (60 + rank)
    return ranks


class ProductionStoryRepository(HNStorySearchRepository):
    """Session-owned, bounded five-minute rankings with explicit lexical fallback."""

    def __init__(self, database_url: str, embedding_base_url: str):
        assert embedding_base_url.startswith(("http://", "https://")), (
            "EMBEDDING_BASE_URL must be an HTTP(S) proxy URL"
        )
        super().__init__(create_db_engine(database_url))
        self.embedding_base_url = embedding_base_url
        self.rankings: dict[tuple, Ranking] = {}
        self.lock = RLock()

    def reset_session(self):
        with self.lock:
            self.rankings.clear()

    def dispose(self):
        self.reset_session()
        super().dispose()

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
        """Rank once per query/filter/sort combination, then refresh source payloads.

        Pages address positions in the saved list. Deleted/demoted entries are
        omitted without moving later IDs between pages. Inference failure freezes
        an explicitly keyword-only list for the same pagination lifetime.
        """
        query = query.strip() if query else None
        assert query or min_date or max_date or include_domains or exclude_domains
        assert 1 <= limit <= 100 and skip >= 0
        assert sort in ("relevance", "score", "date")
        assert not (min_date and max_date) or min_date <= max_date
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
            now = monotonic()
            self.rankings = {
                k: v for k, v in self.rankings.items() if now - v.created < 300
            }
            if key not in self.rankings:
                if skip:
                    raise ValueError(
                        "Search ranking expired or unavailable; start again at page 1"
                    )
                ranking = self._rank(key)
                if len(self.rankings) >= 128:
                    del self.rankings[next(iter(self.rankings))]
                self.rankings[key] = ranking
            ranking = self.rankings[key]
            ids = ranking.ids[skip : skip + limit]
            hits = self._details(ids)
            return SearchResults(
                hits,
                ranking.mode,
                {hit.id: ranking.ranks.get(hit.id, {}) for hit in hits},
                positions={story_id: i for i, story_id in enumerate(ids)},
                remaining=max(0, len(ranking.ids) - skip),
            )

    def _details(self, ids):
        """Recheck live eligibility even when the original ranking is cached."""
        if not ids:
            return []
        with self._engine.connect() as conn:
            rows = (
                conn.execute(
                    text("""
                SELECT i.id,i.title,i.url,i.score,i.by,i.time,i.day
                FROM unnest(CAST(:ids AS bigint[])) WITH ORDINALITY r(id,ord)
                JOIN public.items i ON i.id=r.id
                WHERE public.story_search_eligible(i) ORDER BY r.ord
            """),
                    {"ids": ids},
                )
                .mappings()
                .all()
            )
        return [StorySearchHit(**row) for row in rows]

    def _rank(self, key):
        """Keep proxy HTTP outside a checked-out database connection."""
        query, score, minimum, maximum, include, exclude, sort = key
        params = {"score": score, "query": query}
        clauses = ["public.story_search_eligible(i)", "i.score >= :score"]
        for column, value, op, name in (
            ("day", minimum, ">=", "minimum"),
            ("day", maximum, "<=", "maximum"),
        ):
            if value is not None:
                clauses.append(f"i.{column} {op} :{name}")
                params[name] = value
        for name, values, negate in (
            ("include", include, ""),
            ("exclude", exclude, "NOT"),
        ):
            if values:
                clauses.append(
                    f"{negate} (regexp_replace(lower(coalesce(i.domain,'')), '^www\\.', '') = ANY(CAST(:{name} AS text[])))"
                )
                params[name] = list(values)
        where = " AND ".join(clauses)
        vector = None
        mode = "browse"
        if query:
            with self._engine.connect() as conn:
                recipe = conn.execute(
                    text(
                        "SELECT obj_description('public.story_search'::regclass, 'pg_class')"
                    )
                ).scalar_one()
            assert recipe, "story_search has no embedding recipe"
            try:
                vector = embed_query(self.embedding_base_url, query, recipe)
                mode = "hybrid"
            except (URLError, TimeoutError, ValueError, HTTPException) as exc:
                logging.getLogger(__name__).warning(
                    "Query embedding failed; keyword-only search: %s", exc
                )
                mode = "keyword-only"
        with self._engine.connect() as conn:
            conn.execute(text("SET LOCAL statement_timeout='30s'"))
            conn.execute(text("SET LOCAL hnsw.ef_search=1000"))
            if not query:
                order = (
                    "i.day DESC,i.id DESC"
                    if sort == "date"
                    else "i.score DESC,i.id DESC"
                )
                ids = list(
                    conn.execute(
                        text(
                            f"SELECT i.id FROM public.story_search s JOIN public.items i ON i.id=s.story_id WHERE {where} ORDER BY {order} LIMIT 200"
                        ),
                        params,
                    ).scalars()
                )
                return Ranking(monotonic(), ids, mode, {})
            dense = []
            if vector is not None:
                params["vector"] = vector
                filtered = score > 25 or minimum or maximum or include or exclude
                if filtered:
                    # Materialization guarantees filtering precedes exact distance work.
                    dense_sql = f"""WITH eligible AS MATERIALIZED (
                        SELECT s.story_id,s.embedding FROM public.story_search s
                        JOIN public.items i ON i.id=s.story_id
                        WHERE s.embedding IS NOT NULL AND {where})
                        SELECT story_id FROM eligible
                        ORDER BY embedding <=> CAST(:vector AS halfvec(1024)),story_id LIMIT 100"""
                else:
                    dense_sql = """WITH candidates AS MATERIALIZED (
                        SELECT story_id,embedding <=> CAST(:vector AS halfvec(1024)) AS distance
                        FROM public.story_search WHERE embedding IS NOT NULL
                        ORDER BY embedding <=> CAST(:vector AS halfvec(1024)) LIMIT 100)
                        SELECT c.story_id FROM candidates c JOIN public.items i ON i.id=c.story_id
                        WHERE public.story_search_eligible(i) ORDER BY c.distance,c.story_id"""
                dense = list(conn.execute(text(dense_sql), params).scalars())
            lexical_sql = f"""WITH candidates AS MATERIALIZED (
                SELECT s.story_id,s.title <@> to_bm25query(:query,'story_search_title_bm25') AS distance
                FROM public.story_search s JOIN public.items i ON i.id=s.story_id
                WHERE {where}
                ORDER BY s.title <@> to_bm25query(:query,'story_search_title_bm25') LIMIT 100)
                SELECT story_id FROM candidates WHERE distance < 0 ORDER BY distance,story_id"""
            lexical = list(conn.execute(text(lexical_sql), params).scalars())
            ranks = fuse(dense, lexical)
            ids = sorted(
                ranks, key=lambda story_id: (-ranks[story_id]["rrf"], story_id)
            )
            if ids and sort != "relevance":
                order = (
                    "i.score DESC NULLS LAST,i.id DESC"
                    if sort == "score"
                    else "i.day DESC NULLS LAST,i.id DESC"
                )
                ids = list(
                    conn.execute(
                        text(
                            f"SELECT i.id FROM public.items i WHERE i.id = ANY(CAST(:ids AS bigint[])) ORDER BY {order}"
                        ),
                        {"ids": ids},
                    ).scalars()
                )
        return Ranking(monotonic(), ids, mode, ranks)

    def top_stories_for_date(self, target_date, *, limit=8):
        return self.search_stories(
            None, min_date=target_date, max_date=target_date, limit=limit, sort="score"
        )
