"""Small explicit adapters: identical corpus, candidate depth, filters and ties."""

import json
import time

import duckdb
import polars as pl
import psycopg2

from search_research.engine_data import BASE, DSN, OUT


def filters(args, dialect):
    """Render only known scalar filters; query text is always parameterized."""
    marker = "%s" if dialect == "pg" else "?"
    clauses, params = [], []
    for key, column, op in [
        ("min_score", "score", ">="),
        ("min_date", "day", ">="),
        ("max_date", "day", "<="),
    ]:
        if args.get(key) is not None:
            clauses.append(f"d.{column} {op} {marker}")
            params.append(args[key])
    for key, negate in [("include_domains", False), ("exclude_domains", True)]:
        if args.get(key):
            from search_agent.tools.utils import normalize_domains

            domains = normalize_domains(args[key])
            clauses.append(
                f"d.domain {'NOT IN' if negate else 'IN'} ({','.join([marker] * len(domains))})"
            )
            params.extend(domains)
    return (" AND " + " AND ".join(clauses) if clauses else ""), params


class Postgres:
    """Exact float32 cosine plus English OR / cover-density lexical retrieval."""

    def __init__(self, rank_function="ts_rank_cd", normalization=32):
        assert rank_function in ("ts_rank", "ts_rank_cd")
        assert normalization in (0, 1, 2, 8, 10, 16, 32)
        self.rank_function = rank_function
        self.normalization = normalization
        self.conn = psycopg2.connect(DSN)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        self.cur.execute("SET statement_timeout='120s'")

    def lexical(self, query, args=None):
        where, params = filters(args or {}, "pg")
        self.cur.execute(
            "SELECT tsvector_to_array(to_tsvector('english',%s))", (query,)
        )
        lexemes = self.cur.fetchone()[0]
        if not lexemes:
            return []
        # quote_literal safely handles arbitrary lexemes and produces valid
        # tsquery tokens, unlike replacing spaces/operators in raw user input.
        self.cur.execute(
            "SELECT string_agg(quote_literal(x),' | ')::tsquery FROM unnest(%s::text[]) x",
            (lexemes,),
        )
        tsquery = self.cur.fetchone()[0]
        self.cur.execute(
            f"SELECT d.id FROM documents d WHERE d.tsv @@ %s::tsquery {where} ORDER BY {self.rank_function}(d.tsv,%s::tsquery,{self.normalization}) DESC,d.id LIMIT 100",
            [tsquery, *params, tsquery],
        )
        return [r[0] for r in self.cur.fetchall()]

    def dense(self, vector, dim, args=None):
        where, params = filters(args or {}, "pg")
        literal = json.dumps(vector.tolist(), separators=(",", ":"))
        self.cur.execute(
            # +0 prevents the approximate index ordering from satisfying this
            # exact baseline, without the parallelism barrier of a CTE.
            f"SELECT v.id FROM vectors_{dim} v JOIN documents d USING(id) WHERE true {where} ORDER BY (v.embedding <=> %s::vector)+0, v.id LIMIT 100",
            [*params, literal],
        )
        return [r[0] for r in self.cur.fetchall()]

    def top(self, args):
        where, params = filters(args, "pg")
        self.cur.execute(
            f"SELECT d.id FROM documents d WHERE true {where} ORDER BY score DESC NULLS LAST,day DESC NULLS LAST,id DESC LIMIT 100",
            params,
        )
        return [r[0] for r in self.cur.fetchall()]

    def close(self):
        self.conn.close()


class Duck:
    """Native DuckDB exact cosine, alongside the existing persisted BM25 index."""

    def __init__(self, corpus):
        self.conn = duckdb.connect(str(OUT / "native.duckdb"))
        self.conn.execute("SET memory_limit='4GB'; SET threads=4; LOAD fts")
        # Reuse exactly the existing Porter/tokenization/index configuration.
        self.lex = duckdb.connect(str(BASE / "bm25.duckdb"), read_only=True)
        self.lex.execute("SET memory_limit='2GB'; SET threads=4; LOAD fts")
        self.lex.register("metadata", corpus.to_arrow())
        self.conn.register("corpus_input", corpus.to_arrow())
        self.conn.execute(
            "CREATE OR REPLACE TABLE documents AS SELECT * FROM corpus_input"
        )

    def set_vectors(self, matrix, dim, ids):
        frame = pl.DataFrame(
            {"id": ids, "embedding": pl.Series(matrix, dtype=pl.Array(pl.Float32, dim))}
        )
        self.conn.register("vector_input", frame.to_arrow())
        self.conn.execute(
            "CREATE OR REPLACE TEMP TABLE vectors AS SELECT * FROM vector_input"
        )
        self.conn.unregister("vector_input")

    def lexical(self, query, args=None):
        where, params = filters(args or {}, "duck")
        return [
            r[0]
            for r in self.lex.execute(
                f"SELECT d.id,fts_main_documents.match_bm25(d.id,?,k:=1.2,b:=0.75,conjunctive:=false) AS relevance FROM metadata d WHERE relevance IS NOT NULL {where} ORDER BY relevance DESC,d.id LIMIT 100",
                [query, *params],
            ).fetchall()
        ]

    def dense(self, vector, dim, args=None):
        where, params = filters(args or {}, "duck")
        return [
            r[0]
            for r in self.conn.execute(
                f"SELECT v.id FROM vectors v JOIN documents d USING(id) WHERE true {where} ORDER BY array_cosine_similarity(v.embedding,?::FLOAT[{dim}]) DESC,v.id LIMIT 100",
                [*params, vector.tolist()],
            ).fetchall()
        ]

    def top(self, args):
        where, params = filters(args, "duck")
        return [
            r[0]
            for r in self.conn.execute(
                f"SELECT d.id FROM documents d WHERE true {where} ORDER BY score DESC NULLS LAST,day DESC NULLS LAST,id DESC LIMIT 100",
                params,
            ).fetchall()
        ]

    def close(self):
        self.conn.close()
        self.lex.close()


def timed(fn, *args):
    start = time.perf_counter()
    result = fn(*args)
    return result, (time.perf_counter() - start) * 1000
