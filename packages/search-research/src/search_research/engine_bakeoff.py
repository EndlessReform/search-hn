"""Run six-dimension exact retrieval and frozen trajectory replay in both DBs."""

import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

import polars as pl
from search_agent.journal import Journal

from search_research.dataset import read_jsonl
from search_research.embedding_baseline import shortened
from search_research.engine_backends import Duck, Postgres, timed
from search_research.engine_data import BASE, DIMS, OUT, TRACES, load_vectors
from search_research.hybrid_baseline import fuse, measurements


def corpus_data():
    corpus = pl.read_parquet(BASE / "corpus.parquet")
    return corpus.with_columns(
        pl.Series(
            "domain",
            [
                (urlparse(u or "").hostname or "").lower().removeprefix("www.")
                for u in corpus["url"]
            ],
        )
    )


def summarize(path, destination, grouping):
    """Refresh compact reports from the durable append-only per-query journal."""
    frame = pl.DataFrame(read_jsonl(path))
    frame.write_parquet(destination.with_suffix(".parquet"))
    metrics = [c for c in frame.columns if "@" in c]
    frame.group_by(grouping).agg(
        pl.len().alias("n"),
        pl.col(metrics).mean(),
        pl.col("milliseconds").median().alias("median_ms"),
        pl.col("milliseconds").quantile(0.95).alias("p95_ms"),
    ).sort(grouping).write_csv(destination.with_suffix(".csv"))


def static():
    """Same questions, all dimensions, top-100 candidates and equal RRF k=60."""
    corpus = corpus_data()
    questions = pl.read_parquet(BASE / "questions.parquet")
    documents = load_vectors(BASE, "documents", corpus)
    vectors = load_vectors(BASE, "queries", questions)
    path = OUT / "static.jsonl"
    previous = read_jsonl(path) if path.exists() else []
    done = {(r["engine"], r["method"], r["dimensions"], r["case"]) for r in previous}
    lexical = {
        (r["engine"], r["case"]): (r["ids"], r["milliseconds"])
        for r in previous
        if r["method"] == "lexical"
    }
    pg, duck = Postgres(), Duck(corpus)
    journal = Journal(path)

    def record(q, engine, method, dim, ids, ms):
        key = (engine, method, dim, q["case"])
        if key in done:
            return
        rank = ids.index(q["target_id"]) + 1 if q["target_id"] in ids else None
        journal.write(
            "result",
            engine=engine,
            ids=ids,
            milliseconds=ms,
            **measurements(q, method, dim, rank),
        )
        done.add(key)

    try:
        for engine, backend in [("pg", pg), ("duckdb", duck)]:
            for q in questions.to_dicts():
                if (engine, q["case"]) not in lexical:
                    ids, ms = timed(backend.lexical, q["input"])
                    lexical[engine, q["case"]] = ids, ms
                    record(q, engine, "lexical", 0, ids, ms)
            print(json.dumps({"lexical_complete": engine}), flush=True)
        for dim in DIMS:
            duck.set_vectors(shortened(documents, dim), dim, corpus["id"])
            query_vectors = shortened(vectors, dim)
            for i, q in enumerate(questions.to_dicts()):
                for engine, backend in [("pg", pg), ("duckdb", duck)]:
                    if (engine, "hybrid", dim, q["case"]) in done:
                        continue
                    ids, ms = timed(backend.dense, query_vectors[i], dim)
                    record(q, engine, "dense", dim, ids, ms)
                    lex, lex_ms = lexical[engine, q["case"]]
                    record(q, engine, "hybrid", dim, fuse(ids, lex), ms + lex_ms)
                if (i + 1) % 25 == 0:
                    print(
                        json.dumps(
                            {
                                "dimension": dim,
                                "questions": i + 1,
                                "total": questions.height,
                            }
                        ),
                        flush=True,
                    )
            summarize(path, OUT / "static-summary", ["engine", "method", "dimensions"])
    finally:
        journal.close()
        pg.close()
        duck.close()


def replay():
    """Replace each consumed search list, preserving order and metadata filters.

    K counts individual lists, including alternatives inside a batch, matching
    the existing query_pass@K convention. Raw Qwen array strings stay untouched.
    Date-only/top searches use score ordering in both engines, not embeddings.
    Cache identical query+filter requests, independent of their target story.
    """
    corpus = corpus_data()
    inputs = pl.read_parquet(OUT / "trajectory-inputs.parquet")
    vectors = shortened(load_vectors(OUT, "queries", inputs), 1536)
    lookup = dict(zip(inputs["input"], vectors))
    documents = shortened(load_vectors(BASE, "documents", corpus), 1536)
    queries = pl.read_parquet(TRACES / "queries.parquet").to_dicts()
    path = OUT / "replay.jsonl"
    previous = read_jsonl(path) if path.exists() else []
    done = {
        (r["model"], r["case"], r["query_number"], r["engine"], r["method"])
        for r in previous
    }
    cache = {}
    # Selected by the recorded 12-setting lexical sweep, not a held-out result.
    pg, duck = Postgres(rank_function="ts_rank", normalization=1), Duck(corpus)
    duck.set_vectors(documents, 1536, corpus["id"])
    journal = Journal(path)
    executor = ThreadPoolExecutor(max_workers=2)

    def retrieve(backend, query, args):
        if query.strip():
            lex, lm = timed(backend.lexical, query, args)
            dense, dm = timed(backend.dense, lookup[query], 1536, args)
        else:
            lex, lm = timed(backend.top, args)
            dense, dm = lex, lm
        return [
            ("lexical", lex, lm),
            ("dense", dense, dm),
            ("hybrid", fuse(dense, lex), dm + lm),
        ]

    try:
        for i, q in enumerate(queries):
            args = json.loads(q["arguments"])
            args = {
                k: v
                for k, v in args.items()
                if k not in ("query", "limit") and v is not None
            }
            if q["tool"] == "fetch_top_stories_for_date":
                args = {
                    "min_date": args["target_date"],
                    "max_date": args["target_date"],
                }
            query = q["query"] or ""
            pending = {}
            for engine, backend in [("pg", pg), ("duckdb", duck)]:
                key = (engine, query, json.dumps(args, sort_keys=True))
                if (
                    key not in cache
                    and (q["model"], q["case"], q["query_number"], engine, "hybrid")
                    not in done
                ):
                    pending[key] = executor.submit(retrieve, backend, query, args)
            for engine, backend in [("pg", pg), ("duckdb", duck)]:
                if (q["model"], q["case"], q["query_number"], engine, "hybrid") in done:
                    continue
                cache_key = (engine, query, json.dumps(args, sort_keys=True))
                if cache_key not in cache:
                    cache[cache_key] = pending[cache_key].result()
                for method, ids, ms in cache[cache_key]:
                    key = (q["model"], q["case"], q["query_number"], engine, method)
                    if key in done:
                        continue
                    rank = (
                        ids.index(q["target_id"]) + 1 if q["target_id"] in ids else None
                    )
                    journal.write(
                        "result",
                        model=q["model"],
                        case=q["case"],
                        target_id=q["target_id"],
                        query_number=q["query_number"],
                        query=query,
                        arguments=args,
                        tool=q["tool"],
                        engine=engine,
                        method=method,
                        dimensions=1536,
                        rank=rank,
                        ids=ids,
                        milliseconds=ms,
                    )
                    done.add(key)
            if (i + 1) % 50 == 0:
                print(
                    json.dumps({"replayed_lists": i + 1, "total": len(queries)}),
                    flush=True,
                )
    finally:
        executor.shutdown(wait=True)
        journal.close()
        pg.close()
        duck.close()
    replay_report()


def replay_report():
    """Denominator includes all 196 cases/model, including no-search failures."""
    frame = pl.DataFrame(read_jsonl(OUT / "replay.jsonl"))
    frame.write_parquet(OUT / "replay.parquet")
    universe = pl.read_parquet(TRACES / "metrics.parquet").select("model", "case")
    rows = []
    for engine in ["pg", "duckdb"]:
        for method in ["lexical", "dense", "hybrid"]:
            subset = frame.filter(
                (pl.col("engine") == engine) & (pl.col("method") == method)
            )
            for cutoff in [8, 20]:
                for budget in [1, 3, 5, 10, 20, 100000]:
                    hits = (
                        subset.filter(
                            (pl.col("rank") <= cutoff)
                            & (pl.col("query_number") <= budget)
                        )
                        .select("model", "case")
                        .unique()
                        .with_columns(pl.lit(True).alias("hit"))
                    )
                    stats = (
                        universe.join(hits, on=["model", "case"], how="left")
                        .with_columns(pl.col("hit").fill_null(False))
                        .group_by("model")
                        .agg(
                            pl.len().alias("n"),
                            pl.col("hit").sum().alias("hits"),
                            pl.col("hit").mean().alias("pass"),
                        )
                    )
                    rows.extend(
                        {
                            **r,
                            "engine": engine,
                            "method": method,
                            "cutoff": cutoff,
                            "search_budget": budget,
                        }
                        for r in stats.to_dicts()
                    )
    pl.DataFrame(rows).sort(
        "cutoff", "search_budget", "model", "engine", "method"
    ).write_csv(OUT / "replay-summary.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["static", "replay", "report"])
    action = parser.parse_args().action
    {"static": static, "replay": replay, "report": replay_report}[action]()
