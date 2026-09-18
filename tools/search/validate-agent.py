"""Read-only production agent checks with durable JSON evidence.

Run with uv run --package search-agent python tools/search/validate-agent.py.
DATABASE_URL and EMBEDDING_BASE_URL are required; credentials are never recorded.
The exact vector from the agent's HTTP call is reused in the SQL prototype check,
so inference nondeterminism does not masquerade as a ranking regression.
"""

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from unittest.mock import patch

from sqlalchemy import event, text

from search_agent.production_embeddings import embed_query
from search_agent.production_search import ProductionStoryRepository
from search_agent.tools.fetch_stories import build_fetch_stories_payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--plans", type=Path, help="Save EXPLAIN JSON plans (without executing again)"
    )
    args = parser.parse_args()
    repo = ProductionStoryRepository(
        os.environ["DATABASE_URL"], os.environ["EMBEDDING_BASE_URL"]
    )
    evidence = {"checked_at": datetime.now(UTC).isoformat(), "checks": []}
    vector = None
    sql_timings = []
    sql_statements = []
    plans = []
    sql_started = 0.0

    @event.listens_for(repo._engine, "before_cursor_execute")
    def before_sql(conn, cursor, statement, parameters, context, executemany):
        nonlocal sql_started
        sql_started = perf_counter()

    @event.listens_for(repo._engine, "after_cursor_execute")
    def after_sql(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().startswith(("WITH", "SELECT")):
            sql_statements.append((statement, dict(parameters)))
        sql_timings.append(
            {
                "sql": statement,
                "elapsed_ms": round((perf_counter() - sql_started) * 1000, 2),
            }
        )

    def capture(*args):
        nonlocal vector
        vector = embed_query(*args)
        return vector

    specs = [
        {"query": "How to build a programming language compiler"},
        {"query": "Solar panels and home battery energy storage", "min_score": 500},
        {
            "query": "Growing vegetables with hydroponics indoors",
            "min_date": "2024-01-01",
            "max_date": "2026-09-07",
        },
        {"query": "database", "include_domains": ["github.com"]},
        {
            "query": "Ocean exploration and deep sea submarines",
            "exclude_domains": ["youtube.com"],
            "sort": "date",
        },
        {"min_date": "2026-09-06", "max_date": "2026-09-06"},
    ]
    try:
        with patch("search_agent.production_search.embed_query", capture):
            for spec in specs:
                sql_timings.clear()
                sql_statements.clear()
                start = perf_counter()
                payload = build_fetch_stories_payload(repo, **spec)
                record = {
                    "request": spec,
                    "elapsed_ms": round((perf_counter() - start) * 1000, 2),
                    "response": payload,
                }
                evidence["checks"].append(record)
                record["sql_timings"] = list(sql_timings)
                if args.plans:
                    statements = list(sql_statements)
                    with repo._engine.connect() as conn:
                        conn.execute(text("SET LOCAL hnsw.ef_search=1000"))
                        for statement, parameters in statements:
                            if (
                                "story_search" not in statement
                                or "obj_description" in statement
                            ):
                                continue
                            plan = conn.exec_driver_sql(
                                "EXPLAIN (FORMAT JSON) " + statement, parameters
                            ).scalar_one()
                            plans.append(
                                {"request": spec, "sql": statement, "plan": plan}
                            )
                print(
                    json.dumps({"request": spec, "elapsed_ms": record["elapsed_ms"]}),
                    flush=True,
                )
                results = payload["results"]
                assert payload["retrieval_mode"] == (
                    "hybrid" if spec.get("query") else "browse"
                )
                assert results, f"Expected results for {spec}"
                for result in results:
                    assert result["score"] >= spec.get("min_score", 25)
                    if "min_date" in spec:
                        assert result["date"] >= spec["min_date"]
                    if "max_date" in spec:
                        assert result["date"] <= spec["max_date"]
                ids = [r["id"] for r in results]
                with repo._engine.connect() as conn:
                    rows = (
                        conn.execute(
                            text(
                                "SELECT id,regexp_replace(lower(coalesce(domain,'')), '^www\\.', '') AS domain FROM items WHERE id=ANY(CAST(:ids AS bigint[]))"
                            ),
                            {"ids": ids},
                        )
                        .mappings()
                        .all()
                    )
                    for row in rows:
                        if "include_domains" in spec:
                            assert row["domain"] in spec["include_domains"]
                        if "exclude_domains" in spec:
                            assert row["domain"] not in spec["exclude_domains"]
                if spec == specs[0]:
                    assert vector is not None
                    sql = (
                        Path("tools/search/hybrid-query.sql")
                        .read_text()
                        .replace(":'query_vector'", ":query_vector")
                        .replace(":'query_text'", ":query_text")
                    )
                    # SQLAlchemy named binds need CAST rather than adjacent :: casts.
                    sql = sql.replace(
                        ":query_vector::halfvec(1024)",
                        "CAST(:query_vector AS halfvec(1024))",
                    )
                    with repo._engine.connect() as conn:
                        conn.execute(text("SET LOCAL statement_timeout='30s'"))
                        conn.execute(text("SET LOCAL hnsw.ef_search=1000"))
                        reference = (
                            conn.execute(
                                text(sql),
                                {"query_vector": vector, "query_text": spec["query"]},
                            )
                            .mappings()
                            .all()
                        )
                    assert ids[:10] == [r["story_id"] for r in reference]
                    for actual, expected in zip(results, reference):
                        assert actual["dense_rank"] == expected["dense_rank"]
                        assert actual["bm25_rank"] == expected["bm25_rank"]
                        assert abs(actual["rrf"] - float(expected["rrf"])) < 0.00000051
                    record["prototype_top10_and_rrf_match"] = True
                    page2 = build_fetch_stories_payload(repo, **spec, page=2)
                    page3 = build_fetch_stories_payload(repo, **spec, page=3)
                    all_ids = ids + [
                        r["id"] for p in (page2, page3) for r in p["results"]
                    ]
                    assert len(all_ids) == len(set(all_ids))
                    record["three_pages_disjoint"] = True
        repo.reset_session()
        with patch(
            "search_agent.production_search.embed_query",
            side_effect=TimeoutError("validation outage"),
        ):
            fallback = build_fetch_stories_payload(repo, query="compiler")
            assert fallback["retrieval_mode"] == "keyword-only" and fallback["results"]
            evidence["fallback"] = {
                "mode": fallback["retrieval_mode"],
                "results": len(fallback["results"]),
            }
        evidence["passed"] = True
    finally:
        repo.dispose()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(evidence, indent=2) + "\n")
        if args.plans:
            args.plans.parent.mkdir(parents=True, exist_ok=True)
            args.plans.write_text(json.dumps(plans, indent=2) + "\n")
    print(
        json.dumps(
            {
                "passed": True,
                "output": str(args.output),
                "timings_ms": [r["elapsed_ms"] for r in evidence["checks"]],
            }
        )
    )


if __name__ == "__main__":
    main()
