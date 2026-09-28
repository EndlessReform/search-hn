"""Retrieve initial and repair candidates with a fixed title pool."""

import argparse
import json
import multiprocessing
import os
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from catalog import Catalog
from search_research.resolver_counts import COUNTS_PATH

ROOT = Path(os.environ.get("RESOLVER_RUN_ROOT", "data/research/books-resolver-heal-v1"))
DEFAULT_CATALOG = Path("data/probes/books-catalog-tantivy-v1")
_worker_catalog = None


def initialize_worker(path, title_pool, counts_path):
    """Open native index handles and frozen counts once in each spawned worker."""
    global _worker_catalog
    _worker_catalog = Catalog(path, title_pool, counts_path=counts_path)


def search_query(query):
    """Pass only query inputs and search results across process boundaries."""
    assert _worker_catalog is not None, "Retrieval worker was not initialized"
    return _worker_catalog.search(*query)


def search_cases(cases, path, title_pool, workers, counts_path=COUNTS_PATH):
    """Yield results in input order, regardless of worker completion order.

    Spawn avoids inheriting native Tantivy/DuckDB thread state. Workers own their
    counts dictionaries; the OS shares memory-mapped index pages. One query per
    task balances expensive pools against quick searches. Exceptions abort the
    stage before output publication. Empty repair rounds start no workers.
    """
    assert workers > 0, "Retrieval workers must be positive"
    if not cases:
        return
    queries = ((c["query_title"], c["query_author"], c["person_spans"]) for c in cases)
    if workers == 1:
        catalog = Catalog(path, title_pool, counts_path=counts_path)
        for query in queries:
            yield catalog.search(*query)
        return
    with ProcessPoolExecutor(
        max_workers=min(workers, len(cases)),
        mp_context=multiprocessing.get_context("spawn"),
        initializer=initialize_worker,
        initargs=(path, title_pool, counts_path),
    ) as pool:
        yield from pool.map(search_query, queries, chunksize=1)


def initial_cases(root):
    """Attach complete person NER to references without a catalog-wide join."""
    refs = json.loads((root / "references.json").read_text())
    with (root / "names.jsonl").open() as source:
        names = {row["comment_id"]: row for line in source if (row := json.loads(line))}
    assert {r["comment_id"] for r in refs} <= names.keys(), "NER is incomplete"
    return [
        {
            "id": r["id"],
            "reference": r,
            "query_title": r["title"],
            "query_author": None,
            "person_spans": names[r["comment_id"]]["spans"],
        }
        for r in refs
    ]


def main():
    """Preserve repair provenance while replacing global author joins with lookup.

    The 10,000-title pool is a measured recall/latency choice. The output retains
    the original candidate schema, so reranking and paid request schemas do not
    change. Per-query counters expose time, candidate volume, and pool bounds.
    """
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--round", type=int)
    mode.add_argument("--stage", choices=["first"])
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--title-pool", type=int, default=10_000)
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()
    assert args.workers > 0, "Retrieval workers must be positive"
    started = time.monotonic()
    stage = "first" if args.stage else f"round{args.round}"
    cases = (
        initial_cases(ROOT)
        if args.stage
        else json.loads((ROOT / f"{stage}-queries.json").read_text())["cases"]
    )
    assert (args.catalog / "build.json").exists(), "Catalog build is incomplete"
    metrics = []
    results = search_cases(cases, args.catalog, args.title_pool, args.workers)
    for number, (case, (candidates, measured)) in enumerate(
        zip(cases, results, strict=True), 1
    ):
        case["candidates"] = candidates
        case["retrieved"] = [
            {k: v for k, v in candidate.items() if k not in ("title", "authors")}
            for candidate in candidates
        ]
        case["author_bonus"] = 5.0
        metrics.append({"id": case["id"], **measured})
        if number % 50 == 0:
            print(
                f"Retrieved {number}/{len(cases)} in {time.monotonic() - started:.1f}s",
                flush=True,
            )
    for name, payload in (
        (f"{stage}-cases.json", {"cases": cases}),
        (
            f"{stage}-retrieval-metrics.json",
            {
                "catalog": str(args.catalog),
                "title_pool": args.title_pool,
                "workers": min(args.workers, len(cases)),
                "seconds": time.monotonic() - started,
                "queries": metrics,
            },
        ),
    ):
        target = ROOT / name
        temporary = target.with_suffix(".json.tmp")
        with temporary.open("w") as output:
            json.dump(payload, output)
        temporary.replace(target)
    print(f"Retrieved all {len(cases)} {stage} queries", flush=True)


if __name__ == "__main__":
    main()
