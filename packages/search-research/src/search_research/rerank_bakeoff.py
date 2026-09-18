"""Laptop-owned, durable static reranker experiment; only inference uses SSH.

Score the union of three top-50 candidate sets once per question, then slice
each original ranking before reranking. The top-30 condition never sees the
extra candidates. No target label is sent to the worker. A single known-item
label means recall@k is question success and NDCG@k is discounted target rank.
"""

import argparse
import json
import math
import os
import shlex
import subprocess
import time
from pathlib import Path

import polars as pl

from search_research.fusion_sweep import weighted_fuse

BASE = Path("data/pg-duckdb-bakeoff-20260904")
EMBED = Path("data/te3-large-baseline-20260904")
OUT = BASE / "reranker"


def candidates():
    """Reconstruct exactly the 1536D, lexical-weight-0.5 static comparisons."""
    source = pl.read_parquet(BASE / "comparison.parquet")
    lexical = {
        r["case"]: r["ids"]
        for r in source.filter(
            (pl.col("engine") == "duckdb") & (pl.col("method") == "lexical")
        ).to_dicts()
    }
    dense = source.filter(
        (pl.col("engine") == "duckdb")
        & (pl.col("method") == "dense")
        & (pl.col("dimensions") == 1536)
    )
    pg = {
        r["case"]: r["ids"]
        for r in pl.read_parquet(BASE / "textsearch.parquet")
        .filter(
            (pl.col("variant") == "title-only")
            & (pl.col("method") == "hybrid")
            & (pl.col("dimensions") == 1536)
            & (pl.col("lexical_weight") == 0.5)
        )
        .to_dicts()
    }
    return {
        r["case"]: {
            "dense": r["ids"][:50],
            "pg-hybrid": pg[r["case"]][:50],
            "duckdb-hybrid": weighted_fuse(r["ids"], lexical[r["case"]], 0.5)[:50],
        }
        for r in dense.to_dicts()
    }


def append(path, row):
    """Flush every response on the laptop's persistent filesystem."""
    with path.open("a") as handle:
        handle.write(json.dumps(row) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def rerank(original, scores, pool):
    """Restrict candidates before scoring order; preserve retrieval ties."""
    selected = original[:pool]
    assert len(selected) == len(set(selected)), "Duplicate candidate"
    assert all(math.isfinite(scores[id]) for id in selected)
    return sorted(selected, key=lambda id: -scores[id])


def report():
    """Produce cutoff metrics and paired wins/losses from checkpointed scores."""
    rankings = candidates()
    questions = {
        r["case"]: r for r in pl.read_parquet(EMBED / "questions.parquet").to_dicts()
    }
    rows = []
    for line in (OUT / "scores.jsonl").read_text().splitlines():
        record = json.loads(line)
        scores = dict(zip(record["ids"], record["scores"], strict=True))
        case = record["case"]
        target = questions[case]["target_id"]
        for method, original in rankings[case].items():
            for pool in [30, 50]:
                selected = original[:pool]
                reranked = rerank(original, scores, pool)
                for mode, ids in [("original", selected), ("reranked", reranked)]:
                    rank = ids.index(target) + 1 if target in ids else None
                    for k in [1, 3, 5, 8, 10, 12, 16, 20]:
                        hit = rank is not None and rank <= k
                        rows.append(
                            {
                                "case": case,
                                "method": method,
                                "pool": pool,
                                "mode": mode,
                                "k": k,
                                "rank": rank,
                                "recall": float(hit),
                                "ndcg": 1 / math.log2(rank + 1) if hit else 0.0,
                                "candidate_hit": float(target in selected),
                            }
                        )
    frame = pl.DataFrame(rows)
    frame.write_parquet(OUT / "metrics.parquet")
    summary = (
        frame.group_by("method", "pool", "mode", "k")
        .agg(pl.len().alias("n"), pl.col("recall", "ndcg", "candidate_hit").mean())
        .sort("method", "pool", "mode", "k")
    )
    summary.write_csv(OUT / "summary.csv")
    print(summary.filter(pl.col("k").is_in([5, 8, 20])))


def run(host):
    """Keep orchestration/data local and pipe inference requests to one worker."""
    OUT.mkdir(exist_ok=True)
    rankings = candidates()
    questions = pl.read_parquet(EMBED / "questions.parquet").to_dicts()
    docs = {
        r["id"]: r["input"]
        for r in pl.read_parquet(EMBED / "corpus.parquet").to_dicts()
    }
    done = (
        {
            json.loads(line)["case"]
            for line in (OUT / "scores.jsonl").read_text().splitlines()
        }
        if (OUT / "scores.jsonl").exists()
        else set()
    )
    worker = Path(__file__).with_name("rerank_worker.py").read_text()
    command = (
        "uv run --no-project --with 'torch==2.10.0' --with 'transformers==4.57.6' python -u -c "
        + shlex.quote(worker)
    )
    with (OUT / "worker.log").open("a") as log:
        process = subprocess.Popen(
            ["ssh", host, command],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
        )
        try:
            ready = json.loads(process.stdout.readline())
            assert ready["ready"]
            append(OUT / "runtime.jsonl", ready)
            print(json.dumps(ready), flush=True)

            def request(pairs, batch):
                start = time.perf_counter()
                process.stdin.write(
                    json.dumps({"pairs": pairs, "batch_size": batch}) + "\n"
                )
                process.stdin.flush()
                result = json.loads(process.stdout.readline())
                result["roundtrip_ms"] = (time.perf_counter() - start) * 1000
                assert len(result["scores"]) == len(pairs)
                assert all(math.isfinite(s) for s in result["scores"])
                return result

            sample = [
                [q["input"], docs[id]]
                for q in questions[:8]
                for id in rankings[q["case"]]["dense"]
            ][:256]
            request(sample[:8], 8)  # Exclude cold-start warmup from the sweep.
            timings = []
            for batch in [8, 16, 32, 64, 128]:
                for repeat in range(3):
                    result = request(sample, batch)
                    row = {
                        "kind": "throughput",
                        "batch": batch,
                        "repeat": repeat,
                        "pairs": len(sample),
                        **result,
                    }
                    append(OUT / "timings.jsonl", row)
                    timings.append(row)
                print(
                    f"Batch {batch}: {result['milliseconds']:.0f} ms / 256 pairs; peak {result['peak_allocated_mb']:.0f} MiB",
                    flush=True,
                )
            batch = int(
                pl.DataFrame(timings)
                .group_by("batch")
                .agg(pl.col("milliseconds").median())
                .sort("milliseconds")["batch"][0]
            )
            for pool in [30, 50]:
                for q in questions[:10]:
                    pairs = [
                        [q["input"], docs[id]]
                        for id in rankings[q["case"]]["pg-hybrid"][:pool]
                    ]
                    result = request(pairs, batch)
                    append(
                        OUT / "timings.jsonl",
                        {
                            "kind": "single-query",
                            "pool": pool,
                            "case": q["case"],
                            "batch": batch,
                            **result,
                        },
                    )
            for i, q in enumerate(questions):
                if q["case"] in done:
                    continue
                ids = sorted(set().union(*rankings[q["case"]].values()))
                result = request([[q["input"], docs[id]] for id in ids], batch)
                append(
                    OUT / "scores.jsonl",
                    {
                        "case": q["case"],
                        "ids": ids,
                        "batch": batch,
                        "revision": ready["revision"],
                        **result,
                    },
                )
                if (i + 1) % 10 == 0:
                    print(
                        f"Checkpointed {i + 1}/{len(questions)} questions", flush=True
                    )
        finally:
            process.stdin.close()
            process.wait(timeout=30)
    report()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="melchior")
    parser.add_argument("--report-only", action="store_true")
    args = parser.parse_args()
    report() if args.report_only else run(args.host)
