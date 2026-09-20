"""Bounded Linux storage comparison; does not change the backfill writer.

Use a fresh output directory for each case. Synthetic vectors are independent
uniform int8 values: useful for write-volume comparison, not a compression model
for real embeddings. Timings exclude source generation and include final close.
/proc/self/io reports filesystem I/O accounting, not physical NAND writes.
"""

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb
import httpx
import numpy as np
import polars as pl
import pyarrow as pa
from search_agent.journal import Journal
from search_research.vllm_transport import encode


def source(args):
    """Materialize bounded inputs before timing storage operations."""
    if args.source:
        values = np.load(args.source, allow_pickle=False)
        assert values.dtype == np.int8 and values.shape[1] == 1024
        assert args.rows <= len(values)
        return values[: args.rows]
    return np.random.default_rng(20260919).integers(
        -127, 128, size=(args.rows, 1024), dtype=np.int8
    )


def arrow_batch(vectors, start, end):
    """Pass packed signed bytes to DuckDB without Python per-coordinate objects."""
    return pa.table(
        {
            "input_id": pa.array(np.arange(start, end, dtype=np.int64)),
            "embedding": pa.FixedSizeListArray.from_arrays(
                pa.array(vectors[start:end].reshape(-1), type=pa.int8()), 1024
            ),
        }
    )


def io_counters():
    return {
        key: int(value)
        for key, value in (
            line.split(":") for line in Path("/proc/self/io").read_text().splitlines()
        )
    }


def open_db(args):
    db = duckdb.connect(str(args.output / "vectors.duckdb"))
    if args.checkpoint_threshold:
        db.execute("SET checkpoint_threshold = ?", [args.checkpoint_threshold])
    db.execute(
        "CREATE TABLE vectors(input_id BIGINT PRIMARY KEY, embedding TINYINT[1024] NOT NULL)"
    )
    return db


def insert(db, vectors, start, end):
    batch = arrow_batch(vectors, start, end)
    db.register("incoming", batch)
    db.execute("INSERT INTO vectors SELECT * FROM incoming")
    db.unregister("incoming")


def verify_db(path, vectors):
    """Reopen and compare every coordinate and row ID, not just the row count."""
    db = duckdb.connect(str(path))
    rows = 0
    reader = db.execute("SELECT * FROM vectors ORDER BY input_id").to_arrow_reader(8192)
    for batch in reader:
        ids = batch.column(0).to_numpy()
        values = batch.column(1)
        assert pa.types.is_fixed_size_list(values.type)
        assert values.type.value_type == pa.int8()
        actual = values.flatten().to_numpy().reshape(-1, 1024)
        end = rows + len(ids)
        np.testing.assert_array_equal(ids, np.arange(rows, end))
        np.testing.assert_array_equal(actual, vectors[rows:end])
        rows = end
    assert rows == len(vectors), (rows, len(vectors))
    db.close()


def benchmark(args):
    vectors = source(args)
    args.output.mkdir(parents=True, exist_ok=False)
    before = io_counters()
    began = time.perf_counter()
    latencies = []
    wal_peak = 0
    db = open_db(args) if args.backend == "duckdb" else None
    journal = Journal(args.output / "batches.jsonl") if db is None else None
    threshold = (
        db.execute("SELECT current_setting('checkpoint_threshold')").fetchone()[0]
        if db
        else None
    )
    for start in range(0, len(vectors), args.commit_rows):
        end = min(start + args.commit_rows, len(vectors))
        tick = time.perf_counter()
        if db:
            db.execute("BEGIN")
            insert(db, vectors, start, end)
            db.execute("COMMIT")
            wal = args.output / "vectors.duckdb.wal"
            if wal.exists():
                wal_peak = max(wal_peak, wal.stat().st_size)
        else:
            target = args.output / f"{start:09d}-{end:09d}.npy"
            temporary = target.with_suffix(".partial")
            with temporary.open("wb") as stream:
                np.save(stream, vectors[start:end], allow_pickle=False)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.rename(target)
            journal.write("batch", start=start, end=end)
        latencies.append(time.perf_counter() - tick)
    ingest_seconds = time.perf_counter() - began
    tick = time.perf_counter()
    if db:
        db.execute("CHECKPOINT")
        db.close()
    else:
        journal.close()
    finalization_seconds = time.perf_counter() - tick
    after = io_counters()
    files = list(args.output.iterdir())
    result = {
        "backend": args.backend,
        "rows": len(vectors),
        "commit_rows": args.commit_rows,
        "source": "real" if args.source else "uniform_int8",
        "checkpoint_threshold": threshold,
        "payload_bytes": vectors.nbytes,
        "ingest_seconds": ingest_seconds,
        "finalization_seconds": finalization_seconds,
        "total_seconds": ingest_seconds + finalization_seconds,
        "commit_p50_seconds": float(np.median(latencies)),
        "commit_p95_seconds": float(np.quantile(latencies, 0.95)),
        "commit_max_seconds": max(latencies),
        "commits": len(latencies),
        "file_count": len(files),
        "final_bytes": sum(p.stat().st_size for p in files),
        "allocated_bytes": sum(p.stat().st_blocks * 512 for p in files),
        "observed_wal_peak_bytes": wal_peak,
        "process_write_bytes": after["write_bytes"] - before["write_bytes"],
        "cancelled_write_bytes": after["cancelled_write_bytes"]
        - before["cancelled_write_bytes"],
    }
    tick = time.perf_counter()
    if args.backend == "duckdb":
        verify_db(args.output / "vectors.duckdb", vectors)
    else:
        for start in range(0, len(vectors), args.commit_rows):
            end = min(start + args.commit_rows, len(vectors))
            actual = np.load(
                args.output / f"{start:09d}-{end:09d}.npy", allow_pickle=False
            )
            np.testing.assert_array_equal(actual, vectors[start:end])
    result["exact_roundtrip"] = True
    result["reopen_verify_seconds"] = time.perf_counter() - tick
    print(json.dumps(result), flush=True)


def crash(args):
    """Leave committed rows in the WAL plus an unfinished second transaction.

    os._exit deliberately bypasses connection close and interpreter cleanup.
    This tests abrupt process termination, not an OS crash or power failure.
    """
    vectors = source(args)
    args.output.mkdir(parents=True, exist_ok=False)
    db = open_db(args)
    db.execute("BEGIN")
    insert(db, vectors, 0, args.commit_rows)
    db.execute("COMMIT")
    db.execute("BEGIN")
    insert(db, vectors, args.commit_rows, len(vectors))
    print("committed first transaction; exiting with second uncommitted", flush=True)
    os._exit(73)


def recover(args):
    vectors = source(args)
    path = args.output / "vectors.duckdb"
    verify_db(path, vectors[: args.commit_rows])
    db = duckdb.connect(str(path))
    db.execute("BEGIN")
    insert(db, vectors, args.commit_rows, len(vectors))
    db.execute("COMMIT")
    db.close()
    verify_db(path, vectors)
    print(
        json.dumps(
            {
                "committed_rows_recovered": args.commit_rows,
                "uncommitted_rows_absent": True,
                "resumed_rows": len(vectors),
                "exact_roundtrip": True,
            }
        ),
        flush=True,
    )


def capture(args):
    """Capture the existing real sample once, outside storage timings."""
    assert not args.output.exists()
    inputs = pl.read_parquet(args.inputs)["input"].to_list()
    with (
        httpx.Client(base_url="http://127.0.0.1:18080", timeout=300) as client,
        ThreadPoolExecutor(max_workers=2) as pool,
    ):
        arrays = list(
            pool.map(
                lambda batch: encode(client, batch, priority=1)[0],
                [inputs[i : i + 128] for i in range(0, len(inputs), 128)],
            )
        )
    values = np.concatenate(arrays)
    np.save(args.output, values, allow_pickle=False)
    print(
        json.dumps({"captured_vectors": len(values), "bytes": values.nbytes}),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["capture", "benchmark", "crash", "recover"])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--backend", choices=["duckdb", "npy"], default="duckdb")
    parser.add_argument("--source", type=Path)
    parser.add_argument("--inputs", type=Path)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--commit-rows", type=int, default=32768)
    parser.add_argument("--checkpoint-threshold")
    args = parser.parse_args()
    assert args.rows > 0 and args.commit_rows > 0
    {"capture": capture, "benchmark": benchmark, "crash": crash, "recover": recover}[
        args.mode
    ](args)
