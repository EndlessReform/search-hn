"""Score VM HNSW candidates using frozen lexical lists; no extra timed queries."""

import argparse
from pathlib import Path

import polars as pl
from pplx_hybrid_sweep import fuse

CACHE = Path("data/pplx-vllm-gate-20260905/bf16-full")


def run(root):
    """Score saved candidates only when explicitly invoked, without inference."""
    lexical = dict(pl.read_parquet(CACHE / "lexical.parquet").iter_rows())
    rows = []
    for sample in (
        pl.read_ndjson(root / "samples.jsonl").filter(pl.col("repeat") == 0).to_dicts()
    ):
        ids = fuse(sample["ids"], lexical[sample["case"]], 0.125)
        rank = (
            ids.index(sample["target_id"]) + 1 if sample["target_id"] in ids else None
        )
        rows.append(
            {
                "mode": sample["mode"],
                "case": sample["case"],
                "hits8": int(rank is not None and rank <= 8),
                "hits20": int(rank is not None and rank <= 20),
            }
        )
    frame = pl.DataFrame(rows)
    frame.write_parquet(root / "hybrid-ranks.parquet")
    frame.group_by("mode").agg(pl.col("hits8", "hits20").sum()).sort("mode").write_csv(
        root / "hybrid-quality.csv"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path("data/pplx-vm-latency-20260906")
    )
    run(parser.parse_args().root)
