"""Join reader counts locally and freeze top-three selector inputs."""

import argparse
import json
import math
import os
from pathlib import Path

import duckdb

ROOT = Path(
    os.environ.get("RESOLVER_RUN_ROOT", "data/research/books-resolver-retry-v1")
)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--stage", default="first")
    args = p.parse_args()
    data = json.loads((ROOT / f"{args.stage}-ranked.json").read_text())
    counts = dict(
        duckdb.sql(
            "SELECT work_id,readinglog_count FROM '/tmp/resolver-reading-log-counts.parquet'"
        ).fetchall()
    )
    for c in data["cases"]:
        for d in c["candidates"]:
            d["readinglog_count"] = counts.get(d["id"], 0)
            d["score"] = d["raw_score"] + 0.1 * math.log2(1 + d["readinglog_count"])
        c["top3"] = [
            d["id"] for d in sorted(c["candidates"], key=lambda d: -d["score"])[:3]
        ]
    target = ROOT / f"{args.stage}-ready.json"
    temporary = target.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(data))
    temporary.replace(target)
    print("Ready", args.stage, len(data["cases"]))


if __name__ == "__main__":
    main()
