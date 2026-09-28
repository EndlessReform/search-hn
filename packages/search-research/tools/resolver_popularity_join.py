"""Join an official reading-log dump to a prepared pilot with no API requests.

Run with ``uv run --package search-research python tools/resolver_popularity_join.py
--dump /path/reading-log.txt.gz --root /path/pilot``. DuckDB scans the complete
snapshot once, then retains only candidate work keys. An absent work has zero
recorded log entries in this snapshot; this is distinct from an API lookup failure.
Edition counts, when present, come from the separately cached live API batches.
"""

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
from resolver_popularity_prepare import algorithms, cached_editions


def main():
    """Attach snapshot counts and recompute representatives under every rule."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dump", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    path = args.root / "cases.json"
    data = json.loads(path.read_text())
    ids = sorted({d["id"] for c in data["cases"] for d in c["candidates"]})
    db = duckdb.connect()
    db.execute("CREATE TABLE wanted (work_id VARCHAR PRIMARY KEY)")
    db.executemany("INSERT INTO wanted VALUES (?)", [(key,) for key in ids])
    counts = {
        row[0]: row[1:]
        for row in db.execute(
            """SELECT w.work_id, count(r.column0),
               count(*) FILTER(WHERE r.column2='Want to Read'),
               count(*) FILTER(WHERE r.column2='Currently Reading'),
               count(*) FILTER(WHERE r.column2='Already Read')
               FROM wanted w LEFT JOIN read_csv(?, delim='\t', header=false,
                 all_varchar=true, quote='', escape='') r ON w.work_id=r.column0
               GROUP BY w.work_id""",
            [str(args.dump)],
        ).fetchall()
    }
    metadata = cached_editions(ids, args.root / "popularity_batches")
    fields = (
        "readinglog_count",
        "want_to_read_count",
        "currently_reading_count",
        "already_read_count",
    )
    for case in data["cases"]:
        for candidate in case["candidates"]:
            candidate["popularity"] = metadata[candidate["id"]]
            candidate["popularity"].update(
                dict(zip(fields, counts[candidate["id"]], strict=True))
            )
        case["algorithms"] = algorithms(case["candidates"])
    digest = hashlib.sha256()
    with args.dump.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    data["readinglog_source"] = {
        "url": "https://openlibrary.org/data/ol_dump_reading-log_latest.txt.gz",
        "processed_utc": datetime.now(UTC).isoformat(),
        "sha256": digest.hexdigest(),
        "aggregation": "count snapshot rows by work key; absent work=0 recorded logs",
    }
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    print(
        json.dumps(
            {
                "candidate_ids": len(ids),
                "positive_log_ids": sum(v[0] > 0 for v in counts.values()),
            }
        )
    )


if __name__ == "__main__":
    main()
