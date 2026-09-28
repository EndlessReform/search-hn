"""Frozen reading-log counts shared by retrieval and selector preparation."""

from pathlib import Path

import duckdb

COUNTS_PATH = Path("data/research/resolver-reading-log-counts.parquet")


def load_counts(path: Path = COUNTS_PATH) -> dict[str, int]:
    """Load the frozen snapshot once per stage; missing work rows mean zero.

    A missing snapshot is an input error, not an empty popularity signal.
    """
    with duckdb.connect() as db:
        return dict(
            db.execute(
                "SELECT work_id, readinglog_count FROM read_parquet(?)", [str(path)]
            ).fetchall()
        )
