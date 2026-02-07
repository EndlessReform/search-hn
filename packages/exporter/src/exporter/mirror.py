from __future__ import annotations

import os

from datasets import Dataset
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()


def _get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")
    return database_url


def _get_hf_token() -> str | None:
    return os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACE_TOKEN")


def get_engine() -> Engine:
    return create_engine(_get_database_url())


def run_mirror_job(dry_run: bool = False) -> None:
    engine = get_engine()
    hf_token = _get_hf_token()

    crosswalk_ds = Dataset.from_sql("SELECT * FROM kids;", engine, cache_dir=None)
    if not dry_run:
        if not hf_token:
            raise RuntimeError("HF_TOKEN (or HUGGINGFACE_TOKEN) is required for non-dry runs")
        crosswalk_ds.push_to_hub(
            repo_id="jkeisling/hn-crosswalk",
            max_shard_size="5GB",
            token=hf_token,
        )

    items_ds = Dataset.from_sql("SELECT * FROM items;", engine, cache_dir=None)
    if not dry_run:
        items_ds.push_to_hub(
            repo_id="jkeisling/hn-items",
            max_shard_size="5GB",
            token=hf_token,
        )
