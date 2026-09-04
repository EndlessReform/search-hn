"""Capture environment evidence without persisting credentials."""

import hashlib
import json
import platform
import urllib.request
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path


def capture(root: Path, repository, local_base_url: str):
    from sqlalchemy import text

    from search_research.rollouts import source_hash

    with urllib.request.urlopen(
        local_base_url.rstrip("/") + "/models", timeout=20
    ) as response:
        models = json.load(response)
    with repository._engine.connect() as conn:
        pg_version = conn.execute(text("SELECT version()")).scalar_one()
        indexes = [
            dict(r)
            for r in conn.execute(
                text(
                    "SELECT indexname,indexdef FROM pg_indexes WHERE tablename='items' ORDER BY indexname"
                )
            ).mappings()
        ]
        plan = conn.execute(
            text(
                "EXPLAIN (FORMAT JSON) SELECT id,title,score FROM items WHERE type='story' AND day=DATE '2026-08-01' ORDER BY score DESC NULLS LAST,id DESC LIMIT 20"
            )
        ).scalar_one()
    package_root = Path(__file__).resolve().parents[4]
    payload = {
        "captured_at": datetime.now(UTC).isoformat(),
        "host": platform.node(),
        "local_model_endpoint": local_base_url,
        "python": platform.python_version(),
        "source_sha256": source_hash(),
        "uv_lock_sha256": hashlib.sha256(
            (package_root / "uv.lock").read_bytes()
        ).hexdigest(),
        "versions": {
            p: version(p) for p in ("openai-agents", "openai", "polars", "sqlalchemy")
        },
        "postgres": pg_version,
        "indexes": indexes,
        "daily_top_explain": plan,
        "models": models,
    }
    with (root / "environment.json").open("x") as f:
        json.dump(payload, f, indent=2)
