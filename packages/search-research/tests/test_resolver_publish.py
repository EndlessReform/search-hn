"""Legacy comparison metadata must not prevent publishing completed decisions."""

import importlib.util
import json
import sqlite3
from pathlib import Path

import duckdb


def test_legacy_documents_use_frozen_counts(tmp_path):
    path = Path(__file__).parents[1] / "tools/resolver_heal/publish.py"
    spec = importlib.util.spec_from_file_location("resolver_publish", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    old = sqlite3.connect(":memory:")
    old.executescript(
        "CREATE TABLE selections(model TEXT,payload TEXT); CREATE TABLE documents(id TEXT PRIMARY KEY,payload TEXT);"
    )
    current = sqlite3.connect(":memory:")
    current.execute("CREATE TABLE documents(id TEXT PRIMARY KEY,payload TEXT)")
    for key in ("existing", "counted", "absent", "special:bible", None):
        old.execute(
            "INSERT INTO selections VALUES ('luna',?)",
            (json.dumps({"selection": {"work_id": key}}),),
        )
        if key:
            old.execute(
                "INSERT INTO documents VALUES (?,?)",
                (key, json.dumps({"id": key, "title": key, "authors": []})),
            )
    existing = json.dumps(
        {"id": "existing", "title": "preserve", "authors": [], "readinglog_count": 99}
    )
    current.execute("INSERT INTO documents VALUES ('existing',?)", (existing,))
    counts_path = tmp_path / "counts.parquet"
    with duckdb.connect() as counts:
        counts.execute(
            "COPY (SELECT 'counted' work_id, 17 readinglog_count) TO ? (FORMAT PARQUET)",
            [str(counts_path)],
        )
    module.copy_baseline_documents(old, current, counts_path)
    docs = {
        key: json.loads(payload)
        for key, payload in current.execute("SELECT * FROM documents")
    }
    assert set(docs) == {"existing", "counted", "absent"}
    assert docs["existing"] == json.loads(existing)
    assert docs["counted"]["readinglog_count"] == 17
    assert docs["absent"]["readinglog_count"] == 0
