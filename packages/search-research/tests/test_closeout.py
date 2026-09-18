"""Closeout must exclude secrets/rejected arrays and verify downloaded bytes."""

import importlib.util
import io
import json
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "closeout", Path(__file__).parents[1] / "tools/research_closeout.py"
)
closeout = importlib.util.module_from_spec(spec)
spec.loader.exec_module(closeout)


def test_inventory_policy(tmp_path, monkeypatch):
    monkeypatch.setattr(closeout, "REPO", tmp_path)
    root = tmp_path / "data" / "experiment"
    root.mkdir(parents=True)
    for name in ("rollout.jsonl", "rejected.npy", ".env", "cache.duckdb", "pplx.npy"):
        (root / name).write_bytes(b"evidence")
    files, excluded = closeout.inventory(("experiment",))
    assert {Path(e["path"]).name for e in files} == {"rollout.jsonl", "pplx.npy"}
    assert {Path(e["path"]).name for e in excluded} == {
        ".env",
        "cache.duckdb",
        "rejected.npy",
    }


def test_verify_rejects_corrupt_object(tmp_path, monkeypatch):
    manifest = {
        "schema_version": 1,
        "release": "test",
        "files": [{"path": "data/x/a.json", "sha256": "0" * 64, "size": 3}],
    }

    class Body:
        def iter_chunks(self, **kwargs):
            return iter([b"bad"])

        def close(self):
            pass

    class Store:
        def get_object(self, **kwargs):
            return {
                "Body": io.BytesIO(json.dumps(manifest).encode())
                if kwargs["Key"].endswith("manifest.json")
                else Body()
            }

    monkeypatch.setattr(closeout, "client", lambda: (Store(), "private"))
    with pytest.raises(AssertionError):
        closeout.verify("test", tmp_path)
    assert not (tmp_path / "verified.json").exists()
