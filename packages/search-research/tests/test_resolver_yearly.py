"""New-year baseline isolation, publication, and reference-window batching."""

import importlib.util
import json
import sqlite3
from pathlib import Path

from search_research.resolver_rerank import baseline_scores, pending_batches


def load(name):
    path = Path(__file__).parents[1] / "tools/resolver_heal" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_reference_batches_do_not_split_references():
    cases = [
        {"id": str(i), "candidates": [{"id": str(j)} for j in range(50)]}
        for i in range(129)
    ]
    batches = list(pending_batches(cases, {}))
    assert list(map(len, batches)) == [6400, 50]
    assert {c["id"] for c, _ in batches[0]} == {str(i) for i in range(128)}
    scores = {(str(i), str(j)): 1 for i in range(128) for j in range(50)}
    assert list(map(len, pending_batches(cases, scores))) == [50]


def test_baseline_is_optional_and_uses_raw_scores(tmp_path):
    assert baseline_scores(None) == {}
    path = tmp_path / "old.sqlite"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE rankings(id TEXT,payload TEXT)")
        db.execute(
            "INSERT INTO rankings VALUES (?,?)",
            ("x", json.dumps({"ids": ["a"], "scores": [4], "original_scores": [2]})),
        )
    assert baseline_scores(path) == {("x", "a"): 2}


def test_publish_without_baseline(tmp_path, monkeypatch):
    module = load("publish")
    monkeypatch.setattr(module, "ROOT", tmp_path)
    monkeypatch.delenv("RESOLVER_BASELINE", raising=False)
    case = {
        "id": "1:0:4",
        "reference": {"title": "Dune", "context": "Dune", "start": 0, "end": 4},
        "person_spans": [],
        "query_title": "Dune",
        "author_bonus": 5,
        "retrieved": [],
        "candidates": [],
        "top3": [],
    }
    decision = {
        "id": case["id"],
        "decision": {
            "results": [
                {
                    "title": "Dune",
                    "author": None,
                    "action": "abstain",
                    "work_id": None,
                    "reason": "No match",
                }
            ]
        },
    }
    (tmp_path / "round0-ready.json").write_text(json.dumps({"cases": [case]}))
    (tmp_path / "round0-decisions.jsonl").write_text(json.dumps(decision) + "\n")
    module.main()
    with sqlite3.connect(tmp_path / "checkpoint.sqlite") as db:
        assert db.execute("SELECT model FROM selections").fetchall() == [("luna",)]
        assert db.execute("SELECT count(*) FROM refs").fetchone()[0] == 1


def test_run_manifest_rejects_changed_counts_and_uses_local_config(
    tmp_path, monkeypatch
):
    from argparse import Namespace

    import pytest

    module = load("run")
    slice_path = tmp_path / "slice"
    slice_path.mkdir()
    with sqlite3.connect(slice_path / "index.sqlite") as db:
        db.execute("CREATE TABLE progress(total_rows INTEGER,completed_rows INTEGER)")
        db.execute("INSERT INTO progress VALUES (1,1)")
    recipe = tmp_path / "recipe.json"
    recipe.write_text(
        json.dumps({"anchor_file": "anchor.json", "model_file": "model.ubj"})
    )
    (tmp_path / "anchor.json").write_text("{}")
    (tmp_path / "model.ubj").write_bytes(b"model")
    counts = tmp_path / "counts.parquet"
    counts.write_bytes(b"counts")
    monkeypatch.setattr(module, "COUNTS_PATH", counts)
    config = tmp_path / "config.json"
    config.write_text('{"model":"frozen"}')
    args = Namespace(
        run_root=tmp_path / "run",
        slice=slice_path,
        config=config,
        recipe=recipe,
        baseline=None,
        budget=1,
        concurrency=4,
    )
    module.initialize(args)
    config.unlink()  # The bootstrap source must not be consulted on resumption.
    module.initialize(args)
    assert json.loads((args.run_root / "luna-config.json").read_text()) == {
        "model": "frozen"
    }
    with monkeypatch.context() as changed:
        changed.setattr(module, "NER_RECIPE", module.NER_RECIPE | {"backend": "flash"})
        with pytest.raises(AssertionError, match="Run inputs changed"):
            module.initialize(args)
    counts.write_bytes(b"changed")
    with pytest.raises(AssertionError, match="Run inputs changed"):
        module.initialize(args)
