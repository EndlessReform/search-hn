"""Frozen filter integrity and multi-chunk pooling semantics."""

import importlib
import json
import sqlite3

import numpy as np
import pytest

pytest.importorskip("xgboost")
module = importlib.import_module("search_research.quick_filter")


def recipe(tmp_path):
    anchor = tmp_path / "anchor.json"
    anchor.write_text(json.dumps({"query": [1, 0], "positive_ids": [1]}))
    model = tmp_path / "model.ubj"
    model.write_bytes(b"fixture")
    value = module.Recipe(
        anchor_file=anchor.name,
        anchor_sha256=module.digest(anchor),
        model_file=model.name,
        model_sha256=module.digest(model),
        best_iteration=3,
    )
    path = tmp_path / "recipe.json"
    path.write_text(value.model_dump_json())
    return path, value


def test_filter_max_cosine_and_normalized_mean_pooling(tmp_path, monkeypatch):
    path, expected = recipe(tmp_path)
    np.save(
        tmp_path / "vectors.npy",
        np.array([[127, 0], [0, 127], [-127, 0]], dtype=np.int8),
    )
    with sqlite3.connect(tmp_path / "index.sqlite") as db:
        db.executescript(
            "CREATE TABLE inputs(comment_id INTEGER, vector_row INTEGER, chunk INTEGER); CREATE TABLE progress(total_rows INTEGER,completed_rows INTEGER); INSERT INTO progress VALUES(3,3); INSERT INTO inputs VALUES(1,0,0),(1,1,1),(2,2,0);"
        )

    class Model:
        def __init__(self, **kwargs):
            pass

        def load_model(self, path):
            pass

        def set_param(self, values):
            pass

        def attr(self, name):
            return "3"

        def inplace_predict(self, pooled, iteration_range):
            assert iteration_range == (0, 4)
            np.testing.assert_allclose(pooled, [[2**-0.5, 2**-0.5], [-1, 0]], rtol=1e-6)
            return np.array([0.75, 0.25])

    monkeypatch.setattr(module, "Booster", Model)
    ids, scores, _, _ = module.infer(tmp_path, path)
    assert ids.tolist() == [1, 2]
    np.testing.assert_array_equal(
        scores,
        module.blend(
            np.array([1, -1], dtype=np.float32), np.array([0.75, 0.25]), expected
        ),
    )
    with sqlite3.connect(tmp_path / "index.sqlite") as db:
        db.execute("UPDATE progress SET completed_rows=2")
    with pytest.raises(AssertionError, match="Incomplete embeddings"):
        module.infer(tmp_path, path)


def test_changed_frozen_anchor_is_rejected(tmp_path):
    path, _ = recipe(tmp_path)
    (tmp_path / "anchor.json").write_text("{}")
    with pytest.raises(AssertionError, match="Frozen input changed"):
        module.load_recipe(path)
