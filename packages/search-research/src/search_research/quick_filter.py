"""Frozen centroid/XGBoost inference shared by yearly runs and research sampling."""

import hashlib
import json
import shutil
import sqlite3
from contextlib import closing
from pathlib import Path

import numpy as np
from pydantic import BaseModel, Field
from xgboost import Booster

DEFAULT_RECIPE = Path("data/research/books-quick-filter-v1/recipe.json")


class Recipe(BaseModel):
    """All fitted constants and input identities needed to reproduce the gate."""

    anchor_file: str
    anchor_sha256: str
    model_file: str
    model_sha256: str
    best_iteration: int = Field(ge=0)
    cutoff: float = -0.48585514643850203
    cosine_weight: float = 0.25
    cosine_mean: float = 0.4018084356464245
    cosine_std: float = 0.20650860033072777
    logit_mean: float = -1.6561394556670892
    logit_std: float = 4.033332085834178
    probability_clip: float = 1e-7


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def readonly(path):
    return sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)


def freeze(annotations, model_path, output, expected_anchor):
    """One-time snapshot: runtime never reads mutable annotations afterwards.

    Compare the anchor identities to the recorded original experiment before
    freezing. Full-slice parity independently checks the resulting centroid.
    """
    with closing(readonly(annotations)) as db:
        anchor = json.loads(
            db.execute("SELECT anchor_json FROM rollout_pools WHERE id=1").fetchone()[0]
        )
    assert anchor["positive_ids"] == expected_anchor, "Annotation anchor changed"
    model = Booster()
    model.load_model(model_path)
    output.mkdir(parents=True, exist_ok=False)
    (output / "anchor.json").write_text(json.dumps(anchor, sort_keys=True))
    shutil.copyfile(model_path, output / "model.ubj")
    recipe = Recipe(
        anchor_file="anchor.json",
        anchor_sha256=digest(output / "anchor.json"),
        model_file="model.ubj",
        model_sha256=digest(output / "model.ubj"),
        best_iteration=int(model.attr("best_iteration")),
    )
    (output / "recipe.json").write_text(recipe.model_dump_json(indent=2))


def load_recipe(path):
    """Reject changed model/anchor files instead of silently changing pass IDs."""
    recipe = Recipe.model_validate_json(path.read_text())
    for filename, expected in (
        (recipe.anchor_file, recipe.anchor_sha256),
        (recipe.model_file, recipe.model_sha256),
    ):
        assert digest(path.parent / filename) == expected, (
            f"Frozen input changed: {filename}"
        )
    anchor = json.loads((path.parent / recipe.anchor_file).read_text())
    return recipe, anchor


def blend(cosine, probability, recipe):
    """Apply the fitted normalization and 25/75 mixture without refitting."""
    p = np.clip(
        np.asarray(probability, dtype=np.float64),
        recipe.probability_clip,
        1 - recipe.probability_clip,
    )
    return recipe.cosine_weight * (
        (cosine - recipe.cosine_mean) / recipe.cosine_std
    ) + (1 - recipe.cosine_weight) * (
        (np.log(p / (1 - p)) - recipe.logit_mean) / recipe.logit_std
    )


def infer(root, recipe_path=DEFAULT_RECIPE):
    """Score every comment, pooling all its chunks exactly as the 2025 gate did."""
    recipe, anchor = load_recipe(recipe_path)
    model = Booster(params={"nthread": 16})
    model.load_model(recipe_path.parent / recipe.model_file)
    model.set_param({"nthread": 16})
    assert int(model.attr("best_iteration")) == recipe.best_iteration
    vectors = np.load(root / "vectors.npy", mmap_mode="r")
    query = np.asarray(anchor["query"], dtype=np.float32)
    query /= np.linalg.norm(query)
    with closing(readonly(root / "index.sqlite")) as db:
        pairs = np.array(
            db.execute(
                "SELECT comment_id,vector_row FROM inputs ORDER BY comment_id,chunk"
            ).fetchall(),
            dtype=np.int64,
        )
        progress = db.execute(
            "SELECT total_rows,completed_rows FROM progress"
        ).fetchone()
    assert progress[0] == progress[1] == len(pairs) == len(vectors), (
        "Incomplete embeddings"
    )
    assert len(pairs), "Empty comment slice"
    starts = np.r_[0, np.flatnonzero(np.diff(pairs[:, 0])) + 1, len(pairs)]
    comment_ids = pairs[starts[:-1], 0]
    scores = np.empty(len(comment_ids), dtype=np.float64)
    for i in range(0, len(comment_ids), 8192):
        j = min(i + 8192, len(comment_ids))
        left, right = starts[i], starts[j]
        chunks = vectors[pairs[left:right, 1]].astype(np.float32)
        norms = np.linalg.norm(chunks, axis=1, keepdims=True)
        assert (norms > 0).all()
        chunks /= norms
        local = starts[i:j] - left
        pooled = np.add.reduceat(chunks, local)
        pooled /= np.linalg.norm(pooled, axis=1, keepdims=True)
        cosine = np.maximum.reduceat(chunks @ query, local)
        scores[i:j] = blend(
            cosine,
            model.inplace_predict(
                pooled, iteration_range=(0, recipe.best_iteration + 1)
            ),
            recipe,
        )
        if i % (8192 * 40) == 0:
            print(f"filter {j:,}/{len(comment_ids):,}", flush=True)
    assert np.isfinite(scores).all(), "Nonfinite filter scores"
    return comment_ids, scores, recipe, anchor
