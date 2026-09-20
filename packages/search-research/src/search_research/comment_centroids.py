"""Equal-comment centroids and seeded corpus centering in saved vector space."""

from collections import OrderedDict
from contextlib import closing

import numpy as np

from search_research.comment_index import DIMENSIONS

POOLING_RECIPE = "unit-chunks-mean-unit-comment-v1"


def unit(vector):
    """Normalize a direction, rejecting cancellation rather than inventing one."""
    norm = float(np.linalg.norm(vector))
    if not np.isfinite(norm) or norm < 1e-7:
        raise ValueError(
            "Query direction is zero or nearly zero; change positives or gamma"
        )
    return np.asarray(vector / norm, dtype=np.float32)


class CommentCentroids:
    def __init__(self, explorer, annotations):
        self.explorer = explorer
        self.annotations = annotations
        self.vectors = explorer.vectors
        self.unique_ids = np.unique(explorer.comment_ids)
        self.baseline_cache = OrderedDict()

    def representations(self, ids):
        """Read only requested rows; normalize chunks before equal-comment pooling.

        Sampling operates on unique comment IDs, so a long comment gets one vote.
        Batched SQL uses the existing (comment_id, chunk) index, not a corpus scan.
        """
        result = np.empty((len(ids), DIMENSIONS), dtype=np.float32)
        with closing(self.explorer.connect()) as db:
            for start in range(0, len(ids), 400):
                batch = ids[start : start + 400]
                groups = {cid: [] for cid in batch}
                for cid, row in db.execute(
                    "SELECT comment_id,vector_row FROM inputs WHERE comment_id IN ("
                    + ",".join("?" for _ in batch)
                    + ") ORDER BY comment_id,chunk",
                    batch,
                ):
                    groups[cid].append(row)
                for offset, cid in enumerate(batch):
                    if not groups[cid]:
                        raise ValueError(f"Comment {cid} is absent from this corpus")
                    rows = np.asarray(groups[cid])
                    chunks = (
                        self.vectors[rows].astype(np.float32)
                        / self.explorer.norms[rows, None]
                    )
                    result[start + offset] = unit(chunks.mean(axis=0))
        return result

    def baseline(self, seed, size):
        """Cache nested means; preserve their magnitudes for meaningful subtraction."""
        if size > len(self.unique_ids):
            raise ValueError(
                f"Baseline requests {size} comments; corpus has {len(self.unique_ids)}"
            )
        if seed not in self.baseline_cache:
            ids = self.annotations.baseline(
                seed,
                lambda: (
                    np.random.default_rng(seed)
                    .choice(
                        self.unique_ids,
                        size=min(10000, len(self.unique_ids)),
                        replace=False,
                    )
                    .tolist()
                ),
            )
            reps = self.representations(ids)
            self.baseline_cache[seed] = {
                n: reps[:n].mean(axis=0) for n in (100, 1000, 10000) if n <= len(ids)
            }
            if len(self.baseline_cache) > 4:
                self.baseline_cache.popitem(last=False)
        self.baseline_cache.move_to_end(seed)
        return self.baseline_cache[seed][size]

    def query(self, ids, mode, gamma, seed, size):
        if not ids:
            raise ValueError("Add at least one positive comment to this set")
        positive = self.representations(ids).mean(axis=0)
        background = (
            self.baseline(seed, size)
            if mode == "corrected" and gamma != 0
            else np.zeros(DIMENSIONS, np.float32)
        )
        effective_gamma = gamma if mode == "corrected" else 0
        return unit(positive - effective_gamma * background), {
            "pooling_recipe": POOLING_RECIPE,
            "positive_norm": float(np.linalg.norm(positive)),
            "background_norm": float(np.linalg.norm(background)),
            "gamma": effective_gamma,
        }
