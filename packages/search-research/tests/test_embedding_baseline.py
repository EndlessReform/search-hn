import numpy as np
import polars as pl
from search_research.embedding_api import batches
from search_research.embedding_baseline import exact_ranks, shortened


def test_prefix_is_renormalized_without_changing_original():
    original = np.array([[3.0, 4.0, 12.0]])
    np.testing.assert_allclose(shortened(original, 2), [[0.6, 0.8]])
    assert original[0, 2] == 12


def test_exact_rank_and_ties():
    scores = np.array([[0.1, 0.9, 0.2], [0.5, 0.5, 0.5]])
    assert exact_ranks(scores, np.array([2, 1])).tolist() == [2, 2]


def test_batches_bound_tokens_and_count():
    frame = pl.DataFrame({"tokens": [100] * 600})
    assert list(batches(frame)) == [(0, 200), (200, 400), (400, 600)]
