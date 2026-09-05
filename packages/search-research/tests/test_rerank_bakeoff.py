"""Verify pool isolation and deterministic, honest reranker ordering."""

import pytest
from search_research.rerank_bakeoff import rerank


def test_pool_is_applied_before_reranking():
    assert rerank([1, 2, 3], {1: 1.0, 2: 2.0, 3: 99.0}, 2) == [2, 1]


def test_ties_keep_original_order():
    assert rerank([3, 2, 1], {1: 1.0, 2: 1.0, 3: 1.0}, 3) == [3, 2, 1]


def test_nonfinite_scores_fail():
    with pytest.raises(AssertionError):
        rerank([1], {1: float("nan")}, 1)


def test_duplicates_fail():
    with pytest.raises(AssertionError):
        rerank([1, 1], {1: 1.0}, 2)
