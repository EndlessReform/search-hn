import pytest
from search_research.hybrid_baseline import fuse, measurements


def test_rrf_uses_union_and_rewards_agreement():
    assert fuse([10, 20], [30, 20]) == [20, 10, 30]
    assert fuse([10, 20], []) == [10, 20]
    with pytest.raises(AssertionError):
        fuse([10, 10], [])


def test_missing_target_and_cutoffs():
    q = {"case": "x", "target_id": 1, "style": "entity", "cohort": "recent"}
    assert measurements(q, "hybrid", 256, None)["recall@8"] == 0
    assert measurements(q, "hybrid", 256, 9)["recall@8"] == 0
    assert measurements(q, "hybrid", 256, 8)["recall@8"] == 1
