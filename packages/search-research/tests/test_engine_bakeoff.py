"""Small contracts for filter parity and fixed-query replay aggregation."""

from search_research.engine_backends import filters
from search_research.fusion_sweep import weighted_fuse
from search_research.hybrid_baseline import fuse


def test_filters_share_values_across_engines():
    args = {
        "min_score": 50,
        "min_date": "2025-01-01",
        "include_domains": ["www.Example.com"],
        "exclude_domains": ["noise.example"],
    }
    pg, pg_values = filters(args, "pg")
    duck, duck_values = filters(args, "duck")
    assert pg.replace("%s", "?") == duck
    assert (
        pg_values == duck_values == [50, "2025-01-01", "example.com", "noise.example"]
    )
    assert "NOT IN" in pg


def test_no_filters_and_identical_rank_fusion():
    assert filters({}, "pg") == ("", [])
    assert fuse([9, 4, 7], [9, 4, 7]) == [9, 4, 7]


def test_weighted_fusion_preserves_reference_and_dense_only():
    assert weighted_fuse([2, 1, 9], [9, 3, 1], 1) == fuse([2, 1, 9], [9, 3, 1])
    assert weighted_fuse([2, 1, 9], [9, 3, 1], 0) == [2, 1, 9]
