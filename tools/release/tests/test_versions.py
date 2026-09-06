"""Exercise release-number collision and prerelease decisions."""
import pytest
from hn_release.versions import choices


def test_baseline_choices():
    values = set(choices(["v0.2.0"]).values())
    assert {"0.2.1", "0.3.0", "1.0.0", "0.2.1-canary.1", "0.3.0-pre.1"} <= values


def test_preview_does_not_move_stable_and_continues_series():
    values = set(choices(["v0.2.0", "v0.3.0-canary.1", "v0.3.0-canary.2"]).values())
    assert "0.2.1" in values
    assert "0.3.0-canary.3" in values
    assert "0.3.0-canary.1" not in values
    assert "0.3.0" in values


def test_semver_not_lexical_order():
    assert "0.10.1" in choices(["v0.9.0", "v0.10.0", "unrelated"]).values()


def test_missing_baseline_is_explicit():
    with pytest.raises(ValueError, match="baseline"):
        choices([])
