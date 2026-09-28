"""Validate retry decisions and preservation of the source mention."""

import importlib.util
from pathlib import Path

import pytest
from pydantic import ValidationError


def selector():
    path = Path(__file__).parents[1] / "tools/resolver_retry/select.py"
    spec = importlib.util.spec_from_file_location("retry_select", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "data",
    [
        {
            "action": "retry",
            "work_id": "/works/1",
            "rewritten_title": "The Lord of the Rings",
        },
        {"action": "select", "work_id": None, "rewritten_title": None},
        {"action": "abstain", "work_id": None, "rewritten_title": "Other"},
        {"action": "retry", "work_id": None, "rewritten_title": " "},
    ],
)
def test_invalid_decisions(data):
    with pytest.raises(ValidationError):
        selector().Decision.model_validate(data)


def test_retry_preserves_original_comment():
    module = selector()
    case = {
        "id": "1:0:4",
        "reference": {
            "context": "LOTR by Tolkien",
            "title": "LOTR",
            "start": 0,
            "end": 4,
        },
        "query_title": "The Lord of the Rings",
        "person_spans": [{"text": "Tolkien"}],
        "top3": [],
        "candidates": [],
    }
    result = module.request(case, "retry", {})
    assert "<mention>LOTR</mention> by Tolkien" in result["messages"][1]["content"]
    assert "The Lord of the Rings" in result["messages"][1]["content"]
    assert "retry is forbidden" in result["messages"][0]["content"]
