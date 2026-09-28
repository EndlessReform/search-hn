"""Multi-work contracts, parent provenance, and author-only repairs."""

import importlib.util
from pathlib import Path

import pytest
from pydantic import ValidationError
from search_research.resolver_review import ResolverReview, selected_ids


def schema():
    p = Path(__file__).parents[1] / "tools/resolver_heal/schema.py"
    spec = importlib.util.spec_from_file_location("heal_schema", p)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_mixed_selection_and_multiple_searches():
    m = schema()
    d = m.Decision.model_validate(
        {
            "results": [
                {
                    "title": "Tuxedo Park",
                    "author": "Jennet Conant",
                    "action": "select",
                    "work_id": "/works/A",
                    "reason": "Matching work",
                },
                {
                    "title": "Insisting on the Impossible",
                    "author": "Victor K. McElheny",
                    "action": "search",
                    "work_id": None,
                    "reason": "Second work in merged span",
                },
                {
                    "title": "The Logic of Failure",
                    "author": "Dietrich Dörner",
                    "action": "search",
                    "work_id": None,
                    "reason": "Third work in merged span",
                },
            ]
        }
    )
    assert len(d.results) == 3


def test_author_only_repair_and_child_provenance():
    m = schema()
    parent = {
        "id": "1:0:7",
        "reference": {"title": "Artemis", "context": "Artemis", "start": 0, "end": 7},
        "person_spans": [],
    }
    r = {"title": "Artemis", "author": "Andy Weir", "reason": "Intended author"}
    c = m.child_case(parent, r, 1)
    assert c["reference"] == parent["reference"]
    assert c["query_title"] == "Artemis" and c["query_author"] == "Andy Weir"
    assert c["root_id"] == parent["id"] and c["parent_id"] == parent["id"]
    assert m.query_key("Artemis", None) != m.query_key("Artemis", "Andy Weir")
    assert m.child_case(c, r, 2)["id"] == c["id"]


def test_duplicate_targets_rejected():
    r = {
        "title": "Book",
        "author": None,
        "action": "abstain",
        "work_id": None,
        "reason": "Unresolved",
    }
    with pytest.raises(ValidationError):
        schema().Decision.model_validate({"results": [r, r]})


def test_multi_selection_is_not_abstention():
    receipt = {"selection": {"work_ids": ["/works/A", "/works/B"]}}
    assert selected_ids(receipt) == ["/works/A", "/works/B"]
    assert (
        ResolverReview.category(
            {"labels": {"luna": ("/works/A", "/works/B"), "luna-original": None}},
            "luna-original",
        )
        == "luna_only"
    )
