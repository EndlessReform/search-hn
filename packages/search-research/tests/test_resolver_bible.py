"""Bible name gating and explicit special-identity rendering."""

import pytest
from search_research.resolver_bible import bible_candidate
from search_research.resolver_review_html import choice


@pytest.mark.parametrize(
    "title",
    [
        "Revelations",
        "The Book of Ecclesiastes",
        "II Corinthians",
        "Gospel according to John",
        "John 3:16",
        "The Bible",
        "Genesis",
        "Tobit",
    ],
)
def test_bible_gate(title):
    assert bible_candidate(title)


@pytest.mark.parametrize(
    "title",
    [
        "The Genesis Machine",
        "The Book of Job: A Commentary",
        "Endurance",
        "Steve Jobs",
        "Revelation Space",
    ],
)
def test_unrelated_titles(title):
    assert not bible_candidate(title)


def test_special_choice_without_retrieved_record():
    assert "The Bible" in choice(
        {
            "selections": {"luna": {"selection": {"work_id": "special:bible"}}},
            "candidates": [],
        },
        "luna",
    )
