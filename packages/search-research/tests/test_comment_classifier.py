"""Prompt compilation must preserve user choices and expose taxonomy labels consistently."""

import json

import pytest
import tiktoken
from fastapi.testclient import TestClient
from search_research.comment_classifier import ClassifierDraft, compile_prompt
from search_research.comment_explorer import CommentExplorer
from search_research.comment_explorer_web import create_app
from test_comment_explorer import corpus as corpus_fixture

corpus = corpus_fixture


def draft_body():
    return {
        "description": "Classify whether a comment recommends books.",
        "taxonomy": [
            {
                "name": "Recommendation",
                "description": "Recommends a specific book.",
                "is_positive": True,
            },
            {
                "name": "Meta",
                "description": "Discusses reading without recommending.",
                "is_positive": False,
            },
        ],
        "examples": [],
    }


def test_compile_explicit_polarity_and_rationale_modes():
    body = draft_body()
    catalog = [
        {
            "comment_id": 10,
            "label": "positive",
            "text": "Read Dune.",
            "saved_rationale": "",
        },
        {
            "comment_id": 20,
            "label": "negative",
            "text": "Any recommendations? <|endoftext|>",
            "saved_rationale": "SAVED NOTE",
        },
    ]
    body["examples"] = [{"comment_id": 10}, {"comment_id": 20, "rationale": "saved"}]
    compiled = compile_prompt(ClassifierDraft(**body), catalog)
    assert "## Positive examples:" in compiled["prompt"]
    assert "## Negative examples:" in compiled["prompt"]
    assert "### Recommendation\n\nRecommends a specific book." in compiled["prompt"]
    assert "### Example 1\n\n> Read Dune." in compiled["prompt"]
    assert "**Rationale:** SAVED NOTE" in compiled["prompt"]
    changed = json.loads(json.dumps(body))
    for taxon in changed["taxonomy"]:
        taxon["is_positive"] = not taxon["is_positive"]
    assert "**Label:** Positive (`is_positive: true`)" in compiled["prompt"]
    assert "**Label:** Negative (`is_positive: false`)" in compiled["prompt"]
    flipped = compile_prompt(ClassifierDraft(**changed), catalog)
    assert flipped["prompt"] != compiled["prompt"]
    assert "Recommends a specific book.\n\n**Label:** Negative" in flipped["prompt"]
    assert "The boolean and taxonomy must agree" in compiled["prompt"]
    assert compiled["schema"]["properties"]["taxonomy"]["enum"] == [
        "Recommendation",
        "Meta",
    ]
    assert compiled["schema"]["required"] == ["is_positive", "taxonomy"]
    assert compiled["schema"]["additionalProperties"] is False
    enc = tiktoken.get_encoding("o200k_base")
    assert compiled["prompt_tokens"] == len(
        enc.encode(compiled["prompt"], disallowed_special=())
    )
    assert compiled["schema_tokens"] == len(
        enc.encode(compiled["schema_text"], disallowed_special=())
    )
    body["examples"][1].update(rationale="custom", custom_rationale="MY OWN WORDING")
    custom = compile_prompt(ClassifierDraft(**body), catalog)["prompt"]
    assert "MY OWN WORDING" in custom and "SAVED NOTE" not in custom
    body["examples"][1]["rationale"] = "omit"
    omitted = compile_prompt(ClassifierDraft(**body), catalog)["prompt"]
    assert "SAVED NOTE" not in omitted and "MY OWN WORDING" not in omitted
    body["examples"] = []
    assert "examples:" not in compile_prompt(ClassifierDraft(**body), catalog)["prompt"]


def test_compile_validation():
    body = draft_body()
    with pytest.raises(ValueError, match="Describe the category"):
        compile_prompt(ClassifierDraft(**(body | {"description": ""})), [])
    with pytest.raises(ValueError, match="at least one"):
        compile_prompt(ClassifierDraft(**(body | {"taxonomy": []})), [])
    with pytest.raises(ValueError, match="unique"):
        compile_prompt(
            ClassifierDraft(**(body | {"taxonomy": [body["taxonomy"][0]] * 2})), []
        )
    with pytest.raises(ValueError, match="no longer labeled"):
        compile_prompt(
            ClassifierDraft(**(body | {"examples": [{"comment_id": 999}]})), []
        )


def test_draft_roundtrip_isolation_labels_and_delete(corpus):
    engine = CommentExplorer(corpus, "http://unused", threads=1)
    client = TestClient(create_app(engine))
    sid = client.post("/api/sets", json={"name": "Books"}).json()["id"]
    other = client.post("/api/sets", json={"name": "Other"}).json()["id"]
    url = f"/api/sets/{sid}"
    client.put(url + "/members/10", json={})
    client.put(url + "/negatives/20", json={"note": "ORIGINAL NOTE"})
    before = client.get(url).json()
    body = draft_body()
    body["examples"] = [
        {"comment_id": 20, "rationale": "custom", "custom_rationale": "CUSTOM ONLY"}
    ]
    assert client.put(url + "/classifier", json=body).status_code == 200
    reopened = TestClient(create_app(engine))
    loaded = reopened.get(url + "/classifier").json()
    assert loaded["draft"] == ClassifierDraft(**body).model_dump()
    assert len(loaded["catalog"]) == 2
    assert (
        reopened.get(f"/api/sets/{other}/classifier").json()["draft"]["description"]
        == ""
    )
    assert (
        "CUSTOM ONLY"
        in reopened.post(url + "/classifier/compile", json=body).json()["prompt"]
    )
    assert reopened.get(url).json() == before
    assert reopened.get("/classifier").status_code == 200
    assert (
        reopened.put(url + "/classifier", json={"description": ""}).status_code == 200
    )
    assert reopened.post(url + "/classifier/compile", json={}).status_code == 400
    assert (
        reopened.put(
            url + "/classifier", json=body, headers={"sec-fetch-site": "cross-site"}
        ).status_code
        == 403
    )
    reopened.request("DELETE", url, json={})
    assert reopened.get(url + "/classifier").status_code == 404
    with engine.connect() as db:
        db.execute(
            "ATTACH DATABASE ? AS annotations", (str(corpus / "annotations.sqlite"),)
        )
        assert (
            db.execute("SELECT count(*) FROM annotations.classifier_drafts").fetchone()[
                0
            ]
            == 0
        )
