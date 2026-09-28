"""Check saved-stage joins, unpaired labels, navigation and untrusted text."""

import json
import sqlite3

import pytest
from fastapi.testclient import TestClient
from search_research.resolver_review import ResolverReview
from search_research.resolver_review_web import create_app


@pytest.fixture
def checkpoint(tmp_path):
    path = tmp_path / "checkpoint.sqlite"
    with sqlite3.connect(path) as db:
        db.executescript("""
        CREATE TABLE refs(id TEXT PRIMARY KEY, ordinal INTEGER, payload TEXT);
        CREATE TABLE documents(id TEXT PRIMARY KEY, payload TEXT);
        CREATE TABLE rankings(id TEXT PRIMARY KEY, payload TEXT);
        CREATE TABLE selections(id TEXT, model TEXT, payload TEXT);
        """)
        docs = [
            {
                "id": f"/works/OL{i}W",
                "title": "Book <script>alert(1)</script>",
                "authors": ["Author"],
            }
            for i in range(1, 4)
        ]
        for d in docs:
            db.execute("INSERT INTO documents VALUES (?,?)", (d["id"], json.dumps(d)))
        for i in range(3):
            ident = f"{i}:0:4"
            ref = {
                "id": ident,
                "title": "Book",
                "context": "Book <script>alert(1)</script>",
                "comment_id": i,
                "start": 0,
                "end": 4,
                "score": 0.8,
                "candidates": [
                    {"id": d["id"], "bm25": 10 - n} for n, d in enumerate(docs)
                ],
            }
            ranking = {
                "id": ident,
                "ids": [d["id"] for d in docs],
                "scores": [1, 9, 3],
                "top3": [docs[n]["id"] for n in [1, 2, 0]],
            }
            db.execute("INSERT INTO refs VALUES (?,?,?)", (ident, i, json.dumps(ref)))
            db.execute(
                "INSERT INTO rankings VALUES (?,?)", (ident, json.dumps(ranking))
            )
            for model, work in [("luna", docs[0]["id"]), ("deepseek-native", None)]:
                if i == 1 and model == "deepseek-native":
                    continue
                receipt = {
                    "selection": {"work_id": work},
                    "request": {
                        "messages": [
                            {"role": "system", "content": "Choose"},
                            {
                                "role": "user",
                                "content": json.dumps({"candidates": docs}),
                            },
                        ]
                    },
                }
                db.execute(
                    "INSERT INTO selections VALUES (?,?,?)",
                    (ident, model, json.dumps(receipt)),
                )
    return path


def test_order_and_missing(checkpoint):
    store = ResolverReview(checkpoint)
    detail = store.detail("0:0:4")
    assert [c["rerank_rank"] for c in detail["candidates"]] == [3, 1, 2]
    assert [c["bm25"] for c in detail["candidates"]] == [10, 9, 8]
    assert all(c["metadata_copies"] == 3 for c in detail["candidates"])
    assert store.summary("deepseek-native")["missing"] == 1
    assert store.summary("deepseek-native")["one_group_shortlists"] == 3
    assert len(store.filtered("deepseek-native", "disagree", "", False)) == 2


def test_multiple_choices_outside_initial_shortlist(checkpoint):
    """Split repairs retain every selected work even outside the initial pool."""
    with sqlite3.connect(checkpoint) as db:
        db.execute(
            "INSERT INTO documents VALUES (?,?)",
            (
                "/works/CHILD",
                json.dumps(
                    {
                        "id": "/works/CHILD",
                        "title": "Repaired work",
                        "authors": ["Writer"],
                    }
                ),
            ),
        )
        db.execute(
            "UPDATE selections SET payload=? WHERE id='0:0:4' AND model='luna'",
            (
                json.dumps(
                    {
                        "selection": {"work_ids": ["/works/OL1W", "/works/CHILD"]},
                        "request": {"messages": []},
                    }
                ),
            ),
        )
    store = ResolverReview(checkpoint)
    assert set(store.detail("0:0:4")["selected_documents"]) == {
        "/works/OL1W",
        "/works/CHILD",
    }
    response = TestClient(create_app(checkpoint)).get("/?category=all&id=0:0:4")
    assert response.status_code == 200
    assert "Repaired work" in response.text


def test_html_and_fragments(checkpoint):
    client = TestClient(create_app(checkpoint))
    response = client.get("/?category=all&id=1:0:4")
    assert response.status_code == 200
    assert "Missing output — not an abstention" in response.text
    assert "<script>alert(1)</script>" not in response.text
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in response.text
    assert "<mark>Book</mark>" in response.text
    assert "BM25 SEARCH · ALL 3" in response.text
    fragment = client.get(
        "/?category=all&q=Book&duplicates=1", headers={"HX-Request": "true"}
    )
    assert "<!doctype" not in fragment.text
    assert "duplicates=1" in fragment.text
    assert "3 matches" in fragment.text
    assert client.get("/htmx.js").status_code == 200
    assert client.get("/assets/resolver.css").status_code == 200
    assert client.get("/api/reference/unknown").status_code == 404
    assert client.get("/?model=invalid").status_code == 422
    assert client.get("/?q=absent").status_code == 200
    assert "No matching references" in client.get("/?q=absent").text
    assert client.get("/api/reference/0:0:4").json()["ranking"]["scores"] == [1, 9, 3]


def test_live_source_panels(checkpoint, monkeypatch):
    from search_research import resolver_review_remote as remote

    def fake_fetch(url):
        if "/item/7.json" in url:
            return {
                "id": 7,
                "by": "reader",
                "text": "Shades of Great Expectations.",
                "parent": 6,
            }
        if "/item/6.json" in url:
            return {"id": 6, "text": "<p>Parent context &amp; detail</p>"}
        if "/editions.json" in url:
            return {
                "size": 1,
                "entries": [
                    {
                        "key": "/books/OL1M",
                        "title": "Dramascripts",
                        "subjects": ["Drama"],
                    }
                ],
            }
        return {"key": "/works/OL1W", "title": "<script>unsafe</script>", "revision": 5}

    monkeypatch.setattr(remote, "fetch", fake_fetch)
    client = TestClient(create_app(checkpoint))
    hn = client.get("/source/hn/7").text
    assert "Shades of Great Expectations." not in hn
    assert "Parent context &amp; detail" in hn
    assert "not supplied to the selectors" in hn
    work = client.get("/source/work/OL1W").text
    assert "Dramascripts" in work and "revision" in work
    assert "<script>unsafe</script>" not in work
    assert client.get("/source/work/bad").status_code == 400


def test_previous_choice_outside_new_candidate_pool(checkpoint):
    """A changed search must still render the previous run's selected work."""
    with sqlite3.connect(checkpoint) as db:
        doc = {"id": "/works/OLDW", "title": "Previous work", "authors": ["Writer"]}
        db.execute("INSERT INTO documents VALUES (?,?)", (doc["id"], json.dumps(doc)))
        db.execute(
            "INSERT INTO selections VALUES (?,?,?)",
            (
                "0:0:4",
                "luna-original",
                json.dumps(
                    {"selection": {"work_id": doc["id"]}, "request": {"messages": []}}
                ),
            ),
        )
    response = TestClient(create_app(checkpoint)).get(
        "/?model=luna-original&category=all&id=0:0:4"
    )
    assert response.status_code == 200
    assert "Previous work" in response.text
