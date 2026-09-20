"""Check durable labels, whole-comment weighting, and fractional centroid ranking."""

import sqlite3

import numpy as np
import pytest
from fastapi.testclient import TestClient
from search_research.comment_annotations import AnnotationStore
from search_research.comment_centroids import CommentCentroids, unit
from search_research.comment_explorer import CommentExplorer
from search_research.comment_explorer_web import create_app
from test_comment_explorer import corpus as corpus_fixture

corpus = corpus_fixture


def test_store_reopen_revision_and_identity(tmp_path):
    path = tmp_path / "annotations.sqlite"
    store = AnnotationStore(path, "frozen-a")
    sid = store.create("Books")
    store.member(sid, 10, True)
    store.member(sid, 10, True)
    assert store.get(sid)["revision"] == 1
    reopened = AnnotationStore(path, "frozen-a")
    assert reopened.get(sid)["comment_ids"] == [10]
    reopened.rename(sid, "Novels")
    assert store.list_sets() == [
        {"id": sid, "name": "Novels", "revision": 1, "count": 1, "negative_count": 0}
    ]
    with pytest.raises(ValueError, match="different frozen corpus"):
        AnnotationStore(path, "frozen-b")
    store.member(sid, 10, False)
    assert store.get(sid)["revision"] == 2
    store.delete(sid)
    with pytest.raises(KeyError):
        store.get(sid)
    assert store.create("New") > sid  # Deleted IDs never alias cached rankings.


@pytest.mark.parametrize("dtype", ["int8", "f32"])
def test_fractional_queries_and_comment_pooling(corpus, dtype):
    engine = CommentExplorer(corpus, "http://unused", threads=1, dtype=dtype)
    store = AnnotationStore(corpus / "annotations.sqlite", engine.corpus_id)
    centroids = CommentCentroids(engine, store)
    reps = centroids.representations([10, 20])
    np.testing.assert_allclose(reps[0, :2], [1, 0])
    expected_long = unit(np.array([0, 1]) + unit(np.array([1, 1])))
    np.testing.assert_allclose(reps[1, :2], expected_long, atol=1e-7)
    plain, _ = centroids.query([10, 20], "mean", 7, 5, 100)
    zero, _ = centroids.query([10, 20], "corrected", 0, 5, 100)
    np.testing.assert_array_equal(plain, zero)
    np.testing.assert_allclose(
        plain[:2], unit(np.array([1, 0]) + expected_long), atol=1e-7
    )
    background = np.zeros(1024, np.float32)
    background[:2] = [0.07, 0.11]
    centroids.baseline = lambda seed, size: background
    corrected, diagnostics = centroids.query([10, 20], "corrected", 0.6, 5, 100)
    np.testing.assert_allclose(corrected, unit(reps.mean(axis=0) - 0.6 * background))
    assert diagnostics["background_norm"] == pytest.approx(np.linalg.norm(background))
    query = np.zeros(1024, np.float32)
    query[:2] = [0.0037, -0.0021]
    (rows, scores, _, _), cached = engine.rank_vector(query, ("fractional",))
    assert not cached
    raw = np.load(corpus / "vectors.npy").astype(np.float32)
    expected = (raw / np.linalg.norm(raw, axis=1)[:, None]) @ unit(query)
    np.testing.assert_allclose(scores, expected[rows], atol=2e-7)
    assert engine.rank_vector(query, ("fractional",))[1]
    with pytest.raises(ValueError, match="zero"):
        engine.rank_vector(np.zeros(1024), ("zero",))
    with pytest.raises(ValueError, match="at least one"):
        centroids.query([], "mean", 1, 1, 100)
    centroids.baseline = lambda seed, size: reps.mean(axis=0)
    with pytest.raises(ValueError, match="nearly zero"):
        centroids.query([10, 20], "corrected", 1, 5, 100)


def test_nested_baseline_reproducible_unique_comments(corpus):
    # 10,000 comments, but comment 20 has an extra chunk. Sampling must not
    # give it two tickets, and persisted IDs must survive a new engine wrapper.
    engine = CommentExplorer(corpus, "http://unused", threads=1)
    store = AnnotationStore(corpus / "annotations.sqlite", engine.corpus_id)
    centroids = CommentCentroids(engine, store)
    centroids.unique_ids = np.arange(1, 10001)

    def representations(ids):
        out = np.zeros((len(ids), 1024), np.float32)
        out[:, 0] = np.asarray(ids) / 10000
        out[:, 1] = np.sqrt(1 - out[:, 0] ** 2)
        return out

    centroids.representations = representations
    small = centroids.baseline(17, 100)
    medium = centroids.baseline(17, 1000)
    large = centroids.baseline(17, 10000)
    ids = store.baseline(17, lambda: pytest.fail("Must reuse saved sample"))
    assert len(ids) == len(set(ids)) == 10000
    for n, mean in [(100, small), (1000, medium), (10000, large)]:
        np.testing.assert_allclose(mean, representations(ids[:n]).mean(axis=0))
    centroids.baseline_cache.clear()
    np.testing.assert_array_equal(small, centroids.baseline(17, 100))
    assert not np.array_equal(small, centroids.baseline(18, 100))


def test_api_crud_search_and_boundary(corpus):
    engine = CommentExplorer(corpus, "http://unused", threads=1)
    with pytest.raises(ValueError, match="separate database"):
        create_app(engine, corpus / "index.sqlite")
    client = TestClient(create_app(engine))
    sid = client.post("/api/sets", json={"name": "  Books  "}).json()["id"]
    assert client.post("/api/sets", json={"name": "Books"}).status_code == 409
    assert client.post("/api/sets", json={"name": "   "}).status_code == 422
    assert client.post("/api/sets", data={"name": "unsafe"}).status_code == 415
    assert (
        client.post(
            "/api/sets",
            json={"name": "unsafe"},
            headers={"sec-fetch-site": "cross-site"},
        ).status_code
        == 403
    )
    url = f"/api/sets/{sid}"
    assert client.put(url + "/members/999", json={}).status_code == 404
    assert client.put(url + "/members/10", json={}).json()["count"] == 1
    body = {"mode": "mean", "set_id": sid, "page_size": 1}
    result = client.post("/api/experiment", json=body).json()
    assert result["total"] == 2
    assert result["results"][0]["comment_id"] == 30
    assert result["set_revision"] == 1
    assert client.post("/api/experiment", json=body).json()["cached"]
    assert (
        client.post("/api/experiment", json=body | {"page": 2}).json()["results"][0][
            "comment_id"
        ]
        == 20
    )
    shown = client.post("/api/experiment", json=body | {"hide_positives": False}).json()
    assert shown["total"] == 3 and shown["results"][0]["comment_id"] == 10
    client.put(url + "/members/20", json={})
    changed = client.post("/api/experiment", json=body).json()
    assert not changed["cached"] and changed["set_revision"] == 2
    assert (
        client.post(
            "/api/experiment", json=body | {"mode": "corrected", "gamma": 0}
        ).status_code
        == 200
    )
    too_small = client.post("/api/experiment", json=body | {"mode": "corrected"})
    assert too_small.status_code == 400 and "corpus has 3" in too_small.json()["detail"]
    for updates in (
        {"gamma": -1},
        {"seed": -1},
        {"page": 0},
        {"baseline_size": 42},
        {"mode": "wat"},
    ):
        assert client.post("/api/experiment", json=body | updates).status_code == 422
    assert client.patch(url, json={"name": "Novels"}).json()["name"] == "Novels"
    detail = client.get(url).json()
    assert detail["results"][0]["text"] == "<script>alert(1)</script>"
    assert client.get(url + "?page=0").status_code == 400
    assert client.get("/assets/lab.js").status_code == 200
    assert client.request("DELETE", url + "/members/10", json={}).json()["count"] == 1
    assert client.request("DELETE", url, json={}).status_code == 200
    assert client.get(url).status_code == 404
    with sqlite3.connect(corpus / "annotations.sqlite") as db:
        assert db.execute("SELECT count(*) FROM members").fetchone()[0] == 0


@pytest.mark.parametrize("mode", ["text", "mean", "corrected"])
def test_paging_keeps_applied_membership_after_label_edits(corpus, monkeypatch, mode):
    def encode(client, inputs, *, priority):
        query = np.zeros((1, 1024), np.int8)
        query[0, 0] = 127
        return query, 0, np.ones(1)

    monkeypatch.setattr("search_research.comment_explorer.encode", encode)
    engine = CommentExplorer(corpus, "http://unused", threads=1)
    client = TestClient(create_app(engine))
    sid = client.post("/api/sets", json={"name": "Paging"}).json()["id"]
    client.put(f"/api/sets/{sid}/members/10", json={})
    body = {"mode": mode, "q": "books", "set_id": sid, "gamma": 0, "page_size": 1}
    first = client.post("/api/experiment", json=body).json()
    snapshot = body | {"query_positive_ids": first["query_positive_ids"]}
    second = client.post("/api/experiment", json=snapshot | {"page": 2}).json()
    assert first["results"][0]["comment_id"] == (10 if mode == "text" else 30)
    assert second["results"][0]["comment_id"] == (30 if mode == "text" else 20)
    client.put(f"/api/sets/{sid}/members/20", json={})
    back = client.post("/api/experiment", json=snapshot).json()
    forward = client.post("/api/experiment", json=snapshot | {"page": 2}).json()
    assert back["results"] == first["results"]
    assert forward["results"] == second["results"]
    assert forward["total"] == (3 if mode == "text" else 2) and forward["cached"]
    assert forward["positive_ids"] == [10, 20]
    assert forward["query_positive_ids"] == [10]
    applied = client.post("/api/experiment", json=body).json()
    assert applied["query_positive_ids"] == [10, 20]
    assert applied["total"] == (3 if mode == "text" else 1)
    last_page = applied["total"]  # One result per page in this fixture.
    last = client.post("/api/experiment", json=body | {"page": last_page}).json()
    assert len(last["results"]) == 1
    if mode == "text":
        assert last["results"][0]["comment_id"] == 20
        visible = client.post(
            "/api/experiment", json=body | {"hide_positives": False}
        ).json()
        assert visible["results"] == applied["results"]


def test_negative_notes_persist_and_leave_search_unchanged(corpus):
    engine = CommentExplorer(corpus, "http://unused", threads=1)
    client = TestClient(create_app(engine))
    sid = client.post("/api/sets", json={"name": "Examples"}).json()["id"]
    url = f"/api/sets/{sid}"
    client.put(url + "/members/10", json={})
    body = {"mode": "mean", "set_id": sid}
    before = client.post("/api/experiment", json=body).json()
    note = 'Meta: mentions category\n<script>not markup</script>'
    saved = client.put(url + "/negatives/20", json={"note": note}).json()
    assert saved["comment_ids"] == [10] and saved["revision"] == 1
    after = client.post("/api/experiment", json=body).json()
    assert before["results"] == after["results"] and after["cached"]
    reopened = TestClient(create_app(engine))
    detail = reopened.get(url).json()
    assert [(r["comment_id"], r["label"]) for r in detail["results"]] == [
        (10, "positive"), (20, "negative")]
    assert detail["results"][1]["note"] == note
    assert detail["total"] == 2
    assert reopened.put(url + "/negatives/20", json={"note": ""}).json()["negatives"][0]["note"] == ""
    converted = reopened.put(url + "/negatives/10", json={}).json()
    assert converted["comment_ids"] == [] and converted["negative_count"] == 2
    converted = reopened.put(url + "/members/20", json={}).json()
    assert converted["comment_ids"] == [20]
    assert [n["comment_id"] for n in converted["negatives"]] == [10]
    assert reopened.put(url + "/negatives/999", json={}).status_code == 404
    reopened.request("DELETE", url + "/negatives/10", json={})
    assert reopened.get(url).json()["negative_count"] == 0
    reopened.put(url + "/negatives/10", json={})
    reopened.request("DELETE", url, json={})
    with sqlite3.connect(corpus / "annotations.sqlite") as db:
        assert db.execute("SELECT count(*) FROM negatives").fetchone()[0] == 0


def test_parent_context_local_missing_story_and_unknown(corpus):
    with sqlite3.connect(corpus / "index.sqlite") as db:
        db.execute("""UPDATE comments SET source_json='{"parent":20}'
                      WHERE comment_id=10""")
        db.execute("""UPDATE comments SET source_json='{"parent":999}'
                      WHERE comment_id=20""")
        db.execute("""UPDATE comments SET source_json='{"parent":100}'
                      WHERE comment_id=30""")
    engine = CommentExplorer(corpus, "http://unused", threads=1)
    client = TestClient(create_app(engine))
    context = client.get("/api/comments/10/parent").json()
    assert context["status"] == "available"
    assert context["parent"]["comment_id"] == 20
    assert context["parent"]["text"] == "<script>alert(1)</script>"
    assert client.get("/api/comments/20/parent").json()["status"] == "outside_slice"
    assert client.get("/api/comments/30/parent").json()["status"] == "story"
    assert client.get("/api/comments/999/parent").status_code == 404
    with sqlite3.connect(corpus / "index.sqlite") as db:
        db.execute("UPDATE comments SET source_json='{}' WHERE comment_id=10")
    assert client.get("/api/comments/10/parent").json()["status"] == "unknown"
