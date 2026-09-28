"""Check ranking, frozen joins, pagination, and safe rendering on a small slice."""

import hashlib

import numpy as np
import pytest
from fastapi.testclient import TestClient
from search_research.comment_explorer import CommentExplorer
from search_research.comment_explorer_web import create_app
from search_research.comment_index import (
    FORMAT_VERSION,
    RECIPE,
    connect_index,
    create_index,
    put_metadata,
)


@pytest.fixture
def corpus(tmp_path):
    vectors = np.zeros((4, 1024), dtype=np.int8)
    vectors[:, :2] = [[127, 0], [0, 127], [127, 127], [127, 0]]
    np.save(tmp_path / "vectors.npy", vectors)
    db = connect_index(tmp_path / "index.sqlite")
    create_index(db)
    put_metadata(db, {"format_version": FORMAT_VERSION, "recipe": RECIPE})
    for cid in [10, 20, 30]:
        db.execute(
            "INSERT INTO comments VALUES (?,?,?,?,?,?)",
            (
                cid,
                100,
                "<author>",
                "<script>alert(1)</script>",
                bytes(32),
                '{"story_title":"A book"}',
            ),
        )
    for i, (cid, chunk) in enumerate([(10, 0), (20, 0), (20, 1), (30, 0)]):
        db.execute(
            "INSERT INTO chunks VALUES (?,?,?,?,?,?,?)",
            (i, cid, chunk, 0, 4, 1, bytes(32)),
        )
    db.execute("INSERT INTO progress VALUES (1,4,4)")
    db.execute(
        "INSERT INTO checkpoints(start_row,end_row,sha256) VALUES (0,4,?)",
        (hashlib.sha256(vectors).hexdigest(),),
    )
    db.commit()
    db.close()
    return tmp_path


@pytest.mark.parametrize("dtype", ["int8", "f32"])
def test_exact_ranking_join_paging_and_html(corpus, monkeypatch, dtype):
    calls = []

    def fake_encode(client, inputs, *, priority):
        calls.append(inputs)
        q = np.zeros((1, 1024), dtype=np.int8)
        q[0, 0] = 127
        return q, 0.01, np.ones(1)

    monkeypatch.setattr("search_research.comment_explorer.encode", fake_encode)
    engine = CommentExplorer(corpus, "http://unused", threads=1, dtype=dtype)
    result = engine.search("book review", page_size=2)
    assert [r["comment_id"] for r in result["results"]] == [10, 30]
    other = engine.search("book review", page=2, page_size=2)
    assert other["total"] == 3 and other["results"][0]["comment_id"] == 20
    assert other["results"][0]["chunk"] == 1
    assert other["results"][0]["score"] == pytest.approx(1 / np.sqrt(2))
    assert engine.search("book review", min_score=0.9)["total"] == 2
    assert len(calls) == 1
    assert engine.search("book review", page=100)["results"] == []
    client = TestClient(create_app(engine))
    html = client.get("/", params={"q": "book review"}).text
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
    assert "<script>alert(1)</script>" not in html
    assert client.get("/htmx.js").status_code == 200
    assert (
        client.get("/api/search", params={"q": "book review", "page": 0}).status_code
        == 422
    )
    assert client.get("/api/search", params={"q": "book review"}).json()["total"] == 3
    fragment = client.get(
        "/", params={"q": "book review"}, headers={"HX-Request": "true"}
    ).text
    assert "<article>" in fragment and "<html" not in fragment


def test_reject_incomplete_or_corrupt_slice(corpus):
    db = connect_index(corpus / "index.sqlite")
    db.execute("UPDATE progress SET completed_rows=3")
    db.commit()
    with pytest.raises(AssertionError, match="completed frozen"):
        CommentExplorer(corpus, "http://unused")
    db.execute("UPDATE progress SET completed_rows=4")
    db.commit()
    db.close()
    vector = np.load(corpus / "vectors.npy", mmap_mode="r+")
    vector[0, 0] = 126
    vector.flush()
    with pytest.raises(AssertionError, match="checksum"):
        CommentExplorer(corpus, "http://unused")


def test_storage_version_does_not_change_annotation_identity(corpus):
    compact = CommentExplorer(corpus, "http://unused", dtype="int8")
    db = connect_index(corpus / "index.sqlite")
    with db:
        db.execute("UPDATE metadata SET value='1' WHERE key='format_version'")
    db.close()
    legacy = CommentExplorer(corpus, "http://unused", dtype="int8")
    assert compact.corpus_id == legacy.corpus_id
