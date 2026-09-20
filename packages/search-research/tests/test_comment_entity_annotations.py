"""Review persistence, ownership, Unicode spans and frozen teacher proposals."""

import json
import sqlite3

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from search_research.comment_annotations import AnnotationStore
from search_research.comment_entity_annotation_web import install_entity_annotations
from search_research.comment_entity_annotations import locate_title


def test_short_titles_are_not_matched_inside_words():
    assert locate_title("📚 With It and IT.", "It") == [(7, 9), (14, 16)]
    assert locate_title("Foundation", "Foundation (novel)") == []
    assert locate_title(
        "_Thinkpad: A Different Shade of Blue_", "Thinkpad: A Different Shade of Blue"
    ) == [(1, 36)]
    assert locate_title("__Dune__ and *Dune*", "Dune") == [(2, 6), (14, 18)]


@pytest.fixture
def review(tmp_path):
    annotations = AnnotationStore(tmp_path / "annotations.sqlite", "test-corpus")
    app = FastAPI()
    install_entity_annotations(app, annotations)
    ledger = app.state.entity_annotation_store
    book = {"title": "Dune", "author": "Frank Herbert", "start": 2, "end": 6}
    rows = [
        {
            "comment_id": cid,
            "text": "📚 Dune and Solaris",
            "split": "train",
            "source": "disagreement",
            "prediction": {"has_any_book": True, "books": [book]},
            "comparison": {"has_any_book": False, "books": []},
            "entities": [book],
        }
        for cid in (11, 12)
    ]
    batch = ledger.import_batch("test", {"model": "teacher"}, rows)
    return TestClient(app), ledger, batch


def test_delete_manual_restore_reload_export(review):
    client, ledger, batch = review
    url = f"/api/entity-annotations/{batch}/comments/11"
    original = client.get(url).json()
    eid = original["entities"][0]["id"]
    deleted = client.post(
        url, json={"action": "delete", "entity_id": eid, "revision": 0}
    ).json()
    assert deleted["entities"][0]["deleted"] == 1
    assert deleted["prediction"] == original["prediction"]
    added = client.post(
        url, json={"action": "add", "start": 11, "end": 18, "revision": 1}
    ).json()
    assert added["entities"][1]["title"] == "Solaris"
    assert added["entities"][1]["origin"] == "manual"
    assert client.get(url).json() == added
    assert not client.get(f"/api/entity-annotations/{batch}/export").text
    assert client.post(url, json={"action": "review", "revision": 2}).status_code == 200
    output = json.loads(client.get(f"/api/entity-annotations/{batch}/export").text)
    assert output["extraction"] == {
        "has_any_book": True,
        "books": [{"title": "Solaris", "author": None}],
    }
    restored = client.post(
        url, json={"action": "restore", "entity_id": eid, "revision": 3}
    ).json()
    assert not restored["reviewed"]
    assert not restored["entities"][0]["deleted"]
    # Reopening the ledger must preserve manual edits and the append-only history.
    reopened = type(ledger)(ledger.annotations)
    assert reopened.item(batch, 11) == restored
    with ledger.connect() as db:
        assert (
            db.execute("SELECT count(*) FROM entity_review_events").fetchone()[0] == 4
        )


def test_stale_invalid_and_cross_comment_edits_are_atomic(review):
    client, ledger, batch = review
    url = f"/api/entity-annotations/{batch}/comments/11"
    foreign_id = ledger.item(batch, 12)["entities"][0]["id"]
    assert (
        client.post(
            url, json={"action": "delete", "entity_id": foreign_id, "revision": 0}
        ).status_code
        == 404
    )
    assert (
        client.post(
            url, json={"action": "add", "start": 2, "end": 100, "revision": 0}
        ).status_code
        == 422
    )
    assert (
        client.post(
            url, json={"action": "add", "start": 2, "end": 6, "revision": 0}
        ).status_code
        == 422
    )
    assert ledger.item(batch, 11)["revision"] == 0
    assert client.post(url, json={"action": "review", "revision": 0}).status_code == 200
    assert (
        client.post(url, json={"action": "unreview", "revision": 0}).status_code == 409
    )
    assert ledger.item(batch, 11)["reviewed"] == 1
    with pytest.raises(sqlite3.IntegrityError):
        ledger.import_batch(
            "test",
            {},
            [
                {
                    "comment_id": 1,
                    "text": "x",
                    "split": "train",
                    "source": "test",
                    "prediction": {},
                    "comparison": {},
                    "entities": [],
                }
            ],
        )
    assert len(ledger.batches()) == 1


def test_negative_review_and_no_silent_unmatched_drop(review):
    client, ledger, batch = review
    url = f"/api/entity-annotations/{batch}/comments/11"
    eid = ledger.item(batch, 11)["entities"][0]["id"]
    client.post(url, json={"action": "delete", "entity_id": eid, "revision": 0})
    client.post(url, json={"action": "review", "revision": 1})
    output = json.loads(client.get(f"/api/entity-annotations/{batch}/export").text)
    assert output["extraction"] == {"has_any_book": False, "books": []}
    assert len(output["entities"]) == 1 and output["entities"][0]["deleted"]
    assert client.get("/annotator").status_code == 200


def test_notes_persist_export_and_preserve_review(review):
    client, ledger, batch = review
    url = f"/api/entity-annotations/{batch}/comments/11"
    client.post(url, json={"action": "review", "revision": 0})
    note = "Boundary issue\nRevisit this title later."
    saved = client.post(url, json={"action": "note", "revision": 1, "note": note})
    assert saved.status_code == 200
    assert saved.json()["reviewed"] == 1
    assert client.get(url).json()["note"] == note
    assert ledger.queue(batch)[0]["has_notes"] == 1
    assert (
        json.loads(client.get(f"/api/entity-annotations/{batch}/export").text)["note"]
        == note
    )
    assert (
        client.post(
            url, json={"action": "note", "revision": 1, "note": "stale"}
        ).status_code
        == 409
    )
    client.post(url, json={"action": "note", "revision": 2, "note": ""})
    assert ledger.queue(batch)[0]["has_notes"] == 0
    assert ledger.item(batch, 11)["reviewed"] == 1
