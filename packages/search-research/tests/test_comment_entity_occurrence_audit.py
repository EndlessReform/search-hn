"""Occurrence repairs preserve originals and reject incomplete or unsafe edits."""

import hashlib
import json
import runpy
import sqlite3
from pathlib import Path

import pytest

AUDIT = runpy.run_path(
    str(Path(__file__).parents[1] / "tools/comment_entity_occurrence_audit.py")
)


def test_detection_flags_repetition_without_deciding_referent():
    entities = [
        {"id": 1, "title": "Escape", "start": 0, "end": 6},
        {"id": 2, "title": "Escape", "start": 20, "end": 26},
        {"id": 3, "title": "My Escape", "start": 17, "end": 26},
        {"id": 4, "title": "Unmatched", "start": None, "end": None},
    ]
    assert AUDIT["flagged_ids"](entities) == ({1, 2}, {2, 3})


@pytest.fixture
def repair(tmp_path):
    source = tmp_path / "original.sqlite"
    with sqlite3.connect(source) as db:
        db.execute(
            "CREATE TABLE entity_labels (id INTEGER PRIMARY KEY, deleted INTEGER)"
        )
        db.executemany("INSERT INTO entity_labels VALUES (?,0)", [(1,), (2,), (3,)])
        db.execute("CREATE TABLE notes (text TEXT)")
        db.execute("INSERT INTO notes VALUES ('preserve this')")
    original = source.read_bytes()
    packet = tmp_path / "packet.json"
    packet.write_text(
        json.dumps(
            {
                "source": str(source),
                "sha256": hashlib.sha256(original).hexdigest(),
                "rows": [
                    {
                        "repeated_ids": [1, 2, 3],
                        "nested_ids": [],
                        "entities": [
                            {"id": i, "origin": "manual" if i == 3 else "predicted"}
                            for i in [1, 2, 3]
                        ],
                    }
                ],
            }
        )
    )
    decisions = tmp_path / "decisions.json"
    decisions.write_text(
        json.dumps(
            [
                {
                    "entity_id": i,
                    "action": "remove" if i == 2 else "keep",
                    "reason": "checked occurrence",
                }
                for i in [1, 2, 3]
            ]
        )
    )
    return source, original, packet, decisions, tmp_path / "corrected.sqlite"


def test_apply_changes_only_new_copy_and_records_decisions(repair):
    source, original, packet, decisions, target = repair
    AUDIT["apply"](packet, decisions, target)
    assert source.read_bytes() == original
    with sqlite3.connect(target) as db:
        assert db.execute("SELECT * FROM entity_labels ORDER BY id").fetchall() == [
            (1, 0),
            (2, 1),
            (3, 0),
        ]
        assert db.execute("SELECT text FROM notes").fetchone() == ("preserve this",)
    assert json.loads(target.with_suffix(".corrections.json").read_text())[
        "decisions"
    ] == json.loads(decisions.read_text())
    with pytest.raises(AssertionError, match="Never overwrite"):
        AUDIT["apply"](packet, decisions, target)


@pytest.mark.parametrize("failure", ["missing", "manual", "stale"])
def test_apply_rejects_invalid_repairs_before_creating_target(repair, failure):
    source, _, packet, decisions, target = repair
    edits = json.loads(decisions.read_text())
    if failure == "missing":
        edits.pop()
    elif failure == "manual":
        edits[-1]["action"] = "remove"
    else:
        with sqlite3.connect(source) as db:
            db.execute("INSERT INTO notes VALUES ('changed')")
    decisions.write_text(json.dumps(edits))
    with pytest.raises(AssertionError):
        AUDIT["apply"](packet, decisions, target)
    assert not target.exists()
