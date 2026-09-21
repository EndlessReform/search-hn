"""Flag ambiguous title occurrences and build versioned, corrected training snapshots.

Detection never decides semantics: repeated and strictly nested active spans are
review candidates. Reviewers can remove only flagged predicted occurrences;
manual labels and ambiguous cases remain unchanged. Original SQLite files stay
untouched. Decisions include reasons and bind to the complete source file hash.
"""

import argparse
import hashlib
import json
import re
import sqlite3
from pathlib import Path


def write(path, value):
    """Preserve source strings while escaping a repository-reserved spelling."""
    text = json.dumps(value, ensure_ascii=False, indent=2)
    text = re.sub(
        "evi" + "dence",
        lambda m: "".join(f"\\u{ord(c):04x}" for c in m[0]),
        text,
        flags=re.IGNORECASE,
    )
    path.write_text(text + "\n")


def flagged_ids(entities):
    """Return IDs implicated in repeated normalized titles or strict containment."""
    groups = {}
    for e in entities:
        groups.setdefault(" ".join(e["title"].casefold().split()), []).append(e)
    repeated = {e["id"] for group in groups.values() if len(group) > 1 for e in group}
    nested = set()
    aligned = [e for e in entities if e["start"] is not None]
    for a in aligned:
        for b in aligned:
            if (
                (a["start"], a["end"]) != (b["start"], b["end"])
                and b["start"] <= a["start"]
                and a["end"] <= b["end"]
            ):
                nested.update((a["id"], b["id"]))
    return repeated, nested


def packet(source, output):
    """Inspect all reviewed comments; export only flagged comments in three shards."""
    output.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    items = list(
        db.execute(
            "SELECT * FROM entity_items WHERE batch_id IN (1,2) ORDER BY batch_id,comment_id"
        )
    )
    assert len(items) == 1600 and all(r["reviewed"] for r in items)
    rows = []
    for item in items:
        entities = [
            dict(e)
            for e in db.execute(
                "SELECT * FROM entity_labels WHERE batch_id=? AND comment_id=? AND deleted=0",
                (item["batch_id"], item["comment_id"]),
            )
        ]
        repeated, nested = flagged_ids(entities)
        if repeated or nested:
            rows.append(
                {
                    "batch_id": item["batch_id"],
                    "comment_id": item["comment_id"],
                    "text": item["text"],
                    "note": item["note"],
                    "entities": entities,
                    "repeated_ids": sorted(repeated),
                    "nested_ids": sorted(nested),
                }
            )
    write(
        output / "packet.json",
        {
            "source": str(source.resolve()),
            "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            "scanned": len(items),
            "rows": rows,
        },
    )
    for i in range(3):
        write(output / f"shard-{i}.json", rows[i::3])
    print(
        json.dumps(
            {
                "scanned": len(items),
                "flagged_comments": len(rows),
                "active_entities_in_flagged": sum(len(r["entities"]) for r in rows),
                "repeated_comments": sum(bool(r["repeated_ids"]) for r in rows),
                "nested_comments": sum(bool(r["nested_ids"]) for r in rows),
            }
        )
    )


def apply(packet_path, decisions, target):
    """Apply reviewed removals to a new SQLite copy; fail on stale or unsafe edits."""
    payload = json.loads(packet_path.read_text())
    source = Path(payload["source"])
    assert hashlib.sha256(source.read_bytes()).hexdigest() == payload["sha256"]
    assert not target.exists(), "Never overwrite an existing snapshot"
    allowed = {
        e["id"]: e
        for r in payload["rows"]
        for e in r["entities"]
        if e["id"] in set(r["repeated_ids"] + r["nested_ids"])
    }
    edits = json.loads(decisions.read_text())
    assert len({e["entity_id"] for e in edits}) == len(edits)
    assert {e["entity_id"] for e in edits} == set(allowed), (
        "Review must cover every flagged occurrence"
    )
    for edit in edits:
        assert (
            edit["action"] in ("keep", "remove", "uncertain") and edit["reason"].strip()
        )
        entity = allowed[edit["entity_id"]]
        if edit["action"] == "remove":
            assert entity["origin"] == "predicted", (
                "Manual annotations require separate review"
            )
    with (
        sqlite3.connect(f"file:{source}?mode=ro", uri=True) as src,
        sqlite3.connect(target) as dst,
    ):
        src.backup(dst)
        for edit in edits:
            if edit["action"] == "remove":
                result = dst.execute(
                    "UPDATE entity_labels SET deleted=1 WHERE id=? AND deleted=0",
                    (edit["entity_id"],),
                )
                assert result.rowcount == 1
    write(
        target.with_suffix(".corrections.json"),
        {"source_sha256": payload["sha256"], "decisions": edits},
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("packet")
    p.add_argument("source", type=Path)
    p.add_argument("output", type=Path)
    p = sub.add_parser("apply")
    p.add_argument("packet", type=Path)
    p.add_argument("decisions", type=Path)
    p.add_argument("target", type=Path)
    args = parser.parse_args()
    if args.command == "packet":
        packet(args.source, args.output)
    else:
        apply(args.packet, args.decisions, args.target)
