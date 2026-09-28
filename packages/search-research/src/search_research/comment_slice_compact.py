"""Convert complete legacy slices without changing text, vector rows or digests."""

import hashlib
import json
from itertools import zip_longest
from pathlib import Path

import numpy as np

from search_research.comment_index import (
    DIMENSIONS,
    FORMAT_VERSION,
    RECIPE,
    connect_index,
    create_index,
    directory_lock,
    metadata,
    sync_directory,
    sync_file,
)


def compact(root: Path):
    """Verify a fresh v2 database before atomically replacing a complete v1 slice.

    Hold the slice writer lock throughout. External readers must be stopped for
    replacement; otherwise they retain the old inode. The legacy database stays
    intact until all logical rows and committed vector checksums are verified.
    HTML is deliberately discarded; decoded text is the canonical retained input.
    A failed conversion leaves the original intact and a disposable partial file.
    """
    with directory_lock(root):
        target = root / "index.sqlite"
        source = connect_index(target)
        temporary = root / "index.compact.partial.sqlite"
        destination = None
        try:
            saved = metadata(source)
            if saved["format_version"] == FORMAT_VERSION:
                print("Slice already uses compact format", flush=True)
                return
            assert saved["format_version"] == 1 and saved["recipe"] == RECIPE
            total, completed = source.execute(
                "SELECT total_rows,completed_rows FROM progress"
            ).fetchone()
            assert total == completed, "Finish embeddings before compacting"
            vectors = np.load(root / "vectors.npy", mmap_mode="r")
            assert vectors.shape == (total, DIMENSIONS) and vectors.dtype == np.int8
            end = 0
            for start, stop, expected in source.execute(
                "SELECT start_row,end_row,sha256 FROM checkpoints ORDER BY start_row"
            ):
                assert start == end and stop <= completed
                assert hashlib.sha256(vectors[start:stop]).hexdigest() == expected
                end = stop
            assert end == completed, "Incomplete vector checkpoint ledger"
            del vectors
            # Fold committed WAL pages into the old file before replacement.
            assert source.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0] == 0
            source.execute("PRAGMA journal_mode=DELETE")
            temporary.unlink(missing_ok=True)
            Path(str(temporary) + "-journal").unlink(missing_ok=True)
            destination = connect_index(temporary)
            create_index(destination)
            destination.create_function("hex_bytes", 1, bytes.fromhex)
            destination.execute(
                "ATTACH DATABASE ? AS old", (target.resolve().as_uri() + "?mode=ro",)
            )
            with destination:
                destination.execute(
                    "INSERT INTO comments SELECT comment_id,story_id,author,text,hex_bytes(text_sha256),source_json FROM old.comments"
                )
                destination.execute(
                    "INSERT INTO chunks SELECT vector_row,comment_id,chunk,char_start,char_end,tokens,hex_bytes(input_sha256) FROM old.inputs"
                )
                for table in (
                    "metadata",
                    "exclusions",
                    "progress",
                    "checkpoints",
                    "runs",
                ):
                    destination.execute(
                        f"INSERT INTO {table} SELECT * FROM old.{table}"
                    )
                destination.execute(
                    "UPDATE metadata SET value=? WHERE key='format_version'",
                    (json.dumps(FORMAT_VERSION),),
                )
            destination.execute("DETACH DATABASE old")
            assert destination.execute("PRAGMA integrity_check").fetchone() == ("ok",)
            assert not destination.execute("PRAGMA foreign_key_check").fetchall()
            old_comments = source.execute(
                "SELECT comment_id,story_id,author,text,text_sha256,source_json FROM comments ORDER BY comment_id"
            )
            new_comments = destination.execute(
                "SELECT comment_id,story_id,author,text,lower(hex(text_sha256)),source_json FROM comments ORDER BY comment_id"
            )
            for old, new in zip_longest(old_comments, new_comments):
                assert old == new, "Comment changed during conversion"
                assert hashlib.sha256(new[3].encode()).hexdigest() == new[4]
            digest = hashlib.sha256()
            count = 0
            for old, new in zip_longest(
                source.execute("SELECT * FROM inputs ORDER BY vector_row"),
                destination.execute("SELECT * FROM inputs ORDER BY vector_row"),
            ):
                assert old == new, "Chunk text or coordinates changed during conversion"
                assert new[0] == count
                assert hashlib.sha256(new[6].encode()).hexdigest() == new[7]
                digest.update(json.dumps(new, ensure_ascii=False).encode() + b"\n")
                count += 1
            assert count == total and digest.hexdigest() == saved["inputs_sha256"]
            destination.close()
            destination = None
            source.close()
            before = target.stat().st_size
            sync_file(temporary)
            temporary.replace(target)
            sync_directory(root)
            print(
                json.dumps(
                    {
                        "event": "slice_compacted",
                        "format_version": FORMAT_VERSION,
                        "rows": count,
                        "before_bytes": before,
                        "after_bytes": target.stat().st_size,
                    }
                ),
                flush=True,
            )
        finally:
            source.close()
            if destination is not None:
                destination.close()
