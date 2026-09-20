"""SQLite schema and filesystem publication primitives for frozen comment slices.

One index owns text, input-to-NPY row mappings, and durable checkpoint boundaries.
The vector payload never enters SQLite. A directory lock serializes preparation,
embedding, and verification while SQLite still allows ordinary status reads.
"""

import fcntl
import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

RECIPE = "pplx-0.6b-2c4d510dd4a7-vllm0.28.0-bf16-flash-mean-2048-tanh127-rne-v1"
FORMAT_VERSION = 1
DIMENSIONS = 1024


@contextmanager
def directory_lock(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    with (root / "writer.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def sync_file(path: Path):
    with path.open("rb") as stream:
        os.fsync(stream.fileno())


def sync_directory(path: Path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def connect_index(path: Path, *, readonly=False):
    """Require an existing index when reading; never silently create one."""
    if readonly:
        connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    else:
        connection = sqlite3.connect(path)
    connection.execute("PRAGMA foreign_keys=ON")
    if not readonly:
        connection.execute("PRAGMA synchronous=FULL")
    return connection


def create_index(connection):
    """Create row mappings independently of any particular SQL slice selector."""
    connection.executescript("""
        CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE comments(
            comment_id INTEGER PRIMARY KEY, story_id INTEGER,
            author TEXT, html TEXT NOT NULL, text TEXT NOT NULL,
            text_sha256 TEXT NOT NULL, source_json TEXT NOT NULL
        );
        CREATE TABLE inputs(
            vector_row INTEGER PRIMARY KEY CHECK(vector_row>=0),
            comment_id INTEGER NOT NULL REFERENCES comments(comment_id),
            chunk INTEGER NOT NULL, char_start INTEGER NOT NULL,
            char_end INTEGER NOT NULL, tokens INTEGER NOT NULL CHECK(tokens BETWEEN 1 AND 2048),
            input TEXT NOT NULL, input_sha256 TEXT NOT NULL,
            UNIQUE(comment_id,chunk)
        );
        CREATE TABLE exclusions(
            comment_id INTEGER PRIMARY KEY, reason TEXT NOT NULL, source_json TEXT NOT NULL
        );
        CREATE TABLE progress(
            id INTEGER PRIMARY KEY CHECK(id=1), total_rows INTEGER NOT NULL,
            completed_rows INTEGER NOT NULL CHECK(completed_rows BETWEEN 0 AND total_rows)
        );
        CREATE TABLE checkpoints(
            start_row INTEGER PRIMARY KEY, end_row INTEGER NOT NULL,
            sha256 TEXT NOT NULL, committed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CHECK(end_row>start_row)
        );
        CREATE TABLE runs(
            id INTEGER PRIMARY KEY, settings_json TEXT NOT NULL,
            started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at TEXT, status TEXT NOT NULL
        );
        CREATE VIEW completed_embeddings AS
            SELECT inputs.*, 'vectors.npy' AS vector_file
            FROM inputs, progress WHERE progress.id=1
              AND inputs.vector_row<progress.completed_rows;
    """)


def metadata(connection):
    return {
        key: json.loads(value)
        for key, value in connection.execute("SELECT key,value FROM metadata")
    }


def put_metadata(connection, values):
    connection.executemany(
        "INSERT INTO metadata VALUES (?,?)",
        [(key, json.dumps(value, sort_keys=True)) for key, value in values.items()],
    )
