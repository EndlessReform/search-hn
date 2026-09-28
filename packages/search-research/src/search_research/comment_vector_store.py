"""Incremental int8 NPY storage with SQLite publication of durable row ranges.

The index may only point at bytes already synced to the vector file. Writes past
completed_rows are speculative and can be overwritten after interruption. The
single writer fills a contiguous prefix; inference can run concurrently upstream.
"""

import hashlib
from pathlib import Path

import numpy as np

from search_research.comment_index import (
    DIMENSIONS,
    READABLE_FORMATS,
    RECIPE,
    connect_index,
    directory_lock,
    metadata,
    sync_directory,
    sync_file,
)
from search_research.embedding_backfill import file_hash


class CommentVectorStore:
    """Own the writer lock, SQLite connection, and one exactly sized NPY array."""

    def __init__(self, root: Path):
        self.root = root
        self.path = root / "vectors.npy"
        self._lock = directory_lock(root)
        self.index = None
        self._vectors = None
        self.total_rows = self.completed_rows = self.written_rows = 0

    def __enter__(self):
        self._lock.__enter__()
        try:
            assert (self.root / "index.sqlite").exists(), "Prepare the slice first"
            self.index = connect_index(self.root / "index.sqlite")
            saved = metadata(self.index)
            assert (
                saved["format_version"] in READABLE_FORMATS
                and saved["recipe"] == RECIPE
            ), "Incompatible index format or embedding recipe"
            assert saved["tokenizer_sha256"] == file_hash(
                self.root / "tokenizer.json"
            ), "Tokenizer changed"
            self.index.execute("PRAGMA journal_mode=WAL")
            self.total_rows, self.completed_rows = self.index.execute(
                "SELECT total_rows,completed_rows FROM progress WHERE id=1"
            ).fetchone()
            assert self.total_rows > 0
            self.written_rows = self.completed_rows
            if not self.path.exists():
                assert self.completed_rows == 0, "Committed vectors.npy is missing"
                temporary = self.path.with_suffix(".partial")
                # Only an uncommitted creation attempt may be replaced here.
                values = np.lib.format.open_memmap(
                    temporary,
                    mode="w+",
                    dtype=np.int8,
                    shape=(self.total_rows, DIMENSIONS),
                )
                values.flush()
                del values
                sync_file(temporary)
                temporary.replace(self.path)
                sync_directory(self.root)
            self._vectors = np.lib.format.open_memmap(self.path, mode="r+")
            assert self._vectors.dtype == np.int8
            assert self._vectors.shape == (self.total_rows, DIMENSIONS), (
                "Vector file shape differs from frozen inputs"
            )
            self.verify_checkpoints()
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, exc_type, exc, traceback):
        # Uncommitted dirty pages may reach disk; SQLite still excludes them.
        self._vectors = None
        if self.index is not None:
            self.index.close()
            self.index = None
        self._lock.__exit__(exc_type, exc, traceback)

    def verify_checkpoints(self):
        """Verify every committed range; detect missing, reordered, or damaged bytes."""
        end = 0
        for start, next_end, expected in self.index.execute(
            "SELECT start_row,end_row,sha256 FROM checkpoints ORDER BY start_row"
        ):
            assert start == end and next_end <= self.completed_rows, (
                "Checkpoint ranges are not contiguous"
            )
            actual = hashlib.sha256(self._vectors[start:next_end]).hexdigest()
            assert actual == expected, (
                f"Vector checksum mismatch in rows {start}:{next_end}"
            )
            end = next_end
        assert end == self.completed_rows, "Progress and checkpoint ledger disagree"

    def write(self, start: int, vectors: np.ndarray):
        """Write only the next unpublished range; never modify committed rows."""
        assert start == self.written_rows, "Vector writes must form a contiguous prefix"
        assert vectors.dtype == np.int8 and vectors.ndim == 2
        assert vectors.shape[1] == DIMENSIONS and len(vectors) > 0
        assert np.any(vectors != 0, axis=1).all(), "Zero embedding"
        end = start + len(vectors)
        assert end <= self.total_rows
        self._vectors[start:end] = vectors
        self.written_rows = end

    def checkpoint(self):
        """Sync NPY data before publishing its checksum and progress atomically.

        A crash after the file sync but before SQLite COMMIT leaves extra durable
        bytes, not falsely completed rows. Resume overwrites that unpublished tail.
        A SQLite commit never precedes vector durability. FULL synchronous mode
        is explicit in connect_index; filesystem/device sync semantics still apply.
        """
        start, end = self.completed_rows, self.written_rows
        if start == end:
            return
        digest = hashlib.sha256(self._vectors[start:end]).hexdigest()
        self._vectors.flush()
        sync_file(self.path)
        self._commit_checkpoint(start, end, digest)
        self.completed_rows = end

    def _commit_checkpoint(self, start, end, digest):
        """Separate publication boundary so interruption tests can target it."""
        with self.index:
            self.index.execute(
                "INSERT INTO checkpoints(start_row,end_row,sha256) VALUES (?,?,?)",
                (start, end, digest),
            )
            updated = self.index.execute(
                "UPDATE progress SET completed_rows=? WHERE id=1 AND completed_rows=?",
                (end, start),
            )
            assert updated.rowcount == 1, "Unexpected concurrent progress change"
