"""Frozen inputs, durable vector publication, concurrent failure, and resumption."""

import hashlib
import json
import subprocess
import threading
from pathlib import Path

import httpx
import numpy as np
import pytest
from search_research.comment_index import (
    FORMAT_VERSION,
    RECIPE,
    connect_index,
    create_index,
    put_metadata,
)
from search_research.comment_selection_rows import usable_rows
from search_research.comment_slice_embed import EmbeddingSettings, embed, verify
from search_research.comment_slice_export import CommentSlice, append_comments, prepare
from search_research.comment_vector_store import CommentVectorStore
from search_research.embedding_backfill import file_hash
from tokenizers import Tokenizer, models, pre_tokenizers


def frozen_slice(root: Path, count=6):
    root.mkdir()
    tokenizer = Tokenizer(models.WordLevel({"[UNK]": 0}, unk_token="[UNK]"))
    tokenizer.pre_tokenizer = pre_tokenizers.Whitespace()
    tokenizer.save(str(root / "tokenizer.json"))
    index = connect_index(root / "index.sqlite")
    create_index(index)
    digest = hashlib.sha256()
    rows = [
        {
            "comment_id": 100 + i,
            "story_id": 1,
            "author": "reader",
            "html": f"comment{i}<p>hello &amp; 世界",
        }
        for i in range(count)
    ]
    total = append_comments(index, rows, tokenizer, 0, digest)
    put_metadata(
        index,
        {
            "format_version": FORMAT_VERSION,
            "recipe": RECIPE,
            "selection": CommentSlice().model_dump(),
            "tokenizer_sha256": file_hash(root / "tokenizer.json"),
            "inputs_sha256": digest.hexdigest(),
            "comments": count,
        },
    )
    index.execute("INSERT INTO progress VALUES (1,?,0)", (total,))
    index.commit()
    index.close()
    return root


def test_published_prefix_survives_and_unpublished_tail_is_overwritten(tmp_path):
    root = frozen_slice(tmp_path / "slice")
    with CommentVectorStore(root) as store:
        store.write(0, np.full((2, 1024), 7, dtype=np.int8))
        store.checkpoint()
        store.write(2, np.full((2, 1024), 99, dtype=np.int8))
    with CommentVectorStore(root) as store:
        assert store.completed_rows == store.written_rows == 2
        assert store.index.execute(
            "SELECT count(*) FROM completed_embeddings"
        ).fetchone() == (2,)
        store.write(2, np.full((4, 1024), -3, dtype=np.int8))
        store.checkpoint()
    values = np.load(root / "vectors.npy")
    assert values.dtype == np.int8 and values.shape == (6, 1024)
    assert (values[:2] == 7).all() and (values[2:] == -3).all()
    verify(root)


def test_abrupt_exit_between_vector_sync_and_sqlite_commit(tmp_path):
    root = frozen_slice(tmp_path / "slice")
    with CommentVectorStore(root) as store:
        store.write(0, np.full((2, 1024), 7, dtype=np.int8))
        store.checkpoint()
    script = """
import os,sys
from pathlib import Path
from unittest.mock import patch
import numpy as np
from search_research.comment_vector_store import CommentVectorStore
with CommentVectorStore(Path(sys.argv[1])) as store:
    store.write(2,np.full((4,1024),99,dtype=np.int8))
    with patch.object(store,'_commit_checkpoint',side_effect=lambda *args: os._exit(73)):
        store.checkpoint()
"""
    child = subprocess.run(
        [
            "uv",
            "run",
            "--locked",
            "--package",
            "search-research",
            "python",
            "-c",
            script,
            str(root),
        ],
        check=False,
    )
    assert child.returncode == 73
    with CommentVectorStore(root) as store:
        assert store.completed_rows == 2
        store.write(2, np.full((4, 1024), -4, dtype=np.int8))
        store.checkpoint()
    verify(root)
    assert (np.load(root / "vectors.npy")[2:] == -4).all()


def test_corrupt_or_missing_committed_vectors_are_not_silently_rebuilt(tmp_path):
    root = frozen_slice(tmp_path / "slice")
    with CommentVectorStore(root) as store:
        store.write(0, np.ones((2, 1024), dtype=np.int8))
        store.checkpoint()
    values = np.lib.format.open_memmap(root / "vectors.npy", mode="r+")
    values[0, 0] = 55
    values.flush()
    del values
    with (
        pytest.raises(AssertionError, match="checksum mismatch"),
        CommentVectorStore(root),
    ):
        pass
    (root / "vectors.npy").unlink()
    with pytest.raises(AssertionError, match="missing"), CommentVectorStore(root):
        pass


def test_concurrent_http_failure_resumes_only_uncommitted_inputs(tmp_path, monkeypatch):
    root = frozen_slice(tmp_path / "slice")
    calls = []
    fail = True

    def respond(request):
        inputs = json.loads(request.content)["input"]
        calls.extend(inputs)
        if fail and inputs[0].startswith("comment4"):
            return httpx.Response(503)
        return httpx.Response(
            200,
            json={
                "data": [
                    {"index": i, "embedding": [0.5] * 1024} for i in range(len(inputs))
                ]
            },
        )

    real_client = httpx.Client
    monkeypatch.setattr(
        "search_research.comment_slice_embed.httpx.Client",
        lambda **kwargs: real_client(**kwargs, transport=httpx.MockTransport(respond)),
    )
    settings = EmbeddingSettings(
        base_url="https://example.test", batch_size=2, concurrency=2, checkpoint_rows=4
    )
    with pytest.raises(httpx.HTTPStatusError):
        embed(root, settings)
    fail = False
    calls.clear()
    embed(root, settings)
    assert len(calls) == 2 and all(
        text.startswith(("comment4", "comment5")) for text in calls
    )
    calls.clear()
    embed(root, settings)
    assert calls == []
    verify(root)


def test_existing_slice_cannot_be_reinterpreted_and_writer_is_exclusive(tmp_path):
    root = frozen_slice(tmp_path / "slice")
    prepare(root, CommentSlice(), "unused")
    with pytest.raises(AssertionError, match="selection differs"):
        prepare(root, CommentSlice(kind="year", year=2025), "unused")
    with (
        CommentVectorStore(root),
        pytest.raises(BlockingIOError),
        CommentVectorStore(root),
    ):
        pass


def test_markup_only_top_replies_are_recorded_and_replaced_in_order(
    tmp_path, monkeypatch
):
    index = connect_index(tmp_path / "index.sqlite")
    create_index(index)

    def row(number, html):
        return {
            "story_id": 1,
            "story_title": "story",
            "story_score": 101,
            "story_day": "2025-01-01",
            "comment_id": number,
            "display_order": number,
            "html": html,
            "author": "reader",
        }

    original = [row(1, "<i>"), row(2, "useful"), row(3, "<p>")]

    def replacements(connection, story_id, offset):
        assert story_id == 1 and offset == 3
        yield row(4, "<b>")
        yield row(5, "next")
        yield row(6, "last")
        pytest.fail("Must stop after finding three usable replies")

    monkeypatch.setattr(
        "search_research.comment_selection_rows.replacement_pages", replacements
    )
    selected = list(usable_rows(iter(original), None, CommentSlice(), index))
    assert [item["comment_id"] for item in selected] == [2, 5, 6]
    assert index.execute(
        "SELECT comment_id FROM exclusions ORDER BY comment_id"
    ).fetchall() == [(1,), (3,), (4,)]
    index.close()


def test_year_rows_with_missing_lineage_and_empty_markup(tmp_path):
    index = connect_index(tmp_path / "index.sqlite")
    create_index(index)
    rows = [
        {"comment_id": i, "story_id": None, "author": None, "html": html}
        for i, html in enumerate(["<i>", "hello"])
    ]
    selected = list(
        usable_rows(iter(rows), None, CommentSlice(kind="year", year=2025), index)
    )
    tokenizer = Tokenizer(models.WordLevel({"[UNK]": 0}, unk_token="[UNK]"))
    assert append_comments(index, selected, tokenizer, 0, hashlib.sha256()) == 1
    assert index.execute("SELECT story_id FROM comments").fetchone() == (None,)
    index.close()


def test_out_of_order_responses_keep_each_vector_with_its_input(tmp_path, monkeypatch):
    root = frozen_slice(tmp_path / "slice")
    second_done = threading.Event()

    def respond(request):
        inputs = json.loads(request.content)["input"]
        if inputs[0].startswith("comment0"):
            assert second_done.wait(2), "Second request did not run concurrently"
        elif inputs[0].startswith("comment2"):
            second_done.set()
        values = [
            (int(text.split("\n")[0].removeprefix("comment")) + 1) / 10
            for text in inputs
        ]
        return httpx.Response(
            200,
            json={
                "data": [
                    {"index": i, "embedding": [value] * 1024}
                    for i, value in enumerate(values)
                ]
            },
        )

    real_client = httpx.Client
    monkeypatch.setattr(
        "search_research.comment_slice_embed.httpx.Client",
        lambda **kwargs: real_client(**kwargs, transport=httpx.MockTransport(respond)),
    )
    embed(
        root,
        EmbeddingSettings(
            base_url="https://example.test",
            batch_size=2,
            concurrency=2,
            checkpoint_rows=4,
        ),
    )
    actual = np.load(root / "vectors.npy")
    expected = np.rint(np.tanh(np.arange(1, 7, dtype=np.float32) / 10) * 127).astype(
        np.int8
    )
    np.testing.assert_array_equal(actual, np.repeat(expected[:, None], 1024, axis=1))
