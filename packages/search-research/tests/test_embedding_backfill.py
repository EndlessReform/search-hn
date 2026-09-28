"""Transport compatibility, lossless chunking, and interrupted-job recovery."""

import json

import httpx
import numpy as np
import polars as pl
import pytest
from search_agent.journal import Journal
from search_research.comment_corpus import chunk_comments
from search_research.embedding_backfill import BatchSettings, check_manifest, run_shards
from search_research.vllm_transport import encode
from tokenizers import Tokenizer, models, pre_tokenizers


def test_raw_transport_quantizes_and_preserves_optional_priority():
    bodies = []

    def respond(request):
        bodies.append(json.loads(request.content))
        return httpx.Response(
            200, json={"data": [{"index": 0, "embedding": [1.0] * 1024}]}
        )

    with httpx.Client(
        base_url="https://example.test/raw", transport=httpx.MockTransport(respond)
    ) as client:
        vectors, _, _ = encode(client, ["text"])
        encode(client, ["text"], priority=1)
    assert "priority" not in bodies[0]
    assert bodies[1]["priority"] == 1
    assert vectors.dtype == np.int8
    assert (vectors == 97).all()


def test_failed_batch_resumes_without_repeating_completed_inputs(tmp_path):
    calls = []

    def respond(request):
        body = json.loads(request.content)
        calls.append(body["input"])
        if len(calls) == 2:
            return httpx.Response(503)
        return httpx.Response(
            200,
            json={
                "data": [
                    {"index": i, "embedding": [1.0] * 1024}
                    for i in range(len(body["input"]))
                ]
            },
        )

    journal = Journal(tmp_path / "batches.jsonl")
    settings = BatchSettings(batch_size=2, priority=1)
    try:
        with httpx.Client(
            base_url="https://example.test", transport=httpx.MockTransport(respond)
        ) as client:
            with pytest.raises(httpx.HTTPStatusError):
                run_shards(
                    client,
                    ["a", "b", "c"],
                    tmp_path,
                    journal=journal,
                    kind="test",
                    settings=settings,
                )
            result = run_shards(
                client,
                ["a", "b", "c"],
                tmp_path,
                journal=journal,
                kind="test",
                settings=settings,
            )
            assert result["new_rows"] == 1
            assert calls == [["a", "b"], ["c"], ["c"]]
            result = run_shards(
                client,
                ["a", "b", "c"],
                tmp_path,
                journal=journal,
                kind="test",
                settings=settings,
            )
            assert result["new_rows"] == 0
    finally:
        journal.close()


def test_chunks_reconstruct_unicode_text_and_respect_token_limit():
    tokenizer = Tokenizer(models.WordLevel({"[UNK]": 0}, unk_token="[UNK]"))
    tokenizer.pre_tokenizer = pre_tokenizers.Whitespace()
    text = "Hello 世界!\n" * 20
    frame = pl.DataFrame({"story_id": [1], "comment_id": [2], "text": [text]})
    chunks = chunk_comments(frame, tokenizer, max_tokens=8)
    assert chunks.height > 1
    assert "".join(chunks["input"]) == text
    assert chunks["tokens"].max() <= 8
    for row in chunks.iter_rows(named=True):
        assert text[row["char_start"] : row["char_end"]] == row["input"]


def test_changed_manifest_cannot_resume(tmp_path):
    path = tmp_path / "manifest.json"
    check_manifest(path, {"hash": "one"})
    with pytest.raises(AssertionError, match="Resume mismatch"):
        check_manifest(path, {"hash": "two"})


def test_historical_backfill_keeps_layout_and_resumes(tmp_path, monkeypatch):
    import importlib.util
    from pathlib import Path

    from search_research.embedding_backfill import file_hash

    path = Path(__file__).parents[1] / "tools" / "pplx_vllm_backfill.py"
    spec = importlib.util.spec_from_file_location("historical_backfill", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    corpus = tmp_path / "corpus.parquet"
    questions = tmp_path / "questions.parquet"
    pl.DataFrame({"input": ["old corpus"]}).write_parquet(corpus)
    pl.DataFrame({"input": ["old question"]}).write_parquet(questions)
    monkeypatch.setattr(module, "CORPUS", corpus)
    monkeypatch.setattr(module, "QUESTIONS", questions)
    monkeypatch.setattr(
        module,
        "HASHES",
        {"corpus": file_hash(corpus), "questions": file_hash(questions)},
    )
    calls = []

    def respond(request):
        body = json.loads(request.content)
        calls.append(body)
        assert "priority" not in body
        return httpx.Response(
            200, json={"data": [{"index": 0, "embedding": [1.0] * 1024}]}
        )

    real_client = httpx.Client
    monkeypatch.setattr(
        module.httpx,
        "Client",
        lambda **kwargs: real_client(**kwargs, transport=httpx.MockTransport(respond)),
    )
    output = tmp_path / "historical"
    module.run(output)
    manifest = (output / "manifest.json").read_bytes()
    module.run(output)
    assert len(calls) == 2
    assert (output / "manifest.json").read_bytes() == manifest
    assert json.loads((output / "backfill-config.json").read_text()) == {
        "batch_size": 64
    }
    for kind in ("documents", "queries"):
        assert np.load(output / kind / "0000000-0000001.npy").shape == (1, 1024)


def test_batched_chunking_matches_individual_encoding(monkeypatch):
    """Cross a batch boundary and preserve special tokens and long-text splits."""
    from search_research import comment_corpus
    from tokenizers import processors

    tokenizer = Tokenizer(models.WordLevel({"[UNK]": 0, "[CLS]": 1}, unk_token="[UNK]"))
    tokenizer.pre_tokenizer = pre_tokenizers.Whitespace()
    tokenizer.post_processor = processors.TemplateProcessing(
        single="[CLS] $A", special_tokens=[("[CLS]", 1)]
    )
    texts = ["Hello 世界!", " ", "", "a b c d e f g h i j " * 3] * 513
    frame = pl.DataFrame(
        {"story_id": [1] * len(texts), "comment_id": range(len(texts)), "text": texts}
    )
    batched = chunk_comments(frame, tokenizer, max_tokens=8)

    def individually_encoded(frame, tokenizer):
        for row in frame.iter_rows(named=True):
            yield row, tokenizer.encode(row["text"])

    monkeypatch.setattr(comment_corpus, "encoded_comments", individually_encoded)
    serial = chunk_comments(frame, tokenizer, max_tokens=8)
    assert batched.to_dicts() == serial.to_dicts()
