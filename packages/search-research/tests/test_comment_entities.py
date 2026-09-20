"""Verify context coverage, original offsets, ontology validation, and API lookup."""

import re
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError
from search_research.comment_entities import (
    EntityRequest,
    install_entities,
    text_windows,
)
from search_research.comment_explorer import CommentExplorer
from test_comment_explorer import corpus as corpus_fixture

corpus = corpus_fixture


class Tokenizer:
    model_max_length = 8

    def __call__(self, inputs, **kwargs):
        assert kwargs["truncation"] is False
        return {"input_ids": [[0] * (len(row) + 2) for row in inputs]}


class Processor:
    transformer_tokenizer = Tokenizer()

    def words_splitter(self, text):
        return [(m.group(), m.start(), m.end()) for m in re.finditer(r"\S+", text)]

    def prepare_inputs(self, texts, labels):
        return [labels + row for row in texts], [len(labels)]


def test_windows_preserve_unicode_and_tail_with_overlap():
    model = SimpleNamespace(
        data_processor=Processor(), config=SimpleNamespace(max_len=6, max_width=2)
    )
    text = "😀 One two three four five six seven eight nine tail"
    windows = list(text_windows(model, text, ["book"]))
    assert len(windows) > 1
    covered = set()
    for offset, chunk in windows:
        assert text[offset : offset + len(chunk)] == chunk
        covered.update(range(offset, offset + len(chunk)))
    assert all(i in covered for i, c in enumerate(text) if not c.isspace())
    assert windows[-1][1].endswith("tail")
    assert set(windows[0][1].split()) & set(windows[1][1].split())
    with pytest.raises(ValueError, match="single word"):
        list(text_windows(model, text, ["label"] * 6))


@pytest.mark.parametrize(
    "labels,thresholds",
    [
        ([], {}),
        ([" "], {}),
        (["book", "book"], {}),
        (["book"], {"book": float("nan")}),
        (["book"], {"book": 1.1}),
    ],
)
def test_invalid_ontology(labels, thresholds):
    with pytest.raises(ValidationError):
        EntityRequest(labels=labels, thresholds=thresholds)


def test_api_reads_complete_comment_without_model(corpus):
    explorer = CommentExplorer(corpus, "http://unused", threads=1)
    received = []

    class Extractor:
        def predict(self, text, request):
            received.append((text, request))
            return {"text": text, "spans": []}

    app = FastAPI()
    install_entities(app, explorer, Extractor())
    client = TestClient(app)
    body = {"labels": ["book title"], "thresholds": {"book title": 0.4}}
    response = client.post("/api/comments/10/entities", json=body)
    assert response.status_code == 200
    assert response.json()["text"] == received[0][0]
    assert received[0][1].labels == ["book title"]
    assert client.post("/api/comments/999999/entities", json=body).status_code == 404
    assert (
        client.post("/api/comments/10/entities", json={"labels": []}).status_code == 422
    )
