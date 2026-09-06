"""Protect native-output semantics and quantized ranking boundary cases."""

import json

import httpx
import numpy as np
import polars as pl
import pytest
from search_research.sovereign_run import backfill
from search_research.sovereign_score import hybrid, load_native, top_ids
from search_research.tei_embeddings import (
    JINA_RECIPE,
    NEMOTRON_RECIPE,
    QWEN_RECIPE,
    EmbeddingRecipe,
    TeiEmbeddings,
    float_vectors,
    native_int8,
)


def test_native_quantization_is_not_silent_float_cast():
    np.testing.assert_array_equal(native_int8([[127, -128]], 1, 2), [[127, -128]])
    for bad in ([[0.5, 1]], [[128, 1]], [[0, 0]], [[float("nan"), 1]]):
        with pytest.raises(AssertionError):
            native_int8(bad, 1, 2)


def test_topk_ties_are_resolved_before_cutoff():
    ids = np.array([50, 40, 30, 20, 10])
    scores = np.array([2.0, 1.0, 1.0, 1.0, 1.0])
    assert top_ids(scores, ids, 3) == [50, 10, 20]


def test_shard_ranges_cannot_cancel_out_wrong_row_counts(tmp_path):
    """A short first shard and long second shard must not shift document IDs."""
    (tmp_path / "backfill-config.json").write_text('{"batch_size": 2}')
    recipe = EmbeddingRecipe(dimensions=2)
    (tmp_path / "manifest.json").write_text(json.dumps({"recipe": recipe.model_dump()}))
    directory = tmp_path / "documents"
    directory.mkdir()
    np.save(directory / "0000000-0000002.npy", np.ones((1, 2), dtype=np.int8))
    np.save(directory / "0000002-0000004.npy", np.ones((3, 2), dtype=np.int8))
    with pytest.raises(AssertionError, match="Shard row range differs"):
        load_native(tmp_path, "documents", 4)


def test_second_run_fusion_has_half_lexical_weight():
    assert hybrid([10, 20], [30, 20]) == [20, 10, 30]


def test_transport_preserves_roles_and_disables_truncation_and_normalization():
    seen = []

    def respond(request):
        seen.append(json.loads(request.content))
        return httpx.Response(200, json=[[1, -2]])

    provider = TeiEmbeddings(
        "http://localhost", EmbeddingRecipe(dimensions=2, query_prefix="Q: ")
    )
    provider.client.close()
    provider.client = httpx.Client(
        base_url="http://localhost", transport=httpx.MockTransport(respond)
    )
    try:
        provider.encode(["search"], "query")
        provider.encode(["title\nurl"], "document")
    finally:
        provider.close()
    assert seen == [
        {"inputs": ["Q: search"], "normalize": False, "truncate": False},
        {"inputs": ["title\nurl"], "normalize": False, "truncate": False},
    ]


@pytest.mark.parametrize(
    "selected_recipe", [EmbeddingRecipe(), QWEN_RECIPE, NEMOTRON_RECIPE]
)
def test_completed_resume_preserves_measurement_and_makes_no_requests(
    tmp_path, selected_recipe
):
    class CachedOnly:
        recipe = selected_recipe

        def info(self):
            return {}

        def encode(self, inputs, role):
            pytest.fail("Completed shards must not be embedded again")

    for kind in ("documents", "queries"):
        (tmp_path / kind).mkdir()
        np.save(
            tmp_path / kind / "0000000-0000001.npy",
            np.ones((1, selected_recipe.dimensions), dtype=selected_recipe.numpy_dtype),
        )
    completion = tmp_path / "backfill-complete.json"
    original = (
        '{"session_seconds": 42.0, "session_inputs": 2, "documents": 1, "queries": 1}'
    )
    completion.write_text(original)
    frame = pl.DataFrame({"input": ["test"]})
    backfill(tmp_path, CachedOnly(), frame, frame, 16)
    assert completion.read_text() == original
    assert (tmp_path / "requests.jsonl").read_text() == ""


@pytest.mark.parametrize(
    "recipe,query,document",
    [
        (
            QWEN_RECIPE,
            "Instruct: Given a web search query, retrieve relevant passages that answer the query\nQuery:search",
            "title\nurl",
        ),
        (JINA_RECIPE, "Query: search", "Document: title\nurl"),
        (NEMOTRON_RECIPE, "query: search", "passage: title\nurl"),
    ],
)
def test_float_output_and_exact_role_prefixes(recipe, query, document):
    seen = []

    def respond(request):
        seen.append(json.loads(request.content))
        return httpx.Response(200, json=[[0.125, -0.75]])

    provider = TeiEmbeddings(
        "http://localhost", recipe.model_copy(update={"dimensions": 2})
    )
    provider.client.close()
    provider.client = httpx.Client(
        base_url="http://localhost", transport=httpx.MockTransport(respond)
    )
    try:
        matrix, _ = provider.encode(["search"], "query")
        provider.encode(["title\nurl"], "document")
    finally:
        provider.close()
    assert matrix.dtype == np.float32
    np.testing.assert_array_equal(matrix, [[0.125, -0.75]])
    assert seen[0]["inputs"] == [query]
    assert seen[1]["inputs"] == [document]
    for bad in ([[0, 0]], [[float("inf"), 1]], [[1]]):
        with pytest.raises(AssertionError):
            float_vectors(bad, 1, 2)
