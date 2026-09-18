"""Explicit local embedding recipe and strict TEI transport for research runs.

This boundary keeps query/document roles out of the HTTP and sharding code. It
does not change the production agent or the archived OpenAI embedding harness.
Perplexity's integer-valued output is preserved; cosine normalization belongs to
the retrieval stage, after any future dimension shortening.
"""

import time
from typing import Literal

import httpx
import numpy as np
from pydantic import BaseModel, ConfigDict, Field


class EmbeddingRecipe(BaseModel):
    """A vector space is identified by its full recipe, not just its width."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    model_id: str = "perplexity-ai/pplx-embed-v1-0.6b"
    revision: str = "2c4d510dd4a732063c31a0f70193e35067b51fd8"
    dimensions: int = Field(default=1024, gt=0)
    compute_dtype: Literal["float32", "float16", "bfloat16"] = "float32"
    pooling: Literal["mean", "last-token"] = "mean"
    output: Literal["native-int8", "float32"] = "native-int8"
    query_prefix: str = ""
    document_prefix: str = ""
    truncate: Literal[False] = False
    normalize: Literal[False] = False

    @property
    def numpy_dtype(self):
        """Storage precision is independent of server compute precision."""
        return np.int8 if self.output == "native-int8" else np.float32


QWEN_RECIPE = EmbeddingRecipe(
    model_id="Qwen/Qwen3-Embedding-0.6B",
    revision="97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3",
    compute_dtype="float16",
    pooling="last-token",
    output="float32",
    query_prefix="Instruct: Given a web search query, retrieve relevant passages that answer the query\nQuery:",
)

JINA_RECIPE = EmbeddingRecipe(
    model_id="jinaai/jina-embeddings-v5-text-small-retrieval",
    revision="6856e76bb72982e58de0620458a4e8b3614da340",
    compute_dtype="float16",
    pooling="last-token",
    output="float32",
    query_prefix="Query: ",
    document_prefix="Document: ",
)

NEMOTRON_RECIPE = EmbeddingRecipe(
    model_id="nvidia/Nemotron-3-Embed-1B-BF16",
    revision="c0c9fea93ea424587517f2c59e20db9f1d6bf615",
    dimensions=2048,
    compute_dtype="bfloat16",
    pooling="mean",
    output="float32",
    query_prefix="query: ",
    document_prefix="passage: ",
)


def float_vectors(values, count: int, dimensions: int) -> np.ndarray:
    """Preserve real-valued output, rejecting invalid shapes and zero vectors."""
    matrix = np.asarray(values, dtype=np.float32)
    assert matrix.shape == (count, dimensions), f"Unexpected shape {matrix.shape}"
    assert np.isfinite(matrix).all(), "Nonfinite embeddings"
    assert np.any(matrix != 0, axis=1).all(), "Zero embedding"
    return matrix


def native_int8(values, count: int, dimensions: int) -> np.ndarray:
    """Reject malformed or normalized responses instead of silently casting them."""
    matrix = np.asarray(values, dtype=np.float32)
    assert matrix.shape == (count, dimensions), f"Unexpected shape {matrix.shape}"
    assert np.isfinite(matrix).all(), "Nonfinite embeddings"
    assert np.equal(matrix, np.rint(matrix)).all(), "Expected native integer output"
    assert (matrix >= -128).all() and (matrix <= 127).all(), "Out-of-range int8"
    assert np.any(matrix != 0, axis=1).all(), "Zero embedding"
    return matrix.astype(np.int8)


class TeiEmbeddings:
    """Synchronous bounded requests; failures stay visible and are never hidden.

    TEI may internally batch inputs. Each request explicitly disables truncation,
    even if the server's startup configuration allows shorter maximum lengths.
    The caller owns resume policy and timing journals; this transport never retries.
    """

    def __init__(self, base_url: str, recipe: EmbeddingRecipe):
        self.recipe = recipe
        self.client = httpx.Client(base_url=base_url, timeout=180)

    def info(self) -> dict:
        response = self.client.get("/info")
        response.raise_for_status()
        result = response.json()
        assert result["model_id"] == self.recipe.model_id, result
        assert result["model_sha"] == self.recipe.revision, result
        assert result["model_dtype"] == self.recipe.compute_dtype, result
        expected_pooling = {"mean": "mean", "last-token": "last_token"}[
            self.recipe.pooling
        ]
        assert result["model_type"]["embedding"]["pooling"] == expected_pooling, result
        return result

    def encode(self, inputs: list[str], role: Literal["query", "document"]):
        """Return native vectors and request timing, preserving input order."""
        assert inputs and all(isinstance(s, str) and s for s in inputs)
        assert role in ("query", "document"), role
        prefix = (
            self.recipe.query_prefix if role == "query" else self.recipe.document_prefix
        )
        start = time.perf_counter()
        response = self.client.post(
            "/embed",
            json={
                "inputs": [prefix + s for s in inputs],
                "normalize": False,
                "truncate": False,
            },
        )
        response.raise_for_status()
        validate = native_int8 if self.recipe.output == "native-int8" else float_vectors
        matrix = validate(response.json(), len(inputs), self.recipe.dimensions)
        return matrix, {
            "seconds": time.perf_counter() - start,
            "inputs": len(inputs),
            "headers": {
                k: v for k, v in response.headers.items() if k.startswith("x-")
            },
        }

    def close(self):
        self.client.close()
