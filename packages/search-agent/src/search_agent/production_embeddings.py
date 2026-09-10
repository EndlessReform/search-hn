"""Validate the production proxy contract; never transform or normalize twice."""

import json
from urllib.request import Request, urlopen

from pydantic import BaseModel, Field, field_validator

MODEL = "pplx-embed-v1-0.6b"


class EmbeddingEntry(BaseModel):
    index: int
    embedding: list[float] = Field(min_length=1024, max_length=1024)

    @field_validator("embedding")
    @classmethod
    def native_coordinates(cls, values: list[float]) -> list[float]:
        """Halfvec stores native signed coordinates exactly, including -128."""
        if not all(v.is_integer() and -128 <= v <= 127 for v in values) or not any(
            values
        ):
            raise ValueError("expected nonzero native signed int8 coordinates")
        return values


class EmbeddingResponse(BaseModel):
    model: str
    embedding_recipe: str
    data: list[EmbeddingEntry] = Field(min_length=1, max_length=1)


def embed_query(base_url: str, query: str, recipe: str) -> str:
    """One bounded interactive request; return validated PostgreSQL vector text.

    The table comment is authoritative. Both response recipe locations must agree
    before a vector can be compared with stored documents. Raw vLLM is not a fallback.
    """
    request = Request(
        base_url.rstrip("/") + "/embeddings",
        data=json.dumps(
            {"model": MODEL, "input": [query], "encoding_format": "float"}
        ).encode(),
        headers={
            "Content-Type": "application/json",
            "X-Embedding-Workload": "interactive",
        },
    )
    with urlopen(request, timeout=10) as response:
        body = EmbeddingResponse.model_validate_json(response.read())
        if (
            body.embedding_recipe != recipe
            or response.headers.get("X-Embedding-Recipe") != recipe
        ):
            raise ValueError("embedding recipe mismatch")
    if body.model != MODEL or body.data[0].index != 0:
        raise ValueError("embedding model or response index mismatch")
    return json.dumps(body.data[0].embedding)
