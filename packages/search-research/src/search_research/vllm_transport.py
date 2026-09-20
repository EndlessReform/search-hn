"""Shared raw-vLLM transport and Pplx quantization for research jobs."""

import time

import numpy as np


def encode(client, inputs, *, priority: int | None = None):
    """Encode raw pooled outputs with the pinned Pplx integer transform.

    Priority is optional to preserve the historical gate request contract.
    Background callers explicitly pass 1; no proxy output may enter this path.
    """
    start = time.perf_counter()
    response = client.post(
        "/v1/embeddings",
        json={
            "model": "pplx-embed-v1-0.6b",
            "input": inputs,
            "encoding_format": "float",
            **({"priority": priority} if priority is not None else {}),
        },
    )
    response.raise_for_status()
    data = sorted(response.json()["data"], key=lambda r: r["index"])
    assert [r["index"] for r in data] == list(range(len(inputs)))
    raw = np.asarray([r["embedding"] for r in data], dtype=np.float32)
    assert raw.shape == (len(inputs), 1024) and np.isfinite(raw).all()
    assert np.any(raw != 0, axis=1).all()
    # Perplexity st_quantize.py: tanh -> round(*127) -> clamp -> native int8.
    native = np.clip(np.rint(np.tanh(raw) * 127), -128, 127).astype(np.int8)
    assert np.any(native != 0, axis=1).all(), "Quantization produced a zero embedding"
    return native, time.perf_counter() - start, np.linalg.norm(raw, axis=1)
