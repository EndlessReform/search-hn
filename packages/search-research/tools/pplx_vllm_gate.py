"""Bounded native-vLLM compatibility, numerical agreement and batch-64 pilot.

The server returns unnormalized mean-pooled floats. The only model-specific
client transform is Perplexity's published tanh/round/clamp quantizer. This pilot
never changes old caches or invokes a paid model API.
"""

import argparse
import hashlib
import json
import time
from pathlib import Path

import httpx
import numpy as np
import polars as pl
from search_research.embedding_baseline import shortened
from search_research.sovereign_run import CORPUS, HASHES, QUESTIONS, ROOT
from search_research.sovereign_score import load_native
from tokenizers import Tokenizer


def encode(client, inputs):
    start = time.perf_counter()
    response = client.post(
        "/v1/embeddings",
        json={
            "model": "pplx-embed-v1-0.6b",
            "input": inputs,
            "encoding_format": "float",
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


def run(args):
    root = args.output
    root.mkdir(parents=True, exist_ok=True)
    for name, path in [("corpus", CORPUS), ("questions", QUESTIONS)]:
        assert hashlib.sha256(path.read_bytes()).hexdigest() == HASHES[name]
    corpus = pl.read_parquet(CORPUS)
    questions = pl.read_parquet(QUESTIONS)
    tokenizer = Tokenizer.from_file(str(ROOT / "tokenizer.json"))
    texts = corpus["input"].to_list()
    lengths = np.asarray([len(e.ids) for e in tokenizer.encode_batch(texts)])
    rng = np.random.default_rng(20260905)
    sample = rng.choice(len(corpus), 256, replace=False)
    long = np.argsort(lengths)[-64:]
    selected = np.unique(np.concatenate([sample, long]))
    references = load_native(ROOT, "documents", len(corpus))[selected]
    reference_queries = load_native(ROOT, "queries", len(questions))
    rows = []
    with httpx.Client(base_url="http://127.0.0.1:58080", timeout=180) as client:
        response = client.get("/v1/models")
        response.raise_for_status()
        (root / "server-models.json").write_text(json.dumps(response.json(), indent=2))
        warm = encode(client, [texts[int(sample[0])]])
        output = []
        for start in range(0, len(selected), 64):
            values, seconds, norms = encode(
                client, [texts[int(i)] for i in selected[start : start + 64]]
            )
            output.append(values)
            rows.append(
                {
                    "phase": "document_check",
                    "batch": start,
                    "n": len(values),
                    "seconds": seconds,
                    "raw_norm_min": float(norms.min()),
                    "raw_norm_max": float(norms.max()),
                }
            )
        docs = np.concatenate(output)
        output = []
        for start in range(0, len(questions), 64):
            values, seconds, norms = encode(
                client, questions["input"][start : start + 64].to_list()
            )
            output.append(values)
            rows.append(
                {
                    "phase": "query_check",
                    "batch": start,
                    "n": len(values),
                    "seconds": seconds,
                    "raw_norm_min": float(norms.min()),
                    "raw_norm_max": float(norms.max()),
                }
            )
        queries = np.concatenate(output)
        np.save(root / "document-indices.npy", selected)
        np.save(root / "documents.npy", docs)
        np.save(root / "queries.npy", queries)
        agreement = {}
        for name, actual, reference in [
            ("documents", docs, references),
            ("queries", queries, reference_queries),
        ]:
            cos = np.sum(shortened(actual, 1024) * shortened(reference, 1024), axis=1)
            agreement[name] = {
                "n": len(actual),
                "cosine_min": float(cos.min()),
                "cosine_mean": float(cos.mean()),
                "equal_coordinates": float((actual == reference).mean()),
                "max_absolute_difference": int(
                    np.abs(actual.astype(np.int16) - reference).max()
                ),
            }
        # Identical 256-document sample across sizes; warm each shape before timing.
        for size in [1, 8, 16, 32, 64]:
            encode(client, [texts[int(i)] for i in sample[:size]])
            for repeat in range(2):
                start = time.perf_counter()
                for offset in range(0, len(sample), size):
                    encode(
                        client, [texts[int(i)] for i in sample[offset : offset + size]]
                    )
                seconds = time.perf_counter() - start
                rows.append(
                    {
                        "phase": "throughput",
                        "batch": size,
                        "repeat": repeat,
                        "n": len(sample),
                        "seconds": seconds,
                        "documents_per_second": len(sample) / seconds,
                    }
                )
        # 64 longest corpus inputs stress batching; scheduler may split at token cap.
        _stress, seconds, _ = encode(client, [texts[int(i)] for i in long])
        rows.append(
            {
                "phase": "longest_64",
                "n": 64,
                "tokens": int(lengths[long].sum()),
                "max_tokens": int(lengths[long].max()),
                "seconds": seconds,
            }
        )
    (root / "agreement.json").write_text(json.dumps(agreement, indent=2))
    (root / "timings.json").write_text(json.dumps(rows, indent=2))
    print(
        json.dumps(
            {"agreement": agreement, "timings": rows, "warmup_seconds": warm[1]},
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    run(parser.parse_args())
