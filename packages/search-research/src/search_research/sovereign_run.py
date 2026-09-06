"""Pilot and resumable native-dimension backfill on the second Luna snapshot.

Run pilot first: it times the same seeded document sample at several batch sizes
and predicts full-corpus inference time. Bulk embedding is a separate explicit
command, so a slow pilot cannot accidentally launch a long backfill.
"""

import argparse
import fcntl
import hashlib
import json
import os
import time
from pathlib import Path

import httpx
import numpy as np
import polars as pl
from search_agent.journal import Journal
from tokenizers import Tokenizer

from search_research.tei_embeddings import (
    JINA_RECIPE,
    NEMOTRON_RECIPE,
    QWEN_RECIPE,
    EmbeddingRecipe,
    TeiEmbeddings,
)

ROOT = Path("data/sovereign-embeddings-20260905/pplx-1024")
CORPUS = Path("data/luna-semantic-20260904/corpus.parquet")
QUESTIONS = Path("data/te3-large-baseline-20260904/questions.parquet")
HASHES = {
    "corpus": "5a7b46f7ba78e1a1978b2aca947954168c284a4e0bd205ad310c5a9af6ed2dfd",
    "questions": "737382c55bee6050050bfa712757cab865c45abe38b45477db1ac53aea067e4a",
}


def inputs(root: Path, recipe: EmbeddingRecipe | None = None):
    """Check frozen bytes and count local-model tokens without changing old files."""
    root.mkdir(parents=True, exist_ok=True)
    recipe = recipe if recipe is not None else EmbeddingRecipe()
    frames = []
    tokenizer_path = root / "tokenizer.json"
    if not tokenizer_path.exists():
        url = f"https://huggingface.co/{recipe.model_id}/resolve/{recipe.revision}/tokenizer.json"
        response = httpx.get(url, follow_redirects=True, timeout=60)
        response.raise_for_status()
        tokenizer_path.write_bytes(response.content)
    tokenizer = Tokenizer.from_file(str(tokenizer_path))
    tokenizer.no_truncation()
    tokenizer.no_padding()
    for name, path in (("corpus", CORPUS), ("questions", QUESTIONS)):
        assert hashlib.sha256(path.read_bytes()).hexdigest() == HASHES[name], name
        frame = pl.read_parquet(path)
        prefix = recipe.document_prefix if name == "corpus" else recipe.query_prefix
        counts = [
            len(x.ids)
            for x in tokenizer.encode_batch([prefix + s for s in frame["input"]])
        ]
        assert min(counts) > 0 and max(counts) <= 2048, (name, max(counts))
        frames.append(frame.with_columns(pl.Series("local_tokens", counts)))
    corpus, questions = frames
    assert corpus.height == 64638 and questions.height == 196
    assert corpus["id"].is_sorted() and corpus["id"].n_unique() == corpus.height
    assert set(questions["target_id"]) <= set(corpus["id"])
    manifest = {
        "recipe": recipe.model_dump(),
        "input_sha256": HASHES,
        "tokenizer_sha256": hashlib.sha256(tokenizer_path.read_bytes()).hexdigest(),
        "image_digest": None
        if recipe == NEMOTRON_RECIPE
        else "sha256:a7d82dfef16c3bf1a95e93f5b226f358312512dbb0d585b48c3cf886f9d470a9",
        "stories": corpus.height,
        "questions": questions.height,
        "document_tokens": int(corpus["local_tokens"].sum()),
        "question_tokens": int(questions["local_tokens"].sum()),
        "max_document_tokens": int(corpus["local_tokens"].max()),
        "max_question_tokens": int(questions["local_tokens"].max()),
    }
    path = root / "manifest.json"
    if path.exists():
        assert json.loads(path.read_text()) == manifest, "Resume recipe changed"
    else:
        path.write_text(json.dumps(manifest, indent=2))
    return corpus, questions, recipe


def pilot(root, provider, corpus, sample_size, sizes):
    """Same randomized inputs for each batch size; two repetitions after warmup.

    Include all length ranges in a seeded random sample. Record padded token work
    as well as actual tokens: this FP32 backend's padding can change throughput.
    Longest-input safety is tested separately, not mixed into the representative ETA.
    """
    info = provider.info()
    (root / "server-info.json").write_text(json.dumps(info, indent=2))
    rng = np.random.default_rng(20260905)
    indices = rng.choice(corpus.height, size=sample_size, replace=False)
    sample = corpus[indices.tolist()]
    smoke = [
        "A short test",
        "Linux security vulnerability disclosure",
        "Unicode café 東京 https://example.org/a?q=42",
    ]
    single = np.concatenate([provider.encode([s], "document")[0] for s in smoke])
    together = provider.encode(smoke, "document")[0]
    cosines = np.sum(single.astype(float) * together, axis=1) / (
        np.linalg.norm(single.astype(float), axis=1)
        * np.linalg.norm(together.astype(float), axis=1)
    )
    assert np.min(cosines) >= 0.9999, f"Singleton/batch mismatch: {cosines}"
    provider.encode(
        [corpus.sort("local_tokens", descending=True)["input"][0]], "document"
    )
    (root / "smoke.json").write_text(
        json.dumps({"singleton_batch_cosines": cosines.tolist()})
    )
    path = root / "pilot.json"
    rows = json.loads(path.read_text()) if path.exists() else []
    assert all(r["documents"] == sample_size for r in rows), "Pilot sample size changed"
    for size in sizes:
        provider.encode(sample["input"][:size].to_list(), "document")
        for repeat in range(2):
            if any(r["batch_size"] == size and r["repeat"] == repeat for r in rows):
                continue
            started = time.perf_counter()
            padded = 0
            for a in range(0, sample.height, size):
                batch = sample.slice(a, size)
                provider.encode(batch["input"].to_list(), "document")
                padded += batch.height * int(batch["local_tokens"].max())
            seconds = time.perf_counter() - started
            row = {
                "batch_size": size,
                "repeat": repeat,
                "documents": sample.height,
                "seconds": seconds,
                "docs_per_second": sample.height / seconds,
                "full_corpus_seconds": corpus.height * seconds / sample.height,
                "tokens": int(sample["local_tokens"].sum()),
                "padded_tokens": padded,
            }
            rows.append(row)
            (root / "pilot.json").write_text(json.dumps(rows, indent=2))
            print(json.dumps(row), flush=True)
    pl.DataFrame(rows).write_csv(root / "pilot.csv")


def backfill(root, provider, corpus, questions, size):
    """Durable ordered shards with recipe/shape checks and explicit progress.

    Writes are fsynced and atomically renamed; completed shards can be resumed.
    Failures are journaled and raised. No silent retries, missing rows, or fallback.
    """
    info = provider.info()
    path = root / "backfill-config.json"
    config = {"batch_size": size, "concurrency": 1, "server": info}
    if path.exists():
        assert json.loads(path.read_text()) == config, "Backfill settings changed"
    else:
        path.write_text(json.dumps(config, indent=2))
    journal = Journal(root / "requests.jsonl")
    started, processed, last_print = time.perf_counter(), 0, 0.0
    try:
        for kind, frame, role in (
            ("documents", corpus, "document"),
            ("queries", questions, "query"),
        ):
            directory = root / kind
            directory.mkdir(exist_ok=True)
            for a in range(0, frame.height, size):
                b = min(a + size, frame.height)
                path = directory / f"{a:07d}-{b:07d}.npy"
                if path.exists():
                    existing = np.load(path, allow_pickle=False)
                    assert existing.dtype == provider.recipe.numpy_dtype
                    assert existing.shape == (b - a, provider.recipe.dimensions)
                    assert np.isfinite(existing).all()
                    assert np.any(existing != 0, axis=1).all()
                    continue
                journal.write("attempt", kind=kind, start=a, end=b)
                try:
                    matrix, timing = provider.encode(
                        frame["input"][a:b].to_list(), role
                    )
                except Exception as exc:
                    journal.write("error", kind=kind, start=a, end=b, error=str(exc))
                    raise
                with path.with_suffix(".partial").open("wb") as f:
                    np.save(f, matrix, allow_pickle=False)
                    f.flush()
                    os.fsync(f.fileno())
                path.with_suffix(".partial").replace(path)
                journal.write("complete", kind=kind, start=a, end=b, **timing)
                processed += b - a
                elapsed = time.perf_counter() - started
                if elapsed - last_print >= 15 or b == frame.height:
                    progress = {
                        "kind": kind,
                        "through": b,
                        "total": frame.height,
                        "session_seconds": elapsed,
                        "session_inputs": processed,
                        "docs_per_second": processed / elapsed,
                        "remaining_seconds": (frame.height - b) * elapsed / processed,
                    }
                    (root / "progress.json").write_text(json.dumps(progress, indent=2))
                    print(json.dumps(progress), flush=True)
                    last_print = elapsed
        completion = root / "backfill-complete.json"
        if not completion.exists():
            completion.write_text(
                json.dumps(
                    {
                        "session_seconds": time.perf_counter() - started,
                        "session_inputs": processed,
                        "documents": corpus.height,
                        "queries": questions.height,
                    },
                    indent=2,
                )
            )
        # An already-complete rerun validates shards above but preserves the
        # original measurement rather than replacing it with a zero-work timing.
    finally:
        journal.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("pilot", "embed"))
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument(
        "--model", choices=("pplx", "qwen", "jina", "nemotron"), default="pplx"
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:58080")
    parser.add_argument(
        "--batch-size", type=int, choices=(1, 8, 16, 32, 64), default=32
    )
    parser.add_argument("--sample-size", type=int, default=256)
    parser.add_argument(
        "--batch-sizes",
        type=int,
        nargs="+",
        choices=(1, 8, 16, 32, 64),
        default=[1, 8, 16, 32, 64],
    )
    args = parser.parse_args()
    if args.model != "pplx" and args.root == ROOT:
        dim = 2048 if args.model == "nemotron" else 1024
        args.root = ROOT.parent / f"{args.model}-{dim}"
    args.root.mkdir(parents=True, exist_ok=True)
    with (args.root / "driver.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        selected = {
            "pplx": EmbeddingRecipe(),
            "qwen": QWEN_RECIPE,
            "jina": JINA_RECIPE,
            "nemotron": NEMOTRON_RECIPE,
        }[args.model]
        corpus, questions, recipe = inputs(args.root, selected)
        provider = TeiEmbeddings(args.base_url, recipe)
        try:
            if args.phase == "pilot":
                pilot(args.root, provider, corpus, args.sample_size, args.batch_sizes)
            else:
                backfill(args.root, provider, corpus, questions, args.batch_size)
        finally:
            provider.close()


if __name__ == "__main__":
    main()
