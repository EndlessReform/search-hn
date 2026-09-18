"""Resumable batch-64 BF16 gate on the frozen corpus, separate from FP32 caches."""

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
from pplx_vllm_gate import encode
from search_agent.journal import Journal
from search_research.sovereign_run import CORPUS, HASHES, QUESTIONS
from search_research.tei_embeddings import EmbeddingRecipe


def run(root):
    root.mkdir(parents=True, exist_ok=True)
    with (root / "run.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        recipe = EmbeddingRecipe(compute_dtype="bfloat16")
        manifest = {
            "recipe": recipe.model_dump(),
            "hashes": HASHES,
            "backend": "vllm-0.28.0-native-Qwen3Model-bidirectional-FLASH_ATTN",
            "image": "vllm/vllm-openai@sha256:61fc8a896b0a4fbbbdc063bc4b0dbc25ce98e02b5050c24aeb7830ac02039b14",
            "pooling": "MEAN, use_activation=false",
            "output_transform": "float32 tanh, round(*127), clamp[-128,127], int8",
            "max_num_seqs": 64,
            "max_num_batched_tokens": 8192,
            "max_model_len": 2048,
            "kv_cache_memory_bytes": 0,
            "gpu_memory_utilization": 0.35,
            "enforce_eager": True,
        }
        p = root / "manifest.json"
        if p.exists():
            assert json.loads(p.read_text()) == manifest
        else:
            p.write_text(json.dumps(manifest, indent=2))
        (root / "backfill-config.json").write_text(json.dumps({"batch_size": 64}))
        journal = Journal(root / "batches.jsonl")
        try:
            with httpx.Client(base_url="http://127.0.0.1:58080", timeout=180) as client:
                started = time.perf_counter()
                for kind, path, name in [
                    ("documents", CORPUS, "corpus"),
                    ("queries", QUESTIONS, "questions"),
                ]:
                    assert hashlib.sha256(path.read_bytes()).hexdigest() == HASHES[name]
                    frame = pl.read_parquet(path)
                    directory = root / kind
                    directory.mkdir(exist_ok=True)
                    began = time.perf_counter()
                    count = 0
                    for start in range(0, len(frame), 64):
                        end = min(start + 64, len(frame))
                        out = directory / f"{start:07d}-{end:07d}.npy"
                        if out.exists():
                            existing = np.load(out, allow_pickle=False)
                            assert existing.dtype == np.int8 and existing.shape == (
                                end - start,
                                1024,
                            )
                            assert np.any(existing != 0, axis=1).all(), (
                                f"Zero embedding in cached shard: {out}"
                            )
                            continue
                        vectors, seconds, _ = encode(
                            client, frame["input"][start:end].to_list()
                        )
                        tmp = out.with_suffix(".partial")
                        with tmp.open("wb") as stream:
                            np.save(stream, vectors, allow_pickle=False)
                            stream.flush()
                            os.fsync(stream.fileno())
                        tmp.rename(out)
                        count += len(vectors)
                        journal.write(
                            "batch", kind=kind, start=start, end=end, seconds=seconds
                        )
                        if start // 64 % 64 == 0 or end == len(frame):
                            elapsed = time.perf_counter() - began
                            print(
                                json.dumps(
                                    {
                                        "kind": kind,
                                        "complete": end,
                                        "total": len(frame),
                                        "seconds": elapsed,
                                        "documents_per_second": count / elapsed,
                                        "estimated_remaining_seconds": (
                                            len(frame) - end
                                        )
                                        / (count / elapsed),
                                    }
                                ),
                                flush=True,
                            )
                    journal.write(
                        "kind_complete",
                        kind=kind,
                        seconds=time.perf_counter() - began,
                        new_rows=count,
                        total_rows=len(frame),
                    )
                journal.write("complete", seconds=time.perf_counter() - started)
        finally:
            journal.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    run(parser.parse_args().output)
