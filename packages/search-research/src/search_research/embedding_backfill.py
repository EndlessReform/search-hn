"""Reusable atomic vector shards, restart checks, and paced raw-vLLM batches.

Callers own the dataset manifest and exclusive run lock. This module preserves
historical shard names and journal events so existing frozen runs remain usable.
"""

import hashlib
import json
import os
import time
from pathlib import Path

import numpy as np
from pydantic import BaseModel, ConfigDict, Field

from search_research.vllm_transport import encode


class BatchSettings(BaseModel):
    """Scheduling controls, kept separate from the output-affecting recipe."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    # Dedicated servers can accept larger HTTP batches; their scheduler applies
    # its own token/sequence budget. Keep the shared-server default unchanged.
    batch_size: int = Field(default=64, ge=1)
    pause_seconds: float = Field(default=0, ge=0)
    priority: int | None = Field(default=None, ge=0)


DEFAULT_BATCH_SETTINGS = BatchSettings()


def file_hash(path: Path) -> str:
    """Hash artifacts without materializing them in memory."""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def check_manifest(path: Path, manifest: dict) -> None:
    """Reject reuse of a directory with different inputs or vector semantics."""
    if path.exists():
        assert json.loads(path.read_text()) == manifest, f"Resume mismatch: {path}"
    else:
        temporary = path.with_suffix(".partial")
        temporary.write_text(json.dumps(manifest, indent=2) + "\n")
        temporary.replace(path)


def run_shards(
    client,
    inputs: list[str],
    directory: Path,
    *,
    journal,
    kind: str,
    settings: BatchSettings = DEFAULT_BATCH_SETTINGS,
) -> dict:
    """Resume fixed positional shards, validating every reused vector matrix.

    Each successful response is fsynced before atomic rename and journal append.
    Errors propagate; completed shards survive and a later invocation resumes.
    Pauses follow new batches only and are included in measured throughput.
    """
    directory.mkdir(parents=True, exist_ok=True)
    began = time.perf_counter()
    count = 0
    for start in range(0, len(inputs), settings.batch_size):
        end = min(start + settings.batch_size, len(inputs))
        out = directory / f"{start:07d}-{end:07d}.npy"
        if out.exists():
            existing = np.load(out, allow_pickle=False)
            assert existing.dtype == np.int8 and existing.shape == (end - start, 1024)
            assert np.any(existing != 0, axis=1).all(), f"Zero embedding: {out}"
            continue
        vectors, seconds, _ = encode(
            client, inputs[start:end], priority=settings.priority
        )
        tmp = out.with_suffix(".partial")
        with tmp.open("wb") as stream:
            np.save(stream, vectors, allow_pickle=False)
            stream.flush()
            os.fsync(stream.fileno())
        tmp.rename(out)
        count += len(vectors)
        journal.write("batch", kind=kind, start=start, end=end, seconds=seconds)
        if settings.pause_seconds:
            time.sleep(settings.pause_seconds)
        if start // settings.batch_size % 64 == 0 or end == len(inputs):
            elapsed = time.perf_counter() - began
            print(
                json.dumps(
                    {
                        "kind": kind,
                        "complete": end,
                        "total": len(inputs),
                        "seconds": elapsed,
                        "documents_per_second": count / elapsed,
                        "estimated_remaining_seconds": (len(inputs) - end)
                        / (count / elapsed),
                    }
                ),
                flush=True,
            )
    result = {
        "kind": kind,
        "seconds": time.perf_counter() - began,
        "new_rows": count,
        "total_rows": len(inputs),
    }
    journal.write("kind_complete", **result)
    return result
