"""Bounded, resumable embeddings with conservative per-attempt dollar accounting."""

import asyncio
import base64
import json
import os
from pathlib import Path

import numpy as np
import tiktoken
from aiolimiter import AsyncLimiter
from openai import AsyncOpenAI, RateLimitError
from search_agent.journal import Journal

from search_research.dataset import read_jsonl
from search_research.embedding_data import MODEL, PRICE_PER_MILLION


def batches(frame):
    """Bound both input count and tokens, staying below embedding request limits."""
    start, tokens = 0, 0
    for i, count in enumerate(frame["tokens"]):
        if i - start >= 256 or tokens + count > 20000:
            yield start, i
            start, tokens = i, 0
        tokens += count
    if start < frame.height:
        yield start, frame.height


async def embed(root: Path, corpus, questions):
    """Two requests in flight, <=300 RPM and roughly 600k TPM plus 20k burst.

    Published Tier 1 is 3,000 RPM / 1m TPM. Header limits are recorded; 429s honor
    Retry-After. Each attempted request reserves its whole price before sending,
    even if it fails, so retries and interrupted attempts cannot exceed $2.50.
    Shards are fsynced before the completion event; restart skips validated files.
    """
    journal = Journal(root / "requests.jsonl")
    spent_tokens = sum(
        r["tokens"]
        for r in read_jsonl(root / "requests.jsonl")
        if r["event"] == "attempt"
    )
    rpm, tpm = AsyncLimiter(1, 0.2), AsyncLimiter(20000, 2)
    semaphore = asyncio.Semaphore(2)
    encoding = tiktoken.encoding_for_model(MODEL)
    async with AsyncOpenAI(
        base_url="https://api.openai.com/v1", timeout=120, max_retries=0
    ) as client:

        async def one(kind, frame, start, end):
            nonlocal spent_tokens
            directory = root / kind
            directory.mkdir(exist_ok=True)
            path = directory / f"{start:07d}-{end:07d}.npy"
            if path.exists():
                assert np.load(path, mmap_mode="r").shape == (end - start, 3072)
                return
            async with semaphore:
                inputs = [
                    encoding.encode(s, disallowed_special=())
                    for s in frame["input"][start:end]
                ]
                tokens = sum(map(len, inputs))
                for attempt in range(4):
                    await tpm.acquire(tokens)
                    async with rpm:
                        assert (
                            spent_tokens + tokens
                        ) * PRICE_PER_MILLION / 1e6 < 2.50, (
                            "Spend ceiling reached; ask user"
                        )
                        spent_tokens += tokens
                        journal.write(
                            "attempt", kind=kind, start=start, end=end, tokens=tokens
                        )
                        try:
                            raw = await client.embeddings.with_raw_response.create(
                                model=MODEL,
                                input=inputs,
                                dimensions=3072,
                                encoding_format="base64",
                            )
                        except RateLimitError as exc:
                            journal.write(
                                "rate_limit", kind=kind, start=start, retry=attempt
                            )
                            if attempt == 3:
                                raise
                            await asyncio.sleep(
                                max(
                                    float(
                                        exc.response.headers.get("retry-after", "10")
                                    ),
                                    2**attempt,
                                )
                            )
                            continue
                        response = raw.parse()
                        assert [r.index for r in response.data] == list(
                            range(end - start)
                        )
                        matrix = np.stack(
                            [
                                np.frombuffer(
                                    base64.b64decode(r.embedding), dtype="<f4"
                                )
                                for r in response.data
                            ]
                        )
                        assert (
                            matrix.shape == (end - start, 3072)
                            and np.isfinite(matrix).all()
                        )
                        temp = path.with_suffix(".partial")
                        with temp.open("wb") as f:
                            np.save(f, matrix, allow_pickle=False)
                            f.flush()
                            os.fsync(f.fileno())
                        temp.replace(path)
                        journal.write(
                            "complete",
                            kind=kind,
                            start=start,
                            end=end,
                            usage_tokens=response.usage.total_tokens,
                            headers={
                                k: v
                                for k, v in raw.headers.items()
                                if k.startswith("x-ratelimit-")
                            },
                            request_id=raw.headers.get("x-request-id"),
                        )
                        print(
                            json.dumps(
                                {
                                    "embedded": kind,
                                    "through": end,
                                    "of": frame.height,
                                    "reserved_usd": spent_tokens
                                    * PRICE_PER_MILLION
                                    / 1e6,
                                }
                            ),
                            flush=True,
                        )
                        return

        try:
            tasks = [
                asyncio.create_task(one(kind, frame, a, b))
                for kind, frame in (("documents", corpus), ("queries", questions))
                for a, b in batches(frame)
            ]
            try:
                await asyncio.gather(*tasks)
            except BaseException:
                # Stop pending work before closing the journal/client on failure.
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise
        finally:
            journal.close()
