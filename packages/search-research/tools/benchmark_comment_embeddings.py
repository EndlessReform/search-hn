"""Measure raw-vLLM throughput against frozen comment inputs on a dedicated GPU.

Run on the inference host so nvidia-smi samples the device serving these requests.
Each trial uses the same ordered inputs and validates the production integer
transform. Results include HTTP transfer and quantization, but exclude disk writes.
"""

import argparse
import json
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import polars as pl
from search_research.embedding_backfill import file_hash
from search_research.vllm_transport import encode


def trial(client, inputs, batch_size, concurrency):
    """Time a complete pass while sampling total device memory every 100 ms."""
    stop = threading.Event()
    memory = []

    def sample():
        while not stop.is_set():
            result = subprocess.run(
                [
                    "nvidia-smi",
                    "--id=0",
                    "--query-gpu=memory.used",
                    "--format=csv,noheader,nounits",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            memory.append(int(result.stdout.strip()))
            stop.wait(0.1)

    batches = [inputs[i : i + batch_size] for i in range(0, len(inputs), batch_size)]
    with ThreadPoolExecutor(max_workers=1) as monitor:
        measurement = monitor.submit(sample)
        started = time.perf_counter()
        try:
            with ThreadPoolExecutor(max_workers=concurrency) as workers:
                for _ in workers.map(
                    lambda batch: encode(client, batch, priority=1), batches
                ):
                    pass
            elapsed = time.perf_counter() - started
        finally:
            stop.set()
            measurement.result()
    assert memory, "GPU memory sampler produced no measurements"
    return elapsed, max(memory)


def run(args):
    """Warm each request shape, then repeat fixed-corpus end-to-end trials."""
    frame = pl.read_parquet(args.inputs).head(args.count)
    assert frame.height == args.count, "Requested sample exceeds frozen input count"
    inputs = frame["input"].to_list()
    tokens = frame["tokens"].sum()
    source_hash = file_hash(args.inputs)
    with httpx.Client(base_url=args.base_url, timeout=300) as client:
        for concurrency in args.concurrency:
            for batch_size in args.batch_sizes:
                assert batch_size > 0 and concurrency > 0
                encode(client, inputs[:batch_size], priority=1)
                for repeat in range(args.repeats):
                    seconds, memory = trial(client, inputs, batch_size, concurrency)
                    row = {
                        "server_max_tokens": args.server_max_tokens,
                        "batch_size": batch_size,
                        "concurrency": concurrency,
                        "repeat": repeat,
                        "inputs": len(inputs),
                        "tokens": tokens,
                        "seconds": seconds,
                        "inputs_per_second": len(inputs) / seconds,
                        "tokens_per_second": tokens / seconds,
                        "peak_device_mib": memory,
                        "inputs_sha256": source_hash,
                    }
                    line = json.dumps(row)
                    with args.output.open("a") as output:
                        output.write(line + "\n")
                    print(line, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--base-url", default="http://127.0.0.1:18080")
    parser.add_argument("--server-max-tokens", type=int, required=True)
    parser.add_argument("--count", type=int, default=4096)
    parser.add_argument(
        "--batch-sizes", type=int, nargs="+", default=[32, 64, 128, 256]
    )
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1])
    parser.add_argument("--repeats", type=int, default=2)
    run(parser.parse_args())
