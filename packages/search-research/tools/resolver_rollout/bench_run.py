"""Benchmark packed cross-reference batches and precision on frozen token IDs.

The baseline uses the original per-reference batch policy. Other policies sort
pairs across references by length. Identical inputs and saved pair order make
ranking and raw-score agreement inspectable independently of throughput.
"""

import argparse
import gc
import json
import subprocess
import time
from pathlib import Path

import torch
from common import MODEL, REVISION
from transformers import AutoModelForCausalLM, AutoTokenizer

OUT = Path("data/research/books-resolver-throughput-v1")


def batches(data, size, budget, across):
    """Greedily pack length-sorted pairs within either the corpus or each reference."""
    groups = [range(len(data["tokens"]))] if across else data["groups"]
    result = []
    for group in groups:
        order = sorted(group, key=lambda j: len(data["tokens"][j]))
        batch = []
        maximum = 0
        for j in order:
            longest = max(maximum, len(data["tokens"][j]))
            if batch and (len(batch) >= size or longest * (len(batch) + 1) > budget):
                result.append(batch)
                batch = []
                maximum = 0
            batch.append(j)
            maximum = max(maximum, len(data["tokens"][j]))
        if batch:
            result.append(batch)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--precision", choices=["bf16", "fp16", "fp8"], default="bf16")
    parser.add_argument("--dataset", default="corpus128")
    parser.add_argument("--policy", default="sweep")
    args = parser.parse_args()
    data = json.loads((OUT / f"{args.dataset}.json").read_text())
    torch.set_num_threads(8)
    tokenizer = AutoTokenizer.from_pretrained(
        MODEL, revision=REVISION, local_files_only=True
    )
    tokenizer.padding_side = "left"
    assert tokenizer.decode([9454]) == "Yes"
    model = (
        AutoModelForCausalLM.from_pretrained(
            MODEL,
            revision=REVISION,
            local_files_only=True,
            dtype=torch.float16 if args.precision == "fp16" else torch.bfloat16,
            attn_implementation="sdpa",
        )
        .cuda()
        .eval()
    )
    if args.precision == "fp8":
        from torchao.quantization import (
            Float8DynamicActivationFloat8WeightConfig,
            PerRow,
            quantize_,
        )

        quantize_(
            model,
            Float8DynamicActivationFloat8WeightConfig(granularity=PerRow()),
            filter_fn=lambda m, fqn: (
                isinstance(m, torch.nn.Linear) and fqn != "lm_head"
            ),
        )
    policies = {
        "original": (32, 16384, False),
        "cross32": (32, 16384, True),
        "cross64": (64, 32768, True),
        "cross128": (128, 65536, True),
        "cross256": (256, 131072, True),
    }
    if args.policy != "sweep":
        policies = {args.policy: policies[args.policy]}

    @torch.inference_mode()
    def forward(batch):
        inputs = tokenizer.pad(
            {"input_ids": [data["tokens"][j] for j in batch]},
            padding=True,
            return_tensors="pt",
        ).to("cuda")
        logits = (
            model(**inputs, use_cache=False, logits_to_keep=1)
            .logits[:, -1, 9454]
            .float()
        )
        assert torch.isfinite(logits).all()
        return logits.cpu().tolist()

    for name, (size, budget, across) in policies.items():
        path = OUT / f"{args.dataset}-{args.precision}-{name}.json"
        assert not path.exists(), path
        todo = batches(data, size, budget, across)
        scores = [None] * len(data["tokens"])
        try:
            # Warm largest and smallest shapes before timing this policy.
            forward(todo[-1])
            forward(todo[0])
            torch.cuda.synchronize()
            torch.cuda.reset_peak_memory_stats()
            monitor_file = (
                OUT / f"{args.dataset}-{args.precision}-{name}-gpu.csv"
            ).open("w")
            monitor = subprocess.Popen(
                [
                    "nvidia-smi",
                    "--query-gpu=timestamp,utilization.gpu,utilization.memory,memory.used,power.draw",
                    "--format=csv,noheader,nounits",
                    "-lms",
                    "200",
                ],
                stdout=monitor_file,
            )
            start = time.perf_counter()
            try:
                for batch in todo:
                    for j, v in zip(batch, forward(batch), strict=True):
                        scores[j] = v
                torch.cuda.synchronize()
                elapsed = time.perf_counter() - start
            finally:
                monitor.terminate()
                monitor.wait()
                monitor_file.close()
            result = {
                "precision": args.precision,
                "policy": name,
                "pairs": len(scores),
                "references": len(data["rows"]),
                "seconds": elapsed,
                "pairs_per_second": len(scores) / elapsed,
                "references_per_second": len(data["rows"]) / elapsed,
                "batches": len(todo),
                "padded_tokens": sum(
                    max(len(data["tokens"][j]) for j in b) * len(b) for b in todo
                ),
                "actual_tokens": sum(map(len, data["tokens"])),
                "peak_allocated_bytes": torch.cuda.max_memory_allocated(),
                "scores": scores,
            }
        except torch.cuda.OutOfMemoryError as exc:
            result = {
                "precision": args.precision,
                "policy": name,
                "error": "CUDA OOM",
                "message": str(exc),
            }
            gc.collect()
            torch.cuda.empty_cache()
        path.write_text(json.dumps(result))
        print(
            json.dumps({k: v for k, v in result.items() if k != "scores"}), flush=True
        )


if __name__ == "__main__":
    main()
