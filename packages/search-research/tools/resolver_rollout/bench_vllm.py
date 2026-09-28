"""Compare native vLLM Yes-logit pooling with identical frozen input token IDs.

Uses a one-row classification head copied from the model's original Yes LM-head
row, no activation. Prefix caching reuses real shared comment prefixes; the cache
is cleared after warmup and between datasets to avoid replay-cache speedups.
"""

import argparse
import json
import time
from pathlib import Path

from vllm import LLM
from vllm.config import PoolerConfig

OUT = Path("/workspace/data/research/books-resolver-throughput-v1")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--precision", choices=["bf16", "fp8"], default="bf16")
    p.add_argument("--prefix", action="store_true")
    p.add_argument("--primed", action="store_true")
    p.add_argument("--tokens", type=int, default=16384)
    args = p.parse_args()
    start = time.perf_counter()
    llm = LLM(
        model="zeroentropy/zerank-2-reranker",
        revision="5eae30d5ee3c6b2df2ef6d723bde45172d761c4c",
        runner="pooling",
        convert="classify",
        dtype="bfloat16",
        quantization="fp8" if args.precision == "fp8" else None,
        hf_overrides={
            "classifier_from_token": ["Yes"],
            "method": "no_post_processing",
            "num_labels": 1,
        },
        pooler_config=PoolerConfig(pooling_type="LAST", use_activation=False),
        gpu_memory_utilization=0.80,
        max_model_len=32768,
        max_num_seqs=256,
        max_num_batched_tokens=args.tokens,
        enable_prefix_caching=args.prefix,
        enable_chunked_prefill=True,
    )
    load = time.perf_counter() - start
    for dataset in ["corpus128", "gold"]:
        data = json.loads((OUT / f"{dataset}.json").read_text())
        prompts = [{"prompt_token_ids": tokens} for tokens in data["tokens"]]
        llm.classify(prompts[:2], use_tqdm=False)
        llm.llm_engine.reset_prefix_cache()
        start = time.perf_counter()
        if args.primed:
            assert args.prefix
            first = [g[0] for g in data["groups"] if g]
            first_set = set(first)
            rest = [j for j in range(len(prompts)) if j not in first_set]
            scores = [None] * len(prompts)
            for indices in [first, rest]:
                outputs = llm.classify([prompts[j] for j in indices], use_tqdm=False)
                for j, output in zip(indices, outputs, strict=True):
                    scores[j] = float(output.outputs.probs[0])
        else:
            outputs = llm.classify(prompts, use_tqdm=False)
            scores = [float(o.outputs.probs[0]) for o in outputs]
        elapsed = time.perf_counter() - start
        result = {
            "precision": args.precision,
            "prefix": args.prefix,
            "primed": args.primed,
            "token_budget": args.tokens,
            "seconds": elapsed,
            "load_seconds": load,
            "pairs": len(scores),
            "references": len(data["rows"]),
            "pairs_per_second": len(scores) / elapsed,
            "scores": scores,
        }
        path = (
            OUT
            / f"{dataset}-vllm-{args.precision}-prefix{int(args.prefix)}-t{args.tokens}{chr(45) + 'primed' if args.primed else ''}.json"
        )
        assert not path.exists(), path
        path.write_text(json.dumps(result))
        print(
            json.dumps({k: v for k, v in result.items() if k != "scores"}), flush=True
        )


if __name__ == "__main__":
    main()
