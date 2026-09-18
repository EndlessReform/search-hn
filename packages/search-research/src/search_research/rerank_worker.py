"""Remote GPU-only JSONL worker, launched over SSH by the laptop driver.

Uses the official Qwen yes/no prompt and logit difference, not generation.
Only the last position's vocabulary logits are materialized. SDPA and BF16
avoid requiring a separate flash-attention build on the Blackwell host.
"""

import json
import sys
import time

import torch
import transformers
from transformers import AutoModelForCausalLM, AutoTokenizer

MODEL = "Qwen/Qwen3-Reranker-0.6B"
REVISION = "e61197ed45024b0ed8a2d74b80b4d909f1255473"
TASK = "Given a web search query, retrieve relevant passages that answer the query"
PREFIX = '<|im_start|>system\nJudge whether the Document meets the requirements based on the Query and the Instruct provided. Note that the answer can only be "yes" or "no".<|im_end|>\n<|im_start|>user\n'
SUFFIX = "<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"


def main():
    """Load once, then score bounded batches until the laptop closes stdin."""
    torch.set_num_threads(4)
    revision = REVISION
    tokenizer = AutoTokenizer.from_pretrained(
        MODEL, revision=revision, padding_side="left"
    )
    model = (
        AutoModelForCausalLM.from_pretrained(
            MODEL,
            revision=revision,
            torch_dtype=torch.bfloat16,
            attn_implementation="sdpa",
        )
        .cuda()
        .eval()
    )
    prefix = tokenizer.encode(PREFIX, add_special_tokens=False)
    suffix = tokenizer.encode(SUFFIX, add_special_tokens=False)
    yes, no = [tokenizer.convert_tokens_to_ids(x) for x in ("yes", "no")]
    print(
        json.dumps(
            {
                "ready": True,
                "model": MODEL,
                "revision": revision,
                "torch": torch.__version__,
                "transformers": transformers.__version__,
                "gpu": torch.cuda.get_device_name(),
                "dtype": "bfloat16",
                "attention": "sdpa",
                "instruction": TASK,
            }
        ),
        flush=True,
    )

    @torch.inference_mode()
    def score(pairs, batch_size):
        """Return scores in input order; reject truncation rather than hiding it."""
        scores = []
        max_tokens = 0
        torch.cuda.synchronize()
        torch.cuda.reset_peak_memory_stats()
        start = time.perf_counter()
        for offset in range(0, len(pairs), batch_size):
            texts = [
                f"<Instruct>: {TASK}\n<Query>: {q}\n<Document>: {d}"
                for q, d in pairs[offset : offset + batch_size]
            ]
            tokens = tokenizer(texts, add_special_tokens=False)["input_ids"]
            tokens = [prefix + t + suffix for t in tokens]
            max_tokens = max(max_tokens, max(map(len, tokens)))
            assert max_tokens <= 8192, (
                "Unexpected long document: do not silently truncate"
            )
            inputs = tokenizer.pad(
                {"input_ids": tokens}, padding=True, return_tensors="pt"
            ).to("cuda")
            logits = (
                model(**inputs, use_cache=False, logits_to_keep=1)
                .logits[:, -1, :]
                .float()
            )
            scores.extend((logits[:, yes] - logits[:, no]).cpu().tolist())
        torch.cuda.synchronize()
        return {
            "scores": scores,
            "milliseconds": (time.perf_counter() - start) * 1000,
            "peak_allocated_mb": torch.cuda.max_memory_allocated() / 2**20,
            "max_tokens": max_tokens,
        }

    for line in sys.stdin:
        request = json.loads(line)
        assert 1 <= request["batch_size"] <= 128
        print(json.dumps(score(request["pairs"], request["batch_size"])), flush=True)


if __name__ == "__main__":
    main()
