"""Score fixed book candidates using the published reranker contracts.

Every depth is a prefix of the same retrieval pool. Pair scores are reusable
across depths; a separate fixed subset measures real 20/50/100 request latency.
Full comments are retained, with the target occurrence marked. No truncation,
label inputs, generation, or translation is used by either inference arm.
"""

import argparse
import hashlib
import json
import random
import sqlite3
import time
from pathlib import Path

import torch
import transformers
from transformers import (
    AutoModelForCausalLM,
    AutoModelForSequenceClassification,
    AutoTokenizer,
)

MODELS = {
    "zerank": (
        "zeroentropy/zerank-2-reranker",
        "5eae30d5ee3c6b2df2ef6d723bde45172d761c4c",
    ),
    "bge": ("BAAI/bge-reranker-v2-m3", "953dc6f6f85a1b2dbfca4c34a2796e7dde08d41e"),
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("model", choices=MODELS)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    root = args.root / "data/probes/books-resolver-iteration-v1"
    fixture = json.loads(
        (
            args.root
            / "packages/search-research/docs/comment-classification/assets/resolver-iteration-v1.json"
        ).read_text()
    )
    source = sqlite3.connect(
        f"file:{args.root}/data/comment-2025/index.sqlite?mode=ro", uri=True
    )
    rows = []
    for sample in fixture["rows"]:
        context = source.execute(
            "SELECT text FROM comments WHERE comment_id=?", (sample["comment_id"],)
        ).fetchone()[0]
        assert (
            hashlib.sha256(context.encode()).hexdigest() == sample["context_sha256"]
        ), sample["sample_id"]
        rows.append(sample | {"context": context})
    candidates = json.loads((root / "candidates.json").read_text())
    docs = json.loads((root / "documents.json").read_text())
    name, revision = MODELS[args.model]
    torch.set_num_threads(8)
    torch.manual_seed(20260926)
    load_start = time.perf_counter()
    tokenizer = AutoTokenizer.from_pretrained(
        name, revision=revision, local_files_only=True
    )
    cls = (
        AutoModelForCausalLM
        if args.model == "zerank"
        else AutoModelForSequenceClassification
    )
    model = (
        cls.from_pretrained(
            name,
            revision=revision,
            local_files_only=True,
            dtype=torch.bfloat16,
            attn_implementation="sdpa",
        )
        .cuda()
        .eval()
    )
    if args.model == "zerank":
        tokenizer.padding_side = "left"
        assert tokenizer.decode([9454]) == "Yes", tokenizer.decode([9454])
    load_seconds = time.perf_counter() - load_start

    def query(i, mode):
        row = rows[i]
        if mode == "title":
            return row["title"]
        assert row["context"][row["start"] : row["end"]] == row["title"]
        marked = (
            row["context"][: row["start"]]
            + "<mention>"
            + row["title"]
            + "</mention>"
            + row["context"][row["end"] :]
        )
        return (
            "Identify the book referred to by the marked mention.\nMention: "
            + row["title"]
            + "\nComment:\n"
            + marked
        )

    def document(key):
        doc = docs[key]
        return (
            "Title: "
            + doc["title"]
            + "\nAuthor: "
            + ("; ".join(sorted(doc["authors"])) or "unknown")
        )

    @torch.inference_mode()
    def score(i, mode, depth):
        selected = candidates[i]["candidates"][:depth]
        pairs = [(query(i, mode), document(c["id"])) for c in selected]
        torch.cuda.synchronize()
        start = time.perf_counter()
        if not selected:
            return {
                "sample_id": i,
                "mode": mode,
                "depth": depth,
                "ids": [],
                "scores": [],
                "milliseconds": 0.0,
                "max_tokens": 0,
            }
        if args.model == "zerank":
            texts = [
                tokenizer.apply_chat_template(
                    [
                        {"role": "query", "content": q},
                        {"role": "document", "content": d},
                    ],
                    tokenize=False,
                    add_generation_prompt=True,
                )
                for q, d in pairs
            ]
            encoded = tokenizer(texts, add_special_tokens=False, truncation=False)[
                "input_ids"
            ]
        else:
            encoded = tokenizer(pairs, truncation=False)["input_ids"]
        lengths = list(map(len, encoded))
        assert max(lengths) <= (32768 if args.model == "zerank" else 8192), ("Context limit exceeded", i, max(lengths))
        order = sorted(range(len(encoded)), key=lambda j: len(encoded[j]))
        scores = [None] * len(encoded)
        cursor = 0
        while cursor < len(order):
            take = []
            maximum = 0
            while cursor < len(order) and len(take) < 32:
                idx = order[cursor]
                new_max = max(maximum, len(encoded[idx]))
                if take and new_max * (len(take) + 1) > 16384:
                    break
                take.append(idx)
                maximum = new_max
                cursor += 1
            inputs = tokenizer.pad(
                {"input_ids": [encoded[j] for j in take]},
                padding=True,
                return_tensors="pt",
            ).to("cuda")
            logits = model(
                **inputs,
                **(
                    {"use_cache": False, "logits_to_keep": 1}
                    if args.model == "zerank"
                    else {}
                ),
            ).logits
            values = (
                logits[:, -1, 9454] if args.model == "zerank" else logits.reshape(-1)
            ).float()
            assert torch.isfinite(values).all(), ("Nonfinite scores", i, mode)
            for j, value in zip(take, values.cpu().tolist(), strict=True):
                scores[j] = value
        torch.cuda.synchronize()
        return {
            "sample_id": i,
            "mode": mode,
            "depth": depth,
            "ids": [c["id"] for c in selected],
            "scores": scores,
            "milliseconds": (time.perf_counter() - start) * 1000,
            "max_tokens": max(lengths),
        }

    start = time.perf_counter()
    score(0, "title", 3)
    score(0, "context", 3)
    warmup = time.perf_counter() - start
    torch.cuda.reset_peak_memory_stats()
    runtime = {
        "model": name,
        "revision": revision,
        "dtype": "bfloat16",
        "attention": "sdpa",
        "torch": torch.__version__,
        "transformers": transformers.__version__,
        "gpu": torch.cuda.get_device_name(),
        "load_seconds": load_seconds,
        "warmup_seconds": warmup,
        "max_batch_pairs": 32,
        "batch_token_budget": 16384,
        "candidate_sha256": hashlib.sha256(
            (root / "candidates.json").read_bytes()
        ).hexdigest(),
        "latency_ids": sorted(random.Random(20260926).sample(range(250), 12)),
    }
    runtime_path = root / f"{args.model}-runtime.json"
    runtime_path.write_text(json.dumps(runtime, indent=2))
    print(json.dumps(runtime), flush=True)
    output = root / f"{args.model}-scores.jsonl"
    done = set()
    if output.exists():
        for line in output.open():
            r = json.loads(line)
            assert r["ids"] == [
                c["id"] for c in candidates[r["sample_id"]]["candidates"][: r["depth"]]
            ]
            done.add((r["sample_id"], r["mode"], r["depth"]))
    start = time.perf_counter()
    with output.open("a") as stream:
        for mode in ["title", "context"]:
            for i in range(len(rows)):
                depths = [100, 20, 50] if i in runtime["latency_ids"] else [100]
                for depth in depths:
                    if (i, mode, depth) in done:
                        continue
                    r = score(i, mode, depth)
                    stream.write(json.dumps(r) + "\n")
                    stream.flush()
                if (i + 1) % 10 == 0:
                    print(
                        json.dumps(
                            {
                                "model": args.model,
                                "mode": mode,
                                "completed": i + 1,
                                "seconds": round(time.perf_counter() - start, 1),
                            }
                        ),
                        flush=True,
                    )
    runtime["run_seconds"] = time.perf_counter() - start
    runtime["peak_allocated_bytes"] = torch.cuda.max_memory_allocated()
    runtime_path.write_text(json.dumps(runtime, indent=2))
    print(json.dumps(runtime), flush=True)


if __name__ == "__main__":
    main()
