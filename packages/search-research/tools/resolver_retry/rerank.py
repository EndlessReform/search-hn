"""Score new candidate pairs only, retaining the pinned original reranker inputs.

A rewritten title changes retrieval, not the source mention supplied to the
reranker. Existing scores therefore remain reusable for the same case/work pair.
Incremental JSONL output permits resuming without repeating completed batches.
"""

import argparse
import json
import math
import os
import time
from pathlib import Path

from search_research.resolver_rerank import baseline_scores, pending_batches

ROOT = Path(
    os.environ.get("RESOLVER_RUN_ROOT", "data/research/books-resolver-retry-v1")
)
MODEL = "zeroentropy/zerank-2-reranker"
REVISION = "5eae30d5ee3c6b2df2ef6d723bde45172d761c4c"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", default="first")
    args = parser.parse_args()
    cases = json.loads((ROOT / f"{args.stage}-cases.json").read_text())["cases"]
    scores = baseline_scores(os.environ.get("RESOLVER_BASELINE"))
    path = ROOT / "new-scores.jsonl"
    if path.exists():
        for line in path.open():
            r = json.loads(line)
            scores[(r["id"], r["work_id"])] = r["score"]
    pending = [
        (c, d)
        for c in cases
        for d in c["candidates"]
        if (c["id"], d["id"]) not in scores
    ]
    print(
        json.dumps(
            {
                "stage": args.stage,
                "new_pairs": len(pending),
                "total_pairs": sum(len(c["candidates"]) for c in cases),
            }
        ),
        flush=True,
    )
    if pending:
        from transformers import AutoTokenizer
        from vllm import LLM
        from vllm.config import PoolerConfig

        tokenizer = AutoTokenizer.from_pretrained(
            MODEL, revision=REVISION, local_files_only=True
        )
        llm = LLM(
            model=MODEL,
            revision=REVISION,
            runner="pooling",
            convert="classify",
            dtype="bfloat16",
            quantization="fp8",
            hf_overrides={
                "classifier_from_token": ["Yes"],
                "method": "no_post_processing",
                "num_labels": 1,
            },
            pooler_config=PoolerConfig(pooling_type="LAST", use_activation=False),
            gpu_memory_utilization=0.80,
            max_model_len=32768,
            max_num_seqs=256,
            max_num_batched_tokens=16384,
            enable_prefix_caching=True,
            enable_chunked_prefill=True,
        )
        started = time.monotonic()
        completed = 0
        with path.open("a") as out:
            for batch in pending_batches(cases, scores):
                batch_started = time.monotonic()
                texts = []
                for c, d in batch:
                    r = c["reference"]
                    text = r["context"]
                    a, b = r["start"], r["end"]
                    assert text[a:b] == r["title"]
                    query = (
                        "Identify the book referred to by the marked mention.\nMention: "
                        + r["title"]
                        + "\nComment:\n"
                        + text[:a]
                        + "<mention>"
                        + text[a:b]
                        + "</mention>"
                        + text[b:]
                    )
                    document = (
                        "Title: "
                        + d["title"]
                        + "\nAuthor: "
                        + ("; ".join(sorted(d["authors"])) or "unknown")
                    )
                    texts.append(
                        tokenizer.apply_chat_template(
                            [
                                {"role": "query", "content": query},
                                {"role": "document", "content": document},
                            ],
                            tokenize=False,
                            add_generation_prompt=True,
                        )
                    )
                tokens = tokenizer(texts, add_special_tokens=False, truncation=False)[
                    "input_ids"
                ]
                assert all(len(t) <= 32768 for t in tokens)
                outputs = llm.classify(
                    [{"prompt_token_ids": t} for t in tokens], use_tqdm=False
                )
                for (c, d), result in zip(batch, outputs, strict=True):
                    score = float(result.outputs.probs[0])
                    assert math.isfinite(score)
                    scores[(c["id"], d["id"])] = score
                    out.write(
                        json.dumps({"id": c["id"], "work_id": d["id"], "score": score})
                        + "\n"
                    )
                out.flush()
                os.fsync(out.fileno())
                completed += len(batch)
                batch_seconds = time.monotonic() - batch_started
                print(
                    json.dumps(
                        {
                            "batch_pairs": len(batch),
                            "batch_seconds": batch_seconds,
                            "batch_pairs_per_second": len(batch) / batch_seconds,
                        }
                    ),
                    flush=True,
                )
                print(
                    f"Scored {completed}/{len(pending)}",
                    flush=True,
                )
        elapsed = time.monotonic() - started
        print(
            json.dumps(
                {
                    "pairs": completed,
                    "inference_seconds": elapsed,
                    "pairs_per_second": completed / elapsed,
                    "references_per_batch": 128,
                }
            ),
            flush=True,
        )
        # Close worker processes and background resources before interpreter teardown.
        # The pinned vLLM image can otherwise abort after successfully saving scores.
        llm.llm_engine.engine_core.shutdown()
    for c in cases:
        for d in c["candidates"]:
            d["raw_score"] = scores[(c["id"], d["id"])]
    (ROOT / f"{args.stage}-ranked.json").write_text(json.dumps({"cases": cases}))
    print("Reranking complete", flush=True)


if __name__ == "__main__":
    main()
