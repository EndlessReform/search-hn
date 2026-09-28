"""Score saved candidate pools with and without popularity in one pinned run."""

import argparse
import json
import math
import time
from pathlib import Path

from transformers import AutoTokenizer
from vllm import LLM
from vllm.config import PoolerConfig

MODEL = "zeroentropy/zerank-2-reranker"
REVISION = "5eae30d5ee3c6b2df2ef6d723bde45172d761c4c"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("/pilot"))
    args = parser.parse_args()
    data = json.loads((args.root / "cases.json").read_text())
    cases = data["cases"]
    tokenizer = AutoTokenizer.from_pretrained(
        MODEL, revision=REVISION, local_files_only=True
    )
    assert tokenizer.decode([9454]) == "Yes"
    texts, records = [], []
    for case in cases:
        row = case["reference"]
        context = row["context"]
        assert context[row["start"] : row["end"]] == row["title"]
        marked = (
            context[: row["start"]]
            + "<mention>"
            + row["title"]
            + "</mention>"
            + context[row["end"] :]
        )
        query = (
            "Identify the book referred to by the marked mention.\nMention: "
            + row["title"]
            + "\nComment:\n"
            + marked
        )
        for candidate in case["candidates"]:
            base = (
                "Title: "
                + candidate["title"]
                + "\nAuthor: "
                + ("; ".join(sorted(candidate["authors"])) or "unknown")
            )
            popularity = candidate["popularity"]

            def value(key, popularity=popularity):
                item = popularity.get(key)
                return str(item) if item is not None else "unknown"

            for variant in ["baseline", "popularity"]:
                document = base
                if variant == "popularity":
                    document += (
                        "\nOpen Library reading log count: "
                        + value("readinglog_count")
                        + "\nOpen Library edition count: "
                        + value("edition_count")
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
                records.append(
                    {
                        "case_id": case["id"],
                        "work_id": candidate["id"],
                        "variant": variant,
                        "saved_score": candidate["rerank_score"],
                    }
                )
    tokens = tokenizer(texts, add_special_tokens=False, truncation=False)["input_ids"]
    assert all(len(t) <= 32768 for t in tokens)
    start = time.monotonic()
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
    load_seconds = time.monotonic() - start
    start = time.monotonic()
    outputs = llm.classify([{"prompt_token_ids": t} for t in tokens], use_tqdm=False)
    assert len(outputs) == len(records)
    for record, output, token in zip(records, outputs, tokens, strict=True):
        record["score"] = float(output.outputs.probs[0])
        record["tokens"] = len(token)
        assert math.isfinite(record["score"])
    result = {
        "model": MODEL,
        "revision": REVISION,
        "quantization": "fp8",
        "runtime": "vllm 0.23.0",
        "container_image": "sha256:f37691f675bb82f734f606de8af90e777d3f80a20b120e699fd43fd10e60b8d7",
        "load_seconds": load_seconds,
        "score_seconds": time.monotonic() - start,
        "case_count": len(cases),
        "pair_count": len(records),
        "records": records,
    }
    (args.root / "rerank-results.json").write_text(json.dumps(result))
    print(json.dumps({k: v for k, v in result.items() if k != "records"}), flush=True)


if __name__ == "__main__":
    main()
