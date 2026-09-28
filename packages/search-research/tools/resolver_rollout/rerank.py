"""Resume the measured vLLM FP8 reranker in windows of 128 references.

The single classification weight is copied from the original Yes-token row;
activation is disabled so stored scores remain raw logits. Prefix caching reuses
actual shared comments. Every scored candidate is retained for later calibration.
Run inside the pinned vLLM container through run.sh, not the host environment.
"""

import json
import math
import time

from common import MODEL, REVISION, RUN, connect, marked, status
from transformers import AutoTokenizer
from vllm import LLM
from vllm.config import PoolerConfig


def main():
    db = connect()
    references = db.execute("SELECT count(*) FROM refs").fetchone()[0]
    if references > 0 and references == db.execute(
        "SELECT count(*) FROM rankings"
    ).fetchone()[0]:
        print("Reranking already complete; preserving saved scores.", flush=True)
        return
    assert (
        json.loads(
            db.execute("SELECT payload FROM status WHERE stage='prepare'").fetchone()[0]
        )["state"]
        == "complete"
    )
    settings = {
        "model": MODEL,
        "revision": REVISION,
        "runtime": "vllm 0.23.0",
        "quantization": "fp8",
        "remaining_dtype": "bfloat16",
        "prefix_caching": True,
        "token_budget": 16384,
        "max_num_seqs": 256,
        "window_references": 128,
        "max_model_len": 32768,
        "score": "Yes token 9454, no_post_processing, LAST pooling, no activation",
    }
    runtime = RUN / "rerank-runtime.json"
    if runtime.exists():
        assert json.loads(runtime.read_text()) == settings, (
            "Do not mix scoring configurations in one run."
        )
    else:
        runtime.write_text(json.dumps(settings, indent=2))
    docs = {k: json.loads(p) for k, p in db.execute("SELECT id,payload FROM documents")}
    tokenizer = AutoTokenizer.from_pretrained(
        MODEL, revision=REVISION, local_files_only=True
    )
    assert tokenizer.decode([9454]) == "Yes"
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
    start = time.monotonic()
    status(db, "rerank", {"state": "running", "configuration": settings})
    while True:
        rows = [
            json.loads(p)
            for (p,) in db.execute(
                "SELECT payload FROM refs WHERE id NOT IN (SELECT id FROM rankings) ORDER BY ordinal LIMIT 128"
            )
        ]
        if not rows:
            break
        texts = []
        groups = []
        for row in rows:
            indices = []
            query = (
                "Identify the book referred to by the marked mention.\nMention: "
                + row["title"]
                + "\nComment:\n"
                + marked(row)
            )
            for candidate in row["candidates"]:
                d = docs[candidate["id"]]
                document = (
                    "Title: "
                    + d["title"]
                    + "\nAuthor: "
                    + ("; ".join(sorted(d["authors"])) or "unknown")
                )
                indices.append(len(texts))
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
            groups.append(indices)
        tokens = (
            tokenizer(texts, add_special_tokens=False, truncation=False)["input_ids"]
            if texts
            else []
        )
        assert all(len(t) <= 32768 for t in tokens), (
            "Full input exceeds the model context; no truncation applied."
        )
        t = time.monotonic()
        outputs = (
            llm.classify([{"prompt_token_ids": t} for t in tokens], use_tqdm=False)
            if tokens
            else []
        )
        scores = [float(o.outputs.probs[0]) for o in outputs]
        assert len(scores) == len(tokens) and all(math.isfinite(s) for s in scores)
        elapsed = time.monotonic() - t
        for row, indices in zip(rows, groups, strict=True):
            ids = [c["id"] for c in row["candidates"]]
            values = [scores[j] for j in indices]
            ranked = sorted(zip(ids, values), key=lambda kv: -kv[1])
            payload = {
                "id": row["id"],
                "ids": ids,
                "scores": values,
                "top3": [k for k, v in ranked[:3]],
                "max_tokens": max((len(tokens[j]) for j in indices), default=0),
                "window_seconds": elapsed,
                "window_references": len(rows),
            }
            db.execute(
                "INSERT INTO rankings VALUES (?,?)", (row["id"], json.dumps(payload))
            )
        db.commit()
        progress = {
            "state": "running",
            "completed": db.execute("SELECT count(*) FROM rankings").fetchone()[0],
            "seconds": time.monotonic() - start,
        }
        status(db, "rerank", progress)
        print(json.dumps(progress), flush=True)
    status(
        db,
        "rerank",
        {
            "state": "complete",
            "references": db.execute("SELECT count(*) FROM rankings").fetchone()[0],
        },
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        status(connect(), "rerank", {"state": "failed", "error": repr(exc)})
        raise
