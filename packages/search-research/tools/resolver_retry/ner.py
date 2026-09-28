"""Cache pretrained person spans once per complete comment, without author rules."""

import argparse
import json
import os
import time
from pathlib import Path

import torch
from gliner import GLiNER
from search_research.comment_entities import MODEL_ID
from search_research.ner_batching import (
    batch_schedule,
    configure_backend,
    person_records,
)

ROOT = Path(
    os.environ.get("RESOLVER_RUN_ROOT", "data/research/books-resolver-retry-v1")
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, help="Override all token buckets")
    parser.add_argument("--backend", choices=["eager", "flash"], default="eager")
    args = parser.parse_args()
    assert args.batch_size is None or args.batch_size > 0
    rows = json.loads((ROOT / "references.json").read_text())
    comments = {r["comment_id"]: r["context"] for r in rows}
    path = ROOT / "names.jsonl"
    done = (
        {json.loads(l)["comment_id"] for l in path.open()} if path.exists() else set()
    )
    pending = [(cid, text) for cid, text in comments.items() if cid not in done]
    if not pending:
        print("All person spans already saved", flush=True)
        return
    configure_backend(args.backend)
    model = (
        GLiNER.from_pretrained(MODEL_ID, load_tokenizer=True)
        .to(device="cuda", dtype=torch.bfloat16)
        .eval()
    )
    torch.set_num_threads(4)
    started = time.monotonic()
    schedule = batch_schedule("person", args.backend, args.batch_size)
    with torch.inference_mode(), path.open("a") as out:

        def emit(cid, spans):
            out.write(
                json.dumps(
                    {
                        "comment_id": cid,
                        "model": MODEL_ID,
                        "threshold": 0.3,
                        "spans": spans,
                    }
                )
                + "\n"
            )
            out.flush()
            os.fsync(out.fileno())
            done.add(cid)
            if len(done) % 1000 == 0:
                print(f"NER {len(done)}/{len(comments)}", flush=True)

        person_records(model, pending, schedule, emit)
    measurements = {
        "comments": len(pending),
        "schedule": schedule,
        "ordering": "whole-workload encoder token length",
        "seconds": time.monotonic() - started,
        "peak_gpu_bytes": torch.cuda.max_memory_allocated(),
        "precision": "bfloat16 weights",
        "backend": args.backend,
    }
    (ROOT / "person-ner-manifest.json").write_text(json.dumps(measurements, indent=2))
    print(json.dumps(measurements), flush=True)


if __name__ == "__main__":
    main()
