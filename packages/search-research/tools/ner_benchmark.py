"""Measure GLiNER batching on frozen comments, with span and memory comparisons."""

import argparse
import gc
import json
import os
import random
import sqlite3
import time
from contextlib import nullcontext
from pathlib import Path

import numpy as np
import torch
from gliner import GLiNER
from search_research.comment_entities import MODEL_ID, text_windows


def load_rows(stage):
    """Use the established 2025 workloads, not synthetic short-only sentences."""
    if stage == "title":
        with Path("data/probes/books-gliner-v1/filter_passes.jsonl").open() as source:
            ids = [json.loads(line)["comment_id"] for line in source]
        with sqlite3.connect(
            "file:data/comment-2025/index.sqlite?mode=ro", uri=True
        ) as db:
            return [
                (
                    cid,
                    db.execute(
                        "SELECT text FROM comments WHERE comment_id=?", (cid,)
                    ).fetchone()[0],
                )
                for cid in ids
            ]
    refs = json.loads(
        Path("data/research/books-resolver-2025-heal-v2/references.json").read_text()
    )
    return list({r["comment_id"]: r["context"] for r in refs}.items())


def windows_for(model, rows, labels):
    processor = model.data_processor
    result = []
    for cid, text in rows:
        for offset, chunk in text_windows(model, text, labels):
            words = [w[0] for w in processor.words_splitter(chunk)]
            inputs, _ = processor.prepare_inputs([words], labels)
            tokens = len(
                processor.transformer_tokenizer(
                    inputs, is_split_into_words=True, truncation=False
                )["input_ids"][0]
            )
            result.append((cid, offset, chunk, tokens))
    return result


def span_map(windows, predictions):
    return {
        (cid, off, off + s["start"], off + s["end"], s["text"]): float(s["score"])
        for (cid, off, _, _), spans in zip(windows, predictions, strict=True)
        for s in spans
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--stage", choices=["title", "person"], required=True)
    p.add_argument(
        "--backend", choices=["eager", "compile", "onnx", "flash"], default="eager"
    )
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--sample", type=int, default=1024)
    p.add_argument("--repeat", type=int, default=1)
    p.add_argument("--bf16-weights", action="store_true")
    p.add_argument(
        "--batches", type=int, nargs="+", default=[1, 8, 16, 32, 64, 128, 256]
    )
    p.add_argument("--orders", nargs="+", default=["tokens", "input", "chars"])
    p.add_argument("--onnx-path", type=Path)
    p.add_argument("--min-tokens", type=int, default=0)
    p.add_argument("--max-tokens", type=int, default=100000)
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    torch.set_num_threads(4)
    title = args.stage == "title"
    labels, threshold = (["book title"], 0.17) if title else (["person"], 0.3)
    checkpoint = (
        "data/probes/books-gliner-training-v1/final-refit/model" if title else MODEL_ID
    )
    if args.backend == "flash":
        os.environ["USE_FLASHDEBERTA"] = "1"
    started = time.monotonic()
    if args.backend == "onnx":
        import onnxruntime

        onnxruntime.preload_dlls()
        model = GLiNER.from_pretrained(
            args.onnx_path,
            runtime="onnxruntime",
            map_location="cuda",
            runtime_options={"providers": ["CUDAExecutionProvider"]},
        )
        assert model.model.session.get_providers()[0] == "CUDAExecutionProvider"
    else:
        model = (
            GLiNER.from_pretrained(checkpoint, load_tokenizer=True).to("cuda").eval()
        )
        if args.backend == "flash":
            assert any(
                type(m).__module__.startswith("flashdeberta") for m in model.modules()
            )
        if not title or args.bf16_weights:
            model.to(dtype=torch.bfloat16)
        if args.backend == "compile":
            model.compile()
    print(
        json.dumps(
            {
                "event": "loaded",
                "seconds": time.monotonic() - started,
                "model_class": str(type(model)),
                "backend": args.backend,
            }
        ),
        flush=True,
    )
    rows = load_rows(args.stage)
    chosen = random.Random(20260927).sample(rows, min(args.sample, len(rows)))
    selected = {cid for cid, _ in chosen}
    chosen += [
        r
        for r in sorted(rows, key=lambda r: len(r[1]), reverse=True)[:32]
        if r[0] not in selected
    ]
    windows = windows_for(model, chosen, labels)
    (args.output / "windows.json").write_text(json.dumps(windows))
    windows = [w for w in windows if args.min_tokens < w[3] <= args.max_tokens]
    assert windows, "No windows in requested length band"
    lengths = [w[3] for w in windows]
    print(
        json.dumps(
            {
                "event": "sample",
                "comments": len(chosen),
                "windows": len(windows),
                "token_percentiles": np.percentile(
                    lengths, [0, 50, 90, 95, 99, 100]
                ).tolist(),
                "tokenizer_limit": model.data_processor.transformer_tokenizer.model_max_length,
            }
        ),
        flush=True,
    )
    baseline = None
    baseline_path = args.output / "baseline.json"
    configurations = [(16 if title else 1, "chars" if title else "input")]
    configurations += [
        (b, order)
        for order in args.orders
        for b in args.batches
        if (b, order) not in configurations
    ]
    if args.backend != "eager":
        configurations = [(b, order) for order in args.orders for b in args.batches]
    for trial, (batch, order) in enumerate(configurations * args.repeat):
        ordered = (
            sorted(windows, key=lambda w: w[3] if order == "tokens" else len(w[2]))
            if order != "input"
            else windows
        )
        padded = sum(
            max(w[3] for w in ordered[i : i + batch]) * len(ordered[i : i + batch])
            for i in range(0, len(ordered), batch)
        )
        gc.collect()
        torch.cuda.empty_cache()
        torch.cuda.reset_peak_memory_stats()
        record = {
            "trial": trial,
            "batch": batch,
            "order": order,
            "backend": args.backend,
            "padding_ratio": padded / sum(lengths),
        }
        try:
            autocast = (
                torch.autocast("cuda", dtype=torch.bfloat16)
                if title and args.backend != "onnx"
                else nullcontext()
            )
            with torch.inference_mode(), autocast:
                model.inference(
                    [w[2] for w in ordered[: min(batch, 16)]],
                    labels,
                    batch_size=min(batch, 16),
                    threshold=threshold,
                    flat_ner=not title,
                )
                torch.cuda.synchronize()
                start = time.monotonic()
                if baseline is None and not title and args.backend == "eager":
                    output = [
                        model.predict_entities(w[2], labels, threshold=threshold)
                        for w in ordered
                    ]
                else:
                    output = model.inference(
                        [w[2] for w in ordered],
                        labels,
                        batch_size=batch,
                        threshold=threshold,
                        flat_ner=not title,
                    )
                torch.cuda.synchronize()
                elapsed = time.monotonic() - start
            found = span_map(ordered, output)
            if baseline is None:
                baseline = found
                baseline_path.write_text(
                    json.dumps([[*k, v] for k, v in found.items()])
                )
            added, removed = (
                found.keys() - baseline.keys(),
                baseline.keys() - found.keys(),
            )
            record.update(
                seconds=elapsed,
                windows_per_second=len(windows) / elapsed,
                peak_bytes=torch.cuda.max_memory_allocated(),
                reserved_bytes=torch.cuda.max_memory_reserved(),
                spans=len(found),
                added=len(added),
                removed=len(removed),
                max_score_delta=max(
                    (
                        abs(found[k] - baseline[k])
                        for k in found.keys() & baseline.keys()
                    ),
                    default=0,
                ),
            )
            (args.output / f"{order}-{batch}-changes.json").write_text(
                json.dumps(
                    {
                        "added": [[*k, found[k]] for k in added],
                        "removed": [[*k, baseline[k]] for k in removed],
                    }
                )
            )
            (args.output / f"{order}-{batch}-spans.json").write_text(
                json.dumps([[*k, v] for k, v in found.items()])
            )
        except torch.OutOfMemoryError as exc:
            record.update(
                error="cuda_oom",
                detail=str(exc),
                peak_bytes=torch.cuda.max_memory_allocated(),
            )
        print(json.dumps(record), flush=True)
        with (args.output / "metrics.jsonl").open("a") as out:
            out.write(json.dumps(record) + "\n")


if __name__ == "__main__":
    main()
