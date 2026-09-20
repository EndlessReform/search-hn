"""Compare GLiNER precision, batch scaling, and held-out extraction on the 5090.

Run with uv run --locked --package search-research --extra ner python
packages/search-research/tools/comment_entity_benchmark.py. Samples are created
by comment_entity_samples.py; this script never writes to the corpus or labels.
"""

import json
from pathlib import Path
from statistics import median
from time import perf_counter

import numpy as np
import torch
from gliner import GLiNER
from search_research.comment_entities import MODEL_ID, text_windows

OUT = Path("data/probes/books-gliner-v1")
LABELS = ["book", "author", "url"]
THRESHOLDS = {"book": 0.35, "author": 0.5, "url": 0.5}


def read_rows(name):
    with (OUT / name).open() as stream:
        return [json.loads(line) for line in stream]


def save(name, data):
    (OUT / name).write_text(json.dumps(data, indent=2))


def windows_for(model, rows, labels):
    """Record ownership and original offsets before any length-based sorting."""
    return [
        (i, offset, text)
        for i, r in enumerate(rows)
        for offset, text in text_windows(model, r["text"], labels)
    ]


def infer(model, rows, windows, labels, batch_size, *, threshold=0.35):
    """Time complete batched tokenization/transfer/forward/decode, synchronized."""
    torch.cuda.synchronize()
    started = perf_counter()
    predictions = model.inference(
        [w[2] for w in windows],
        labels,
        batch_size=batch_size,
        threshold=threshold,
        flat_ner=False,
        multi_label=True,
    )
    torch.cuda.synchronize()
    elapsed = perf_counter() - started
    results = [{} for _ in rows]
    for (i, offset, chunk), spans in zip(windows, predictions, strict=True):
        for span in spans:
            label = span["label"]
            if span["score"] < (0.35 if label in {"book", "book title"} else 0.5):
                continue
            left, right = offset + span["start"], offset + span["end"]
            assert rows[i]["text"][left:right] == span["text"]
            key = (left, right, label)
            result = {
                "start": left,
                "end": right,
                "text": span["text"],
                "label": label,
                "score": float(span["score"]),
            }
            if key not in results[i] or result["score"] > results[i][key]["score"]:
                results[i][key] = result
    return [
        sorted(r.values(), key=lambda s: (s["start"], s["end"], s["label"]))
        for r in results
    ], elapsed


def agreement(reference, candidate):
    """Separate score drift from changed decoded span/label sets and book gates."""
    diffs = []
    changed = []
    gate = []
    for i, (left, right) in enumerate(zip(reference, candidate, strict=True)):
        a = {(s["start"], s["end"], s["label"]): s["score"] for s in left}
        b = {(s["start"], s["end"], s["label"]): s["score"] for s in right}
        diffs.extend(abs(a[k] - b[k]) for k in a.keys() & b.keys())
        if a.keys() != b.keys():
            changed.append(i)
        if any(s["label"] == "book" for s in left) != any(
            s["label"] == "book" for s in right
        ):
            gate.append(i)
    return {
        "comments": len(reference),
        "changed_span_comments": changed,
        "changed_book_gate_comments": gate,
        "matched_spans": len(diffs),
        "max_score_delta": max(diffs, default=0),
        "p99_score_delta": float(np.quantile(diffs, 0.99)) if diffs else 0,
    }


def main():
    assert torch.cuda.is_available() and torch.cuda.is_bf16_supported()
    torch.set_num_threads(8)
    review = read_rows("review-inputs.jsonl")
    benchmark = read_rows("benchmark.jsonl")
    model = GLiNER.from_pretrained(MODEL_ID, load_tokenizer=True).to("cuda").eval()
    metadata = {
        "model": MODEL_ID,
        "gpu": torch.cuda.get_device_name(),
        "torch": torch.__version__,
        "initial_dtype": str(next(model.parameters()).dtype),
        "labels": LABELS,
        "thresholds": THRESHOLDS,
    }
    with torch.inference_mode():
        review_windows = windows_for(model, review, LABELS)
        # Same batch composition for the precision comparison.
        fp32, fp32_seconds = infer(model, review, review_windows, LABELS, 8)
        model.to(dtype=torch.bfloat16)
        bf16, bf16_seconds = infer(model, review, review_windows, LABELS, 8)
        drift = agreement(fp32, bf16)
        save(
            "precision.json",
            metadata
            | {
                "comparison": drift,
                "fp32_seconds": fp32_seconds,
                "bf16_seconds": bf16_seconds,
                "rows": [
                    {"comment_id": r["comment_id"], "fp32": a, "bf16": b}
                    for r, a, b in zip(review, fp32, bf16, strict=True)
                ],
            },
        )
        assert not drift["changed_book_gate_comments"], (
            "BF16 changed the book-presence gate; inspect precision.json"
        )
        alt_labels = ["book title", "author", "url"]
        alternate, _ = infer(
            model, review, windows_for(model, review, alt_labels), alt_labels, 8
        )
        with (OUT / "review-predictions.jsonl").open("w") as stream:
            for row, current, other in zip(review, bf16, alternate, strict=True):
                stream.write(
                    json.dumps(
                        row | {"predictions": current, "book_title_predictions": other}
                    )
                    + "\n"
                )
        for group in ["positive_1", "positive_2", "negative_1", "negative_2"]:
            with (OUT / f"{group}.jsonl").open("w") as stream:
                for row, current, other in zip(review, bf16, alternate, strict=True):
                    if row["review_group"] == group:
                        stream.write(
                            json.dumps(
                                {
                                    "comment_id": row["comment_id"],
                                    "text": row["text"],
                                    "predictions": current,
                                    "book_title_predictions": other,
                                }
                            )
                            + "\n"
                        )
        print("REVIEW PACKETS READY " + json.dumps(drift), flush=True)
        windows = windows_for(model, benchmark, LABELS)
        sorted_windows = sorted(windows, key=lambda w: len(w[2]))
        all_results = []
        for batch in [1, 2, 4, 8, 16, 32, 64, 128]:
            try:
                infer(model, benchmark, sorted_windows[:batch], LABELS, batch)
                timings = []
                torch.cuda.reset_peak_memory_stats()
                for _ in range(3):
                    _results, seconds = infer(
                        model, benchmark, sorted_windows, LABELS, batch
                    )
                    timings.append(seconds)
                entry = {
                    "batch_size": batch,
                    "seconds": timings,
                    "median_seconds": median(timings),
                    "comments_per_second": len(benchmark) / median(timings),
                    "peak_allocated_gib": torch.cuda.max_memory_allocated() / 2**30,
                    "peak_reserved_gib": torch.cuda.max_memory_reserved() / 2**30,
                }
                all_results.append(entry)
                print(json.dumps(entry), flush=True)
                save(
                    "batch-results.json",
                    metadata
                    | {
                        "dtype": "bfloat16",
                        "comments": len(benchmark),
                        "windows": len(windows),
                        "length_sorted": True,
                        "results": all_results,
                    },
                )
            except torch.OutOfMemoryError:
                torch.cuda.empty_cache()
                all_results.append({"batch_size": batch, "error": "CUDA out of memory"})
                save(
                    "batch-results.json",
                    metadata
                    | {
                        "dtype": "bfloat16",
                        "comments": len(benchmark),
                        "windows": len(windows),
                        "length_sorted": True,
                        "results": all_results,
                    },
                )
                break
        successful = [r for r in all_results if "error" not in r]
        best = max(r["comments_per_second"] for r in successful)
        knee = next(r for r in successful if r["comments_per_second"] >= 0.9 * best)
        batch = knee["batch_size"]
        _, unsorted_seconds = infer(model, benchmark, windows, LABELS, batch)
        bf16_results, bf16_best = infer(model, benchmark, sorted_windows, LABELS, batch)
        # Also compare precision on the unbiased filter-passed sample at the chosen batch.
        model.float()
        fp32_results, fp32_best = infer(model, benchmark, sorted_windows, LABELS, batch)
        comparison = agreement(fp32_results, bf16_results)
        save(
            "benchmark-precision.json",
            comparison
            | {
                "fp32_seconds": fp32_best,
                "bf16_seconds": bf16_best,
                "changed_rows": [
                    benchmark[i] | {"fp32": fp32_results[i], "bf16": bf16_results[i]}
                    for i in comparison["changed_span_comments"]
                ],
            },
        )
        population = json.loads((OUT / "sample-summary.json").read_text())
        save(
            "throughput-summary.json",
            metadata
            | {
                "dtype": "bfloat16",
                "knee_definition": "smallest batch within 90% of highest median throughput",
                "knee": knee,
                "unsorted_seconds": unsorted_seconds,
                "passed_comments": population["passed"],
                "estimated_ner_seconds": population["passed"]
                / knee["comments_per_second"],
                "filter_seconds": population["filter_seconds"],
                "benchmark_precision": comparison,
                "benchmark_lengths": {
                    "chars_median": float(
                        np.median([len(r["text"]) for r in benchmark])
                    ),
                    "chars_p95": float(
                        np.quantile([len(r["text"]) for r in benchmark], 0.95)
                    ),
                    "chars_max": max(len(r["text"]) for r in benchmark),
                },
            },
        )
        print("BENCHMARK COMPLETE", flush=True)


if __name__ == "__main__":
    main()
