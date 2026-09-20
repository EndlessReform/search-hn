"""Measure lookup-to-output cost and diagnose misses on three reviewed comments."""

import json
import sqlite3
from statistics import median
from time import perf_counter

import torch
from comment_entity_benchmark import LABELS, OUT, infer, read_rows, save, windows_for
from gliner import GLiNER
from search_research.comment_entities import MODEL_ID


def main():
    torch.set_num_threads(8)
    model = (
        GLiNER.from_pretrained(MODEL_ID, load_tokenizer=True)
        .to(device="cuda", dtype=torch.bfloat16)
        .eval()
    )
    # Repack RNN weights after conversion, following PyTorch's documented method.
    for layer in model.modules():
        if isinstance(layer, torch.nn.RNNBase):
            layer.flatten_parameters()
    sample = read_rows("benchmark.jsonl")
    timings = []
    with (
        torch.inference_mode(),
        sqlite3.connect("file:data/comment-2025/index.sqlite?mode=ro", uri=True) as db,
    ):
        for _ in range(4):
            torch.cuda.synchronize()
            start = perf_counter()
            rows = [
                {
                    "comment_id": r["comment_id"],
                    "text": db.execute(
                        "select text from comments where comment_id=?",
                        (r["comment_id"],),
                    ).fetchone()[0],
                }
                for r in sample
            ]
            lookup = perf_counter() - start
            windows = sorted(windows_for(model, rows, LABELS), key=lambda w: len(w[2]))
            prepared = perf_counter() - start
            result, inference = infer(model, rows, windows, LABELS, 16)
            encoded = "\n".join(
                json.dumps({"comment_id": r["comment_id"], "spans": p})
                for r, p in zip(rows, result, strict=True)
            )
            torch.cuda.synchronize()
            timings.append(
                {
                    "total_seconds": perf_counter() - start,
                    "lookup_seconds": lookup,
                    "lookup_and_windows_seconds": prepared,
                    "model_seconds": inference,
                    "output_bytes": len(encoded),
                }
            )
        measured = timings[1:]
        population = json.loads((OUT / "sample-summary.json").read_text())
        save(
            "pipeline-timing.json",
            {
                "comments": len(sample),
                "batch_size": 16,
                "repetitions": measured,
                "median_seconds": median(r["total_seconds"] for r in measured),
                "comments_per_second": len(sample)
                / median(r["total_seconds"] for r in measured),
                "estimated_all_passed_seconds": population["passed"]
                * median(r["total_seconds"] for r in measured)
                / len(sample),
            },
        )
        reviews = read_rows("review-inputs.jsonl")
        diagnoses = []
        for row in reviews:
            if row["comment_id"] not in {46392391, 42655045, 45711672}:
                continue
            text = row["text"]
            full = model.predict_entities(
                text, LABELS, threshold=0.05, flat_ner=False, multi_label=True
            )
            lines = [line for line in text.splitlines() if line.strip()]
            separate = model.inference(
                lines,
                LABELS,
                threshold=0.35,
                flat_ner=False,
                multi_label=True,
                batch_size=16,
            )
            diagnoses.append(
                {
                    "comment_id": row["comment_id"],
                    "text": text,
                    "full_books_at_005": [s for s in full if s["label"] == "book"],
                    "line_predictions": [
                        {
                            "text": line,
                            "books": [s for s in spans if s["label"] == "book"],
                        }
                        for line, spans in zip(lines, separate, strict=True)
                    ],
                    "model_max_width": model.config.max_width,
                    "word_count": len(list(model.data_processor.words_splitter(text))),
                }
            )
            if row["comment_id"] == 42655045:
                # Diagnostic only: retain character positions while removing
                # literal emphasis delimiters from prose, leaving URL lines intact.
                cleaned = "\n".join(
                    line if line.startswith("http") else line.replace("_", " ")
                    for line in text.split("\n")
                )
                cleaned_spans = model.predict_entities(
                    cleaned, LABELS, threshold=0.35, flat_ner=False, multi_label=True
                )
                save(
                    "format-diagnostic.json",
                    {
                        "comment_id": row["comment_id"],
                        "treatment": "Replace underscores with spaces in non-URL lines; offsets and lengths preserved",
                        "full_books": [s for s in cleaned_spans if s["label"] == "book"],
                    },
                )
        save("miss-diagnostics.json", diagnoses)
    print((OUT / "pipeline-timing.json").read_text())


if __name__ == "__main__":
    main()
