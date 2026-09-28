"""Extract title proposals and resolver references from a frozen comment slice.

Run on the GPU host with uv run --no-sync --package search-research python
packages/search-research/tools/resolver_heal/titles.py --slice data/comment-2025
--passes data/probes/books-gliner-v1/filter_passes.jsonl --output NEW_DIRECTORY.
The output directory must be new so changed inputs cannot reuse stale results.
"""

import argparse
import hashlib
import json
import sqlite3
import time
from collections import defaultdict
from contextlib import closing
from pathlib import Path

from search_research.comment_entities import text_windows
from search_research.ner_batching import (
    batch_schedule,
    configure_backend,
    predict_windows,
    prepare_windows,
)

CHECKPOINT = Path("data/probes/books-gliner-training-v1/final-refit/model")
LABELS = ["book title"]


def digest(path):
    """Hash an input without loading its contents into memory."""
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def passing_ids(path):
    """Read filter IDs in their frozen order and reject duplicate work."""
    with path.open() as source:
        ids = [json.loads(line)["comment_id"] for line in source]
    assert all(type(cid) is int for cid in ids), "Filter comment IDs must be integers"
    assert len(ids) == len(set(ids)), "Duplicate filter comment IDs"
    return ids


def extract_block(model, rows, batch_size, threshold, schedule=None):
    """Merge overlapping windows while preserving the original proposal order.

    Length sorting and stable insertion order reproduce the original 2025 runner.
    Repeated offsets keep their maximum score. Every span is checked against the
    complete source text; inference never silently truncates a comment's tail.
    """
    if schedule is None:
        windows = sorted(
            [
                (cid, off, chunk)
                for cid, text in rows
                for off, chunk in text_windows(model, text, LABELS)
            ],
            key=lambda row: len(row[2]),
        )
    else:
        planned = sorted(
            prepare_windows(model, rows, LABELS), key=lambda w: len(w.text)
        )
        windows = [(w.comment_id, w.offset, w.text) for w in planned]
    merged = defaultdict(dict)
    texts = dict(rows)
    if windows:
        if schedule is None:
            predictions = model.inference(
                [w[2] for w in windows],
                LABELS,
                batch_size=batch_size,
                threshold=threshold,
                flat_ner=False,
            )
        else:
            predictions = predict_windows(
                model, planned, LABELS, threshold, False, schedule
            )
        for (cid, offset, _), spans in zip(windows, predictions, strict=True):
            for span in spans:
                start, end = offset + span["start"], offset + span["end"]
                assert 0 <= start < end <= len(texts[cid]), "Invalid title offsets"
                assert texts[cid][start:end] == span["text"], (
                    "Title differs from source"
                )
                score = float(span["score"])
                assert 0 <= score <= 1, "Invalid title score"
                if score >= threshold:
                    key = (start, end)
                    merged[cid][key] = max(merged[cid].get(key, 0), score)
    return [
        {
            "comment_id": cid,
            "spans": [
                {"start": a, "end": b, "title": text[a:b], "score": score}
                for (a, b), score in merged[cid].items()
            ],
        }
        for cid, text in rows
    ]


def unique_spans(spans, threshold):
    """Match rollout preparation: lowercase, collapse whitespace, keep first.

    First means proposal order, as in resolver_rollout/prepare.py; a later or
    higher-scoring repetition must not change the reference ID and its offsets.
    """
    seen = set()
    for span in spans:
        if span["score"] < threshold:
            continue
        norm = " ".join(span["title"].lower().split())
        if norm not in seen:
            seen.add(norm)
            yield span


def references(record, text, threshold):
    """Build the existing resolver schema from deduplicated title proposals."""
    cid = record["comment_id"]
    return [
        span
        | {
            "id": f"{cid}:{span['start']}:{span['end']}",
            "comment_id": cid,
            "context": text,
        }
        for span in unique_spans(record["spans"], threshold)
    ]


def compare_proposals(actual_path, baseline_path, threshold):
    """Compare all spans and first-occurrence references, ignoring score jitter.

    Return every added/removed span with its saved score, plus score drift on
    shared spans. Missing-side scores are unknown, not assumed below threshold.
    """

    def read(path):
        with path.open() as source:
            rows = [json.loads(line) for line in source]
        result = {r["comment_id"]: r for r in rows}
        assert len(rows) == len(result), "Duplicate proposal comment IDs"
        return result

    actual, baseline = read(actual_path), read(baseline_path)
    assert actual.keys() == baseline.keys(), "Proposal comment sets differ"
    changes, reference_changes, deltas = [], [], []
    counts = {"actual_references": 0, "baseline_references": 0}
    for cid, row in actual.items():
        old = baseline[cid]
        left = {
            (s["start"], s["end"], s["title"]): s
            for s in old["spans"]
            if s["score"] >= threshold
        }
        right = {
            (s["start"], s["end"], s["title"]): s
            for s in row["spans"]
            if s["score"] >= threshold
        }
        for key in left.keys() & right.keys():
            deltas.append(abs(left[key]["score"] - right[key]["score"]))
        for side, source, other in (("removed", left, right), ("added", right, left)):
            changes.extend(
                {"comment_id": cid, "change": side, **source[k]}
                for k in sorted(source.keys() - other.keys())
            )
        previous = [
            (s["start"], s["end"], s["title"])
            for s in unique_spans(old["spans"], threshold)
        ]
        current = [
            (s["start"], s["end"], s["title"])
            for s in unique_spans(row["spans"], threshold)
        ]
        counts["baseline_references"] += len(previous)
        counts["actual_references"] += len(current)
        if previous != current:
            reference_changes.append(cid)
    return {
        "comments": len(actual),
        **counts,
        "span_changes": changes,
        "reference_changed_comments": reference_changes,
        "shared_spans": len(deltas),
        "max_score_delta": max(deltas, default=0),
        "changed_scores": sum(d > 0 for d in deltas),
    }


def main():
    """Run BF16-autocast inference and atomically finish artifacts in a new run."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slice", type=Path, required=True)
    parser.add_argument("--passes", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, default=CHECKPOINT)
    parser.add_argument(
        "--batch-size", type=int, help="Override all token buckets with one batch size"
    )
    parser.add_argument(
        "--legacy-order",
        action="store_true",
        help="Use original block/character ordering for comparisons",
    )
    parser.add_argument("--threshold", type=float, default=0.17)
    parser.add_argument("--backend", choices=["eager", "flash"], default="eager")
    parser.add_argument(
        "--baseline", type=Path, help="Compare saved span proposals after extraction"
    )
    args = parser.parse_args()
    assert args.batch_size is None or args.batch_size > 0
    assert 0 <= args.threshold <= 1
    schedule = batch_schedule("title", args.backend, args.batch_size)
    ids = passing_ids(args.passes)
    block_size = 2048 if args.legacy_order else max(1, len(ids))
    args.output.mkdir(parents=True, exist_ok=False)

    import torch
    from gliner import GLiNER

    assert torch.cuda.is_available() and torch.cuda.is_bf16_supported()
    torch.set_num_threads(4)
    configure_backend(args.backend)
    model = (
        GLiNER.from_pretrained(args.checkpoint, load_tokenizer=True).to("cuda").eval()
    )
    started = time.monotonic()
    refs = []
    occurrences = passed = 0
    temporary = args.output / "title-proposals.jsonl.tmp"
    uri = (args.slice / "index.sqlite").resolve().as_uri() + "?mode=ro"
    with (
        closing(sqlite3.connect(uri, uri=True)) as db,
        temporary.open("x") as out,
        torch.inference_mode(),
        torch.autocast("cuda", dtype=torch.bfloat16),
    ):
        for base in range(0, len(ids), block_size):
            rows = []
            for cid in ids[base : base + block_size]:
                row = db.execute(
                    "SELECT text FROM comments WHERE comment_id=?", (cid,)
                ).fetchone()
                assert row is not None, f"Comment {cid} is absent from slice"
                rows.append((cid, row[0]))
            proposals = extract_block(
                model,
                rows,
                args.batch_size or 16,
                args.threshold,
                None if args.legacy_order else schedule,
            )
            for record, (_, text) in zip(proposals, rows, strict=True):
                out.write(json.dumps(record) + "\n")
                refs.extend(references(record, text, args.threshold))
                occurrences += len(record["spans"])
                passed += bool(record["spans"])
            out.flush()
            print(
                json.dumps(
                    {
                        "comments": base + len(rows),
                        "seconds": round(time.monotonic() - started, 1),
                    }
                ),
                flush=True,
            )
    temporary.replace(args.output / "title-proposals.jsonl")
    ref_path = args.output / "references.json.tmp"
    ref_path.write_text(json.dumps(refs))
    ref_path.replace(args.output / "references.json")
    manifest = {
        "slice": str(args.slice),
        "passes": str(args.passes),
        "passes_sha256": digest(args.passes),
        "checkpoint": str(args.checkpoint),
        "checkpoint_sha256": {
            p.name: digest(p) for p in sorted(args.checkpoint.iterdir()) if p.is_file()
        },
        "batch_size": args.batch_size,
        "backend": args.backend,
        "threshold": args.threshold,
        "comments_per_block": block_size,
        "schedule": None if args.legacy_order else schedule,
        "ordering": "block characters" if args.legacy_order else "whole-year tokens",
        "precision": "float32 weights, bfloat16 autocast",
        "comments": len(ids),
        "passed_comments": passed,
        "span_occurrences": occurrences,
        "references": len(refs),
        "seconds": time.monotonic() - started,
        "peak_gpu_bytes": torch.cuda.max_memory_allocated(),
    }
    (args.output / "title-manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest), flush=True)
    if args.baseline:
        comparison = compare_proposals(
            args.output / "title-proposals.jsonl", args.baseline, args.threshold
        )
        (args.output / "title-comparison.json").write_text(
            json.dumps(comparison, indent=2)
        )
        print(json.dumps(comparison), flush=True)


if __name__ == "__main__":
    main()
