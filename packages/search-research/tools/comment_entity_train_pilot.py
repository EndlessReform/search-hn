"""Bounded GLiNER training pilot: frozen reviewed data, timing, and test scoring.

This deliberately runs only 20 optimizer updates. It does not select a checkpoint
or threshold from test results, and never modifies the annotation database.
"""

import math
import random
import re
import sqlite3
import statistics
import sys
from pathlib import Path
from time import perf_counter

import torch
from comment_book_llm import encode_record
from gliner import GLiNER
from gliner.data_processing.collator import UniEncoderSpanDataCollator
from gliner.training import Trainer, TrainingArguments
from search_research.comment_entities import MODEL_ID, text_windows
from transformers import TrainerCallback, set_seed

OUT = Path("data/probes/books-gliner-training-pilot-v1")
LABELS = ["book title"]
SEED = 20260920


def save(name, obj):
    (OUT / name).write_text(encode_record(obj) + "\n")


def snapshot():
    """Take an online SQLite snapshot, then read all rows from that snapshot."""
    OUT.mkdir(parents=True, exist_ok=True)
    if not (OUT / "reviewed.sqlite").exists():
        with (
            sqlite3.connect(
                "file:data/comment-2025/annotations.sqlite?mode=ro", uri=True
            ) as source,
            sqlite3.connect(OUT / "reviewed.sqlite") as target,
        ):
            source.backup(target)
    with sqlite3.connect(OUT / "reviewed.sqlite") as db:
        db.row_factory = sqlite3.Row
        rows = [
            dict(r)
            for r in db.execute(
                "SELECT * FROM entity_items WHERE batch_id=1 ORDER BY comment_id"
            )
        ]
        assert len(rows) == 1300 and all(r["reviewed"] for r in rows)
        for row in rows:
            row["entities"] = [
                dict(e)
                for e in db.execute(
                    "SELECT * FROM entity_labels WHERE batch_id=1 AND comment_id=? AND deleted=0",
                    (row["comment_id"],),
                )
            ]
    assert len({r["text"] for r in rows}) == len(rows), (
        "Group duplicate texts before splitting"
    )
    rng = random.Random(SEED)
    test_ids = set()
    # Allocate each class proportionally across teacher-agreement strata.
    for positive, count in ((True, 105), (False, 195)):
        groups = {}
        for row in rows:
            if bool(row["entities"]) == positive:
                groups.setdefault(row["source"], []).append(row)
        total = sum(map(len, groups.values()))
        quotas = {k: count * len(v) / total for k, v in groups.items()}
        sizes = {k: math.floor(q) for k, q in quotas.items()}
        for key in sorted(
            groups, key=lambda k: (quotas[k] - sizes[k], k), reverse=True
        )[: count - sum(sizes.values())]:
            sizes[key] += 1
        for key, group in sorted(groups.items()):
            rng.shuffle(group)
            test_ids.update(r["comment_id"] for r in group[: sizes[key]])
    for row in rows:
        row["pilot_split"] = "test" if row["comment_id"] in test_ids else "train"
    save(
        "split.json",
        [
            {
                "comment_id": r["comment_id"],
                "split": r["pilot_split"],
                "positive": bool(r["entities"]),
            }
            for r in rows
        ],
    )
    return rows


def align(model, rows):
    """Bracket whole comments on nonrepresentable spans; never invent negatives."""
    accepted, excluded, training = [], [], []
    for row in rows:
        words = list(model.data_processor.words_splitter(row["text"]))
        starts = {w[1]: i for i, w in enumerate(words)}
        ends = {w[2]: i for i, w in enumerate(words)}
        spans, reasons = [], []
        for entity in row["entities"]:
            left, right = entity["start"], entity["end"]
            if left is None:
                pattern = r"\s+".join(re.escape(s) for s in entity["title"].split())
                matches = list(re.finditer(pattern, row["text"], flags=re.IGNORECASE))
                if len(matches) == 1:
                    left, right = matches[0].span()
                else:
                    reasons.append("unaligned title")
                    continue
            assert (
                " ".join(row["text"][left:right].split()).casefold()
                == " ".join(entity["title"].split()).casefold()
            )
            if left not in starts or right not in ends:
                reasons.append("token boundary")
                continue
            assert words[starts[left]][1] == left and words[ends[right]][2] == right
            assert ends[right] - starts[left] + 1 <= 32
            spans.append((left, right))
        if reasons:
            excluded.append(
                {
                    "comment_id": row["comment_id"],
                    "split": row["pilot_split"],
                    "reasons": reasons,
                    "titles": len(row["entities"]),
                }
            )
            continue
        row["spans"] = sorted(set(spans))
        row["windows"] = list(text_windows(model, row["text"], LABELS))
        covered = set()
        for offset, chunk in row["windows"]:
            tokens = list(model.data_processor.words_splitter(chunk))
            a = {w[1] + offset: i for i, w in enumerate(tokens)}
            b = {w[2] + offset: i for i, w in enumerate(tokens)}
            ner = [
                [a[l], b[r], LABELS[0]] for l, r in row["spans"] if l in a and r in b
            ]
            covered.update((l, r) for l, r in row["spans"] if l in a and r in b)
            if row["pilot_split"] == "train":
                training.append(
                    {
                        "tokenized_text": [w[0] for w in tokens],
                        "ner": ner,
                        "ner_labels": LABELS,
                    }
                )
        assert covered == set(row["spans"]), "Windowing lost a title"
        accepted.append(row)
    save("excluded.json", excluded)
    save("training.json", training)
    save(
        "data-summary.json",
        {
            "excluded": len(excluded),
            "training_windows": len(training),
            "splits": {
                split: {
                    "positive": sum(
                        bool(r["spans"]) for r in accepted if r["pilot_split"] == split
                    ),
                    "negative": sum(
                        not r["spans"] for r in accepted if r["pilot_split"] == split
                    ),
                }
                for split in ("train", "test")
            },
        },
    )
    return accepted, training


def score(model, rows, name):
    """Score original character spans, deduplicating overlapping windows."""
    model.eval()
    windows = [
        (r, offset, chunk)
        for r in rows
        if r["pilot_split"] == "test"
        for offset, chunk in r["windows"]
    ]
    torch.cuda.synchronize()
    started = perf_counter()
    with torch.inference_mode(), torch.autocast("cuda", dtype=torch.bfloat16):
        predictions = model.inference(
            [w[2] for w in windows], LABELS, batch_size=8, threshold=0.5, flat_ner=False
        )
    torch.cuda.synchronize()
    elapsed = perf_counter() - started
    by_id = {r["comment_id"]: set() for r in rows if r["pilot_split"] == "test"}
    for (row, offset, chunk), entities in zip(windows, predictions, strict=True):
        for entity in entities:
            assert chunk[entity["start"] : entity["end"]] == entity["text"]
            by_id[row["comment_id"]].add(
                (offset + entity["start"], offset + entity["end"])
            )
    tp = fp = fn = gate_tp = gate_fp = gate_fn = gate_tn = 0
    for row in rows:
        if row["pilot_split"] != "test":
            continue
        gold, pred = set(row["spans"]), by_id[row["comment_id"]]
        tp += len(gold & pred)
        fp += len(pred - gold)
        fn += len(gold - pred)
        gate_tp += bool(gold) and bool(pred)
        gate_fp += not gold and bool(pred)
        gate_fn += bool(gold) and not pred
        gate_tn += not gold and not pred
    result = {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": tp / max(1, tp + fp),
        "recall": tp / max(1, tp + fn),
        "f1": 2 * tp / max(1, 2 * tp + fp + fn),
        "gate_tp": gate_tp,
        "gate_fp": gate_fp,
        "gate_fn": gate_fn,
        "gate_tn": gate_tn,
        "seconds": elapsed,
        "comments": len(by_id),
        "windows": len(windows),
    }
    save(name + ".json", result)
    save(name + "-spans.json", {str(k): sorted(v) for k, v in by_id.items()})
    print(name, result, flush=True)


class Timing(TrainerCallback):
    """Synchronize optimizer-step timing so CUDA work is included."""

    def __init__(self):
        self.steps = []

    def on_step_begin(self, args, state, control, **kwargs):
        torch.cuda.synchronize()
        self.started = perf_counter()

    def on_step_end(self, args, state, control, **kwargs):
        torch.cuda.synchronize()
        self.steps.append(perf_counter() - self.started)


def main():
    set_seed(SEED)
    rows = snapshot()
    model = GLiNER.from_pretrained(MODEL_ID, max_width=32, load_tokenizer=True).to(
        "cuda"
    )
    if "--checkpointing" in sys.argv:
        model.model.token_rep_layer.bert_layer.model.gradient_checkpointing_enable(
            gradient_checkpointing_kwargs={"use_reentrant": False}
        )
    rows, training = align(model, rows)
    if "--stress-only" in sys.argv:
        stress(model, training)
        return
    if not (OUT / "baseline.json").exists():
        score(model, rows, "baseline")
    timing = Timing()
    forwards = []

    def before(module, args):
        torch.cuda.synchronize()
        module.pilot_started = perf_counter()

    def after(module, args, output):
        torch.cuda.synchronize()
        forwards.append(perf_counter() - module.pilot_started)

    hooks = [
        model.register_forward_pre_hook(before),
        model.register_forward_hook(after),
    ]
    args = TrainingArguments(
        output_dir=str(OUT / "trainer"),
        max_steps=20,
        per_device_train_batch_size=8,
        gradient_accumulation_steps=2,
        learning_rate=1e-5,
        others_lr=5e-5,
        weight_decay=0.01,
        others_weight_decay=0.01,
        warmup_steps=2,
        bf16=True,
        save_strategy="no",
        logging_steps=1,
        remove_unused_columns=False,
        report_to="none",
        seed=SEED,
        focal_loss_alpha=0.8,
        focal_loss_gamma=0,
        loss_reduction="sum",
    )
    trainer = Trainer(
        model=model,
        args=args,
        train_dataset=training,
        data_collator=UniEncoderSpanDataCollator(
            model.config, data_processor=model.data_processor
        ),
        callbacks=[timing],
    )
    torch.cuda.reset_peak_memory_stats()
    started = perf_counter()
    trainer.train()
    elapsed = perf_counter() - started
    for hook in hooks:
        hook.remove()
    result = {
        "steps": timing.steps,
        "forward_seconds": forwards,
        "wall_seconds": elapsed,
        "peak_allocated_gib": torch.cuda.max_memory_allocated() / 2**30,
        "peak_reserved_gib": torch.cuda.max_memory_reserved() / 2**30,
        "median_step_seconds": statistics.median(timing.steps[2:]),
        "median_forward_seconds": statistics.median(forwards[4:]),
        "projected_epoch_seconds": statistics.mean(timing.steps[2:])
        * math.ceil(len(training) / 16),
        "training_windows": len(training),
        "microbatch": 8,
        "accumulation": 2,
    }
    save("timing.json", result)
    print("TIMING", result, flush=True)
    score(model, rows, "after-20-steps")


def stress(model, training):
    """Check the eight longest tokenized windows with Adam states resident."""
    collator = UniEncoderSpanDataCollator(
        model.config, data_processor=model.data_processor
    )
    tokenizer = model.data_processor.transformer_tokenizer

    def length(row):
        inputs, _ = model.data_processor.prepare_inputs([row["tokenized_text"]], LABELS)
        return len(
            tokenizer(inputs, is_split_into_words=True, truncation=False)["input_ids"][
                0
            ]
        )

    ordered = sorted(training, key=length)
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-5)
    model.train()
    result = {
        "model": MODEL_ID,
        "parameters": sum(p.numel() for p in model.parameters()),
        "parameter_gib": sum(p.numel() * p.element_size() for p in model.parameters())
        / 2**30,
        "checkpointing": "--checkpointing" in sys.argv,
        "token_lengths": [length(r) for r in ordered[-8:]],
        "stages": [],
    }
    try:
        for batch_rows in (ordered[:8], ordered[-8:]):
            batch = {
                k: v.to("cuda") if isinstance(v, torch.Tensor) else v
                for k, v in collator(batch_rows).items()
            }
            torch.cuda.synchronize()
            stage = {
                "input_shape": list(batch["input_ids"].shape),
                "before_gib": torch.cuda.memory_allocated() / 2**30,
            }
            result["stages"].append(stage)
            torch.cuda.reset_peak_memory_stats()
            started = perf_counter()
            with torch.autocast("cuda", dtype=torch.bfloat16):
                output = model(**batch)
                loss = output.loss
            torch.cuda.synchronize()
            stage["after_forward_gib"] = torch.cuda.memory_allocated() / 2**30
            result["forward_seconds"] = perf_counter() - started
            loss.backward()
            stage["after_backward_gib"] = torch.cuda.memory_allocated() / 2**30
            optimizer.step()
            optimizer.zero_grad(set_to_none=True)
            stage["after_optimizer_gib"] = torch.cuda.memory_allocated() / 2**30
            stage["optimizer_state_gib"] = (
                sum(
                    v.numel() * v.element_size()
                    for state in optimizer.state.values()
                    for v in state.values()
                    if isinstance(v, torch.Tensor)
                )
                / 2**30
            )
            torch.cuda.synchronize()
            result["step_seconds"] = perf_counter() - started
        result["status"] = "passed"
    except torch.cuda.OutOfMemoryError as exc:
        result["status"] = "oom"
        result["error"] = str(exc)
    result["peak_allocated_gib"] = torch.cuda.max_memory_allocated() / 2**30
    result["peak_reserved_gib"] = torch.cuda.max_memory_reserved() / 2**30
    save(
        "longest-batch-checkpointing.json"
        if "--checkpointing" in sys.argv
        else "longest-batch.json",
        result,
    )
    print("STRESS", result, flush=True)


if __name__ == "__main__":
    main()
