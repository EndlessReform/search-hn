"""Add existing DS-positive comments to corrected gold at a fixed update budget.

The comparison reuses the completed gold-only linear arm. Negative windows inside
positive comments are retained; only DS-negative comments are omitted. Frozen
holdout membership and all optimizer settings remain the same. Development is
measured at the control's update numbers rather than at new epoch boundaries.
"""

import gc
import json
import logging
import sqlite3
from pathlib import Path
from time import perf_counter

import comment_entity_train_pilot as pilot
import comment_entity_train_runs as runs
import comment_entity_tune as tune
import comment_entity_wsd as paired
import torch
from comment_entity_silver_run import THRESHOLDS, metrics
from gliner.training import Trainer, TrainingArguments
from search_research.comment_entity_annotations import locate_title
from transformers import TrainerCallback, set_seed

ROOT = Path("data/probes/books-gliner-positive-silver-v1")
CONTROL = Path("data/probes/books-gliner-wsd-v1")
SILVER = Path("data/probes/books-silver3000-v1")
STEPS = 135


def positive_rows():
    """Reuse teacher outputs verbatim and verify disjoint IDs and normalized texts."""
    with sqlite3.connect(f"file:{paired.SOURCE}?mode=ro", uri=True) as db:
        reviewed = list(db.execute("SELECT comment_id,text FROM entity_items"))
    forbidden = {cid for cid, _ in reviewed}
    texts = {" ".join(text.casefold().split()) for _, text in reviewed}
    inputs = {r["comment_id"]: r for r in runs.read_lines(SILVER / "input.jsonl")}
    rows = []
    for record in runs.read_lines(SILVER / "deepseek/responses.jsonl"):
        assert record["status"] == 200 and not record.get("error")
        if not record["extraction"]["books"]:
            continue
        cid = record["comment_id"]
        text = inputs[cid]["text"]
        assert cid not in forbidden and " ".join(text.casefold().split()) not in texts
        entities = []
        for book in record["extraction"]["books"]:
            for a, b in locate_title(text, book["title"]) or [(None, None)]:
                entities.append(book | {"start": a, "end": b})
        rows.append(
            {
                "comment_id": cid,
                "text": text,
                "entities": entities,
                "pilot_split": "train",
            }
        )
    assert len(rows) == 423
    return rows


class Curve(TrainerCallback):
    """Evaluate at the exact same update counts as the gold-only control."""

    def __init__(self, dev):
        self.dev = dev
        self.history = []
        self.best = -1

    def on_step_end(self, args, state, control, model, **kwargs):
        if state.global_step not in (27, 54, 81, 108, 135):
            return
        _, scores = tune.score_grid(model, self.dev)
        grid = [metrics(self.dev, scores, t) for t in THRESHOLDS]
        best = max(grid, key=lambda r: r["f1"])
        self.history.append(
            {"epoch": state.epoch, "step": state.global_step, "grid": grid}
        )
        pilot.save("curve.json", self.history)
        pilot.save(f"dev-scores-step{state.global_step}.json", scores)
        if best["f1"] > self.best:
            self.best = best["f1"]
            model.save_pretrained(pilot.OUT / "best-model")
            pilot.save(
                "selection.json",
                {"epoch": state.epoch, "step": state.global_step, "metrics": best},
            )
        print(
            "CHECKPOINT", state.global_step, "fixed", grid[9], "best", best, flush=True
        )
        model.train()


def main():
    ROOT.mkdir(exist_ok=False)
    logging.getLogger("gliner.training.trainer").addHandler(tune.FailSkipped())
    paired.ROOT = ROOT
    model = runs.model_new()
    gold, dev, test, fresh = paired.prepare(model)
    assert gold == json.loads((CONTROL / "train/training.json").read_text())
    pilot.OUT = ROOT / "silver"
    pilot.OUT.mkdir()
    accepted, extra = pilot.align(model, positive_rows())
    assert len(accepted) == 413 and all(r["spans"] for r in accepted)
    train = gold + extra
    pilot.OUT = ROOT
    pilot.save(
        "manifest.json",
        {
            "control": str(CONTROL / "linear"),
            "gold_comments": 838,
            "gold_positive": 400,
            "gold_negative": 438,
            "silver_positive_comments": 413,
            "silver_negative_comments": 0,
            "silver_positive_before_alignment": 423,
            "silver_excluded": 10,
            "gold_windows": len(gold),
            "silver_windows": len(extra),
            "total_windows": len(train),
            "max_steps": STEPS,
            "effective_batch": 32,
            "scheduler": "linear",
            "warmup_ratio": 0.1,
            "encoder_lr": 1e-5,
            "other_lr": 5e-5,
            "seed": pilot.SEED,
            "evaluation_steps": [27, 54, 81, 108, 135],
            "source": str(paired.SOURCE),
            "holdouts": "same frozen 147 dev, 299 original test and 300 fresh test",
        },
    )
    pilot.save("silver-comment-ids.json", [r["comment_id"] for r in accepted])
    # Reload to exactly reproduce initialization and RNG reset from the control.
    del model
    gc.collect()
    torch.cuda.empty_cache()
    model = runs.model_new()
    set_seed(pilot.SEED)
    collator = tune.CountCollator(model)
    callback = Curve(dev)
    args = TrainingArguments(
        output_dir=str(ROOT / "trainer"),
        max_steps=STEPS,
        per_device_train_batch_size=8,
        gradient_accumulation_steps=4,
        learning_rate=1e-5,
        others_lr=5e-5,
        weight_decay=0.01,
        others_weight_decay=0.01,
        lr_scheduler_type="linear",
        warmup_ratio=0.1,
        bf16=True,
        save_strategy="no",
        logging_steps=10,
        remove_unused_columns=False,
        report_to="none",
        seed=pilot.SEED,
        data_seed=pilot.SEED,
        focal_loss_alpha=0.8,
        focal_loss_gamma=0,
        loss_reduction="sum",
    )
    trainer = Trainer(
        model=model,
        args=args,
        train_dataset=train,
        data_collator=collator,
        callbacks=[callback],
    )
    torch.cuda.reset_peak_memory_stats()
    started = perf_counter()
    trainer.train()
    assert trainer.state.global_step == STEPS
    assert [r["step"] for r in callback.history] == [27, 54, 81, 108, 135]
    baseline = json.loads((CONTROL / "linear/run.json").read_text())
    assert abs(collator.count / baseline["samples_seen"] - 1) < 0.03
    model.save_pretrained(ROOT / "final-model")
    pilot.save(
        "run.json",
        {
            "steps": trainer.state.global_step,
            "epochs_completed": trainer.state.epoch,
            "samples_seen": collator.count,
            "control_samples_seen": baseline["samples_seen"],
            "sample_order_hash": collator.digest.hexdigest(),
            "wall_seconds": perf_counter() - started,
            "peak_allocated_gib": torch.cuda.max_memory_allocated() / 2**30,
            "history": trainer.state.log_history,
        },
    )
    del trainer, model, collator
    gc.collect()
    torch.cuda.empty_cache()
    selection = json.loads((ROOT / "selection.json").read_text())
    paired.evaluate(
        "selected", ROOT / "best-model", selection["metrics"]["threshold"], test, fresh
    )
    curve = json.loads((ROOT / "curve.json").read_text())
    threshold = max(curve[-1]["grid"], key=lambda r: r["f1"])["threshold"]
    paired.evaluate("endpoint", ROOT / "final-model", threshold, test, fresh)


if __name__ == "__main__":
    main()
