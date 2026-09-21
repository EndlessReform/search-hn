"""Paired linear/WSD run on corrected gold with immutable historical split IDs.

Both arms see identical samples, initialization, optimizer settings and evaluation
rules. Epoch and threshold selection use development only; test scores are saved
for the selected checkpoint and the prespecified five-epoch endpoint.
"""

import gc
import json
import logging
import math
import sqlite3
from pathlib import Path
from time import perf_counter

import comment_entity_train_pilot as pilot
import comment_entity_train_runs as runs
import comment_entity_tune as tune
import torch
from comment_entity_silver_run import THRESHOLDS, metrics
from gliner import GLiNER
from gliner.training import Trainer, TrainingArguments
from search_research.comment_entities import text_windows
from transformers import TrainerCallback, set_seed

ROOT = Path("data/probes/books-gliner-wsd-v1")
SOURCE = Path("data/probes/books-occurrence-audit-v1/reviewed-corrected.sqlite")
OLD = Path("data/probes/books-gliner-training-v1")
EPOCHS = 5


def prepare(model):
    """Reuse split membership, rebuilding spans from the corrected snapshot."""
    split = {
        r["comment_id"]: r["split"]
        for r in json.loads(
            Path("data/probes/books-gliner-training-pilot-v1/split.json").read_text()
        )
    }
    dev_ids = set(json.loads((OLD / "development-ids.json").read_text()))
    with sqlite3.connect(f"file:{SOURCE}?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        rows = [
            dict(r)
            for r in db.execute(
                "SELECT * FROM entity_items ORDER BY batch_id,comment_id"
            )
        ]
        for r in rows:
            assert r["reviewed"]
            r["entities"] = [
                dict(e)
                for e in db.execute(
                    "SELECT * FROM entity_labels WHERE batch_id=? AND comment_id=? AND deleted=0",
                    (r["batch_id"], r["comment_id"]),
                )
            ]
            r["pilot_split"] = split[r["comment_id"]] if r["batch_id"] == 1 else "fresh"
    assert len(rows) == 1600
    groups = {
        "train": [
            r
            for r in rows
            if r["pilot_split"] == "train" and r["comment_id"] not in dev_ids
        ],
        "dev": [
            {**r, "pilot_split": "test"} for r in rows if r["comment_id"] in dev_ids
        ],
        "test": [r for r in rows if r["pilot_split"] == "test"],
    }
    accepted, train = {}, None
    for name, group in groups.items():
        pilot.OUT = ROOT / name
        pilot.OUT.mkdir()
        accepted[name], windows = pilot.align(model, group)
        if name == "train":
            train = windows
    fresh = [r for r in rows if r["batch_id"] == 2]
    for r in fresh:
        r["titles"] = [e["title"] for e in r["entities"]]
        r["spans"] = []
        r["windows"] = list(text_windows(model, r["text"], pilot.LABELS))
    assert [len(accepted[k]) for k in ("train", "dev", "test")] == [838, 147, 299]
    assert len(train) == 841
    pilot.OUT = ROOT
    pilot.save(
        "manifest.json",
        {
            "source": str(SOURCE),
            "split_membership": "historical frozen IDs",
            "train_comments": 838,
            "train_windows": 841,
            "dev_comments": 147,
            "test_comments": 299,
            "fresh_comments": 300,
            "epochs": EPOCHS,
            "effective_batch": 32,
            "encoder_lr": 1e-5,
            "other_lr": 5e-5,
            "seed": pilot.SEED,
            "warmup_ratio": 0.1,
            "wsd_decay_ratio": 0.2,
            "wsd_decay_type": "linear",
        },
    )
    return train, accepted["dev"], accepted["test"], fresh


class Curve(TrainerCallback):
    """Cache dev predictions and retain the best checkpoint plus final endpoint."""

    def __init__(self, dev):
        self.dev = dev
        self.history = []
        self.best = -1

    def on_epoch_end(self, args, state, control, model, **kwargs):
        _, scores = tune.score_grid(model, self.dev)
        grid = [metrics(self.dev, scores, t) for t in THRESHOLDS]
        best = max(grid, key=lambda r: r["f1"])
        epoch = round(state.epoch)
        self.history.append({"epoch": epoch, "step": state.global_step, "grid": grid})
        pilot.save("curve.json", self.history)
        pilot.save(f"dev-scores-{epoch}.json", scores)
        if best["f1"] > self.best:
            self.best = best["f1"]
            model.save_pretrained(pilot.OUT / "best-model")
            pilot.save(
                "selection.json",
                {"epoch": epoch, "step": state.global_step, "metrics": best},
            )
        print(
            "EPOCH", pilot.OUT.name, epoch, "fixed", grid[9], "best", best, flush=True
        )
        model.train()


def train_arm(name, train, dev):
    """Only scheduler differs; exact sample-stream hashes must match afterwards."""
    pilot.OUT = ROOT / name
    pilot.OUT.mkdir()
    model = runs.model_new()
    set_seed(pilot.SEED)
    collator = tune.CountCollator(model)
    callback = Curve(dev)
    steps = math.ceil(math.ceil(len(train) / 8) / 4) * EPOCHS
    kwargs = (
        {"num_decay_steps": math.ceil(steps * 0.2), "decay_type": "linear"}
        if name == "wsd"
        else {}
    )
    args = TrainingArguments(
        output_dir=str(pilot.OUT / "trainer"),
        num_train_epochs=EPOCHS,
        per_device_train_batch_size=8,
        gradient_accumulation_steps=4,
        learning_rate=1e-5,
        others_lr=5e-5,
        weight_decay=0.01,
        others_weight_decay=0.01,
        lr_scheduler_type="warmup_stable_decay" if name == "wsd" else "linear",
        lr_scheduler_kwargs=kwargs,
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
    assert trainer.state.global_step == steps
    assert collator.count == len(train) * EPOCHS
    model.save_pretrained(pilot.OUT / "final-model")
    pilot.save(
        "run.json",
        {
            "steps": steps,
            "samples_seen": collator.count,
            "sample_order_hash": collator.digest.hexdigest(),
            "wall_seconds": perf_counter() - started,
            "peak_allocated_gib": torch.cuda.max_memory_allocated() / 2**30,
            "history": trainer.state.log_history,
        },
    )
    del trainer, model, collator
    gc.collect()
    torch.cuda.empty_cache()


def title_metrics(rows, scores, threshold):
    """Keep the prior fresh-300 unique-title metric, including unaligned gold."""

    def norm(s):
        return " ".join(s.casefold().split())

    tp = fp = fn = gtp = gfp = gfn = gtn = 0
    for r in rows:
        gold = {norm(t) for t in r["titles"]}
        pred = {
            norm(r["text"][a:b])
            for a, b, s in scores[str(r["comment_id"])]
            if s >= threshold
        }
        tp += len(gold & pred)
        fp += len(pred - gold)
        fn += len(gold - pred)
        gtp += bool(gold) and bool(pred)
        gfp += not gold and bool(pred)
        gfn += bool(gold) and not pred
        gtn += not gold and not pred
    return {
        "threshold": threshold,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": tp / max(1, tp + fp),
        "recall": tp / max(1, tp + fn),
        "f1": 2 * tp / max(1, 2 * tp + fp + fn),
        "gate_tp": gtp,
        "gate_fp": gfp,
        "gate_fn": gfn,
        "gate_tn": gtn,
    }


def evaluate(name, path, threshold, test, fresh):
    """Score frozen holdouts only after development selection has finished."""
    pilot.OUT = ROOT / name
    pilot.OUT.mkdir(exist_ok=True)
    model = GLiNER.from_pretrained(path, load_tokenizer=True).to("cuda").eval()
    results = {}
    for label, rows, scorer in [
        ("test", test, metrics),
        ("fresh", fresh, title_metrics),
    ]:
        _, scores = tune.score_grid(model, rows)
        pilot.save(f"{label}-scores.json", scores)
        results[label] = scorer(rows, scores, threshold)
        results[label + "-fixed"] = scorer(rows, scores, 0.5)
    pilot.save("evaluation.json", results)
    print("EVALUATION", name, results, flush=True)
    del model
    gc.collect()
    torch.cuda.empty_cache()


def main():
    ROOT.mkdir(exist_ok=False)
    logging.getLogger("gliner.training.trainer").addHandler(tune.FailSkipped())
    model = runs.model_new()
    train, dev, test, fresh = prepare(model)
    del model
    gc.collect()
    torch.cuda.empty_cache()
    for name in ("linear", "wsd"):
        train_arm(name, train, dev)
    a, b = [json.loads((ROOT / n / "run.json").read_text()) for n in ("linear", "wsd")]
    assert a["sample_order_hash"] == b["sample_order_hash"]
    for name in ("linear", "wsd"):
        selection = json.loads((ROOT / name / "selection.json").read_text())
        evaluate(
            name + "/selected",
            ROOT / name / "best-model",
            selection["metrics"]["threshold"],
            test,
            fresh,
        )
        curve = json.loads((ROOT / name / "curve.json").read_text())
        threshold = max(curve[-1]["grid"], key=lambda r: r["f1"])["threshold"]
        evaluate(
            name + "/endpoint", ROOT / name / "final-model", threshold, test, fresh
        )
    evaluate("previous", OLD / "final-refit/model", 0.5, test, fresh)


if __name__ == "__main__":
    main()
