"""Gold-only batch/LR comparisons with equal sample exposure and cached scores."""

import gc
import hashlib
import json
import logging
from pathlib import Path
from time import perf_counter

import comment_entity_train_pilot as pilot
import comment_entity_train_runs as runs
import torch
from gliner.data_processing.collator import UniEncoderSpanDataCollator
from gliner.training import Trainer, TrainingArguments
from transformers import TrainerCallback, set_seed

ROOT = Path("data/probes/books-gliner-tuning-v1")
OLD = Path("data/probes/books-gliner-training-v1")


class FailSkipped(logging.Handler):
    def emit(self, record):
        if "Skipping batch" in record.getMessage():
            raise ValueError("Training skipped a batch; invalidate this run")


class CountCollator:
    """Count and hash the exact sample stream, including the partial last batch."""

    def __init__(self, model):
        self.inner = UniEncoderSpanDataCollator(
            model.config, data_processor=model.data_processor
        )
        self.count = 0
        self.digest = hashlib.sha256()

    def __call__(self, rows):
        for row in rows:
            self.count += 1
            self.digest.update(json.dumps(row, sort_keys=True).encode())
        return self.inner(rows)


def score_grid(model, dev):
    """One inference pass retains scored spans; thresholds need no new forwards."""
    model.eval()
    windows = [(r, offset, text) for r in dev for offset, text in r["windows"]]
    with torch.inference_mode(), torch.autocast("cuda", dtype=torch.bfloat16):
        output = model.inference(
            [w[2] for w in windows],
            pilot.LABELS,
            batch_size=8,
            threshold=0.05,
            flat_ner=False,
        )
    scores = {r["comment_id"]: {} for r in dev}
    for (row, offset, text), entities in zip(windows, output, strict=True):
        for entity in entities:
            a, b = offset + entity["start"], offset + entity["end"]
            assert row["text"][a:b] == entity["text"]
            scores[row["comment_id"]][(a, b)] = max(
                entity["score"], scores[row["comment_id"]].get((a, b), 0)
            )
    grid = []
    for threshold in [i / 20 for i in range(1, 20)]:
        tp = fp = fn = gtp = gfp = gfn = gtn = 0
        for row in dev:
            gold = set(row["spans"])
            pred = {
                span
                for span, score in scores[row["comment_id"]].items()
                if score >= threshold
            }
            tp += len(gold & pred)
            fp += len(pred - gold)
            fn += len(gold - pred)
            gtp += bool(gold) and bool(pred)
            gfp += not gold and bool(pred)
            gfn += bool(gold) and not pred
            gtn += not gold and not pred
        grid.append(
            {
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
        )
    return grid, {
        str(cid): [[a, b, float(score)] for (a, b), score in spans.items()]
        for cid, spans in scores.items()
    }


class Curve(TrainerCallback):
    def __init__(self, dev):
        self.dev = dev
        self.history = []

    def on_epoch_begin(self, args, state, control, **kwargs):
        torch.cuda.synchronize()
        self.started = perf_counter()

    def on_epoch_end(self, args, state, control, model, **kwargs):
        torch.cuda.synchronize()
        seconds = perf_counter() - self.started
        epoch = round(state.epoch)
        grid, scores = score_grid(model, self.dev)
        record = {
            "epoch": epoch,
            "step": state.global_step,
            "train_seconds": seconds,
            "grid": grid,
        }
        self.history.append(record)
        pilot.save("curve.json", self.history)
        pilot.save(f"scores-{epoch}.json", scores)
        print(
            "EPOCH",
            pilot.OUT.name,
            epoch,
            "default",
            grid[9],
            "best",
            max(grid, key=lambda r: r["f1"]),
            flush=True,
        )
        model.train()


def run(name, train, dev, seed, accum, epochs, mult):
    pilot.OUT = ROOT / name
    pilot.OUT.mkdir()
    model = runs.model_new()
    set_seed(seed)
    collator = CountCollator(model)
    callback = Curve(dev)
    args = TrainingArguments(
        output_dir=str(pilot.OUT / "trainer"),
        num_train_epochs=epochs,
        per_device_train_batch_size=8,
        gradient_accumulation_steps=accum,
        learning_rate=1e-5 * mult,
        others_lr=5e-5 * mult,
        weight_decay=0.01,
        others_weight_decay=0.01,
        warmup_ratio=0.1,
        bf16=True,
        save_strategy="no",
        logging_steps=10,
        remove_unused_columns=False,
        report_to="none",
        seed=seed,
        data_seed=seed,
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
    assert collator.count == len(train) * epochs, (collator.count, len(train) * epochs)
    pilot.save(
        "run.json",
        {
            "seed": seed,
            "effective_batch": 8 * accum,
            "epochs": epochs,
            "lr_multiplier": mult,
            "samples_seen": collator.count,
            "sample_order_hash": collator.digest.hexdigest(),
            "steps": trainer.state.global_step,
            "wall_seconds": perf_counter() - started,
            "peak_allocated_gib": torch.cuda.max_memory_allocated() / 2**30,
            "log_history": trainer.state.log_history,
        },
    )
    del trainer, model, collator
    gc.collect()
    torch.cuda.empty_cache()


def main():
    logging.getLogger("gliner.training.trainer").addHandler(FailSkipped())
    ROOT.mkdir(exist_ok=False)
    rows = pilot.snapshot()
    ids = set(json.loads((OLD / "development-ids.json").read_text()))
    dev = [{**r, "pilot_split": "test"} for r in rows if r["comment_id"] in ids]
    pilot.OUT = ROOT
    model = runs.model_new()
    dev, _ = pilot.align(model, dev)
    pilot.save(
        "dev-gold.json",
        [{"comment_id": r["comment_id"], "spans": r["spans"]} for r in dev],
    )
    del model
    gc.collect()
    torch.cuda.empty_cache()
    train = json.loads((OLD / "gold850/training.json").read_text())
    for seed in (pilot.SEED, pilot.SEED + 11):
        for accum in (2, 4):
            run(f"batch{8 * accum}-seed{seed}", train, dev, seed, accum, 5, 1)
        a = json.loads((ROOT / f"batch16-seed{seed}/run.json").read_text())
        b = json.loads((ROOT / f"batch32-seed{seed}/run.json").read_text())
        assert a["sample_order_hash"] == b["sample_order_hash"], (
            "Batch comparison changed sample order"
        )
    for mult in (0.5, 1, 2):
        run(f"lr{mult:g}", train, dev, pilot.SEED, 2, 3, mult)


if __name__ == "__main__":
    main()
