"""Add the fresh 3,000 DS rows, keeping all reviewed evaluation splits frozen."""

import gc
import json
import logging
import sqlite3
from pathlib import Path
from time import perf_counter

import comment_entity_train_pilot as pilot
import comment_entity_train_runs as runs
import comment_entity_tune as tune
import torch
from gliner import GLiNER
from gliner.training import Trainer, TrainingArguments
from search_research.comment_entities import text_windows
from search_research.comment_entity_annotations import locate_title
from transformers import TrainerCallback, set_seed

ROOT = Path("data/probes/books-gliner-silver3000-v1")
SILVER = Path("data/probes/books-silver3000-v1")
OLD = Path("data/probes/books-gliner-training-v1")
THRESHOLDS = [i / 20 for i in range(1, 20)] + [0.975, 0.99, 0.995, 0.999]


def metrics(rows, scores, t):
    tp = fp = fn = gtp = gfp = gfn = gtn = 0
    for r in rows:
        gold = set(r["spans"])
        pred = {(a, b) for a, b, s in scores[str(r["comment_id"])] if s >= t}
        tp += len(gold & pred)
        fp += len(pred - gold)
        fn += len(gold - pred)
        gtp += bool(gold) and bool(pred)
        gfp += not gold and bool(pred)
        gfn += bool(gold) and not pred
        gtn += not gold and not pred
    return {
        "threshold": t,
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


class Curve(TrainerCallback):
    def __init__(self, dev, collator):
        self.dev = dev
        self.collator = collator
        self.history = []
        self.best = -1
        self.started = perf_counter()

    def measure(self, state, model, label):
        _, scores = tune.score_grid(model, self.dev)
        grid = [metrics(self.dev, scores, t) for t in THRESHOLDS]
        best = max(grid, key=lambda r: r["f1"])
        record = {
            "label": label,
            "epoch": state.epoch,
            "step": state.global_step,
            "samples_seen": self.collator.count,
            "seconds": perf_counter() - self.started,
            "grid": grid,
        }
        self.history.append(record)
        pilot.save("curve.json", self.history)
        pilot.save(f"scores-{label}.json", scores)
        if best["f1"] > self.best:
            self.best = best["f1"]
            model.save_pretrained(ROOT / "best-model")
            pilot.save(
                "selection.json",
                {
                    "label": label,
                    "epoch": state.epoch,
                    "step": state.global_step,
                    "metrics": best,
                    "selection": "Development span F1 only; earliest measured checkpoint and lowest threshold on ties",
                },
            )
        print("CHECKPOINT", label, "default", grid[9], "best", best, flush=True)
        model.train()

    def on_step_end(self, args, state, control, model, **kwargs):
        if state.global_step == 135:
            self.measure(state, model, "step135")

    def on_epoch_end(self, args, state, control, model, **kwargs):
        self.measure(state, model, f"epoch{round(state.epoch)}")


def main():
    logging.getLogger("gliner.training.trainer").addHandler(tune.FailSkipped())
    rows = pilot.snapshot()
    ROOT.mkdir(exist_ok=False)
    model = runs.model_new()
    dev_ids = set(json.loads((OLD / "development-ids.json").read_text()))
    pilot.OUT = ROOT / "dev"
    pilot.OUT.mkdir()
    dev, _ = pilot.align(
        model,
        [{**r, "pilot_split": "test"} for r in rows if r["comment_id"] in dev_ids],
    )
    pilot.OUT = ROOT / "test"
    pilot.OUT.mkdir()
    test, _ = pilot.align(model, [r for r in rows if r["pilot_split"] == "test"])
    frozen = Path("data/probes/books-annotation-random300-v2/reviewed.sqlite")
    with sqlite3.connect(frozen) as db:
        db.row_factory = sqlite3.Row
        fresh = [
            dict(r) for r in db.execute("select * from entity_items where batch_id=2")
        ]
        for r in fresh:
            assert r["reviewed"]
            r["titles"] = [
                e[0]
                for e in db.execute(
                    "select title from entity_labels where batch_id=2 and comment_id=? and deleted=0",
                    (r["comment_id"],),
                )
            ]
            r["spans"] = []
            r["windows"] = list(text_windows(model, r["text"], pilot.LABELS))
    forbidden = {r["comment_id"] for r in rows + fresh}
    texts = {" ".join(r["text"].casefold().split()) for r in rows + fresh}
    inputs = {r["comment_id"]: r for r in runs.read_lines(SILVER / "input.jsonl")}
    silver = []
    for r in runs.read_lines(SILVER / "deepseek/responses.jsonl"):
        assert r["status"] == 200 and not r.get("error")
        cid = r["comment_id"]
        text = inputs[cid]["text"]
        assert cid not in forbidden and " ".join(text.casefold().split()) not in texts
        entities = []
        for book in r["extraction"]["books"]:
            for a, b in locate_title(text, book["title"]) or [(None, None)]:
                entities.append(book | {"start": a, "end": b})
        silver.append(
            {
                "comment_id": cid,
                "text": text,
                "entities": entities,
                "pilot_split": "train",
            }
        )
    assert len(silver) == 3000
    pilot.OUT = ROOT / "silver"
    pilot.OUT.mkdir()
    accepted, extra = pilot.align(model, silver)
    gold = json.loads((OLD / "gold850/training.json").read_text())
    train = gold + extra
    pilot.OUT = ROOT
    pilot.save(
        "manifest.json",
        {
            "gold_windows": len(gold),
            "silver_comments": len(accepted),
            "silver_windows": len(extra),
            "total_windows": len(train),
            "scheduler": "linear",
            "warmup_ratio": 0.1,
            "seed": pilot.SEED,
            "epochs": 5,
            "effective_batch": 32,
        },
    )
    set_seed(pilot.SEED)
    collator = tune.CountCollator(model)
    callback = Curve(dev, collator)
    args = TrainingArguments(
        output_dir=str(ROOT / "trainer"),
        num_train_epochs=5,
        per_device_train_batch_size=8,
        gradient_accumulation_steps=4,
        learning_rate=1e-5,
        others_lr=5e-5,
        lr_scheduler_type="linear",
        warmup_ratio=0.1,
        weight_decay=0.01,
        others_weight_decay=0.01,
        bf16=True,
        save_strategy="no",
        logging_steps=20,
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
    assert collator.count == len(train) * 5
    pilot.save(
        "run.json",
        {
            "wall_seconds": perf_counter() - started,
            "steps": trainer.state.global_step,
            "samples_seen": collator.count,
            "peak_allocated_gib": torch.cuda.max_memory_allocated() / 2**30,
            "history": trainer.state.log_history,
        },
    )
    del trainer, model
    gc.collect()
    torch.cuda.empty_cache()
    selection = json.loads((ROOT / "selection.json").read_text())
    threshold = selection["metrics"]["threshold"]
    model = (
        GLiNER.from_pretrained(ROOT / "best-model", load_tokenizer=True)
        .to("cuda")
        .eval()
    )
    _, scores = tune.score_grid(model, test)
    pilot.save("test-scores.json", scores)
    pilot.save("test.json", metrics(test, scores, threshold))
    _, scores = tune.score_grid(model, fresh)
    pilot.save("fresh-scores.json", scores)

    def norm(s):
        return " ".join(s.casefold().split())

    tp = fp = fn = gtp = gfp = gfn = gtn = 0
    for r in fresh:
        gold = {norm(s) for s in r["titles"]}
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
    result = {
        "threshold": threshold,
        "title_tp": tp,
        "title_fp": fp,
        "title_fn": fn,
        "precision": tp / max(1, tp + fp),
        "recall": tp / max(1, tp + fn),
        "f1": 2 * tp / max(1, 2 * tp + fp + fn),
        "gate_tp": gtp,
        "gate_fp": gfp,
        "gate_fn": gfn,
        "gate_tn": gtn,
    }
    pilot.save("fresh-test.json", result)
    print("FRESH TEST", result, flush=True)


if __name__ == "__main__":
    main()
