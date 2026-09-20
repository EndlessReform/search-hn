"""Gold/DeepSeek learning curves and paired fixed-three-epoch experiments.

The pilot split and annotation snapshot are immutable inputs. Development picks
the mix and epoch; test is used only for the predetermined pair and final refit.
"""

import gc
import json
import random
from pathlib import Path
from time import perf_counter

import comment_entity_train_pilot as pilot
import torch
from gliner import GLiNER
from gliner.data_processing.collator import UniEncoderSpanDataCollator
from gliner.training import Trainer, TrainingArguments
from search_research.comment_entity_annotations import locate_title
from transformers import TrainerCallback, set_seed

ROOT = Path("data/probes/books-gliner-training-v1")
API = Path("data/probes/books-api-bakeoff-v1")


def read_lines(path):
    with path.open() as stream:
        return [json.loads(line) for line in stream]


def model_new():
    """Identical initialization and encoder checkpointing for every run."""
    set_seed(pilot.SEED)
    model = GLiNER.from_pretrained(
        pilot.MODEL_ID, max_width=32, load_tokenizer=True
    ).to("cuda")
    model.model.token_rep_layer.bert_layer.model.gradient_checkpointing_enable(
        gradient_checkpointing_kwargs={"use_reentrant": False}
    )
    return model


def prepare():
    """Freeze development membership before exclusions and select DS-only silver."""
    rows = pilot.snapshot()
    ROOT.mkdir(parents=True, exist_ok=False)
    pilot.OUT = ROOT
    rng = random.Random(pilot.SEED + 1)
    dev_ids = set()
    for positive, count in ((True, 53), (False, 97)):
        candidates = [
            r
            for r in rows
            if r["pilot_split"] == "train" and bool(r["entities"]) == positive
        ]
        rng.shuffle(candidates)
        dev_ids.update(r["comment_id"] for r in candidates[:count])
    pilot.save("development-ids.json", sorted(dev_ids))
    gold_ids = {r["comment_id"] for r in rows}
    gold_text = {r["text"] for r in rows}
    texts = {
        r["comment_id"]: r["text"]
        for name in ("throughput.jsonl", "sustain4096.jsonl")
        for r in read_lines(API / name)
    }
    silver = []
    seen_text = set(gold_text)
    for run in ("deepseek-low-c64", "deepseek-low-sustain-c64"):
        for row in read_lines(API / run / "responses.jsonl"):
            cid = row["comment_id"]
            if cid in gold_ids:
                continue
            assert row["status"] == 200 and row["extraction"] is not None
            text = texts[cid]
            if text in seen_text:
                continue
            seen_text.add(text)
            entities = []
            for book in row["extraction"]["books"]:
                matches = locate_title(text, book["title"])
                for left, right in matches or [(None, None)]:
                    entities.append(
                        {"title": book["title"], "start": left, "end": right}
                    )
            silver.append(
                {
                    "comment_id": cid,
                    "text": text,
                    "entities": entities,
                    "pilot_split": "train",
                }
            )
    positive = [r for r in silver if r["entities"]]
    negative = [r for r in silver if not r["entities"]]
    rng.shuffle(negative)
    # The agreed 711 negatives are held fixed across subset training and refit.
    silver = positive + negative[:711]
    pilot.save(
        "silver-selection.json",
        [
            {"comment_id": r["comment_id"], "positive": bool(r["entities"])}
            for r in silver
        ],
    )
    model = model_new()
    datasets = {}
    evaluation = {}
    for name, subset in (
        ("gold1000", rows),
        ("gold850", [r for r in rows if r["comment_id"] not in dev_ids]),
        ("silver", silver),
        (
            "dev",
            [{**r, "pilot_split": "test"} for r in rows if r["comment_id"] in dev_ids],
        ),
    ):
        pilot.OUT = ROOT / name
        pilot.OUT.mkdir()
        accepted, training = pilot.align(model, subset)
        datasets[name] = training
        evaluation[name] = accepted
    datasets["gold_ds"] = datasets["gold850"] + datasets["silver"]
    test = [r for r in evaluation["gold1000"] if r["pilot_split"] == "test"]
    dev = evaluation["dev"]
    pilot.OUT = ROOT
    pilot.score(model, dev, "baseline-dev")
    pilot.score(model, test, "baseline-test")
    del model
    gc.collect()
    torch.cuda.empty_cache()
    return datasets, dev, test


class Curve(TrainerCallback):
    """Score development after each epoch, restoring training mode afterward."""

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
        if self.dev:
            pilot.score(model, self.dev, f"dev-epoch-{epoch}")
            result = json.loads((pilot.OUT / f"dev-epoch-{epoch}.json").read_text())
        else:
            result = {}
        result.update(
            epoch=epoch,
            train_seconds=seconds,
            peak_allocated_gib=torch.cuda.max_memory_allocated() / 2**30,
        )
        self.history.append(result)
        pilot.save("curve.json", self.history)
        model.train()
        print("EPOCH", result, flush=True)


def run(name, training, epochs, dev=None, test=None):
    """Run a fixed schedule; never feed test scores into training decisions."""
    pilot.OUT = ROOT / name
    pilot.OUT.mkdir()
    model = model_new()
    callback = Curve(dev)
    args = TrainingArguments(
        output_dir=str(pilot.OUT / "trainer"),
        num_train_epochs=epochs,
        per_device_train_batch_size=8,
        gradient_accumulation_steps=2,
        learning_rate=1e-5,
        others_lr=5e-5,
        weight_decay=0.01,
        others_weight_decay=0.01,
        warmup_ratio=0.1,
        bf16=True,
        save_strategy="no",
        logging_steps=10,
        remove_unused_columns=False,
        report_to="none",
        seed=pilot.SEED,
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
        callbacks=[callback],
    )
    torch.cuda.reset_peak_memory_stats()
    started = perf_counter()
    trainer.train()
    pilot.save(
        "run.json",
        {
            "epochs": epochs,
            "windows": len(training),
            "wall_seconds": perf_counter() - started,
            "peak_allocated_gib": torch.cuda.max_memory_allocated() / 2**30,
            "history": trainer.state.log_history,
        },
    )
    if test:
        pilot.score(model, test, "test")
    if name == "final-refit":
        model.save_pretrained(pilot.OUT / "model")
    history = callback.history
    del trainer, model
    gc.collect()
    torch.cuda.empty_cache()
    return history


def main():
    datasets, dev, test = prepare()
    curves = {}
    curves["gold"] = run("gold850-10", datasets["gold850"], 10, dev=dev)
    curves["gold_ds"] = run("gold-ds-10", datasets["gold_ds"], 10, dev=dev)
    run("gold850-3", datasets["gold850"], 3, test=test)
    run("gold1000-3", datasets["gold1000"], 3, test=test)
    # Prefer the earlier epoch and gold-only when development F1 ties.
    choices = [
        (r["f1"], -r["epoch"], mix == "gold", mix, r["epoch"])
        for mix, curve in curves.items()
        for r in curve
    ]
    _, _, _, mix, epochs = max(choices)
    pilot.OUT = ROOT
    pilot.save(
        "selection.json",
        {
            "mix": mix,
            "epochs": epochs,
            "criterion": "development exact-span F1 at threshold 0.5; earlier epoch, then gold on ties",
        },
    )
    training = datasets["gold1000"] + (datasets["silver"] if mix == "gold_ds" else [])
    run("final-refit", training, epochs, test=test)


if __name__ == "__main__":
    main()
