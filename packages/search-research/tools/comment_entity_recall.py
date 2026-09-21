"""Describe gate/title recall tradeoffs on the frozen random 300.

Default analysis uses saved scores and requires no GPU. --infer runs the saved
reference model on melchior at a lower score floor before analysis. Operating
points are selected post hoc on this audit, not independently validated cutoffs.
"""

import argparse
import json
import sqlite3
from pathlib import Path

ROOT = Path("data/probes/books-gliner-recall-v1")
SOURCE = Path("data/probes/books-occurrence-audit-v1/reviewed-corrected.sqlite")
REFERENCE = Path("data/probes/books-gliner-training-v1/final-refit/model")
GOLD_SCORES = Path("data/probes/books-gliner-wsd-v1/linear/endpoint/fresh-scores.json")


def norm(text):
    return " ".join(text.casefold().split())


def reviewed():
    """Include unmatched titles in gold and deduplicate titles within each comment."""
    with sqlite3.connect(f"file:{SOURCE}?mode=ro", uri=True) as db:
        rows = []
        for cid, text, done in db.execute(
            "SELECT comment_id,text,reviewed FROM entity_items WHERE batch_id=2 ORDER BY comment_id"
        ):
            assert done
            titles = {
                norm(t[0])
                for t in db.execute(
                    "SELECT title FROM entity_labels WHERE batch_id=2 AND comment_id=? AND deleted=0",
                    (cid,),
                )
            }
            rows.append((cid, text, titles))
    assert len(rows) == 300
    return rows


def infer(rows):
    """Run a BF16 probe without changing services or model deployment."""
    import torch
    from gliner import GLiNER
    from search_research.comment_entities import text_windows

    model = GLiNER.from_pretrained(REFERENCE, load_tokenizer=True).to("cuda").eval()
    windows = [
        (cid, off, chunk)
        for cid, text, _ in rows
        for off, chunk in text_windows(model, text, ["book title"])
    ]
    scores = {str(cid): {} for cid, _, _ in rows}
    with torch.inference_mode(), torch.autocast("cuda", dtype=torch.bfloat16):
        output = model.inference(
            [w[2] for w in windows],
            ["book title"],
            batch_size=8,
            threshold=1e-6,
            flat_ner=False,
        )
    for (cid, off, _), entities in zip(windows, output, strict=True):
        for e in entities:
            span = (off + e["start"], off + e["end"])
            scores[str(cid)][span] = max(e["score"], scores[str(cid)].get(span, 0))
    result = {
        cid: [[a, b, float(s)] for (a, b), s in spans.items()]
        for cid, spans in scores.items()
    }
    (ROOT / "fresh-scores-floor.json").write_text(json.dumps(result) + "\n")


def metric(rows, scores, threshold):
    """Gate FPR uses negative comments; title precision uses emitted candidates."""
    tp = fp = fn = gtp = gfp = gfn = gtn = 0
    for cid, text, gold in rows:
        pred = {norm(text[a:b]) for a, b, s in scores[str(cid)] if s >= threshold}
        tp += len(gold & pred)
        fp += len(pred - gold)
        fn += len(gold - pred)
        gtp += bool(gold) and bool(pred)
        gfp += not gold and bool(pred)
        gfn += bool(gold) and not pred
        gtn += not gold and not pred
    return {
        "threshold": threshold,
        "title_tp": tp,
        "title_fp": fp,
        "title_fn": fn,
        "title_recall": tp / max(1, tp + fn),
        "title_precision": tp / max(1, tp + fp),
        "title_f1": 2 * tp / max(1, 2 * tp + fp + fn),
        "gate_tp": gtp,
        "gate_fp": gfp,
        "gate_fn": gfn,
        "gate_tn": gtn,
        "gate_recall": gtp / max(1, gtp + gfn),
        "gate_precision": gtp / max(1, gtp + gfp),
        "gate_fpr": gfp / max(1, gfp + gtn),
        "gate_f1": 2 * gtp / max(1, 2 * gtp + gfp + gfn),
        "passed": gtp + gfp,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--infer",
        action="store_true",
        help="Run low-floor GPU inference before analysis",
    )
    args = parser.parse_args()
    ROOT.mkdir(exist_ok=True)
    rows = reviewed()
    if args.infer:
        infer(rows)
    models = {}
    for name, path, floor, points in [
        ("previous", ROOT / "fresh-scores-floor.json", 1e-6, [0.5, 0.17, 1e-6]),
        ("gold-five-epoch", GOLD_SCORES, 0.05, [0.5, 0.06, 0.05]),
    ]:
        scores = json.loads(path.read_text())
        assert set(scores) == {str(cid) for cid, _, _ in rows}
        thresholds = sorted(
            {floor, 1.0} | {s for spans in scores.values() for _, _, s in spans},
            reverse=True,
        )
        curve = [metric(rows, scores, t) for t in thresholds]
        targets = {}
        for kind in ["gate", "title"]:
            targets[kind] = {
                str(target): next(
                    (r for r in curve if r[kind + "_recall"] >= target), None
                )
                for target in [0.9, 0.95, 0.99]
            }
        models[name] = {
            "score_floor": floor,
            "points": [metric(rows, scores, t) for t in points],
            "targets": targets,
            "curve": curve,
        }
    result = {
        "scope": "Post-hoc descriptive thresholds on fresh random 300. No deployment changes. Titles use normalized exact text per comment.",
        "comments": len(rows),
        "positive_comments": sum(bool(g) for _, _, g in rows),
        "title_references": sum(len(g) for _, _, g in rows),
        "models": models,
    }
    (ROOT / "analysis.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({name: r["points"] for name, r in models.items()}, indent=2))


if __name__ == "__main__":
    main()
