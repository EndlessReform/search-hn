"""Aggregate the four bounded Luna reviews, checking every emitted title span."""

import json
from pathlib import Path

from pydantic import BaseModel

ROOT = Path("data/probes/books-gliner-v1")
GROUPS = ["positive_1", "positive_2", "negative_1", "negative_2"]


class Review(BaseModel):
    comment_id: int
    explicit_titles: list[str]
    has_explicit_title: bool
    current_valid_titles: list[str]
    current_invalid_titles: list[str]
    current_missed_titles: list[str]
    alternate_valid_titles: list[str]
    alternate_invalid_titles: list[str]
    alternate_missed_titles: list[str]
    notes: str


def rows(path):
    with path.open() as stream:
        return [json.loads(line) for line in stream]


def summarize(records, variant):
    """Report comment gates separately from clean title-name coverage."""
    field = "predictions" if variant == "current" else "book_title_predictions"
    label = "book" if variant == "current" else "book title"
    tp = fn = fp = tn = valid_comments = complete = clean = gold_names = (
        missed_names
    ) = valid_spans = invalid_spans = 0
    gates = {
        str(t): {"tp": 0, "fp": 0, "fn": 0, "tn": 0}
        for t in [0.35, 0.5, 0.7, 0.85, 0.95]
    }
    for source, review in records:
        valid = review[f"{variant}_valid_titles"]
        invalid = review[f"{variant}_invalid_titles"]
        missed = review[f"{variant}_missed_titles"]
        predicted = [s for s in source[field] if s["label"] == label]
        assert {s["text"] for s in predicted} == set(valid) | set(invalid), (
            source["comment_id"],
            variant,
        )
        assert not set(valid) & set(invalid)
        truth = review["has_explicit_title"]
        emitted = bool(predicted)
        tp += truth and emitted
        fn += truth and not emitted
        fp += not truth and emitted
        tn += not truth and not emitted
        valid_comments += bool(valid)
        complete += truth and not missed
        clean += truth and not missed and not invalid
        gold_names += len(review["explicit_titles"])
        missed_names += len(missed)
        valid_spans += sum(s["text"] in valid for s in predicted)
        invalid_spans += sum(s["text"] in invalid for s in predicted)
        for threshold, counts in gates.items():
            positive = any(s["score"] >= float(threshold) for s in predicted)
            counts[
                "tp"
                if truth and positive
                else "fn"
                if truth
                else "fp"
                if positive
                else "tn"
            ] += 1
    return {
        "comments": len(records),
        "has_explicit_title": sum(r["has_explicit_title"] for _, r in records),
        "gate": {"tp": tp, "fn": fn, "fp": fp, "tn": tn},
        "comments_with_valid_extraction": valid_comments,
        "all_named_titles_found": complete,
        "all_named_titles_found_without_extra_title_spans": clean,
        "unique_titles_per_comment": gold_names,
        "missed_title_names": missed_names,
        "valid_emitted_spans": valid_spans,
        "invalid_emitted_spans": invalid_spans,
        "higher_threshold_gates": gates,
    }


def main():
    records = []
    for group in GROUPS:
        sources = {r["comment_id"]: r for r in rows(ROOT / f"{group}.jsonl")}
        reviews = [
            Review.model_validate(r).model_dump()
            for r in rows(ROOT / f"{group}_review.jsonl")
        ]
        assert len(sources) == len(reviews) == 16
        assert set(sources) == {r["comment_id"] for r in reviews}
        for review in reviews:
            source = sources[review["comment_id"]]
            assert review["has_explicit_title"] == bool(review["explicit_titles"])
            assert all(t in source["text"] for t in review["explicit_titles"])
            records.append((source | {"group": group}, review))
    output = {}
    for name, predicate in [
        ("all", lambda s: True),
        ("luna_positive", lambda s: s["group"].startswith("positive")),
        ("luna_negative", lambda s: s["group"].startswith("negative")),
    ]:
        subset = [(s, r) for s, r in records if predicate(s)]
        output[name] = {
            variant: summarize(subset, variant) for variant in ["current", "alternate"]
        }
    (ROOT / "audit-summary.json").write_text(json.dumps(output, indent=2))
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
