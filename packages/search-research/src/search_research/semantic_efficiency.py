"""Paired efficiency of frozen Luna trajectories; no new inference or retrieval.

Reuse the report's successful-context exposure semantics. A turn is a successful
model response, not a search list (one tool call may batch many alternatives).
First exposure includes consuming the evidence, not merely returning a tool result.
"""

import json

import polars as pl

from search_research.dataset import read_jsonl
from search_research.report import evaluate
from search_research.semantic_snapshot import ROOT


def measure(events):
    """Locate the first successful model context that contains target evidence."""
    full = evaluate(events)
    first = None
    for index, event in enumerate(events):
        if event["event"] == "model_output":
            prefix = evaluate(events[: index + 1])
            if prefix["exposed"]:
                first = prefix
                break
    turns = full["model_requests"]
    row = {
        "case": full["case"],
        "style": full["style"],
        "exposed": full["exposed"],
        "completed": full["status"] == "complete",
        "turns": turns,
        "search_lists": full["query_count"],
        "input_tokens": full["input_tokens"],
        "output_tokens": full["output_tokens"],
        "first_exposure_turn": first["model_requests"] if first else None,
        "lists_consumed_at_exposure": first["query_count"] if first else None,
        "input_tokens_to_exposure": first["input_tokens"] if first else None,
        "turns_after_exposure": turns - first["model_requests"] if first else None,
        "completed_without_target": full["status"] == "complete"
        and not full["exposed"],
    }
    for k in (2, 3, 5, 8, 10):
        row[f"exposure_by_turn_{k}"] = (
            first is not None and first["model_requests"] <= k
        )
    return row


def main():
    rows = []
    for mode in ("dense", "hybrid"):
        for path in sorted((ROOT / mode / "trajectories/gpt-5.6-luna").glob("*.jsonl")):
            rows.append({"treatment": mode, **measure(read_jsonl(path))})
    frame = pl.DataFrame(rows)
    assert frame.select(pl.struct("treatment", "case").n_unique()).item() == 392
    assert len(frame) == 392
    frame.write_parquet(ROOT / "efficiency.parquet")
    d = frame.filter(pl.col("treatment") == "dense")
    h = frame.filter(pl.col("treatment") == "hybrid")
    paired = d.join(h, on="case", suffix="_hybrid")
    common = paired.filter(pl.col("exposed") & pl.col("exposed_hybrid"))["case"]
    metrics = [
        "turns",
        "search_lists",
        "input_tokens",
        "output_tokens",
        "first_exposure_turn",
        "lists_consumed_at_exposure",
        "input_tokens_to_exposure",
        "turns_after_exposure",
    ]
    summaries = []
    comparisons = []
    for cohort, selected in (
        ("all", frame),
        ("both_exposed", frame.filter(pl.col("case").is_in(common.implode()))),
    ):
        summary = (
            selected.group_by("treatment")
            .agg(
                pl.len().alias("n"),
                *[pl.col(m).mean().alias(m + "_mean") for m in metrics],
                *[pl.col(m).median().alias(m + "_median") for m in metrics],
                pl.col("completed_without_target").sum(),
                *[pl.col(f"exposure_by_turn_{k}").mean() for k in (2, 3, 5, 8, 10)],
            )
            .with_columns(pl.lit(cohort).alias("cohort"))
        )
        summaries.append(summary)
        pairs = (
            paired
            if cohort == "all"
            else paired.filter(pl.col("case").is_in(common.implode()))
        )
        for metric in metrics:
            valid = pairs.filter(
                pl.col(metric).is_not_null() & pl.col(metric + "_hybrid").is_not_null()
            )
            delta = valid[metric + "_hybrid"] - valid[metric]
            comparisons.append(
                {
                    "cohort": cohort,
                    "metric": metric,
                    "n": len(valid),
                    "dense_lower": (delta > 0).sum(),
                    "hybrid_lower": (delta < 0).sum(),
                    "tie": (delta == 0).sum(),
                    "hybrid_minus_dense_mean": delta.mean(),
                }
            )
    pl.concat(summaries).write_csv(ROOT / "efficiency-summary.csv")
    frame.group_by("treatment", "style").agg(
        pl.len().alias("n"),
        pl.col("exposed").sum().alias("hits"),
        pl.col("exposed").mean().alias("exposure_rate"),
        *[pl.col(m).mean().alias(m + "_mean") for m in metrics],
        pl.col("completed_without_target").sum(),
    ).sort("style", "treatment").write_csv(ROOT / "efficiency-strata.csv")
    pl.DataFrame(comparisons).write_csv(ROOT / "efficiency-paired.csv")
    print(json.dumps(pl.concat(summaries).to_dicts(), indent=2))
    print(json.dumps(comparisons, indent=2))


if __name__ == "__main__":
    main()
