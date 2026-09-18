"""Matched E2E quality, resource use, and story-cluster bootstrap differences."""

import argparse
import json
from itertools import combinations
from pathlib import Path

import numpy as np
import polars as pl

from search_research.dataset import read_jsonl
from search_research.report import report
from search_research.sovereign_repository import ARMS


def validate_traces(paths):
    """Reject unfinished, infrastructure, or duplicate attempts before aggregation."""
    cases = set()
    for path in paths:
        events = read_jsonl(path)
        assert events and (
            events[-1]["event"] == "complete"
            or events[-1].get("error_type") == "MaxTurnsExceeded"
        ), f"Nonterminal/infrastructure trace: {path}"
        case = events[0]["metadata"]["case"]
        assert case not in cases, f"Duplicate terminal trace: {case}"
        cases.add(case)


def validate_observations(frame, root):
    """Require one terminal observation per frozen case in every comparison arm.

    A partial run is useful operational progress, but silently changing its
    denominator would turn this final comparison into a biased scorecard.
    """
    expected = None
    assert set(frame["arm"]) == set(ARMS), "Final report requires every arm"
    for arm in ARMS:
        dataset = read_jsonl(root / arm / "eval.jsonl")
        cases = {
            f"{row['id']}-{style}"
            for row in dataset
            for style in ("entity", "paraphrase")
        }
        assert len(cases) == 2 * len(dataset), "Duplicate evaluation targets"
        if expected is None:
            expected = cases
        assert cases == expected, "Evaluation cases differ between arms"
        observations = frame.filter(pl.col("arm") == arm)
        assert observations.height == len(cases), f"Incomplete/duplicate arm: {arm}"
        assert set(observations["case"]) == cases, f"Case coverage differs: {arm}"


def summarize(root):
    """Keep infrastructure attempts outside quality denominators, visible in outcomes."""
    frames = []
    for arm in ARMS:
        paths = list((root / arm / "trajectories").glob("*/*.jsonl"))
        assert paths, f"No observations for arm: {arm}"
        validate_traces(paths)
        report(root / arm)
        frames.append(
            pl.read_parquet(root / arm / "metrics.parquet").with_columns(
                pl.lit(arm).alias("arm")
            )
        )
    assert frames, "No completed scientific observations"
    frame = pl.concat(frames)
    validate_observations(frame, root)
    frame.write_parquet(root / "metrics.parquet")
    summary = (
        frame.group_by("arm")
        .agg(
            pl.len().alias("n"),
            (pl.col("status") == "complete").sum().alias("completed"),
            pl.col("exposed").sum().alias("exposed_n"),
            pl.col("cited_evidence").sum().alias("cited_evidence_n"),
            *[
                pl.col(c).mean()
                for c in (
                    "exposed",
                    "cited_evidence",
                    "query_count",
                    "model_requests",
                    "elapsed",
                    "input_tokens",
                    "output_tokens",
                )
            ],
            pl.col("input_tokens").sum().alias("total_input_tokens"),
            pl.col("output_tokens").sum().alias("total_output_tokens"),
        )
        .sort("arm")
    )
    summary.write_csv(root / "summary.csv")
    print(summary)
    paired = []
    for left, right in combinations(ARMS, 2):
        joined = frame.filter(pl.col("arm") == left).join(
            frame.filter(pl.col("arm") == right), on="case", suffix="_right"
        )
        for metric in ("exposed", "cited_evidence"):
            clusters = (
                joined.with_columns(
                    (
                        pl.col(metric).cast(pl.Float64)
                        - pl.col(metric + "_right").cast(pl.Float64)
                    ).alias("difference")
                )
                .group_by("target_id")
                .agg(pl.col("difference").mean(), pl.len().alias("n"))
            )
            clusters = clusters.filter(pl.col("n") == 2)
            if not len(clusters):
                continue
            values = clusters.sort("target_id")["difference"].to_numpy()
            draws = (
                np.random.default_rng(20260905)
                .choice(values, (10000, len(values)))
                .mean(axis=1)
            )
            paired.append(
                {
                    "left": left,
                    "right": right,
                    "metric": metric,
                    "clusters": len(values),
                    "difference": float(values.mean()),
                    "ci_low": float(np.quantile(draws, 0.025)),
                    "ci_high": float(np.quantile(draws, 0.975)),
                    "left_only": joined.filter(
                        pl.col(metric) & ~pl.col(metric + "_right")
                    ).height,
                    "right_only": joined.filter(
                        ~pl.col(metric) & pl.col(metric + "_right")
                    ).height,
                }
            )
    if paired:
        pl.DataFrame(paired).write_csv(root / "paired.csv")
    charges = {}
    for row in read_jsonl(root / "budget.jsonl"):
        if row["event"] == "charge":
            charges[row["id"]] = row
    costs = pl.DataFrame(list(charges.values())).with_columns(
        pl.col("case").str.split("/").list.first().alias("arm")
    )
    costs.group_by("arm", "kind").agg(
        pl.col("usd").sum(), pl.len().alias("requests")
    ).write_csv(root / "costs.csv")
    (root / "summary.json").write_text(json.dumps(summary.to_dicts(), indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    summarize(parser.parse_args().root)
