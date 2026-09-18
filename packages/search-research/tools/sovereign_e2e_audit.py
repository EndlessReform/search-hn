"""Audit terminal E2E traces and derive exposure efficiency without new API calls."""

import argparse
import json
from datetime import datetime
from pathlib import Path

import polars as pl
from search_research.dataset import read_jsonl
from search_research.semantic_efficiency import measure


def audit(root):
    """Verify frozen prompts, streamed billing, unique cases, and resolved charges."""
    manifest = json.loads((root / "manifest.json").read_text())
    rows, calls = [], []
    latest = {}
    for row in read_jsonl(root / "outcomes.jsonl"):
        if row["event"] == "outcome":
            latest[row["key"]] = row
    for arm in manifest["arms"]:
        dataset = {r["id"]: r for r in read_jsonl(root / arm / "eval.jsonl")}
        for path in sorted((root / arm / "trajectories").glob("*/*.jsonl")):
            events = read_jsonl(path)
            start, end = events[0], events[-1]
            assert (
                end["event"] == "complete"
                or end.get("error_type") == "MaxTurnsExceeded"
            )
            metadata = start["metadata"]
            assert (
                start["prompt"]
                == dataset[metadata["target_id"]]["questions"][
                    metadata["style"] + "_question"
                ]
            )
            assert metadata["source_sha256"] == manifest["source_sha256"]
            assert metadata["dataset_sha256"] == manifest["eval_sha256"]
            assert metadata["embedding_arm"] == arm
            assert start["model"] == manifest["model"]
            assert start["settings"]["extra_body"] == manifest["routing"]
            key = arm + "/" + metadata["case"]
            assert latest[key]["status"] in ("complete", "max_turns")
            assert latest[key]["trajectory"] == str(path)
            row = {"arm": arm, **measure(events), **latest[key]["retrieval"]}
            row["elapsed"] = end["elapsed"]
            rows.append(row)
            pending, billing = None, None
            for event in events:
                if event["event"] == "model_input":
                    assert pending is None
                    pending = event
                elif event["event"] == "billing":
                    billing = event
                elif event["event"] == "model_output":
                    assert pending is not None and billing is not None
                    assert event["response_id"] == billing["response_id"]
                    usage = billing["usage"]
                    calls.append(
                        {
                            "arm": arm,
                            "case": metadata["case"],
                            "response_id": event["response_id"],
                            "seconds": (
                                datetime.fromisoformat(event["at"])
                                - datetime.fromisoformat(pending["at"])
                            ).total_seconds(),
                            "input_tokens": usage["input_tokens"],
                            "output_tokens": usage["output_tokens"],
                            "cached_tokens": usage["input_tokens_details"][
                                "cached_tokens"
                            ],
                            "usd": usage["cost"],
                        }
                    )
                    pending, billing = None, None
            assert pending is None and billing is None
    frame = pl.DataFrame(rows)
    assert frame.select(pl.struct("arm", "case").n_unique()).item() == len(frame)
    frame.write_parquet(root / "efficiency.parquet")
    metrics = [
        "turns",
        "search_lists",
        "input_tokens",
        "output_tokens",
        "first_exposure_turn",
        "input_tokens_to_exposure",
        "turns_after_exposure",
        "elapsed",
        "embedding_requests",
        "embedding_cache_hits",
        "retrieval_ms",
    ]
    frame.group_by("arm").agg(
        pl.len().alias("n"),
        *[pl.col(c).mean().alias(c + "_mean") for c in metrics],
        pl.col("elapsed").median().alias("elapsed_median"),
        pl.col("elapsed").quantile(0.95).alias("elapsed_p95"),
    ).sort("arm").write_csv(root / "efficiency.csv")
    callframe = pl.DataFrame(calls)
    assert callframe["response_id"].n_unique() == len(callframe)
    callframe.write_parquet(root / "calls.parquet")
    callframe.group_by("arm").agg(
        pl.len().alias("requests"),
        pl.col("seconds").mean().alias("model_seconds_mean"),
        pl.col("seconds").median().alias("model_seconds_median"),
        pl.col("seconds").quantile(0.95).alias("model_seconds_p95"),
        *[
            pl.col(c).sum()
            for c in ("input_tokens", "cached_tokens", "output_tokens", "usd")
        ],
    ).sort("arm").write_csv(root / "model-efficiency.csv")
    result = {
        "terminal_sessions": len(frame),
        "streamed_billed_requests": len(callframe),
        "reported_usd": callframe["usd"].sum(),
        "outcome_statuses": dict(
            pl.DataFrame(list(latest.values())).group_by("status").len().iter_rows()
        ),
        "protocol_audit": "passed",
    }
    (root / "audit.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    audit(parser.parse_args().root)
