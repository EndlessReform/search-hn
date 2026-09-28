"""Summarize useful selector batches without changing any model request body."""

import json
import statistics
import time
from collections import Counter


def distribution(values):
    """Describe observed values; an empty series is unavailable, not zero."""
    if not values:
        return None
    ordered = sorted(values)

    def quantile(p):
        position = (len(ordered) - 1) * p
        index = int(position)
        return ordered[index] + (
            ordered[min(index + 1, len(ordered) - 1)] - ordered[index]
        ) * (position - index)

    return {
        "n": len(values),
        "sum": sum(values),
        "min": min(values),
        "mean": statistics.mean(values),
        "p50": quantile(0.5),
        "p90": quantile(0.9),
        "p95": quantile(0.95),
        "p99": quantile(0.99),
        "max": max(values),
    }


class RunMetrics:
    """Measure dispatch-to-drain time, request service time, and journal commits.

    Readiness loading and request construction are excluded from throughput;
    queue drain and durable response commits are included. Record token lengths
    alongside latency so successive real-work batches can be compared fairly.
    No response text or request payload is duplicated into the metrics file.
    """

    def __init__(self, root, round_number, concurrency, label, target_ids):
        self.root = root
        self.round = round_number
        self.concurrency = concurrency
        self.label = label
        self.target_ids = set(target_ids)
        self.started_unix = time.time()
        self.started = time.monotonic()
        self.attempts = []

    def add(self, record, commit_seconds):
        response = record.get("response", {})
        usage = response.get("usage", {})
        self.attempts.append(
            {
                "id": record["id"],
                "attempt": record["attempt"],
                "success": "decision" in record,
                "http_status": record.get("http_status"),
                "error": record.get("error"),
                "error_type": record.get("error_type"),
                "service_seconds": record["timing"]["service_seconds"],
                "started_unix": record["timing"]["started_unix"],
                "commit_seconds": commit_seconds,
                "retry_after": record.get("retry_after"),
                "input_tokens": usage.get("prompt_tokens"),
                "output_tokens": usage.get("completion_tokens"),
                "reasoning_tokens": usage.get("completion_tokens_details", {}).get(
                    "reasoning_tokens"
                ),
                "cached_input_tokens": usage.get("prompt_tokens_details", {}).get(
                    "cached_tokens"
                ),
                "cost": usage.get("cost", 0) or 0,
            }
        )

    def finish(self, done, stop_reason):
        elapsed = time.monotonic() - self.started
        successful = [a for a in self.attempts if a["success"]]
        completed = len(self.target_ids & done)
        summary = {
            "label": self.label,
            "round": self.round,
            "concurrency": self.concurrency,
            "started_unix": self.started_unix,
            "finished_unix": time.time(),
            "seconds": elapsed,
            "target": len(self.target_ids),
            "completed": completed,
            "attempts": len(self.attempts),
            "successful_per_second": completed / elapsed if elapsed else 0,
            "cost": sum(a["cost"] for a in self.attempts),
            "stop_reason": stop_reason,
            "http_status_counts": dict(
                Counter(str(a["http_status"]) for a in self.attempts)
            ),
            "failed_attempts": len(self.attempts) - len(successful),
            "service_seconds": distribution([a["service_seconds"] for a in successful]),
            "commit_seconds": distribution(
                [a["commit_seconds"] for a in self.attempts]
            ),
            "tokens": {
                k: distribution([a[k] for a in successful if a[k] is not None])
                for k in (
                    "input_tokens",
                    "output_tokens",
                    "reasoning_tokens",
                    "cached_input_tokens",
                )
            },
            "requests": self.attempts,
        }
        target = self.root / f"selector-metrics-{self.label}.json"
        assert not target.exists(), f"Metrics label already exists: {self.label}"
        temporary = target.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(summary))
        temporary.replace(target)
        print(
            json.dumps({k: v for k, v in summary.items() if k != "requests"}),
            flush=True,
        )
        return summary
