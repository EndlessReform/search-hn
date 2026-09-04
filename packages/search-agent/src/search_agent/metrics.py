"""Turn-level performance metrics for the search agent."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from agents import ModelResponse


@dataclass(frozen=True)
class _TurnMetrics:
    """Renderer-neutral summary of one completed agent turn."""

    elapsed_seconds: float
    conversation_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    cached_tokens: int | None = None
    reasoning_tokens: int | None = None


def _collect_turn_metrics(
    raw_responses: Sequence[ModelResponse],
    *,
    elapsed_seconds: float,
) -> _TurnMetrics:
    """Collect elapsed time and best-effort usage details for one turn.

    We intentionally use the *last* model response in the run because that
    final request sees the most complete conversation state after any tool
    calls. If a provider omits usage, we still report elapsed time.
    """

    if not raw_responses:
        return _TurnMetrics(elapsed_seconds=elapsed_seconds)

    last_response = raw_responses[-1]
    usage = last_response.usage
    cached_tokens = usage.input_tokens_details.cached_tokens
    reasoning_tokens = usage.output_tokens_details.reasoning_tokens

    return _TurnMetrics(
        elapsed_seconds=elapsed_seconds,
        conversation_tokens=usage.input_tokens if usage.input_tokens > 0 else None,
        output_tokens=usage.output_tokens if usage.output_tokens > 0 else None,
        total_tokens=usage.total_tokens if usage.total_tokens > 0 else None,
        cached_tokens=cached_tokens if cached_tokens > 0 else None,
        reasoning_tokens=reasoning_tokens if reasoning_tokens > 0 else None,
    )


def _format_turn_metrics(metrics: _TurnMetrics) -> str:
    """Format a concise verbose-only status line for one completed turn.

    Example output::

        turn 8.21s | context 4,231 tok | output 312 | reasoning 50 | total 4,543
    """

    parts = [f"turn {metrics.elapsed_seconds:.2f}s"]
    if metrics.conversation_tokens is not None:
        parts.append(f"context {metrics.conversation_tokens:,} tok")
    if metrics.cached_tokens is not None:
        parts.append(f"cached {metrics.cached_tokens:,}")
    if metrics.output_tokens is not None:
        parts.append(f"output {metrics.output_tokens:,}")
    if metrics.reasoning_tokens is not None:
        parts.append(f"reasoning {metrics.reasoning_tokens:,}")
    if metrics.total_tokens is not None:
        parts.append(f"total {metrics.total_tokens:,}")
    return " | ".join(parts)
