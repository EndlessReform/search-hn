"""Retry pre-output streaming 429s, which HTTP-level SDK retries cannot handle."""

import asyncio
import re
import time

from agents.models.interface import Model, ModelProvider
from openai import APIError, RateLimitError


def is_rate_limit(error_type, message):
    """Streaming errors may lack the HTTP 429 subclass; do not retry other errors."""
    return error_type == "RateLimitError" or (
        error_type == "APIError" and "rate limit reached" in message.lower()
    )


class RateRetryModel(Model):
    """Preserve one trajectory/model request while waiting for real token capacity."""

    def __init__(self, model, budget):
        self.model = model
        self.budget = budget

    async def get_response(self, *args, **kwargs):
        return await self.model.get_response(*args, **kwargs)

    async def stream_response(self, *args, **kwargs):
        for attempt in range(10):
            await asyncio.sleep(max(0, self.budget.pause_until - time.monotonic()))
            meaningful = False
            try:
                async for event in self.model.stream_response(*args, **kwargs):
                    meaningful |= event.type in (
                        "response.output_item.added",
                        "response.output_text.delta",
                        "response.reasoning_summary_text.delta",
                    )
                    yield event
                return
            except APIError as exc:
                if not (
                    isinstance(exc, RateLimitError)
                    or is_rate_limit(type(exc).__name__, str(exc))
                ):
                    raise
                if meaningful or attempt == 9:
                    raise
                match = re.search(
                    r"try again in ([\d.]+)(ms|s)", str(exc), re.IGNORECASE
                )
                delay = (
                    float(match[1]) * (0.001 if match[2] == "ms" else 1)
                    if match
                    else 5.0
                )
                self.budget.pause_until = max(
                    self.budget.pause_until, time.monotonic() + delay + 0.5
                )
                self.budget.journal.write(
                    "stream_retry", attempt=attempt, seconds=delay + 0.5
                )


class RateRetryProvider(ModelProvider):
    """Wrap the configured provider without changing model name or instructions."""

    def __init__(self, provider, budget):
        self.provider = provider
        self.budget = budget

    def get_model(self, model_name):
        return RateRetryModel(self.provider.get_model(model_name), self.budget)
