"""Streaming rate limits are retryable only before meaningful output escapes."""

import asyncio
from types import SimpleNamespace

import httpx
import pytest
from openai import APIError
from search_research.rate_retry import RateRetryModel


@pytest.mark.parametrize("partial", [False, True])
def test_generic_stream_error_recovery(partial):
    class Upstream:
        attempts = 0

        async def stream_response(self):
            self.attempts += 1
            if self.attempts == 1:
                if partial:
                    yield SimpleNamespace(type="response.output_text.delta")
                raise APIError(
                    "Rate limit reached. Please try again in 0ms.",
                    httpx.Request("POST", "https://example.invalid"),
                    body=None,
                )
            yield SimpleNamespace(type="response.completed")

    class Journal:
        def write(self, *args, **kwargs):
            pass

    upstream = Upstream()
    model = RateRetryModel(upstream, SimpleNamespace(pause_until=0, journal=Journal()))

    async def consume():
        return [event.type async for event in model.stream_response()]

    if partial:
        with pytest.raises(APIError):
            asyncio.run(consume())
        assert upstream.attempts == 1
    else:
        assert asyncio.run(consume()) == ["response.completed"]
        assert upstream.attempts == 2
