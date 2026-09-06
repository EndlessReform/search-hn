# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx>=0.28,<1"]
# ///
"""Operator-only short load check: show native pooling priority overtakes queued bulk.

Run with uv run priority.py http://127.0.0.1:18080. This deliberately uses the raw
route to create a queue larger than proxy admission permits; never run in a client
integration or against a busy shared service without administrator authorization.
"""
import asyncio
import json
import sys
import time

import httpx


async def main():
    origin = sys.argv[1].rstrip('/')
    start = time.monotonic()
    events = []
    text = 'Database indexes use tree structures to locate matching records. ' * 100
    async with httpx.AsyncClient(timeout=60) as client:
        async def submit(name, count, priority):
            sent = time.monotonic() - start
            response = await client.post(f'{origin}/v1/embeddings', json={
                'model': 'pplx-embed-v1-0.6b', 'input': [text] * count,
                'encoding_format': 'float', 'priority': priority,
            })
            response.raise_for_status()
            result = {'name': name, 'sent_s': round(sent, 3),
                      'done_s': round(time.monotonic() - start, 3)}
            events.append(result)
            return result

        async def waiting():
            response = await client.get(f'{origin}/metrics')
            response.raise_for_status()
            return sum(float(line.split()[-1]) for line in response.text.splitlines()
                       if line.startswith('vllm:num_requests_waiting{'))

        assert await waiting() == 0, 'Engine already has queued work; run during a quiet interval'
        filler = asyncio.create_task(submit('bulk_filler', 128, 1))
        observed = 0
        for _ in range(200):
            observed = await waiting()
            if observed >= 16:
                break
            await asyncio.sleep(.01)
        assert observed >= 16, 'Did not establish a queue; no priority claim can be made'
        bulk = asyncio.create_task(submit('earlier_bulk_single', 1, 1))
        await asyncio.sleep(.1)
        query = asyncio.create_task(submit('later_interactive_single', 1, 0))
        await asyncio.gather(filler, bulk, query)
        assert query.result()['done_s'] < bulk.result()['done_s'], events
        assert bulk.result()['sent_s'] < query.result()['sent_s'], events
        print(json.dumps({'waiting_before_probe': observed, 'completion_order': events,
                          'interactive_overtook_bulk': True}, indent=2))


asyncio.run(main())
