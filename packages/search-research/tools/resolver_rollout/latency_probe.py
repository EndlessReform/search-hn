"""Measure stream TTFT and completion latency on identical saved selector inputs.

Native currently exposes V4.1 Flash; keep that version difference explicit.
Diagnostic results are separate from the calibration labels.
"""

import asyncio
import json
import os
import time
from pathlib import Path

import httpx
from common import RUN, connect
from dotenv import load_dotenv


async def main():
    load_dotenv(Path.cwd() / ".env")
    db = connect()
    rows = db.execute(
        "SELECT s.id,s.payload FROM selections s JOIN refs r ON r.id=s.id WHERE s.model='deepseek' ORDER BY r.ordinal LIMIT 6"
    ).fetchall()
    out = RUN / "latency-diagnosis.jsonl"
    assert not out.exists()
    async with httpx.AsyncClient(
        timeout=240, limits=httpx.Limits(max_connections=24)
    ) as client:

        async def one(route, ident, saved):
            body = json.loads(saved)["request"]
            body["stream"] = True
            body["stream_options"] = {"include_usage": True}
            if route == "native-v4.1":
                endpoint = "https://api.deepseek.com/chat/completions"
                key = os.environ["DEEPSEEK_API_KEY"]
                body.pop("provider")
                body.pop("reasoning")
                body.update(
                    model="deepseek-flash",
                    reasoning_effort="low",
                    thinking={"type": "enabled"},
                    response_format={"type": "json_object"},
                )
                body["messages"][0]["content"] += (
                    "\nReturn JSON with exactly one key, work_id (string or null)."
                )
            else:
                endpoint = "https://openrouter.ai/api/v1/chat/completions"
                key = os.environ["OPENROUTER_API_KEY"]
                body["provider"]["only"] = [route]
            receipt = {
                "route": route,
                "id": ident,
                "endpoint": endpoint,
                "request": body,
                "events": [],
            }
            start = time.monotonic()
            try:
                async with client.stream(
                    "POST",
                    endpoint,
                    headers={"Authorization": "Bearer " + key},
                    json=body,
                ) as resp:
                    receipt["headers_seconds"] = time.monotonic() - start
                    receipt["http_status"] = resp.status_code
                    if resp.status_code != 200:
                        receipt["error"] = (await resp.aread()).decode()
                    else:
                        async for line in resp.aiter_lines():
                            if (
                                not line.startswith("data:")
                                or line[5:].strip() == "[DONE]"
                            ):
                                continue
                            data = json.loads(line[5:])
                            elapsed = time.monotonic() - start
                            receipt["events"].append({"seconds": elapsed, "data": data})
                            for choice in data.get("choices", []):
                                d = choice.get("delta", {})
                                if (
                                    d.get("reasoning")
                                    or d.get("reasoning_content")
                                    or d.get("content")
                                ):
                                    receipt.setdefault("first_token_seconds", elapsed)
                                if d.get("content"):
                                    receipt.setdefault("first_answer_seconds", elapsed)
                                if choice.get("finish_reason"):
                                    receipt["finish_reason"] = choice["finish_reason"]
                            if data.get("usage"):
                                receipt["usage"] = data["usage"]
            except (httpx.HTTPError, ValueError) as exc:
                receipt["error"] = repr(exc)
            receipt["total_seconds"] = time.monotonic() - start
            usage = receipt.get("usage", {})
            decode = receipt["total_seconds"] - receipt.get(
                "first_token_seconds", receipt["total_seconds"]
            )
            if decode > 0 and usage.get("completion_tokens"):
                receipt["output_tokens_per_second"] = (
                    usage["completion_tokens"] / decode
                )
            with out.open("a") as f:
                f.write(json.dumps(receipt) + "\n")
            print(
                json.dumps(
                    {k: v for k, v in receipt.items() if k not in ["request", "events"]}
                ),
                flush=True,
            )

        await asyncio.gather(
            *(
                one(route, i, s)
                for route in ["open-inference", "streamlake", "native-v4.1"]
                for i, s in rows
            )
        )


if __name__ == "__main__":
    asyncio.run(main())
