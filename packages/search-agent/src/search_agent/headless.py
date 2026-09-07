"""Single-turn CLI and reusable durable runner over the production search core."""

import asyncio
import json
import time
from pathlib import Path

from search_agent.journal import Journal, JournalHooks
from search_agent.runtime import SearchRuntime


async def run_headless(
    runtime: SearchRuntime,
    prompt: str,
    path: Path,
    *,
    metadata: dict | None = None,
    timeout: float = 600,
    hooks_factory=JournalHooks,
):
    """Journal even failed/incomplete runs; metadata never enters model context.

    Each invocation is a fresh conversation. Tool calls that finish on the final
    allowed turn are recorded but do not count as model exposure unless a later
    model_input contains their payload. Errors remain distinct from retrieval misses.
    """
    runtime.context.repository.reset_session()
    journal = Journal(path)
    started = time.monotonic()
    result = None
    journal.write(
        "start",
        prompt=prompt,
        model=runtime.agent.model,
        base_url=runtime.base_url,
        max_turns=runtime.max_turns,
        settings=runtime.settings.to_json_dict(),
        metadata=metadata or {},
    )
    try:
        result = runtime.start(prompt, hooks=hooks_factory(journal))
        async with asyncio.timeout(timeout):
            async for event in result.stream_events():
                if event.type == "run_item_stream_event":
                    journal.write(
                        "run_item", name=event.name, item=event.item.to_input_item()
                    )
        final = result.final_output_as(str)
        journal.write("complete", final=final, elapsed=time.monotonic() - started)
        return final
    except BaseException as exc:
        if result is not None:
            result.cancel()
        journal.write(
            "error",
            error_type=type(exc).__name__,
            error=str(exc),
            elapsed=time.monotonic() - started,
        )
        raise
    finally:
        journal.close()


async def run_cli(args):
    import os
    from dotenv import load_dotenv
    from search_agent.cli import _resolve_startup_model
    from search_agent.model_config import load_model_config, ModelRuntime

    load_dotenv()
    config, selection = _resolve_startup_model(
        load_model_config(args.config),
        model_override=args.model or os.getenv("OPENAI_MODEL"),
        base_url_override=args.base_url or os.getenv("OPENAI_BASE_URL"),
    )
    models = ModelRuntime(
        config, selection, api_key_override=args.api_key, api=args.api
    )
    runtime = SearchRuntime(
        model=selection.model,
        base_url=models.provider.base_url,
        model_runtime=models,
        database_url=args.database_url,
        api_key=args.api_key,
        current_date=args.system_date,
        max_turns=args.max_turns,
        max_tokens=args.max_tokens,
        api=args.api,
        retrieval=args.retrieval,
        comments_database_url=args.comments_database_url,
    )
    try:
        output = await run_headless(
            runtime, args.prompt, Path(args.output), timeout=args.timeout
        )
        print(json.dumps({"final": output, "trajectory": args.output}))
    finally:
        await runtime.close()
