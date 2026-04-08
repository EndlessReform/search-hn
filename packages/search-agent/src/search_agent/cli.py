"""CLI entry point for search-agent."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence

from agents import (
    Agent,
    RunContextWrapper,
    set_default_openai_client,
    set_tracing_disabled,
    run_demo_loop,
)
from openai import AsyncOpenAI

from search_agent.runtime_context import (
    SearchAgentContext,
    build_search_agent_context,
    dispose_search_agent_context,
)
from search_agent.tools import fetch_stories, fetch_top_comments


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI flags for the interactive agent loop."""

    parser = argparse.ArgumentParser(description="Example agent")
    parser.add_argument(
        "--model",
        type=str,
        default="Qwen-3.5-35B-A3B",
        help="Model to use (default: Qwen-3.5-35B-A3B)",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default="http://melchior-1:5000/v1",
        help="Base URL for the API (default: http://melchior-1:5000/v1)",
    )
    parser.add_argument(
        "--database-url",
        type=str,
        default=None,
        help=(
            "Optional database URL override. If omitted, uses DATABASE_URL "
            "from environment/.env."
        ),
    )
    return parser.parse_args(argv)


def _agent_instructions(
    ctx: RunContextWrapper[SearchAgentContext],
    _agent: Agent[SearchAgentContext],
) -> str:
    """Provide stable, context-aware instructions for the learning loop.

    This callable shape is intentionally used to demonstrate that the same
    `RunContextWrapper` mechanism used by tools is also available for dynamic
    instructions and other runtime hooks.
    """

    _ = ctx
    return (
        "You are a helpful assistant answering questions from mirrored Hacker News data. "
        "Use fetch_stories for story lookup and fetch_top_comments for top-level comments of a known story."
    )


async def _run(args: argparse.Namespace) -> None:
    """Run the REPL loop with one shared, persistent repository context."""

    context = build_search_agent_context(args.database_url)

    agent: Agent[SearchAgentContext] = Agent(
        name="Hacker News Research Assistant",
        instructions=_agent_instructions,
        model=args.model,
        tools=[fetch_stories, fetch_top_comments],
    )

    custom_client = AsyncOpenAI(
        base_url=args.base_url,
        api_key="dummy_key_or_vertex_token",  # TODO make this configurable if using proprietary backend
    )

    set_default_openai_client(custom_client)
    set_tracing_disabled(True)

    try:
        await run_demo_loop(agent, context=context)
    finally:
        dispose_search_agent_context(context)


def main(argv: Sequence[str] | None = None) -> int:
    """Synchronous console-script entrypoint used by `search-agent`."""

    args = parse_args(argv)
    asyncio.run(_run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
