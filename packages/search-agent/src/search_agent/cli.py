"""CLI entry point for search-agent with a Textual TUI."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence
from datetime import date

from agents import Agent, set_default_openai_client, set_tracing_disabled
from openai import AsyncOpenAI

from search_agent.agent_config import _agent_instructions
from search_agent.hooks import _TUIHooks
from search_agent.runtime_context import (
    build_search_agent_context,
    dispose_search_agent_context,
)
from search_agent.tools import (
    fetch_stories,
    fetch_top_comments,
    fetch_top_stories_for_date,
)
from search_agent.tui import SearchAgentApp


def _parse_system_date_override(raw_value: str) -> date:
    """Parse a CLI date override used to spoof the agent's notion of today.

    We accept either full ISO dates (``YYYY-MM-DD``) or a bare year such as
    ``1862``/``2029`` for playful prompt experiments. Bare years expand to
    January 1st of that year so downstream date-aware tools remain coherent.
    """

    clean = raw_value.strip()
    if len(clean) == 4 and clean.isdigit():
        return date(int(clean), 1, 1)

    try:
        return date.fromisoformat(clean)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "--system-date must be YYYY-MM-DD or a bare YYYY year"
        ) from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI flags for the interactive agent loop."""

    parser = argparse.ArgumentParser(description="HN search agent TUI")
    parser.add_argument(
        "--model",
        type=str,
        default="qwen-3.6-27b",
        help="Model to use (default: qwen-3.6-27b)",
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
    parser.add_argument(
        "--system-date",
        type=_parse_system_date_override,
        default=None,
        help=(
            "Override the date shown to the model and used as the default "
            "for 'today' in date-based tools. Accepts YYYY-MM-DD or a bare "
            "YYYY year such as 1862 or 2029."
        ),
    )
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> None:
    """Run the Textual TUI with one shared, persistent repository context."""

    context = build_search_agent_context(
        args.database_url,
        current_date_override=args.system_date,
    )

    agent: Agent = Agent(
        name="Hacker News Research Assistant",
        instructions=_agent_instructions,
        model=args.model,
        tools=[fetch_stories, fetch_top_stories_for_date, fetch_top_comments],
    )

    custom_client = AsyncOpenAI(
        base_url=args.base_url,
        api_key="dummy_key_or_vertex_token",
    )

    set_default_openai_client(custom_client)
    set_tracing_disabled(True)

    app = SearchAgentApp(agent=agent, agent_context=context, base_url=args.base_url)
    app._hooks = _TUIHooks(app)

    try:
        await app.run_async()
    finally:
        app.close_conversation_session()
        dispose_search_agent_context(context)


def main(argv: Sequence[str] | None = None) -> int:
    """Synchronous console-script entrypoint used by ``search-agent``."""

    args = parse_args(argv)
    asyncio.run(_run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
