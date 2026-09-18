"""CLI entry point for search-agent with a Textual TUI."""

from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

from search_agent.agent_config import (
    DEFAULT_MODEL,
)
from search_agent.model_config import (
    ModelRuntime,
    ModelSelection,
    ProviderConfig,
    SearchAgentModelConfig,
    load_model_config,
)
from search_agent.runtime import SearchRuntime, resolve_api_key


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


def _parse_web_inspection_call_limit(raw_value: str) -> int:
    """Parse the configurable page-tool budget constrained to three through five."""

    try:
        value = int(raw_value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "web inspection call limit must be an integer from 3 through 5"
        ) from exc
    if not 3 <= value <= 5:
        raise argparse.ArgumentTypeError(
            "web inspection call limit must be from 3 through 5"
        )
    return value


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI flags for the interactive agent loop."""

    parser = argparse.ArgumentParser(description="HN search agent TUI")
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="Startup model ID or preset alias (overrides config and OPENAI_MODEL)",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default=None,
        help=(
            "Startup OpenAI-compatible API base URL (overrides config and "
            "OPENAI_BASE_URL)"
        ),
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Model/provider TOML path (default: ~/.config/search-agent/config.toml)",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help=(
            "API key override. Otherwise uses OPENAI_API_KEY. Local, non-OpenAI "
            "endpoints receive a non-secret placeholder when no key is configured."
        ),
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
    parser.add_argument(
        "--web-inspection-call-limit",
        type=_parse_web_inspection_call_limit,
        default=os.getenv("SEARCH_AGENT_WEB_CALL_LIMIT", "4"),
        help=(
            "Consecutive webpage-tool calls allowed before refusal; 3-5 "
            "(default: SEARCH_AGENT_WEB_CALL_LIMIT or 4)."
        ),
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run one prompt without importing Textual.",
    )
    parser.add_argument("--prompt", help="User question for a headless turn.")
    parser.add_argument(
        "--output", help="Durable JSONL trajectory path (required headlessly)."
    )
    parser.add_argument("--max-turns", type=int, default=10)
    parser.add_argument("--max-tokens", type=int, default=None)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--api", choices=["responses", "chat"], default="responses")
    parser.add_argument(
        "--retrieval",
        choices=["production", "fts", "dense", "hybrid"],
        default=None,
        help="Default: SEARCH_RETRIEVAL or production hybrid. fts is the legacy backend; dense/hybrid are frozen research backends.",
    )
    parser.add_argument(
        "--comments-database-url",
        help="Live mirror URL for comment fetches when using a semantic snapshot.",
    )
    parser.add_argument(
        "--embedding-base-url",
        help="Production embedding proxy base URL; otherwise EMBEDDING_BASE_URL from environment/.env.",
    )
    args = parser.parse_args(argv)
    if args.headless and (not args.prompt or not args.output):
        parser.error("--headless requires --prompt and --output")
    return args


def _resolve_api_key(*, base_url: str, api_key_override: str | None) -> str:
    """Resolve credentials without silently sending a dummy key to OpenAI.

    Many local OpenAI-compatible servers require the client field even though
    they do not authenticate it. First-party OpenAI is different: a missing key
    is a configuration error and should fail before the TUI starts.
    """

    return resolve_api_key(base_url, api_key_override)


def _resolve_startup_model(
    config: SearchAgentModelConfig,
    *,
    model_override: str | None,
    base_url_override: str | None,
) -> tuple[SearchAgentModelConfig, ModelSelection]:
    """Resolve CLI/environment overrides without hiding them from the picker.

    A URL matching a configured provider reuses that provider.  An unmatched
    URL becomes an in-memory ``override`` provider, so the active selection is
    still represented honestly in the modal without persisting CLI state.
    """

    if base_url_override is None and model_override is None:
        return config, config.default_selection()

    if base_url_override is None and model_override is not None:
        preset = config.resolve_preset(model_override)
        if preset is not None:
            return config, preset
        default = config.default_selection()
        return config, ModelSelection(default.provider_id, model_override)

    assert base_url_override is not None
    normalized_url = base_url_override.rstrip("/")
    provider_id: str | None = None
    for candidate_id, provider in config.provider_items():
        if provider.base_url.rstrip("/") == normalized_url:
            provider_id = candidate_id
            break

    effective_config = config
    if provider_id is None:
        provider_id = "override"
        providers = dict(config.providers)
        providers[provider_id] = ProviderConfig(
            name="Current override",
            base_url=normalized_url,
            models=(() if model_override is None else (model_override,)),
        )
        effective_config = SearchAgentModelConfig(
            default_preset=config.default_preset,
            providers=providers,
            presets=config.presets,
        )

    if model_override is not None:
        model = model_override
    else:
        provider = effective_config.provider(provider_id)
        model = provider.models[0].id if provider.models else DEFAULT_MODEL
    return effective_config, ModelSelection(provider_id, model)


async def _run(args: argparse.Namespace) -> None:
    """Run the Textual TUI with one shared, persistent repository context."""

    load_dotenv()
    config = load_model_config(args.config)
    config, selection = _resolve_startup_model(
        config,
        model_override=args.model or os.getenv("OPENAI_MODEL"),
        base_url_override=args.base_url or os.getenv("OPENAI_BASE_URL"),
    )

    from search_agent.hooks import _TUIHooks
    from search_agent.tui import SearchAgentApp

    models = ModelRuntime(
        config, selection, api_key_override=args.api_key, api=args.api
    )
    runtime = SearchRuntime(
        model=selection.model,
        base_url=models.provider.base_url,
        model_runtime=models,
        database_url=args.database_url,
        current_date=args.system_date,
        max_turns=args.max_turns,
        max_tokens=args.max_tokens,
        api=args.api,
        retrieval=args.retrieval,
        embedding_base_url=args.embedding_base_url,
        comments_database_url=args.comments_database_url,
        enable_web=True,
        web_inspection_call_limit=args.web_inspection_call_limit,
    )
    app = SearchAgentApp(
        agent=runtime.agent,
        agent_context=runtime.context,
        base_url=runtime.base_url,
        model_runtime=models,
        runtime=runtime,
    )
    app._hooks = _TUIHooks(app)

    try:
        await app.run_async()
    finally:
        app.close_conversation_session()
        await runtime.close()


def main(argv: Sequence[str] | None = None) -> int:
    """Synchronous console-script entrypoint used by ``search-agent``."""

    args = parse_args(argv)
    if args.headless:
        from search_agent.headless import run_cli

        asyncio.run(run_cli(args))
    else:
        asyncio.run(_run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
