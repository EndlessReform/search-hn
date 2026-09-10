"""Presentation-independent agent construction, provider isolation and turn lifecycle.

Each runtime owns its DB pool and HTTP client. Providers are scoped to RunConfig,
never SDK globals: concurrent experiments cannot accidentally route to another
model's endpoint. Consumers own session memory and rendering; both frontends
consume the same SDK stream, including completion and session bookkeeping.
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime

from agents import (
    Agent,
    ModelSettings,
    OpenAIProvider,
    RunConfig,
    RunHooks,
    Runner,
    SQLiteSession,
)
from openai import AsyncOpenAI

from search_agent.agent_config import (
    _agent_instructions,
    _build_model_settings,
    _build_recovery_model_settings,
    _format_tool_approval_rejection,
    _is_openai_first_party_base_url,
)
from search_agent.data_access import HNStorySearchRepository
from search_agent.execution_hooks import ExecutionHooks
from search_agent.model_config import ModelRuntime
from search_agent.turn_budget import build_max_turns_error_handlers
from search_agent.runtime_context import (
    SearchAgentContext,
    build_search_agent_context,
    dispose_search_agent_context,
)
from search_agent.tools import (
    fetch_stories,
    fetch_top_comments,
    fetch_top_stories_for_date,
    open_webpage,
    read_webpage,
    find_in_webpage,
)


def resolve_api_key(base_url: str, override: str | None = None) -> str:
    """Require real credentials for OpenAI; local servers may be unauthenticated."""
    key = override or os.getenv("OPENAI_API_KEY")
    if key:
        return key
    assert not _is_openai_first_party_base_url(base_url), "OPENAI_API_KEY is required"
    return "local-openai-compatible-no-key"


def start_turn(
    *,
    agent,
    context,
    prompt,
    session=None,
    hooks=None,
    base_url,
    verbose=False,
    max_turns=10,
    run_config=None,
    model_settings=None,
):
    """Start the common stream, resetting per-turn state before either frontend.

    Resource bounds and provider options are independent of presentation verbosity.
    The caller must exhaust stream_events(), or cancel the result on interruption.
    """
    context.turn_state.reset()
    agent.model_settings = _build_model_settings(base_url, verbose=verbose).resolve(
        model_settings or ModelSettings()
    )
    options = {} if run_config is None else {"run_config": run_config}
    return Runner.run_streamed(
        agent,
        input=prompt,
        context=context,
        hooks=ExecutionHooks(hooks),
        max_turns=max_turns,
        session=session,
        error_handlers=build_max_turns_error_handlers(
            recovery_model_settings=_build_recovery_model_settings(
                base_url, verbose=verbose
            )
        ),
        **options,
    )


class SearchRuntime:
    """Own one independent search agent, with explicit close semantics."""

    def __init__(
        self,
        *,
        model: str,
        base_url: str,
        database_url: str | None = None,
        api_key: str | None = None,
        current_date: date | None = None,
        max_turns: int = 10,
        max_tokens: int | None = None,
        api: str = "responses",
        request_timeout: float = 180,
        repository: HNStorySearchRepository | None = None,
        retrieval: str | None = None,
        embedding_base_url: str | None = None,
        comments_database_url: str | None = None,
        model_runtime: ModelRuntime | None = None,
        enable_web: bool = False,
        web_inspection_call_limit: int = 4,
    ):
        self.base_url = base_url
        self.max_turns = max_turns
        if repository is None and retrieval in ("dense", "hybrid"):
            from search_agent.semantic_search import SemanticStoryRepository

            assert database_url and comments_database_url, (
                "Semantic search needs scratch and comment database URLs"
            )
            repository = SemanticStoryRepository(
                database_url, comments_database_url, mode=retrieval
            )
        self.context = (
            SearchAgentContext(
                repository=repository,
                current_date=current_date or datetime.now(UTC).astimezone().date(),
            )
            if repository is not None
            else build_search_agent_context(
                database_url,
                current_date_override=current_date,
                enable_web=enable_web,
                web_inspection_call_limit=web_inspection_call_limit,
                retrieval=retrieval,
                embedding_base_url=embedding_base_url,
            )
        )
        self._model_runtime = model_runtime
        self.client = (
            model_runtime.client
            if model_runtime is not None
            else AsyncOpenAI(
                base_url=base_url,
                api_key=resolve_api_key(base_url, api_key),
                timeout=request_timeout,
                max_retries=1,
            )
        )
        self.settings = ModelSettings(max_tokens=max_tokens)
        self._config = RunConfig(
            model_provider=OpenAIProvider(
                openai_client=self.client, use_responses=api == "responses"
            ),
            tracing_disabled=True,
            tool_error_formatter=_format_tool_approval_rejection,
        )
        self.agent = Agent(
            name="Hacker News Research Assistant",
            instructions=_agent_instructions,
            model=model,
            tools=[fetch_stories, fetch_top_stories_for_date, fetch_top_comments]
            + ([open_webpage, read_webpage, find_in_webpage] if enable_web else []),
        )

    @property
    def config(self) -> RunConfig:
        """Resolve the current picker transport for every turn, including resumes."""
        return (
            self._model_runtime.run_config
            if self._model_runtime is not None
            else self._config
        )

    def start(
        self,
        prompt: str,
        *,
        session: SQLiteSession | None = None,
        hooks: RunHooks[SearchAgentContext] | None = None,
        verbose: bool = False,
    ):
        return start_turn(
            agent=self.agent,
            context=self.context,
            prompt=prompt,
            session=session,
            hooks=hooks,
            base_url=self.base_url,
            verbose=verbose,
            max_turns=self.max_turns,
            run_config=self.config,
            model_settings=self.settings,
        )

    async def close(self):
        dispose_search_agent_context(self.context)
        if self._model_runtime is not None:
            await self._model_runtime.close()
        else:
            await self.client.close()
