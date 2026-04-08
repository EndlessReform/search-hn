"""Runtime context wiring for OpenAI Agents + local wrappers.

Why this exists:
- OpenAI Agents expects dependency injection through `Runner.run(..., context=...)`.
- Tool functions can then read dependencies from `RunContextWrapper.context`.
- We keep construction/teardown in one place so both wrappers (agent loop and
  HTTP API) instantiate database state the same way.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date

from dotenv import load_dotenv

from search_agent.data_access import HNStorySearchRepository

ENV_DATABASE_URL = "DATABASE_URL"
"""Primary environment variable used by search-agent wrappers."""


@dataclass
class SearchAgentTurnState:
    """Mutable per-turn state that tools can use to avoid repeated nudges.

    This lives only in local runtime memory and is reset at the start of each
    user turn. It is intentionally tiny: we only track state that affects tool
    messaging, not core search behavior.
    """

    no_results_guidance_emitted: bool = False

    def reset(self) -> None:
        """Reset per-turn latches before a new user request is processed."""

        self.no_results_guidance_emitted = False


@dataclass(frozen=True)
class SearchAgentContext:
    """Dependency container passed through the Agents SDK run context.

    This object is never sent to the model. It exists only in local runtime
    code, where tool handlers and wrapper callbacks can access shared state.
    """

    repository: HNStorySearchRepository
    current_date: date = field(default_factory=date.today)
    turn_state: SearchAgentTurnState = field(default_factory=SearchAgentTurnState)


def resolve_database_url(database_url_override: str | None = None) -> str:
    """Resolve `DATABASE_URL`, optionally preferring an explicit override.

    We load `.env` here to keep local CLI usage simple while still allowing
    shell-level overrides in CI/production environments.
    """

    load_dotenv()
    resolved = database_url_override or os.getenv(ENV_DATABASE_URL)
    assert resolved, (
        f"{ENV_DATABASE_URL} is required. Set it in your environment or .env file, "
        "for example: DATABASE_URL=postgresql://user:pass@host:5432/searchhn_test"
    )
    return resolved


def build_search_agent_context(
    database_url_override: str | None = None,
    *,
    current_date_override: date | None = None,
) -> SearchAgentContext:
    """Create a fully-initialized context object with persistent DB resources.

    ``current_date_override`` exists mainly for CLI experiments and tests where
    we want the agent's notion of "today" to differ from the host clock.
    """

    database_url = resolve_database_url(database_url_override)
    repository = HNStorySearchRepository.from_database_url(database_url)
    return SearchAgentContext(
        repository=repository,
        current_date=current_date_override or date.today(),
    )


def dispose_search_agent_context(context: SearchAgentContext) -> None:
    """Dispose resources created by `build_search_agent_context`."""

    context.repository.dispose()
