"""Runtime context wiring for OpenAI Agents + local wrappers.

Why this exists:
- OpenAI Agents expects dependency injection through `Runner.run(..., context=...)`.
- Tool functions can then read dependencies from `RunContextWrapper.context`.
- We keep construction/teardown in one place so both wrappers (agent loop and
  HTTP API) instantiate database state the same way.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

from search_agent.data_access import HNStorySearchRepository

ENV_DATABASE_URL = "DATABASE_URL"
"""Primary environment variable used by search-agent wrappers."""


@dataclass(frozen=True)
class SearchAgentContext:
    """Dependency container passed through the Agents SDK run context.

    This object is never sent to the model. It exists only in local runtime
    code, where tool handlers and wrapper callbacks can access shared state.
    """

    repository: HNStorySearchRepository


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


def build_search_agent_context(database_url_override: str | None = None) -> SearchAgentContext:
    """Create a fully-initialized context object with persistent DB resources."""

    database_url = resolve_database_url(database_url_override)
    repository = HNStorySearchRepository.from_database_url(database_url)
    return SearchAgentContext(repository=repository)


def dispose_search_agent_context(context: SearchAgentContext) -> None:
    """Dispose resources created by `build_search_agent_context`."""

    context.repository.dispose()
