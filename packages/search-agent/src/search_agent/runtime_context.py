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
from datetime import UTC, date, datetime

from dotenv import load_dotenv

from search_agent.data_access import HNStorySearchRepository
from search_agent.web import (
    PublisherPolicy,
    WebConversationState,
    WebPageService,
    build_local_defuddle_extractor,
)
from search_agent.web.fetcher import WebPageFetcher

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


@dataclass
class ResearchBudgetState:
    """Conversation-scoped approval state for an extra research pass."""

    pending_request: str | None = None

    def request(self, message: str) -> None:
        """Record the user-facing request produced by the recovery agent."""

        clean = message.strip()
        assert clean, "budget request must not be empty"
        self.pending_request = clean

    def clear(self) -> None:
        """Resolve or discard any outstanding approval request."""

        self.pending_request = None


@dataclass
class ToolApprovalFeedbackState:
    """Model-visible explanations attached to rejected SDK tool calls.

    The SDK approval API records only the decision.  This small call-ID keyed
    mailbox lets the TUI preserve arbitrary corrective feedback and lets the
    run-level error formatter turn it into the rejected tool's output.
    """

    rejection_messages: dict[str, str] = field(default_factory=dict)

    def reject(self, call_id: str, message: str) -> None:
        """Record the explanation the model should receive for one rejection."""

        clean_call_id = call_id.strip()
        clean_message = message.strip()
        assert clean_call_id, "rejected tool call must have an ID"
        assert clean_message, "rejected tool call message must not be empty"
        self.rejection_messages[clean_call_id] = clean_message

    def pop_rejection_message(self, call_id: str) -> str | None:
        """Consume a rejection explanation when the SDK resumes the run."""

        return self.rejection_messages.pop(call_id, None)

    def clear(self) -> None:
        """Discard feedback belonging to an abandoned conversation."""

        self.rejection_messages.clear()


@dataclass(frozen=True)
class SearchAgentContext:
    """Dependency container passed through the Agents SDK run context.

    This object is never sent to the model. It exists only in local runtime
    code, where tool handlers and wrapper callbacks can access shared state.
    """

    repository: HNStorySearchRepository
    current_date: date = field(default_factory=date.today)
    turn_state: SearchAgentTurnState = field(default_factory=SearchAgentTurnState)
    budget_state: ResearchBudgetState = field(default_factory=ResearchBudgetState)
    tool_approval_feedback: ToolApprovalFeedbackState = field(
        default_factory=ToolApprovalFeedbackState
    )
    web_state: WebConversationState = field(default_factory=WebConversationState)
    web_service: WebPageService | None = None


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
    enable_web: bool = False,
    web_inspection_call_limit: int = 4,
) -> SearchAgentContext:
    """Create a fully-initialized context object with persistent DB resources.

    ``current_date_override`` exists mainly for CLI experiments and tests where
    we want the agent's notion of "today" to differ from the host clock.
    """

    database_url = resolve_database_url(database_url_override)
    repository = HNStorySearchRepository.from_database_url(database_url)
    web_state = WebConversationState(
        inspection_call_limit=web_inspection_call_limit,
    )
    web_service = None
    if enable_web:
        extractor, extractor_error = build_local_defuddle_extractor()
        web_service = WebPageService(
            state=web_state,
            policy=PublisherPolicy.load(),
            fetcher=WebPageFetcher(),
            extractor=extractor,
            extractor_error=extractor_error,
        )

    return SearchAgentContext(
        repository=repository,
        current_date=current_date_override or datetime.now(UTC).astimezone().date(),
        web_state=web_state,
        web_service=web_service,
    )


def dispose_search_agent_context(context: SearchAgentContext) -> None:
    """Dispose resources created by `build_search_agent_context`."""

    context.repository.dispose()
