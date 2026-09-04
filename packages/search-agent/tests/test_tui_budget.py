"""TUI tests for the explicit research-budget approval state."""

from __future__ import annotations

import asyncio

from agents import Agent
from textual.containers import VerticalScroll
from textual.widgets import Input, Static

from search_agent.runtime_context import SearchAgentContext
from search_agent.tui import SearchAgentApp


def test_budget_prompt_and_reply_routing_are_explicit() -> None:
    """Show A/R choices and reserve arbitrary text for corrective guidance."""

    context = SearchAgentContext(repository=object())  # type: ignore[arg-type]
    app = SearchAgentApp(
        agent=Agent(name="Fixture", model="fixture-model"),
        agent_context=context,
        base_url="http://localhost:8000/v1",
    )

    async def exercise() -> None:
        async with app.run_test():
            context.budget_state.request("May I continue? One lead remains.")
            app._show_budget_approval_prompt()

            prompt = app.query_one("#prompt-input", Input)
            assert prompt.placeholder == (
                "A approve · R reject · or type corrective guidance"
            )
            log = app.query_one("#chat-log", VerticalScroll)
            status = list(log.query(Static))[-1]
            rendered_status = str(status.render())
            assert "(A)pprove" in rendered_status
            assert "(R)eject" in rendered_status
            assert "anything else" in rendered_status

            agent_input, summary_only = app._consume_budget_reply(
                "Stop trying publisher pages"
            )
            assert "Stop trying publisher pages" in agent_input
            assert summary_only is False
            assert context.budget_state.pending_request is None
            assert prompt.placeholder == "Ask about Hacker News…"

    try:
        asyncio.run(exercise())
    finally:
        app.close_conversation_session()


def test_reject_forces_summary_while_approve_grants_research_pass() -> None:
    """Map the two single-letter choices to their distinct controlled paths."""

    context = SearchAgentContext(repository=object())  # type: ignore[arg-type]
    app = SearchAgentApp(
        agent=Agent(name="Fixture", model="fixture-model"),
        agent_context=context,
        base_url="http://localhost:8000/v1",
    )

    async def exercise() -> None:
        async with app.run_test():
            context.budget_state.request("May I continue? One lead remains.")
            approved_input, approved_summary_only = app._consume_budget_reply("A")
            assert "approved" in approved_input
            assert approved_summary_only is False

            context.budget_state.request("May I continue? One lead remains.")
            rejected_input, rejected_summary_only = app._consume_budget_reply("R")
            assert "rejected" in rejected_input
            assert rejected_summary_only is True

    try:
        asyncio.run(exercise())
    finally:
        app.close_conversation_session()
