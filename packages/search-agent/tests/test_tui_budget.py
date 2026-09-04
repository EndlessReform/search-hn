"""TUI tests for the explicit research-budget approval state."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

from agents import Agent, ToolApprovalItem
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


def test_ctrl_b_toggles_between_prompt_and_transcript() -> None:
    """Make the common prompt-focus shortcut work in both directions."""

    context = SearchAgentContext(repository=object())  # type: ignore[arg-type]
    app = SearchAgentApp(
        agent=Agent(name="Fixture", model="fixture-model"),
        agent_context=context,
        base_url="http://localhost:8000/v1",
    )

    async def exercise() -> None:
        async with app.run_test() as pilot:
            assert app.focused is app.query_one("#prompt-input", Input)
            await pilot.press("ctrl+b")
            assert app.focused is app.query_one("#chat-log", VerticalScroll)
            await pilot.press("ctrl+b")
            assert app.focused is app.query_one("#prompt-input", Input)

    try:
        asyncio.run(exercise())
    finally:
        app.close_conversation_session()


def test_arrows_navigate_application_message_history_and_restore_draft() -> None:
    """Keep history across chat resets without losing in-progress prompt text."""

    context = SearchAgentContext(repository=object())  # type: ignore[arg-type]
    app = SearchAgentApp(
        agent=Agent(name="Fixture", model="fixture-model"),
        agent_context=context,
        base_url="http://localhost:8000/v1",
    )

    async def exercise() -> None:
        async with app.run_test() as pilot:
            prompt = app.query_one("#prompt-input", Input)
            app._remember_message("first question")
            app._remember_message("second question")
            app._reset_conversation()
            prompt.value = "unfinished draft"

            await pilot.press("up")
            assert prompt.value == "second question"
            assert prompt.cursor_position == len(prompt.value)
            await pilot.press("up")
            assert prompt.value == "first question"
            await pilot.press("up")
            assert prompt.value == "first question"
            await pilot.press("down")
            assert prompt.value == "second question"
            await pilot.press("down")
            assert prompt.value == "unfinished draft"
            await pilot.press("down")
            assert prompt.value == "unfinished draft"

    try:
        asyncio.run(exercise())
    finally:
        app.close_conversation_session()


def test_history_collapses_only_consecutive_duplicates() -> None:
    """Avoid sticky duplicate entries while retaining meaningful repetition."""

    context = SearchAgentContext(repository=object())  # type: ignore[arg-type]
    app = SearchAgentApp(
        agent=Agent(name="Fixture", model="fixture-model"),
        agent_context=context,
        base_url="http://localhost:8000/v1",
    )
    try:
        app._remember_message("one")
        app._remember_message("one")
        app._remember_message("two")
        app._remember_message("one")

        assert app._message_history == ["one", "two", "one"]
    finally:
        app.close_conversation_session()


def test_comment_link_tui_workflow_explains_and_routes_feedback() -> None:
    """Pause an SDK result and preserve arbitrary guidance on rejection."""

    context = SearchAgentContext(repository=object())  # type: ignore[arg-type]
    agent = Agent(name="Fixture", model="fixture-model")
    app = SearchAgentApp(
        agent=agent,
        agent_context=context,
        base_url="http://localhost:8000/v1",
    )

    async def exercise() -> None:
        async with app.run_test():
            approval = ToolApprovalItem(
                agent=agent,
                raw_item={
                    "name": "open_webpage",
                    "call_id": "call-comment-1",
                    "arguments": '{"url":"https://notes.example/post"}',
                },
            )
            run_state = MagicMock()
            interrupted = MagicMock()
            interrupted.interruptions = [approval]
            interrupted.to_state.return_value = run_state
            app._pause_for_tool_approval(interrupted)

            status = list(app.query_one("#chat-log", VerticalScroll).query(Static))[-1]
            rendered = str(status.render())
            assert "user-authored HN comment" in rendered
            assert "open this exact URL once" in rendered
            assert "continue with other evidence" in rendered

            resumed = app._consume_tool_approval_reply(
                "That archive URL is stale; use the original host"
            )
            assert resumed is run_state
            run_state.reject.assert_called_once_with(approval)
            message = context.tool_approval_feedback.pop_rejection_message(
                "call-comment-1"
            )
            assert message is not None
            assert "archive URL is stale" in message

    try:
        asyncio.run(exercise())
    finally:
        app.close_conversation_session()


def test_tui_reports_selected_web_extractor_at_startup() -> None:
    """Make runtime selection visible before the first webpage tool call."""

    extractor = SimpleNamespace(
        name="defuddle-local@0.18.1",
        runtime_source="fnm v20.19.4",
    )
    context = SearchAgentContext(
        repository=object(),  # type: ignore[arg-type]
        web_service=SimpleNamespace(  # type: ignore[arg-type]
            extractor=extractor,
            extractor_error=None,
        ),
    )
    app = SearchAgentApp(
        agent=Agent(name="Fixture", model="fixture-model"),
        agent_context=context,
        base_url="http://localhost:8000/v1",
    )

    async def exercise() -> None:
        async with app.run_test():
            statuses = app.query_one("#chat-log", VerticalScroll).query(Static)
            rendered = "\n".join(str(status.render()) for status in statuses)
            assert "defuddle-local@0.18.1" in rendered
            assert "fnm v20.19.4" in rendered

    try:
        asyncio.run(exercise())
    finally:
        app.close_conversation_session()
