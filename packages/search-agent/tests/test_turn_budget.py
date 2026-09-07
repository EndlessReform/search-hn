"""Tests for the forced terminal choice after research-budget exhaustion."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from agents import Agent, ModelSettings, RunErrorHandlerResult

from search_agent.runtime_context import SearchAgentContext
from search_agent.turn_budget import (
    build_max_turns_error_handlers,
    build_rejection_summary_agent,
    classify_budget_reply,
)


def test_budget_reply_classifier_reserves_only_explicit_a_and_r() -> None:
    """Treat every non-command response as corrective guidance verbatim."""

    assert classify_budget_reply("A") == "approve"
    assert classify_budget_reply(" approve ") == "approve"
    assert classify_budget_reply("R") == "reject"
    assert classify_budget_reply("reject") == "reject"
    assert classify_budget_reply("Stop trying publisher pages") == "guidance"


def test_rejection_agent_can_only_summarize() -> None:
    """Prevent a rejected budget request from starting more research."""

    agent = build_rejection_summary_agent(
        model="fixture-model",
        model_settings=ModelSettings(
            tool_choice="required",
            parallel_tool_calls=False,
        ),
    )

    assert [tool.name for tool in agent.tools] == ["summarize_known_findings"]
    assert agent.tool_use_behavior == "stop_on_first_tool"
    assert agent.model_settings.tool_choice == "required"


def test_max_turns_handler_runs_one_terminal_tool_only_agent() -> None:
    """Give the recovery model history but no way to continue researching."""

    context = SearchAgentContext(repository=object())  # type: ignore[arg-type]
    original_agent: Agent[SearchAgentContext] = Agent(
        name="Researcher",
        model="fixture-model",
    )
    handler_input = SimpleNamespace(
        context=SimpleNamespace(context=context),
        run_data=SimpleNamespace(
            last_agent=original_agent,
            history=[{"role": "user", "content": "Find the answer"}],
        ),
    )
    fake_result = SimpleNamespace(final_output_as=lambda _type: "Known findings")
    handlers = build_max_turns_error_handlers(
        recovery_model_settings=ModelSettings(
            tool_choice="required",
            parallel_tool_calls=False,
        )
    )

    with patch(
        "search_agent.turn_budget.Runner.run",
        new=AsyncMock(return_value=fake_result),
    ) as mock_run:
        result = asyncio.run(handlers["max_turns"](handler_input))

    assert isinstance(result, RunErrorHandlerResult)
    assert result.final_output == "Known findings"
    assert result.include_in_history is True

    recovery_agent = mock_run.call_args.args[0]
    assert recovery_agent.model == "fixture-model"
    assert recovery_agent.tool_use_behavior == "stop_on_first_tool"
    assert recovery_agent.model_settings.tool_choice == "required"
    assert recovery_agent.model_settings.parallel_tool_calls is False
    assert {tool.name for tool in recovery_agent.tools} == {
        "summarize_known_findings",
        "request_more_budget",
    }
    mock_run.assert_awaited_once_with(
        recovery_agent,
        input=handler_input.run_data.history,
        context=context,
        max_turns=1,
    )
