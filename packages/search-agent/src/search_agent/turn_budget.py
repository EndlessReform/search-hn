"""Graceful recovery when a research run exhausts its model-turn budget.

The primary agent gets several chances to search, read, and refine. If it uses
all of them, the user should still receive a useful conversational result—not
an SDK exception. This module constrains one final model call to a pair of
terminal tools so it must either summarize the evidence already collected or
ask the user for another bounded research pass.
"""

from __future__ import annotations

from typing import Annotated, Literal

from agents import (
    Agent,
    Model,
    ModelSettings,
    RunContextWrapper,
    RunErrorHandlerInput,
    RunErrorHandlerResult,
    RunErrorHandlers,
    Runner,
    function_tool,
)
from pydantic import Field

from search_agent.runtime_context import SearchAgentContext


@function_tool
def summarize_known_findings(
    summary: Annotated[
        str,
        Field(
            min_length=1,
            description=(
                "A useful final answer based only on evidence already gathered. "
                "Clearly note important uncertainty and preserve valid HN citations."
            ),
        ),
    ],
) -> str:
    """Admit the research limit was reached and answer with what is known so far."""

    return summary.strip()


@function_tool
def request_more_budget(
    ctx: RunContextWrapper[SearchAgentContext],
    request: Annotated[
        str,
        Field(
            min_length=1,
            max_length=240,
            description="One short sentence asking the user for another research pass.",
        ),
    ],
    justification: Annotated[
        str,
        Field(
            min_length=1,
            max_length=240,
            description=(
                "One short sentence naming the concrete unresolved lead and why "
                "more research is likely to improve the answer."
            ),
        ),
    ],
) -> str:
    """Ask for another turn budget in two short, user-facing sentences."""

    message = f"{request.strip()} {justification.strip()}"
    ctx.context.budget_state.request(message)
    return message


BudgetReply = Literal["approve", "reject", "guidance"]
"""How the TUI should interpret input while a budget request is pending."""


def classify_budget_reply(user_text: str) -> BudgetReply:
    """Recognize terse approval/rejection; all other text is model guidance."""

    normalized = user_text.strip().lower()
    if normalized in {"a", "approve"}:
        return "approve"
    if normalized in {"r", "reject"}:
        return "reject"
    return "guidance"


_RECOVERY_INSTRUCTIONS = """You have exhausted this research pass's model-turn budget.

You must call exactly one of the two available tools; do not produce a normal
assistant message and do not attempt any more research.

- Prefer `summarize_known_findings`. Give the user the best useful answer that
  the accumulated evidence supports, acknowledge material gaps, and retain
  exact HN cursor citations such as `【story:123】` when available.
- Use `request_more_budget` only when there is a specific unresolved lead and
  another research pass has a credible chance of materially improving the
  answer. Keep the request and justification to one short sentence each.
"""

_REJECTION_INSTRUCTIONS = """The user rejected your request for another research pass.

You have no research tools. Call `summarize_known_findings` exactly once with
the best answer supported by evidence already in the conversation. Clearly
acknowledge material gaps and preserve valid HN cursor citations. Do not ask
for more budget again.
"""


def build_rejection_summary_agent(
    *,
    model: str | Model | None,
    model_settings: ModelSettings,
) -> Agent[SearchAgentContext]:
    """Build the one-tool agent used after the user rejects more research."""

    return Agent(
        name="Research Budget Rejected",
        instructions=_REJECTION_INSTRUCTIONS,
        model=model,
        model_settings=model_settings,
        tools=[summarize_known_findings],
        tool_use_behavior="stop_on_first_tool",
    )


def build_max_turns_error_handlers(
    *,
    recovery_model_settings: ModelSettings,
) -> RunErrorHandlers[SearchAgentContext]:
    """Build the SDK error handler that converts exhaustion into a final choice.

    The handler receives the complete input and tool history from the exhausted
    run. A separate one-turn agent sees that history but has no research tools.
    Requiring a tool call, disabling parallel calls, and stopping on the first
    tool makes the two recovery outcomes mutually exclusive.
    """

    async def recover_from_max_turns(
        handler_input: RunErrorHandlerInput[SearchAgentContext],
    ) -> RunErrorHandlerResult:
        recovery_agent: Agent[SearchAgentContext] = Agent(
            name="Research Budget Recovery",
            instructions=_RECOVERY_INSTRUCTIONS,
            model=handler_input.run_data.last_agent.model,
            model_settings=recovery_model_settings,
            tools=[summarize_known_findings, request_more_budget],
            tool_use_behavior="stop_on_first_tool",
        )
        recovery_result = await Runner.run(
            recovery_agent,
            input=handler_input.run_data.history,
            context=handler_input.context.context,
            max_turns=1,
        )
        return RunErrorHandlerResult(
            final_output=recovery_result.final_output_as(str),
            include_in_history=True,
        )

    return {"max_turns": recover_from_max_turns}
