"""Approval policy tests for webpage calls sourced from HN comments."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from agents import Agent, Model, ModelResponse, RunContextWrapper, Runner
from agents.usage import Usage
from openai.types.responses import (
    ResponseFunctionToolCall,
    ResponseOutputMessage,
    ResponseOutputText,
)

from search_agent.agent_config import _run_config
from search_agent.runtime_context import SearchAgentContext
from search_agent.tools.open_webpage import open_webpage


class ApprovalFixtureModel(Model):
    """Request one webpage call, then finish after its approval is resolved."""

    def __init__(self) -> None:
        self.inputs: list[object] = []

    async def get_response(self, *args: Any, **kwargs: Any) -> ModelResponse:
        model_input = kwargs.get("input", args[1] if len(args) > 1 else None)
        self.inputs.append(model_input)
        if len(self.inputs) == 1:
            output = [
                ResponseFunctionToolCall(
                    arguments='{"url":"https://comment.example/link"}',
                    call_id="comment-call",
                    name="open_webpage",
                    type="function_call",
                )
            ]
        else:
            output = [
                ResponseOutputMessage(
                    id="message-1",
                    content=[
                        ResponseOutputText(
                            annotations=[],
                            text="continued",
                            type="output_text",
                        )
                    ],
                    role="assistant",
                    status="completed",
                    type="message",
                )
            ]
        return ModelResponse(output=output, usage=Usage(), response_id=None)

    async def stream_response(self, *args: Any, **kwargs: Any) -> AsyncIterator[object]:
        if False:  # pragma: no cover - this fixture exercises non-streamed SDK state
            yield object()


def test_only_comment_sourced_urls_require_approval() -> None:
    """Submission and pulled-page links stay automatic; comment links pause."""

    context = SearchAgentContext(repository=object())  # type: ignore[arg-type]
    state = context.web_state
    state.authorize(
        "https://submission.example/article",
        depth=0,
        story_id=1,
        source="story",
    )
    state.authorize(
        "https://comment.example/link",
        depth=0,
        story_id=1,
        source="top-level-comment",
    )
    parent = state.authorization_for("https://submission.example/article")
    assert parent is not None
    state.authorize_page_links(
        '<a href="https://docs.example/detail">details</a>',
        base_url=parent.url,
        parent=parent,
    )

    callback = open_webpage.needs_approval
    assert callable(callback)
    wrapper = RunContextWrapper(context=context)

    async def exercise() -> None:
        assert not await callback(
            wrapper, {"url": "https://submission.example/article"}, "story-call"
        )
        assert await callback(
            wrapper, {"url": "https://comment.example/link"}, "comment-call"
        )
        assert not await callback(
            wrapper, {"url": "https://docs.example/detail"}, "page-call"
        )
        assert not await callback(wrapper, {"url": "not a URL"}, "bad-call")

    asyncio.run(exercise())


def test_sdk_pauses_comment_call_and_resumes_with_rejection_guidance() -> None:
    """Exercise the real SDK interruption boundary used by the TUI."""

    context = SearchAgentContext(repository=object())  # type: ignore[arg-type]
    context.web_state.authorize(
        "https://comment.example/link",
        depth=0,
        story_id=1,
        source="top-level-comment",
    )
    model = ApprovalFixtureModel()
    agent = Agent[SearchAgentContext](
        name="Approval fixture",
        model=model,
        tools=[open_webpage],
    )

    async def exercise() -> None:
        run_config = _run_config()
        run_config.tracing_disabled = True
        interrupted = await Runner.run(
            agent,
            "Open the comment link",
            context=context,
            max_turns=2,
            run_config=run_config,
        )
        assert len(interrupted.interruptions) == 1
        assert len(model.inputs) == 1

        approval = interrupted.interruptions[0]
        assert approval.call_id == "comment-call"
        state = interrupted.to_state()
        context.tool_approval_feedback.reject(
            "comment-call",
            "The user rejected this URL: use the original source instead.",
        )
        state.reject(approval)

        completed = await Runner.run(agent, state, run_config=run_config)
        assert completed.final_output == "continued"
        assert len(model.inputs) == 2
        assert "use the original source instead" in str(model.inputs[1])

    asyncio.run(exercise())
