"""RunHooks implementation for the TUI and related abstractions.

We define a narrow ``_AppInterface`` Protocol here so that ``_TUIHooks`` can
call back into the app without importing ``app.py`` — which would create a
circular import (``app.py`` imports ``hooks.py``). ``SearchAgentApp`` satisfies
``_AppInterface`` structurally; no explicit declaration is required.
"""

from __future__ import annotations

from typing import Protocol

from agents import RunHooks
from rich.markup import escape

from search_agent.runtime_context import SearchAgentContext
from search_agent.tool_output import (
    _MAX_CONSECUTIVE_TOOL_FAILURES,
    _summarize_tool_result,
)


class _AppInterface(Protocol):
    """Narrow interface used by ``_TUIHooks`` to push updates into the app.

    Defining this Protocol here — rather than importing ``SearchAgentApp``
    directly — breaks the potential circular dependency between hooks and app.
    Any object with these four methods satisfies the contract.
    """

    def finish_llm_activity(self) -> None: ...
    def append_status(self, markup: str) -> None: ...
    def record_tool_result(self, tool_name: str, raw_output: str) -> None: ...
    def begin_llm_activity(self) -> None: ...


class _ToolFailureAbort(Exception):
    """Raised to break out of a run when tools fail repeatedly."""


class _TUIHooks(RunHooks[SearchAgentContext]):
    """RunHooks that push status updates into the Textual app's log."""

    def __init__(self, app: _AppInterface) -> None:
        self._app = app
        self._consecutive_failures = 0

    def reset(self) -> None:
        self._consecutive_failures = 0

    async def on_tool_start(self, context, agent, tool) -> None:
        self._app.finish_llm_activity()
        self._app.append_status(f"[bold cyan]⚙ Calling tool:[/] {tool.name}")

    async def on_tool_end(self, context, agent, tool, result) -> None:
        # Detect tool-level error messages returned to the model
        is_error = result.startswith("Error") or "validation error" in result.lower()
        if is_error:
            self._consecutive_failures += 1
            self._app.append_status(
                f"[bold red]✗ {tool.name} error ({self._consecutive_failures}/"
                f"{_MAX_CONSECUTIVE_TOOL_FAILURES}):[/] {escape(result[:500])}"
            )
            if self._consecutive_failures >= _MAX_CONSECUTIVE_TOOL_FAILURES:
                raise _ToolFailureAbort(
                    f"Aborting after {_MAX_CONSECUTIVE_TOOL_FAILURES} consecutive tool failures"
                )
        else:
            self._consecutive_failures = 0
            self._app.record_tool_result(tool.name, result)
            self._app.append_status(
                f"[bold green]✓ {tool.name}:[/] {_summarize_tool_result(tool.name, result)}"
            )

    async def on_agent_start(self, context, agent) -> None:
        self._app.append_status(f"[dim]Agent started: {agent.name}[/]")

    async def on_llm_start(self, context, agent, system_prompt, input_items) -> None:
        self._app.begin_llm_activity()

    async def on_llm_end(self, context, agent, response) -> None:
        self._app.finish_llm_activity()
