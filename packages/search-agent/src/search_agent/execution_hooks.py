"""Execution policy shared by every frontend, with optional event observers."""

from agents import RunHooks


class ToolFailureAbort(Exception):
    """Three consecutive model-visible tool errors end the current trajectory."""


class ExecutionHooks(RunHooks):
    """Enforce the existing TUI failure budget without coupling it to rendering."""

    def __init__(self, observer=None):
        self.observer = observer
        self.failures = 0

    async def on_tool_start(self, context, agent, tool):
        if self.observer:
            await self.observer.on_tool_start(context, agent, tool)

    async def on_tool_end(self, context, agent, tool, result):
        if self.observer:
            await self.observer.on_tool_end(context, agent, tool, result)
        error = result.startswith("Error") or "validation error" in result.lower()
        self.failures = self.failures + 1 if error else 0
        if self.failures >= 3:
            raise ToolFailureAbort("Aborting after 3 consecutive tool failures")

    async def on_agent_start(self, context, agent):
        if self.observer:
            await self.observer.on_agent_start(context, agent)

    async def on_agent_end(self, context, agent, output):
        if self.observer:
            await self.observer.on_agent_end(context, agent, output)

    async def on_llm_start(self, context, agent, system_prompt, input_items):
        if self.observer:
            await self.observer.on_llm_start(context, agent, system_prompt, input_items)

    async def on_llm_end(self, context, agent, response):
        if self.observer:
            await self.observer.on_llm_end(context, agent, response)
