"""CLI entry point for search-agent with a Textual TUI."""

from __future__ import annotations

import argparse
import asyncio
import traceback
from collections.abc import Sequence

from agents import (
    Agent,
    RunContextWrapper,
    RunHooks,
    Runner,
    set_default_openai_client,
    set_tracing_disabled,
)
import json

from openai import AsyncOpenAI
from rich.markup import escape
from textual import work
from textual.app import App, ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import Footer, Header, Input, Static

from search_agent.runtime_context import (
    SearchAgentContext,
    build_search_agent_context,
    dispose_search_agent_context,
)
from search_agent.tools import fetch_stories, fetch_top_comments, fetch_top_stories_for_date


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI flags for the interactive agent loop."""

    parser = argparse.ArgumentParser(description="HN search agent TUI")
    parser.add_argument(
        "--model",
        type=str,
        default="Qwen-3.5-35B-A3B",
        help="Model to use (default: Qwen-3.5-35B-A3B)",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default="http://melchior-1:5000/v1",
        help="Base URL for the API (default: http://melchior-1:5000/v1)",
    )
    parser.add_argument(
        "--database-url",
        type=str,
        default=None,
        help=(
            "Optional database URL override. If omitted, uses DATABASE_URL "
            "from environment/.env."
        ),
    )
    return parser.parse_args(argv)


def _agent_instructions(
    ctx: RunContextWrapper[SearchAgentContext],
    _agent: Agent[SearchAgentContext],
) -> str:
    _ = ctx
    from datetime import date

    today = date.today().isoformat()
    return (
        "You are a research assistant answering questions from a mirrored Hacker News database.\n\n"
        f"Today's date is **{today}**.\n\n"
        "## Tools\n"
        "- **fetch_stories**: full-text search over HN story titles and URLs. Supports "
        "optional filters: min_score, min_date, max_date, include_domains, exclude_domains.\n"
        "- **fetch_top_stories_for_date**: top stories by score for a single calendar date. "
        "No text query needed — just pass a date (defaults to today). Great for 'what happened "
        "on Monday?' or 'top stories yesterday'.\n"
        "- **fetch_top_comments**: retrieve top-level comments for a known story ID.\n\n"
        "## Search strategy\n"
        "Before searching, consider whether the topic is **evergreen** or **time-bound**:\n"
        "- *Evergreen topics* (e.g. zettelkasten, functional programming, vim tips) are "
        "discussed repeatedly over many years. Omit date filters and prefer higher min_score "
        "(e.g. 50+) to surface the most upvoted, canonical discussions.\n"
        "- *Time-bound topics* (e.g. a specific product launch, breaking news, policy "
        "announcement) are relevant within a narrow window. Use min_date/max_date to "
        "target the period of interest and keep min_score low or omitted so you don't "
        "miss coverage.\n"
        "- *Daily digest* questions ('what's hot today', 'what happened last week') should "
        "use fetch_top_stories_for_date for specific days.\n"
        "- When a user asks about a *domain* (e.g. 'arxiv papers', 'github projects'), "
        "use include_domains to scope results.\n"
        "- When results are noisy, use exclude_domains to filter out low-signal sources.\n\n"
        "Use these filters judiciously — most simple queries need no filters at all. "
        "Apply filters when they meaningfully improve result quality."
    )


def _summarize_tool_result(tool_name: str, raw: str) -> str:
    """Turn raw tool JSON into a compact one-liner for the status log."""
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return escape(raw[:200])

    if tool_name == "fetch_stories":
        results = data.get("results", [])
        query = escape(data.get("query", "?"))
        if not results:
            return f'no stories for "{query}"'
        titles = ", ".join(
            escape(r.get("title", "?")[:60]) for r in results[:4]
        )
        suffix = f" (+{len(results) - 4} more)" if len(results) > 4 else ""
        return f'{len(results)} stories for "{query}": {titles}{suffix}'

    if tool_name == "fetch_top_stories_for_date":
        results = data.get("results", [])
        d = escape(data.get("date", "?"))
        if not results:
            return f"no stories for {d}"
        titles = ", ".join(
            escape(r.get("title", "?")[:60]) for r in results[:4]
        )
        suffix = f" (+{len(results) - 4} more)" if len(results) > 4 else ""
        return f"{len(results)} top stories for {d}: {titles}{suffix}"

    if tool_name == "fetch_top_comments":
        total = data.get("total_top_level_comments", 0)
        returned = data.get("returned", 0)
        sid = data.get("story_id", "?")
        return f"{returned}/{total} comments for story {sid}"

    return escape(raw[:200])


_MAX_CONSECUTIVE_TOOL_FAILURES = 3


class _ToolFailureAbort(Exception):
    """Raised to break out of a run when tools fail repeatedly."""


class _TUIHooks(RunHooks[SearchAgentContext]):
    """RunHooks that push status updates into the Textual app's log."""

    def __init__(self, app: SearchAgentApp) -> None:
        self._app = app
        self._consecutive_failures = 0

    def reset(self) -> None:
        self._consecutive_failures = 0

    async def on_tool_start(self, context, agent, tool) -> None:
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
            self._app.append_status(
                f"[bold green]✓ {tool.name}:[/] {_summarize_tool_result(tool.name, result)}"
            )

    async def on_agent_start(self, context, agent) -> None:
        self._app.append_status(f"[dim]Agent started: {agent.name}[/]")

    async def on_llm_start(self, context, agent, system_prompt, input_items) -> None:
        self._app.append_status("[dim]Thinking…[/]")


class SearchAgentApp(App[None]):
    """Textual TUI for the HN search agent."""

    CSS = """
    #chat-log {
        height: 1fr;
        border: solid $accent;
        padding: 1;
    }
    .user-msg {
        color: $text;
        background: $surface;
        margin: 0 0 1 0;
        padding: 0 1;
    }
    .assistant-msg {
        color: $success;
        margin: 0 0 1 0;
        padding: 0 1;
    }
    .status-msg {
        color: $text-muted;
        margin: 0 0 0 2;
    }
    #prompt-input {
        dock: bottom;
        margin: 1 0 0 0;
    }
    """

    BINDINGS = [
        ("ctrl+c", "quit", "Quit"),
    ]

    def __init__(
        self,
        agent: Agent[SearchAgentContext],
        agent_context: SearchAgentContext,
        hooks: _TUIHooks | None = None,
    ) -> None:
        super().__init__()
        self._agent = agent
        self._agent_context = agent_context
        self._hooks = hooks
        self._input_history: list[dict] = []

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield VerticalScroll(id="chat-log")
        yield Input(placeholder="Ask about Hacker News…", id="prompt-input")
        yield Footer()

    def on_mount(self) -> None:
        self.title = "HN Search Agent"
        self.sub_title = self._agent.model or ""
        self.query_one("#prompt-input", Input).focus()

    # ── public helpers used by hooks ──────────────────────────────────

    def append_status(self, markup: str) -> None:
        """Append a status/tool message to the chat log (thread-safe)."""
        log = self.query_one("#chat-log", VerticalScroll)
        log.mount(Static(markup, classes="status-msg"))
        log.scroll_end(animate=False)

    def _append_user(self, text: str) -> None:
        log = self.query_one("#chat-log", VerticalScroll)
        log.mount(Static(f"[bold]You:[/] {escape(text)}", classes="user-msg"))
        log.scroll_end(animate=False)

    def _append_assistant(self, text: str) -> None:
        log = self.query_one("#chat-log", VerticalScroll)
        log.mount(Static(f"[bold green]Agent:[/] {escape(text)}", classes="assistant-msg"))
        log.scroll_end(animate=False)

    # ── input handling ────────────────────────────────────────────────

    def _reset_conversation(self) -> None:
        """Clear chat log and conversation history."""
        log = self.query_one("#chat-log", VerticalScroll)
        log.remove_children()
        self._input_history.clear()
        self.append_status("[dim]Conversation reset.[/]")

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        user_text = event.value.strip()
        if not user_text:
            return
        event.input.value = ""

        if user_text == "/new":
            self._reset_conversation()
            return
        if user_text == "/q":
            self.exit()
            return

        event.input.disabled = True
        self._append_user(user_text)
        self._run_agent(user_text)

    @work(exclusive=True, thread=False)
    async def _run_agent(self, user_text: str) -> None:
        """Run the agent in a Textual async worker (stays on the event loop)."""

        # Build input: either first message or continuation
        if self._input_history:
            self._input_history.append({"role": "user", "content": user_text})
            agent_input = list(self._input_history)
        else:
            agent_input = user_text

        if self._hooks:
            self._hooks.reset()

        try:
            result = await Runner.run(
                self._agent,
                input=agent_input,
                context=self._agent_context,
                hooks=self._hooks,
                max_turns=10,
            )
            # Store conversation for multi-turn
            self._input_history = result.to_input_list()

            # Extract final text output
            output = result.final_output_as(str)
            self._append_assistant(output)
        except _ToolFailureAbort as exc:
            self.append_status(f"[bold red]{escape(str(exc))}[/]")
        except Exception as exc:
            tb = traceback.format_exception(exc)
            self.append_status(
                f"[bold red]Error:[/] {escape(str(exc))}\n{escape(''.join(tb))}"
            )
        finally:
            prompt = self.query_one("#prompt-input", Input)
            prompt.disabled = False
            prompt.focus()


async def _run(args: argparse.Namespace) -> None:
    """Run the Textual TUI with one shared, persistent repository context."""

    context = build_search_agent_context(args.database_url)

    agent: Agent[SearchAgentContext] = Agent(
        name="Hacker News Research Assistant",
        instructions=_agent_instructions,
        model=args.model,
        tools=[fetch_stories, fetch_top_stories_for_date, fetch_top_comments],
    )

    custom_client = AsyncOpenAI(
        base_url=args.base_url,
        api_key="dummy_key_or_vertex_token",
    )

    set_default_openai_client(custom_client)
    set_tracing_disabled(True)

    app = SearchAgentApp(agent=agent, agent_context=context)
    app._hooks = _TUIHooks(app)

    try:
        await app.run_async()
    finally:
        dispose_search_agent_context(context)


def main(argv: Sequence[str] | None = None) -> int:
    """Synchronous console-script entrypoint used by `search-agent`."""

    args = parse_args(argv)
    asyncio.run(_run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
