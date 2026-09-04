"""Textual TUI application for the HN search agent.

``SearchAgentApp`` owns the full UI lifecycle: composing the widget tree,
routing user input to the agent worker, streaming live reasoning and tool
status updates, and rendering final assistant replies with citation links.
"""

from __future__ import annotations

import time
import traceback
from typing import ClassVar

from agents import (
    Agent,
    RawResponsesStreamEvent,
    RunItemStreamEvent,
    SQLiteSession,
)
from openai.types.responses import (
    ResponseReasoningSummaryTextDeltaEvent,
    ResponseReasoningTextDeltaEvent,
)
from rich.markup import escape
from textual import work
from textual.app import App, ComposeResult
from textual.containers import VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Footer, Header, Input, Markdown, Static

from search_agent.agent_config import (
    _VERBOSE_OFF_COMMAND,
    _VERBOSE_ON_COMMAND,
    DEFAULT_MODEL,
    _is_openai_first_party_base_url,
    _new_conversation_session,
    _parse_model_command,
    _parse_verbose_command,
    _start_streamed_turn,
)
from search_agent.citations import CitationReference, CitationRegistry
from search_agent.hooks import _ToolFailureAbort, _TUIHooks
from search_agent.metrics import _collect_turn_metrics, _format_turn_metrics
from search_agent.runtime_context import SearchAgentContext
from search_agent.tool_output import (
    _extract_tool_call_name_and_arguments,
    _format_tool_call_preview,
)


class HelpModal(ModalScreen):
    """A popup screen for displaying agent help."""

    CSS = """
    HelpModal {
        align: center middle;
    }
    #help-container {
        width: 60;
        height: auto;
        max-height: 80%;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
    }
    """

    def compose(self) -> ComposeResult:
        yield VerticalScroll(
            Markdown(SearchAgentApp.HELP),
            id="help-container",
        )

    def on_mount(self) -> None:
        self.title = "Help"

    def on_key(self, event) -> None:
        if event.key == "escape":
            self.app.pop_screen()


class SearchAgentApp(App[None]):
    """Textual TUI for the HN search agent."""

    HELP = """
# HN Search Agent

## Commands (type in the prompt)

- **/new** — Reset conversation (clear chat log and history)
- **/q** — Quit
- **/verbose on|off** — Toggle verbose reasoning display
- **/model &lt;name&gt;** — Switch to a different model (e.g. ``/model gpt-4o``)
- **/m &lt;name&gt;** — Alias for ``/model``
- **/model default** or **/m default** — Reset to the default model

## Key bindings

- **Ctrl+C** — Quit
"""

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
        color: $accent;
        margin: 0 0 1 0;
        padding: 0 1;
    }
    .status-msg {
        color: $text-muted;
        margin: 0 0 0 2;
    }
    .reasoning-msg {
        color: $text-muted;
        margin: 0 0 0 2;
    }
    #prompt-input {
        dock: bottom;
        margin: 1 0 0 0;
    }
    """

    BINDINGS: ClassVar[list[tuple[str, str, str]]] = [
        ("ctrl+c", "quit", "Quit"),
        ("f1", "show_help", "Help"),
    ]

    def __init__(
        self,
        agent: Agent[SearchAgentContext],
        agent_context: SearchAgentContext,
        base_url: str,
        hooks: _TUIHooks | None = None,
    ) -> None:
        super().__init__()
        self._agent = agent
        self._agent_context = agent_context
        self._base_url = base_url
        self._hooks = hooks
        self._conversation_session: SQLiteSession = _new_conversation_session()
        self._verbose = True
        self._citation_registry = CitationRegistry()
        self._active_llm_widget: Static | None = None
        self._active_llm_text = ""
        self._active_llm_has_content = False
        self._active_llm_kind: str | None = None
        self._model_name: str = agent.model or DEFAULT_MODEL

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield VerticalScroll(id="chat-log")
        yield Input(placeholder="Ask about Hacker News…", id="prompt-input")
        yield Footer()

    def on_mount(self) -> None:
        self.title = "HN Search Agent"
        self._refresh_sub_title()
        self.query_one("#prompt-input", Input).focus()

    # ── public helpers used by hooks ──────────────────────────────────

    def _refresh_sub_title(self) -> None:
        """Keep the subtitle aligned with the current verbose and model settings."""

        self.sub_title = (
            f"{self._model_name} | verbose {'on' if self._verbose else 'off'}"
        )

    def append_status(self, markup: str) -> None:
        """Append a status/tool message to the chat log (thread-safe)."""

        log = self.query_one("#chat-log", VerticalScroll)
        log.mount(Static(markup, classes="status-msg"))
        log.scroll_end(animate=False)

    def action_show_help(self) -> None:
        """Push the help modal screen."""

        self.push_screen(HelpModal())

    def record_tool_result(self, tool_name: str, raw_output: str) -> None:
        """Feed one successful tool result into the app-owned citation registry."""

        self._citation_registry.ingest_tool_result(tool_name, raw_output)

    def maybe_append_tool_call_preview(
        self, tool_name: str, arguments: str | None
    ) -> None:
        """Append a verbose preview for tool calls when useful."""

        if not self._verbose:
            return
        preview = _format_tool_call_preview(tool_name, arguments)
        if preview is None:
            return
        self.append_status(f"[dim]{escape(preview)}[/]")

    def begin_llm_activity(self) -> None:
        """Create the live status widget for the next LLM phase."""

        self.finish_llm_activity()

        log = self.query_one("#chat-log", VerticalScroll)
        widget = Static("[dim]Thinking…[/]", classes="reasoning-msg")
        log.mount(widget)
        log.scroll_end(animate=False)

        self._active_llm_widget = widget
        self._active_llm_text = ""
        self._active_llm_has_content = False
        self._active_llm_kind = None

    def _reasoning_display_enabled(self) -> bool:
        """Whether this run should try to display model reasoning details."""

        return self._verbose and not _is_openai_first_party_base_url(self._base_url)

    def _update_active_llm_widget(self) -> None:
        """Refresh the live LLM status widget from the accumulated buffer."""

        if self._active_llm_widget is None:
            return
        if not self._active_llm_has_content:
            self._active_llm_widget.update("[dim]Thinking…[/]")
            return
        self._active_llm_widget.update(f"[dim]{escape(self._active_llm_text)}[/]")

    def append_reasoning_delta(self, delta: str, *, kind: str) -> None:
        """Append streamed reasoning text to the live LLM widget."""

        if not self._reasoning_display_enabled():
            return
        if not delta:
            return
        if self._active_llm_widget is None:
            self.begin_llm_activity()

        clean_delta = delta.replace("\r\n", "\n")
        if not self._active_llm_has_content:
            label = "Summary" if kind == "summary" else "Reasoning"
            self._active_llm_text = f"{label}:\n{clean_delta}"
        elif self._active_llm_kind != kind:
            label = "Summary" if kind == "summary" else "Reasoning"
            self._active_llm_text += f"\n\n{label}:\n{clean_delta}"
        else:
            self._active_llm_text += clean_delta

        self._active_llm_has_content = True
        self._active_llm_kind = kind
        self._update_active_llm_widget()

    def set_reasoning_snapshot_if_empty(self, snapshot: str, *, kind: str) -> None:
        """Populate the live LLM widget from a final reasoning item when needed."""

        if not self._reasoning_display_enabled():
            return
        if self._active_llm_has_content:
            return
        if not snapshot.strip():
            return

        self.append_reasoning_delta(snapshot, kind=kind)

    def finish_llm_activity(self) -> None:
        """Release the current live LLM widget reference."""

        self._active_llm_widget = None
        self._active_llm_text = ""
        self._active_llm_has_content = False
        self._active_llm_kind = None

    def _append_user(self, text: str) -> None:
        log = self.query_one("#chat-log", VerticalScroll)
        log.mount(Static(f"[bold]You:[/] {escape(text)}", classes="user-msg"))
        log.scroll_end(animate=False)

    def _append_assistant(self, text: str) -> None:
        """Append the assistant message with resolved citations.

        The assistant still writes plain text with inline citation markers.
        We resolve those markers here into numbered references and then render a
        small Markdown view. This keeps the citation format independent from
        Textual itself while giving the TUI clickable links.
        """

        log = self.query_one("#chat-log", VerticalScroll)
        rendered = self._citation_registry.render_text(text)
        log.mount(
            Markdown(
                self._assistant_markdown(rendered.text, rendered.references),
                classes="assistant-msg",
            )
        )
        log.scroll_end(animate=False)

    def _assistant_markdown(
        self,
        body_text: str,
        references: list[CitationReference],
    ) -> str:
        """Build a Markdown view of one assistant message plus its citations."""

        sections = [f"**Agent**\n\n{body_text}"]
        if references:
            source_lines = ["**Sources**", ""]
            for reference in references:
                source_lines.append(self._reference_markdown(reference))
            sections.append("\n".join(source_lines))
        return "\n\n".join(sections)

    def _reference_markdown(self, reference: CitationReference) -> str:
        """Render one numbered citation for the Textual Markdown widget."""

        entry = reference.entry
        if entry.kind == "story":
            parts = [
                f"{reference.number}. Story `{entry.item_id}`",
                f"[HN discussion]({entry.hn_url})",
            ]
            if entry.source_url:
                parts.append(f"[source]({entry.source_url})")
            if entry.title:
                return f"{parts[0]}: {entry.title} ({', '.join(parts[1:])})"
            return f"{parts[0]} ({', '.join(parts[1:])})"

        author = entry.author or "unknown author"
        story_suffix = (
            f" on story `{entry.story_id}`" if entry.story_id is not None else ""
        )
        return (
            f"{reference.number}. Comment `{entry.item_id}` by {author}{story_suffix} "
            f"([HN permalink]({entry.hn_url}))"
        )

    # ── input handling ────────────────────────────────────────────────

    def _reset_conversation(self) -> None:
        """Clear chat log and conversation history."""

        log = self.query_one("#chat-log", VerticalScroll)
        log.remove_children()
        self._replace_conversation_session()
        self._citation_registry.clear()
        self.finish_llm_activity()
        self.append_status("[dim]Conversation reset.[/]")

    def _replace_conversation_session(self) -> None:
        """Start a brand-new SDK conversation session and close the old one."""

        old_session = self._conversation_session
        self._conversation_session = _new_conversation_session()
        old_session.close()

    def close_conversation_session(self) -> None:
        """Release the SDK session backing the current TUI conversation."""

        self._conversation_session.close()

    def _set_verbose(self, verbose: bool) -> None:
        """Toggle verbose reasoning display mode."""

        self._verbose = verbose
        self._refresh_sub_title()
        state = "enabled" if verbose else "disabled"
        self.append_status(f"[dim]Verbose reasoning {state}.[/]")

    def _set_model(self, model_name: str) -> None:
        """Switch the agent to a different model.

        Pass ``"default"`` to reset to the built-in default.
        """

        if model_name.lower() == "default":
            self._model_name = DEFAULT_MODEL
        else:
            self._model_name = model_name
        self._agent.model = self._model_name
        self._refresh_sub_title()
        self.append_status(f"[dim]Model set to ``{self._model_name}``.[/]")

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
        if user_text == "/help":
            self.action_show_help()
            return

        verbose_toggle = _parse_verbose_command(user_text)
        if verbose_toggle is not None:
            self._set_verbose(verbose_toggle)
            return
        if user_text.lower().startswith("/verbose"):
            self.append_status(
                f"[dim]Usage: {_VERBOSE_ON_COMMAND} or {_VERBOSE_OFF_COMMAND}[/]"
            )
            return

        model_match = _parse_model_command(user_text)
        if model_match is not None:
            self._set_model(model_match)
            return

        event.input.disabled = True
        self._append_user(user_text)
        self._run_agent(user_text)

    @work(exclusive=True, thread=False)
    async def _run_agent(self, user_text: str) -> None:
        """Run the agent in a Textual async worker (stays on the event loop)."""

        if self._hooks:
            self._hooks.reset()
        self._agent_context.turn_state.reset()

        turn_started_at = time.perf_counter()

        try:
            result = _start_streamed_turn(
                agent=self._agent,
                user_text=user_text,
                agent_context=self._agent_context,
                hooks=self._hooks,
                verbose=self._verbose,
                base_url=self._base_url,
                conversation_session=self._conversation_session,
            )

            async for event in result.stream_events():
                if isinstance(event, RawResponsesStreamEvent):
                    data = event.data
                    if isinstance(data, ResponseReasoningSummaryTextDeltaEvent):
                        self.append_reasoning_delta(data.delta, kind="summary")
                    elif isinstance(data, ResponseReasoningTextDeltaEvent):
                        self.append_reasoning_delta(data.delta, kind="reasoning")
                elif isinstance(event, RunItemStreamEvent):
                    if (
                        event.name == "tool_called"
                        and event.item.type == "tool_call_item"
                    ):
                        tool_name, arguments = _extract_tool_call_name_and_arguments(
                            event.item
                        )
                        self.maybe_append_tool_call_preview(
                            tool_name or "",
                            arguments,
                        )
                        continue
                    if (
                        event.name != "reasoning_item_created"
                        or event.item.type != "reasoning_item"
                    ):
                        continue
                    summary_parts = [
                        part.text for part in event.item.raw_item.summary if part.text
                    ]
                    content_parts = [
                        part.text
                        for part in (event.item.raw_item.content or [])
                        if part.text
                    ]
                    if summary_parts:
                        self.set_reasoning_snapshot_if_empty(
                            "\n\n".join(summary_parts),
                            kind="summary",
                        )
                    elif content_parts:
                        self.set_reasoning_snapshot_if_empty(
                            "\n\n".join(content_parts),
                            kind="reasoning",
                        )

            # Extract final text output
            output = result.final_output_as(str)
            self._append_assistant(output)

            if self._verbose:
                elapsed_seconds = time.perf_counter() - turn_started_at
                metrics = _collect_turn_metrics(
                    result.raw_responses,
                    elapsed_seconds=elapsed_seconds,
                )
                self.append_status(f"[dim]{escape(_format_turn_metrics(metrics))}[/]")

        except _ToolFailureAbort as exc:
            self.append_status(f"[bold red]{escape(str(exc))}[/]")
        except Exception as exc:  # noqa: BLE001 - the TUI must restore input after any run failure
            tb = traceback.format_exception(exc)
            self.append_status(
                f"[bold red]Error:[/] {escape(str(exc))}\n{escape(''.join(tb))}"
            )
        finally:
            self.finish_llm_activity()
            prompt = self.query_one("#prompt-input", Input)
            prompt.disabled = False
            prompt.focus()
