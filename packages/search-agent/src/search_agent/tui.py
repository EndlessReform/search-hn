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
    RunState,
    SQLiteSession,
    ToolApprovalItem,
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
    _resume_streamed_turn,
    _start_rejection_summary_turn,
    _start_streamed_turn,
)
from search_agent.approval import (
    BUDGET_APPROVAL_PROMPT,
    ApprovalPrompt,
    classify_approval_reply,
    comment_url_approval_prompt,
)
from search_agent.citations import CitationReference, CitationRegistry
from search_agent.hooks import _ToolFailureAbort, _TUIHooks
from search_agent.metrics import _collect_turn_metrics, _format_turn_metrics
from search_agent.model_config import ModelRuntime, ModelSelection
from search_agent.model_picker import ModelPickerModal
from search_agent.runtime_context import SearchAgentContext
from search_agent.tool_output import (
    _extract_tool_call_name_and_arguments,
    _format_tool_call_preview,
)

_DEFAULT_PROMPT_PLACEHOLDER = "Ask about Hacker News…"
_APPROVAL_PROMPT_PLACEHOLDER = "A approve · R reject · or type corrective guidance"


class ChatLog(VerticalScroll):
    """Scrollable transcript that can receive keyboard focus via Ctrl+B."""

    can_focus = True


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
- **/model** or **/m** — Choose a provider and model
- **/model &lt;name&gt;** — Use a preset alias or exact model ID
- **/model default** or **/m default** — Reset to the configured default preset

## Key bindings

- **Ctrl+C** — Quit
- **Ctrl+B** — Toggle focus between the transcript and prompt bar
- **Up/Down** — Recall earlier/later messages from this application run
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
        ("ctrl+b", "toggle_prompt_focus", "Prompt"),
        ("f1", "show_help", "Help"),
    ]

    def __init__(
        self,
        agent: Agent[SearchAgentContext],
        agent_context: SearchAgentContext,
        base_url: str,
        hooks: _TUIHooks | None = None,
        model_runtime: ModelRuntime | None = None,
    ) -> None:
        super().__init__()
        self._agent = agent
        self._agent_context = agent_context
        self._base_url = base_url
        self._model_runtime = model_runtime
        self._hooks = hooks
        self._conversation_session: SQLiteSession = _new_conversation_session()
        self._verbose = True
        self._citation_registry = CitationRegistry()
        self._active_llm_widget: Static | None = None
        self._active_llm_text = ""
        self._active_llm_has_content = False
        self._active_llm_kind: str | None = None
        self._model_name: str = agent.model or DEFAULT_MODEL
        self._provider_name = (
            model_runtime.provider.name if model_runtime is not None else "Local"
        )
        self._pending_run_state: RunState[SearchAgentContext] | None = None
        self._pending_tool_approval: ToolApprovalItem | None = None
        self._message_history: list[str] = []
        self._history_index: int | None = None
        self._history_draft = ""

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield ChatLog(id="chat-log")
        yield Input(placeholder=_DEFAULT_PROMPT_PLACEHOLDER, id="prompt-input")
        yield Footer()

    def on_mount(self) -> None:
        self.title = "HN Search Agent"
        self._refresh_sub_title()
        self.query_one("#prompt-input", Input).focus()
        self._show_web_extractor_status()

    # ── public helpers used by hooks ──────────────────────────────────

    def _refresh_sub_title(self) -> None:
        """Keep the subtitle aligned with the current verbose and model settings."""

        self.sub_title = (
            f"{self._provider_name} · {self._model_name} | "
            f"verbose {'on' if self._verbose else 'off'}"
        )

    def _show_web_extractor_status(self) -> None:
        """Report the fixed webpage extractor selection once at TUI startup."""

        service = self._agent_context.web_service
        if service is None:
            return
        if service.extractor is None:
            reason = service.extractor_error or "no usable local runtime"
            self.append_status(
                f"[bold yellow]Web extraction unavailable:[/] {escape(reason)}"
            )
            return
        self.append_status(
            "[dim]Web extraction:[/] "
            f"[green]{escape(service.extractor.name)}[/] via "
            f"{escape(service.extractor.runtime_source)}"
        )

    def append_status(self, markup: str) -> None:
        """Append a status/tool message to the chat log (thread-safe)."""

        log = self.query_one("#chat-log", VerticalScroll)
        log.mount(Static(markup, classes="status-msg"))
        log.scroll_end(animate=False)

    def action_show_help(self) -> None:
        """Push the help modal screen."""

        self.push_screen(HelpModal())

    def action_toggle_prompt_focus(self) -> None:
        """Toggle keyboard focus between the input bar and chat transcript."""

        prompt = self.query_one("#prompt-input", Input)
        log = self.query_one("#chat-log", ChatLog)
        if self.focused is prompt:
            log.focus()
        elif not prompt.disabled:
            prompt.focus()

    def on_key(self, event) -> None:
        """Navigate application-lifetime message history from the prompt bar."""

        prompt = self.query_one("#prompt-input", Input)
        if self.focused is not prompt or prompt.disabled:
            return
        if event.key == "up":
            event.stop()
            self._recall_older_message(prompt)
        elif event.key == "down":
            event.stop()
            self._recall_newer_message(prompt)

    def _remember_message(self, text: str) -> None:
        """Store one model-visible user message for this process lifetime.

        Consecutive duplicates add little value and make keyboard navigation
        feel sticky, so they collapse to one entry. ``/new`` intentionally does
        not clear this list: conversation state and input history have separate
        lifetimes.
        """

        if not self._message_history or self._message_history[-1] != text:
            self._message_history.append(text)

    def _reset_history_navigation(self) -> None:
        """Leave history-navigation mode after any input is submitted."""

        self._history_index = None
        self._history_draft = ""

    @staticmethod
    def _replace_prompt_value(prompt: Input, value: str) -> None:
        """Replace recalled text and place the editing cursor at its end."""

        prompt.value = value
        prompt.cursor_position = len(value)

    def _recall_older_message(self, prompt: Input) -> None:
        """Move toward older messages, preserving the unfinished current draft."""

        if not self._message_history:
            return
        if self._history_index is None:
            self._history_draft = prompt.value
            self._history_index = len(self._message_history) - 1
        elif self._history_index > 0:
            self._history_index -= 1
        self._replace_prompt_value(prompt, self._message_history[self._history_index])

    def _recall_newer_message(self, prompt: Input) -> None:
        """Move toward newer messages and eventually restore the saved draft."""

        if self._history_index is None:
            return
        if self._history_index < len(self._message_history) - 1:
            self._history_index += 1
            value = self._message_history[self._history_index]
        else:
            self._history_index = None
            value = self._history_draft
            self._history_draft = ""
        self._replace_prompt_value(prompt, value)

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
        self._agent_context.web_state.clear()
        self._agent_context.budget_state.clear()
        self._agent_context.tool_approval_feedback.clear()
        self._pending_run_state = None
        self._pending_tool_approval = None
        prompt = self.query_one("#prompt-input", Input)
        prompt.placeholder = _DEFAULT_PROMPT_PLACEHOLDER
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

    def _model_change_blocked(self) -> bool:
        """Prevent changing the transport beneath a resumable interrupted turn."""

        return (
            self._pending_run_state is not None
            or self._agent_context.budget_state.pending_request is not None
        )

    def _show_model_picker(self) -> None:
        """Open the provider/model modal and consume its result asynchronously."""

        if self._model_change_blocked():
            self.append_status(
                "[yellow]Finish the pending approval before changing models.[/]"
            )
            return
        if self._model_runtime is None:
            self.append_status(
                "[yellow]The model picker is unavailable in this run.[/]"
            )
            return
        self.push_screen(
            ModelPickerModal(
                self._model_runtime.config,
                self._model_runtime.selection,
            ),
            self._model_picker_dismissed,
        )

    def _model_picker_dismissed(self, selection: ModelSelection | None) -> None:
        """Schedule the client swap returned by a dismissed modal."""

        if selection is not None:
            self.run_worker(
                self._apply_model_selection(selection),
                group="model-switch",
                exclusive=True,
            )

    async def _apply_model_selection(self, selection: ModelSelection) -> None:
        """Apply a provider/model pair and report failures without breaking the TUI."""

        try:
            if self._model_runtime is not None:
                await self._model_runtime.activate(selection)
                provider = self._model_runtime.provider
                self._base_url = provider.base_url
                self._provider_name = provider.name
            self._model_name = selection.model
            self._agent.model = selection.model
        except Exception as exc:  # noqa: BLE001 - the TUI must remain usable
            self.append_status(
                f"[bold red]Could not switch model:[/] {escape(str(exc))}"
            )
            return
        self._refresh_sub_title()
        self.append_status(
            f"[dim]Using {escape(self._provider_name)} · {escape(self._model_name)}.[/]"
        )

    async def _set_model_from_command(self, model_name: str) -> None:
        """Resolve a preset alias, default, or raw model ID from a slash command."""

        if self._model_change_blocked():
            self.append_status(
                "[yellow]Finish the pending approval before changing models.[/]"
            )
            return
        if self._model_runtime is None:
            selection = ModelSelection("local", model_name)
        elif model_name.casefold() == "default":
            selection = self._model_runtime.config.default_selection()
        else:
            selection = self._model_runtime.config.resolve_preset(
                model_name
            ) or ModelSelection(
                self._model_runtime.selection.provider_id,
                model_name,
            )
        await self._apply_model_selection(selection)

    def _show_approval_prompt(self, prompt_spec: ApprovalPrompt) -> None:
        """Render the common A/R/correction interaction with policy-specific meaning."""

        self.append_status(
            f"[bold yellow]{escape(prompt_spec.title)}.[/] "
            f"{escape(prompt_spec.explanation)}\n"
            f"[bold](A)pprove[/] — {escape(prompt_spec.approve_meaning)}\n"
            f"[bold](R)eject[/] — {escape(prompt_spec.reject_meaning)}\n"
            "Type anything else to tell the agent what it is doing wrong."
        )
        self.query_one(
            "#prompt-input", Input
        ).placeholder = _APPROVAL_PROMPT_PLACEHOLDER

    def _show_budget_approval_prompt(self) -> None:
        """Show the budget-specific instance of the shared approval UI."""

        self._show_approval_prompt(BUDGET_APPROVAL_PROMPT)

    def _consume_budget_reply(self, user_text: str) -> tuple[str, bool]:
        """Resolve a pending budget request into a controlled next-turn prompt.

        Returns the text sent to the SDK and whether that turn must be limited
        to an evidence-only summary.
        """

        decision = classify_approval_reply(user_text)
        self._agent_context.budget_state.clear()
        self.query_one("#prompt-input", Input).placeholder = _DEFAULT_PROMPT_PLACEHOLDER

        if decision == "approve":
            return (
                "The user approved one additional research pass. Continue from "
                "the strongest unresolved lead you identified. Never retry a "
                "publisher URL marked comments_only; use its HN comments instead.",
                False,
            )
        if decision == "reject":
            return (
                "The user rejected the request for more research. Summarize the "
                "evidence already gathered and its limitations now.",
                True,
            )
        return (
            "The user is correcting your proposed research approach. Follow this "
            f"direction in the next pass:\n\n{user_text}",
            False,
        )

    def _pause_for_tool_approval(self, result) -> None:
        """Retain an interrupted SDK run and explain its first pending web call."""

        assert result.interruptions, "approval pause requires an interruption"
        approval = result.interruptions[0]
        tool_name, arguments = _extract_tool_call_name_and_arguments(approval)
        assert tool_name == "open_webpage", (
            f"unexpected approval request from tool {tool_name!r}"
        )
        preview = _format_tool_call_preview(tool_name, arguments)
        url = preview.removeprefix("webpage: ") if preview else "unknown URL"
        self._pending_run_state = result.to_state()
        self._pending_tool_approval = approval
        self._show_approval_prompt(comment_url_approval_prompt(url))

    def _consume_tool_approval_reply(
        self, user_text: str
    ) -> RunState[SearchAgentContext]:
        """Apply one A/R/correction response and return the resumable SDK state."""

        run_state = self._pending_run_state
        approval = self._pending_tool_approval
        assert run_state is not None and approval is not None, (
            "tool approval reply requires a pending run"
        )
        decision = classify_approval_reply(user_text)
        if decision == "approve":
            run_state.approve(approval)
        else:
            call_id = approval.call_id
            assert call_id, "tool approval item must expose a call ID"
            if decision == "reject":
                model_message = (
                    "The user rejected this webpage request. Do not open this URL; "
                    "continue with other available evidence."
                )
            else:
                model_message = (
                    "The user rejected this webpage request and supplied corrective "
                    f"guidance. Follow it: {user_text}"
                )
            self._agent_context.tool_approval_feedback.reject(call_id, model_message)
            run_state.reject(approval)

        self._pending_run_state = None
        self._pending_tool_approval = None
        self.query_one("#prompt-input", Input).placeholder = _DEFAULT_PROMPT_PLACEHOLDER
        return run_state

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        user_text = event.value.strip()
        if not user_text:
            return
        event.input.value = ""
        self._reset_history_navigation()

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

        if user_text.strip().casefold() in {"/model", "/m"}:
            self._show_model_picker()
            return

        model_match = _parse_model_command(user_text)
        if model_match is not None:
            await self._set_model_from_command(model_match)
            return

        summary_only = False
        agent_input = user_text
        if self._pending_run_state is not None:
            agent_input = self._consume_tool_approval_reply(user_text)
        elif self._agent_context.budget_state.pending_request is not None:
            agent_input, summary_only = self._consume_budget_reply(user_text)

        self._remember_message(user_text)
        event.input.disabled = True
        self._append_user(user_text)
        self._run_agent(agent_input, summary_only)

    @work(exclusive=True, thread=False)
    async def _run_agent(
        self,
        user_text: str | RunState[SearchAgentContext],
        summary_only: bool = False,
    ) -> None:
        """Run the agent in a Textual async worker (stays on the event loop)."""

        resuming_approval = isinstance(user_text, RunState)
        if not resuming_approval:
            if self._hooks:
                self._hooks.reset()
            self._agent_context.turn_state.reset()
            self._agent_context.web_state.reset_inspection_budget()

        turn_started_at = time.perf_counter()

        try:
            if resuming_approval:
                result = _resume_streamed_turn(
                    agent=self._agent,
                    run_state=user_text,
                    hooks=self._hooks,
                    conversation_session=self._conversation_session,
                    verbose=self._verbose,
                    base_url=self._base_url,
                )
            else:
                start_turn = (
                    _start_rejection_summary_turn
                    if summary_only
                    else _start_streamed_turn
                )
                result = start_turn(
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

            if result.interruptions:
                self._pause_for_tool_approval(result)
                return

            # Extract final text output
            output = result.final_output_as(str)
            self._append_assistant(output)
            if self._agent_context.budget_state.pending_request is not None:
                self._show_budget_approval_prompt()

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
