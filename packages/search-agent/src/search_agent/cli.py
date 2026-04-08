"""CLI entry point for search-agent with a Textual TUI."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import date
import time
import traceback
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

from agents import (
    Agent,
    ModelResponse,
    ModelSettings,
    RawResponsesStreamEvent,
    RunContextWrapper,
    RunHooks,
    RunItemStreamEvent,
    Runner,
    SQLiteSession,
    set_default_openai_client,
    set_tracing_disabled,
)
import json

from openai import AsyncOpenAI
from openai.types.responses import (
    ResponseReasoningSummaryTextDeltaEvent,
    ResponseReasoningTextDeltaEvent,
)
from openai.types.shared import Reasoning
from rich.markup import escape
from textual import work
from textual.app import App, ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import Footer, Header, Input, Markdown, Static

from search_agent.citations import CitationReference, CitationRegistry
from search_agent.runtime_context import (
    SearchAgentContext,
    build_search_agent_context,
    dispose_search_agent_context,
)
from search_agent.tools import fetch_stories, fetch_top_comments, fetch_top_stories_for_date


def _parse_system_date_override(raw_value: str) -> date:
    """Parse a CLI date override used to spoof the agent's notion of today.

    We accept either full ISO dates (``YYYY-MM-DD``) or a bare year such as
    ``1862``/``2029`` for playful prompt experiments. Bare years expand to
    January 1st of that year so downstream date-aware tools remain coherent.
    """

    clean = raw_value.strip()
    if len(clean) == 4 and clean.isdigit():
        return date(int(clean), 1, 1)

    try:
        return date.fromisoformat(clean)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "--system-date must be YYYY-MM-DD or a bare YYYY year"
        ) from exc


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
    parser.add_argument(
        "--system-date",
        type=_parse_system_date_override,
        default=None,
        help=(
            "Override the date shown to the model and used as the default "
            "for 'today' in date-based tools. Accepts YYYY-MM-DD or a bare "
            "YYYY year such as 1862 or 2029."
        ),
    )
    return parser.parse_args(argv)


def _agent_instructions(
    ctx: RunContextWrapper[SearchAgentContext],
    _agent: Agent[SearchAgentContext],
) -> str:
    _ = ctx

    today = ctx.context.current_date.isoformat()
    return (
        "You are a research assistant answering questions from a mirrored Hacker News database.\n\n"
        f"Today's date is **{today}**.\n\n"
        "## Tools\n"
        "- **fetch_stories**: full-text search over HN story titles and URLs. Usually pass "
        "one query string, but you may pass a list of up to 5 queries when comparing nearby "
        "phrasings in one tool call. Supports optional filters: min_score, min_date, "
        "max_date, include_domains, exclude_domains.\n"
        "- **fetch_top_stories_for_date**: top stories by score for a single calendar date. "
        "No text query needed — just pass a date (defaults to today). Great for 'what happened "
        "on Monday?' or 'top stories yesterday'.\n"
        "- **fetch_top_comments**: retrieve top-level comments for a known story ID. Usually "
        "pass one story ID, but you may pass a list of up to 5 story IDs when checking several "
        "candidate stories in one tool call.\n\n"
        "## Citations\n"
        "- Tool results include lightweight cursor fields such as `story:123` and "
        "`comment:456`.\n"
        "- When you rely on a specific story or comment, cite it inline by copying that cursor "
        "exactly inside full-width brackets: `【story:123】`, `【comment:456】`.\n"
        "- Never invent cursors, and do not cite plain URLs when a story/comment cursor is "
        "available.\n"
        "- It is fine to attach multiple citations to one sentence, for example "
        "`This thread focused on pricing【story:123】【comment:456】`.\n\n"
        "## Search strategy\n"
        "Before searching, consider whether the topic is **evergreen** or **time-bound**:\n"
        "- `fetch_stories` uses fairly classical PostgreSQL keyword search over titles and URLs, "
        "not broad semantic retrieval. Do not assume pgvector-like behavior: it will not reliably "
        "understand paraphrases, latent topic similarity, or long natural-language descriptions of "
        "what the user means. Long prompts, highly specific composite phrasings, and 'describe the "
        "thing in prose' searches may miss obvious matches.\n"
        "- As a sanity-check fallback, make sure at least one intentionally dumb named-entity or "
        "generic anchor lookup is in the mix whenever possible: company names, product names, "
        "person names, repo names, acronyms, or short topic labels. This is often the best way to "
        "ground the search space before trusting narrower phrasings.\n"
        "- For many questions, have 1-2 broad anchor queries in the mix even if you also test "
        "narrower phrasings. If the topic is important or the first searches are sparse, try the "
        "simplest named-entity lookup you can think of before concluding the corpus lacks coverage.\n"
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
        "- When results are noisy, use exclude_domains to filter out low-signal sources.\n"
        "- Headlines are often vague or misleading. For important or high-signal stories, prefer "
        "opening top comments and grounding your answer in those discussions.\n"
        "- Prefer reading comments on a few higher-signal stories over building an answer from a "
        "large pile of shallow, low-score stories.\n\n"
        "Use these filters judiciously — most simple queries need no filters at all. "
        "Prefer one query or one story ID by default, and batch only when it meaningfully "
        "reduces back-and-forth while keeping the output manageable."
    )


def _summarize_story_batches(story_batches: list[dict]) -> str:
    """Summarize one or more story-search batches for the status log."""

    if not story_batches:
        return "no story searches returned"

    if len(story_batches) == 1:
        batch = story_batches[0]
        results = batch.get("results", [])
        query = escape(str(batch.get("query", "?")))
        if not results:
            return f'no stories for "{query}"'
        titles = ", ".join(
            escape(str(result.get("title", "?"))[:60]) for result in results[:4]
        )
        suffix = f" (+{len(results) - 4} more)" if len(results) > 4 else ""
        return f'{len(results)} stories for "{query}": {titles}{suffix}'

    parts = []
    for batch in story_batches[:3]:
        query = escape(str(batch.get("query", "?")))
        result_count = len(batch.get("results", []))
        parts.append(f'"{query}" ({result_count})')
    suffix = f", +{len(story_batches) - 3} more queries" if len(story_batches) > 3 else ""
    return f"{len(story_batches)} story searches: {', '.join(parts)}{suffix}"


def _summarize_comment_batches(comment_batches: list[dict]) -> str:
    """Summarize one or more comment fetch batches for the status log."""

    if not comment_batches:
        return "no comment lookups returned"

    if len(comment_batches) == 1:
        batch = comment_batches[0]
        total = batch.get("total_top_level_comments", 0)
        returned = batch.get("returned", 0)
        story_id = batch.get("story_id", "?")
        return f"{returned}/{total} comments for story {story_id}"

    parts = []
    for batch in comment_batches[:3]:
        story_id = batch.get("story_id", "?")
        returned = batch.get("returned", 0)
        total = batch.get("total_top_level_comments", 0)
        parts.append(f"{story_id} ({returned}/{total})")
    suffix = f", +{len(comment_batches) - 3} more stories" if len(comment_batches) > 3 else ""
    return f"{len(comment_batches)} comment lookups: {', '.join(parts)}{suffix}"


def _summarize_tool_result(tool_name: str, raw: str) -> str:
    """Turn raw tool JSON into a compact one-liner for the status log."""
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return escape(raw[:200])

    if tool_name == "fetch_stories":
        story_batches = data.get("queries")
        if isinstance(story_batches, list):
            return _summarize_story_batches(story_batches)
        return _summarize_story_batches([data])

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
        comment_batches = data.get("stories")
        if isinstance(comment_batches, list):
            return _summarize_comment_batches(comment_batches)
        return _summarize_comment_batches([data])

    return escape(raw[:200])


_MAX_CONSECUTIVE_TOOL_FAILURES = 3
_VERBOSE_ON_COMMAND = "/verbose on"
_VERBOSE_OFF_COMMAND = "/verbose off"


def _is_openai_first_party_base_url(base_url: str) -> bool:
    """Return whether the configured API base URL points at OpenAI first-party.

    This is an inference based on the hostname. We keep the rule narrow on
    purpose so local gateways and custom OpenAI-compatible providers do not get
    treated as first-party by accident.
    """

    host = urlparse(base_url).hostname or ""
    return host == "api.openai.com" or host.endswith(".openai.com")


def _build_model_settings(base_url: str, *, verbose: bool) -> ModelSettings:
    """Build per-run model settings for the current provider and UI mode.

    OpenAI first-party requests intentionally leave reasoning metadata off,
    per the requested UX. For local/OpenAI-compatible providers, when verbose
    mode is enabled we ask for reasoning summaries using the standard
    Responses API reasoning field. Providers that do not support it may ignore
    it, while providers that do support it can emit summary deltas that the TUI
    surfaces live.
    """

    if not verbose or _is_openai_first_party_base_url(base_url):
        return ModelSettings()

    return ModelSettings(reasoning=Reasoning(summary="auto"))


def _parse_verbose_command(user_text: str) -> bool | None:
    """Parse a `/verbose on|off` command into the desired state."""

    normalized = " ".join(user_text.strip().lower().split())
    if normalized == _VERBOSE_ON_COMMAND:
        return True
    if normalized == _VERBOSE_OFF_COMMAND:
        return False
    return None


def _new_conversation_session() -> SQLiteSession:
    """Create a fresh SDK-managed conversation memory for one TUI chat thread.

    The Agents SDK docs recommend `session=` for ordinary multi-turn chat apps:
    the runner reloads prior items before each turn and persists the exact new
    user/assistant/tool items it generated after the turn finishes. That is
    less fragile than manually stitching together `result.to_input_list()`
    across turns in UI code.
    """

    return SQLiteSession(f"search-agent-{uuid4().hex}")


def _start_streamed_turn(
    *,
    agent: Agent[SearchAgentContext],
    user_text: str,
    agent_context: SearchAgentContext,
    hooks: _TUIHooks | None,
    verbose: bool,
    base_url: str,
    conversation_session: SQLiteSession,
):
    """Start one streamed turn using SDK-managed session history.

    We intentionally pass only the *new* user text here. The session supplies
    prior turns on the SDK side, which keeps the request pattern aligned with
    the local Agents SDK documentation for multi-turn conversations.
    """

    agent.model_settings = _build_model_settings(
        base_url,
        verbose=verbose,
    )

    return Runner.run_streamed(
        agent,
        input=user_text,
        context=agent_context,
        hooks=hooks,
        max_turns=10,
        session=conversation_session,
    )


@dataclass(frozen=True)
class _TurnMetrics:
    """Renderer-neutral summary of one completed agent turn.

    ``conversation_tokens`` reflects the input-token count reported for the
    last upstream model request in the turn. That is the closest available
    proxy for "current conversation length" when providers expose Responses API
    usage data.
    """

    elapsed_seconds: float
    conversation_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    cached_tokens: int | None = None
    reasoning_tokens: int | None = None


def _collect_turn_metrics(
    raw_responses: Sequence[ModelResponse],
    *,
    elapsed_seconds: float,
) -> _TurnMetrics:
    """Collect elapsed time and best-effort usage details for one turn.

    We intentionally use the *last* model response in the run because that
    final request sees the most complete conversation state after any tool
    calls. If a provider omits usage, we still report elapsed time.
    """

    if not raw_responses:
        return _TurnMetrics(elapsed_seconds=elapsed_seconds)

    last_response = raw_responses[-1]
    usage = last_response.usage
    cached_tokens = usage.input_tokens_details.cached_tokens
    reasoning_tokens = usage.output_tokens_details.reasoning_tokens

    return _TurnMetrics(
        elapsed_seconds=elapsed_seconds,
        conversation_tokens=usage.input_tokens if usage.input_tokens > 0 else None,
        output_tokens=usage.output_tokens if usage.output_tokens > 0 else None,
        total_tokens=usage.total_tokens if usage.total_tokens > 0 else None,
        cached_tokens=cached_tokens if cached_tokens > 0 else None,
        reasoning_tokens=reasoning_tokens if reasoning_tokens > 0 else None,
    )


def _format_turn_metrics(metrics: _TurnMetrics) -> str:
    """Format a concise verbose-only status line for one completed turn."""

    parts = [f"turn {metrics.elapsed_seconds:.2f}s"]
    if metrics.conversation_tokens is not None:
        parts.append(f"context {metrics.conversation_tokens:,} tok")
    if metrics.cached_tokens is not None:
        parts.append(f"cached {metrics.cached_tokens:,}")
    if metrics.output_tokens is not None:
        parts.append(f"output {metrics.output_tokens:,}")
    if metrics.reasoning_tokens is not None:
        parts.append(f"reasoning {metrics.reasoning_tokens:,}")
    if metrics.total_tokens is not None:
        parts.append(f"total {metrics.total_tokens:,}")
    return " | ".join(parts)


def _format_tool_call_preview(tool_name: str, arguments: str | None) -> str | None:
    """Return a concise verbose preview of an imminent tool call.

    The current UX need is to expose what the model is searching for. We keep
    this formatter narrow on purpose so it can later be reused by a web/API
    client without depending on Textual widgets.
    """

    if tool_name != "fetch_stories" or not arguments:
        return None

    try:
        payload = json.loads(arguments)
    except (json.JSONDecodeError, TypeError):
        return None

    raw_query = payload.get("query")
    if isinstance(raw_query, str):
        queries = [raw_query.strip()]
    elif isinstance(raw_query, list):
        queries = [
            candidate.strip()
            for candidate in raw_query
            if isinstance(candidate, str) and candidate.strip()
        ]
    else:
        return None

    if not queries:
        return None

    preview = ", ".join(f'"{query}"' for query in queries[:5])
    if len(queries) == 1:
        return f'search query: {preview}'
    return f"search queries ({len(queries)}): {preview}"


def _extract_tool_call_name_and_arguments(item: object) -> tuple[str | None, str | None]:
    """Extract a tool-call name and argument string from a streamed run item.

    The Agents SDK has helper properties on ``ToolCallItem``, but we keep this
    logic tolerant of minor SDK shape differences by reading the dataclass field
    and underlying raw item directly.
    """

    raw_item = _safe_getattr(item, "raw_item")
    tool_name = _safe_getattr(item, "tool_name")
    if not isinstance(tool_name, str) or not tool_name:
        tool_name = _extract_tool_name_from_raw_item(raw_item)

    arguments = _extract_tool_arguments_from_raw_item(raw_item)
    return tool_name, arguments


def _extract_tool_name_from_raw_item(raw_item: object) -> str | None:
    """Best-effort extraction of the tool name from a raw SDK item."""

    candidate: object | None = None
    if isinstance(raw_item, dict):
        candidate = raw_item.get("name") or raw_item.get("tool_name")
    else:
        candidate = _safe_getattr(raw_item, "name") or _safe_getattr(raw_item, "tool_name")

    return candidate if isinstance(candidate, str) and candidate else None


def _extract_tool_arguments_from_raw_item(raw_item: object) -> str | None:
    """Best-effort extraction of tool arguments from a raw SDK item."""

    candidate: object | None = None
    if isinstance(raw_item, dict):
        candidate = raw_item.get("arguments")
        if candidate is None:
            candidate = raw_item.get("params") or raw_item.get("input")
    else:
        candidate = _safe_getattr(raw_item, "arguments")
        if candidate is None:
            candidate = _safe_getattr(raw_item, "params") or _safe_getattr(raw_item, "input")

    if candidate is None:
        return None
    if isinstance(candidate, str):
        return candidate
    try:
        return json.dumps(candidate)
    except (TypeError, ValueError):
        return str(candidate)


def _safe_getattr(value: object, attr_name: str) -> Any | None:
    """Return ``getattr`` if present, but tolerate SDK items without the field."""

    try:
        return getattr(value, attr_name)
    except AttributeError:
        return None


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
    .reasoning-msg {
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
        base_url: str,
        hooks: _TUIHooks | None = None,
    ) -> None:
        super().__init__()
        self._agent = agent
        self._agent_context = agent_context
        self._base_url = base_url
        self._hooks = hooks
        self._conversation_session = _new_conversation_session()
        self._verbose = True
        self._citation_registry = CitationRegistry()
        self._active_llm_widget: Static | None = None
        self._active_llm_text = ""
        self._active_llm_has_content = False
        self._active_llm_kind: str | None = None

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
        """Keep the subtitle aligned with the current verbose setting."""

        model_name = self._agent.model or ""
        self.sub_title = f"{model_name} | verbose {'on' if self._verbose else 'off'}"

    def append_status(self, markup: str) -> None:
        """Append a status/tool message to the chat log (thread-safe)."""
        log = self.query_one("#chat-log", VerticalScroll)
        log.mount(Static(markup, classes="status-msg"))
        log.scroll_end(animate=False)

    def record_tool_result(self, tool_name: str, raw_output: str) -> None:
        """Feed one successful tool result into the app-owned citation registry."""

        self._citation_registry.ingest_tool_result(tool_name, raw_output)

    def maybe_append_tool_call_preview(self, tool_name: str, arguments: str | None) -> None:
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
            Markdown(self._assistant_markdown(rendered.text, rendered.references), classes="assistant-msg")
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
        story_suffix = f" on story `{entry.story_id}`" if entry.story_id is not None else ""
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

        verbose_toggle = _parse_verbose_command(user_text)
        if verbose_toggle is not None:
            self._set_verbose(verbose_toggle)
            return
        if user_text.lower().startswith("/verbose"):
            self.append_status(
                f"[dim]Usage: {_VERBOSE_ON_COMMAND} or {_VERBOSE_OFF_COMMAND}[/]"
            )
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
                    if event.name == "tool_called" and event.item.type == "tool_call_item":
                        tool_name, arguments = _extract_tool_call_name_and_arguments(event.item)
                        self.maybe_append_tool_call_preview(
                            tool_name or "",
                            arguments,
                        )
                        continue
                    if event.name != "reasoning_item_created" or event.item.type != "reasoning_item":
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
        except Exception as exc:
            tb = traceback.format_exception(exc)
            self.append_status(
                f"[bold red]Error:[/] {escape(str(exc))}\n{escape(''.join(tb))}"
            )
        finally:
            self.finish_llm_activity()
            prompt = self.query_one("#prompt-input", Input)
            prompt.disabled = False
            prompt.focus()


async def _run(args: argparse.Namespace) -> None:
    """Run the Textual TUI with one shared, persistent repository context."""

    context = build_search_agent_context(
        args.database_url,
        current_date_override=args.system_date,
    )

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

    app = SearchAgentApp(agent=agent, agent_context=context, base_url=args.base_url)
    app._hooks = _TUIHooks(app)

    try:
        await app.run_async()
    finally:
        app.close_conversation_session()
        dispose_search_agent_context(context)


def main(argv: Sequence[str] | None = None) -> int:
    """Synchronous console-script entrypoint used by `search-agent`."""

    args = parse_args(argv)
    asyncio.run(_run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
