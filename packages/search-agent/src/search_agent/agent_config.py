"""Agent configuration and session management helpers.

Contains the agent instructions callable, model settings builder, and the
thin wrapper around ``Runner.run_streamed`` that sets up each turn.
"""

from __future__ import annotations

from urllib.parse import urlparse
from uuid import uuid4

from agents import (
    Agent,
    ModelSettings,
    RunHooks,
    Runner,
    SQLiteSession,
    RunContextWrapper,
)
from openai.types.shared import Reasoning

from search_agent.runtime_context import SearchAgentContext


_VERBOSE_ON_COMMAND = "/verbose on"
_VERBOSE_OFF_COMMAND = "/verbose off"

_MODEL_COMMAND_PREFIXES = ("/model ", "/m ")


# ── agent instructions ────────────────────────────────────────────────────


def _agent_instructions(
    ctx: RunContextWrapper[SearchAgentContext],
    _agent: Agent[SearchAgentContext],
) -> str:
    """Dynamic system prompt injecting today's date and search guidance."""

    _ = ctx

    today = ctx.context.current_date.isoformat()
    return (
       "You are a research assistant answering questions from a mirrored Hacker News database.\n\n"
         f"Today's date is **{today}**.\n\n"
         "## Knowledge cutoff\n"
         "Your training data has a knowledge cutoff. The user may ask about events, products, or "
         "people that post-date your cutoff. Do not attempt to correct or second-guess the user "
         "on these topics — defer to their framing. Your job is to search the HN database for "
         "relevant discussions and report what you find, not to fact-check whether an entity "
         "exists in the real world.\n\n"
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


# ── provider helpers ──────────────────────────────────────────────────────


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


# ── verbose command parser ────────────────────────────────────────────────


def _parse_verbose_command(user_text: str) -> bool | None:
    """Parse a ``/verbose on|off`` command into the desired state."""

    normalized = " ".join(user_text.strip().lower().split())
    if normalized == _VERBOSE_ON_COMMAND:
        return True
    if normalized == _VERBOSE_OFF_COMMAND:
        return False
    return None


def _parse_model_command(user_text: str) -> str | None:
    """Parse a ``/model <name>`` or ``/m <name>`` command.

    Returns the model name string, or ``None`` if the input is not a model
    command.  The special value ``"default"`` resets to the built-in default.
    """

    lower = user_text.lower()
    for prefix in _MODEL_COMMAND_PREFIXES:
        if lower.startswith(prefix):
            name = user_text[len(prefix):].strip()
            if name:
                return name
    return None


# ── session management ────────────────────────────────────────────────────


def _new_conversation_session() -> SQLiteSession:
    """Create a fresh SDK-managed conversation memory for one TUI chat thread.

    The Agents SDK docs recommend ``session=`` for ordinary multi-turn chat
    apps: the runner reloads prior items before each turn and persists the
    exact new user/assistant/tool items it generated after the turn finishes.
    That is less fragile than manually stitching together
    ``result.to_input_list()`` across turns in UI code.
    """

    return SQLiteSession(f"search-agent-{uuid4().hex}")


# ── turn runner ───────────────────────────────────────────────────────────


def _start_streamed_turn(
    *,
    agent: Agent[SearchAgentContext],
    user_text: str,
    agent_context: SearchAgentContext,
    hooks: RunHooks[SearchAgentContext] | None,
    verbose: bool,
    base_url: str,
    conversation_session: SQLiteSession,
):
    """Start one streamed turn using SDK-managed session history.

    We intentionally pass only the *new* user text here. The session supplies
    prior turns on the SDK side, which keeps the request pattern aligned with
    the local Agents SDK documentation for multi-turn conversations.
    """

    agent.model_settings = _build_model_settings(base_url, verbose=verbose)

    return Runner.run_streamed(
        agent,
        input=user_text,
        context=agent_context,
        hooks=hooks,
        max_turns=10,
        session=conversation_session,
    )
