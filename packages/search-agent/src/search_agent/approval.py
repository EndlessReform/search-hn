"""Shared language and reply parsing for user approval prompts.

Approval requests arise for different reasons, but the TUI should teach one
interaction: approve with ``A``, reject with ``R``, or type a correction.  The
request-specific policy lives in an immutable prompt specification rather than
being scattered across event handlers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


ApprovalReply = Literal["approve", "reject", "guidance"]
"""How the TUI interprets input while any approval request is pending."""


@dataclass(frozen=True)
class ApprovalPrompt:
    """User-visible explanation of what one approval decision controls."""

    title: str
    explanation: str
    approve_meaning: str
    reject_meaning: str


def classify_approval_reply(user_text: str) -> ApprovalReply:
    """Reserve only explicit A/R replies; treat all other text as guidance."""

    normalized = user_text.strip().lower()
    if normalized in {"a", "approve"}:
        return "approve"
    if normalized in {"r", "reject"}:
        return "reject"
    return "guidance"


BUDGET_APPROVAL_PROMPT = ApprovalPrompt(
    title="Research budget requested",
    explanation="The agent has reached this pass's turn limit.",
    approve_meaning="grant one additional research pass",
    reject_meaning="summarize the evidence gathered so far",
)


def comment_url_approval_prompt(url: str) -> ApprovalPrompt:
    """Explain why a comment-only destination receives a human checkpoint."""

    return ApprovalPrompt(
        title="Comment link approval required",
        explanation=(
            "This URL appeared only inside a user-authored HN comment, not as a "
            f"top-level submission: {url}"
        ),
        approve_meaning="open this exact URL once",
        reject_meaning="do not open it and continue with other evidence",
    )
