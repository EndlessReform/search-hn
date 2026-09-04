"""Conversation-scoped webpage retrieval and extraction.

The package is deliberately independent of the Agents SDK.  The SDK tool and
the standalone diagnostic CLI both call the same :class:`WebPageService`, which
keeps authorization and network behavior testable without running a model.
"""

from search_agent.web.extractor import build_local_defuddle_extractor
from search_agent.web.policy import PublisherPolicy
from search_agent.web.service import WebPageService
from search_agent.web.state import WebConversationState

__all__ = [
    "PublisherPolicy",
    "WebConversationState",
    "WebPageService",
    "build_local_defuddle_extractor",
]
