"""Function tools exposed to the OpenAI Agents runtime.

This package keeps each tool in its own module so the public surface stays
small while the per-tool logic remains easy to read in isolation.
"""

from search_agent.tools.fetch_stories import fetch_stories
from search_agent.tools.fetch_top_comments import fetch_top_comments
from search_agent.tools.fetch_top_stories_for_date import fetch_top_stories_for_date
from search_agent.tools.find_in_webpage import find_in_webpage
from search_agent.tools.open_webpage import open_webpage
from search_agent.tools.read_webpage import read_webpage

__all__ = [
    "fetch_stories",
    "fetch_top_comments",
    "fetch_top_stories_for_date",
    "find_in_webpage",
    "open_webpage",
    "read_webpage",
]
