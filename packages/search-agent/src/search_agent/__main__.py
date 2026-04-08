"""Allow running as: `uv run python -m search_agent`."""

from __future__ import annotations

from search_agent.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
