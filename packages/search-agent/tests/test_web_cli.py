"""Tests for the no-harness webpage diagnostic command."""

from __future__ import annotations

import json

from search_agent.runtime_context import SearchAgentContext
from search_agent.tools.find_in_webpage import (
    build_find_in_webpage_payload,
    find_in_webpage,
)
from search_agent.web_cli import main, parse_args


def test_diagnostic_cli_accepts_cached_read_and_find_options() -> None:
    args = parse_args(
        ["https://example.com/article", "--read-next", "--find", "release date"]
    )

    assert args.read_next is True
    assert args.find == "release date"


def test_invalid_diagnostic_url_returns_structured_failure(capsys) -> None:
    exit_code = main(["file:///tmp/not-a-web-page", "--story-id", "123"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert payload["status"] == "not_authorized"
    assert payload["story_id"] == 123


def test_find_tool_boundary_treats_string_null_as_an_omitted_cursor() -> None:
    class RecordingService:
        def find(self, *, page_id: str, term: str, cursor: str | None):
            return {"page_id": page_id, "term": term, "cursor": cursor}

    context = SearchAgentContext(
        repository=object(),  # type: ignore[arg-type]
        web_service=RecordingService(),  # type: ignore[arg-type]
    )

    payload = build_find_in_webpage_payload(
        context,
        page_id="page:1",
        term="needle",
        cursor="null",
    )

    assert payload["cursor"] is None


def test_find_tool_schema_does_not_require_optional_cursor() -> None:
    required = find_in_webpage.params_json_schema["required"]

    assert "cursor" not in required
