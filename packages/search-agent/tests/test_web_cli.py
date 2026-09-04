"""Tests for the no-harness webpage diagnostic command."""

from __future__ import annotations

import json

from search_agent.web_cli import main


def test_invalid_diagnostic_url_returns_structured_failure(capsys) -> None:
    exit_code = main(["file:///tmp/not-a-web-page", "--story-id", "123"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert payload["status"] == "not_authorized"
    assert payload["story_id"] == 123
