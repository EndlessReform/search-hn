"""Tests for verbose reasoning helper behavior in the TUI CLI."""

from __future__ import annotations

import unittest

from search_agent.cli import (
    _build_model_settings,
    _is_openai_first_party_base_url,
    _parse_verbose_command,
)


class VerboseHelperTests(unittest.TestCase):
    """Exercise provider detection and verbose command parsing."""

    def test_detects_openai_first_party_hostnames(self) -> None:
        self.assertTrue(_is_openai_first_party_base_url("https://api.openai.com/v1"))
        self.assertTrue(_is_openai_first_party_base_url("https://foo.openai.com/v1"))
        self.assertFalse(_is_openai_first_party_base_url("http://localhost:8000/v1"))
        self.assertFalse(_is_openai_first_party_base_url("http://melchior-1:5000/v1"))

    def test_build_model_settings_requests_reasoning_for_local_verbose_runs(self) -> None:
        settings = _build_model_settings("http://localhost:8000/v1", verbose=True)

        self.assertIsNotNone(settings.reasoning)
        self.assertEqual(settings.reasoning.summary, "auto")

    def test_build_model_settings_skips_reasoning_for_openai_or_non_verbose(self) -> None:
        openai_settings = _build_model_settings("https://api.openai.com/v1", verbose=True)
        quiet_local_settings = _build_model_settings("http://localhost:8000/v1", verbose=False)

        self.assertIsNone(openai_settings.reasoning)
        self.assertIsNone(quiet_local_settings.reasoning)

    def test_parse_verbose_command(self) -> None:
        self.assertTrue(_parse_verbose_command("/verbose on"))
        self.assertFalse(_parse_verbose_command("/verbose off"))
        self.assertTrue(_parse_verbose_command(" /VERBOSE   ON "))
        self.assertIsNone(_parse_verbose_command("/verbose maybe"))


if __name__ == "__main__":
    unittest.main()
