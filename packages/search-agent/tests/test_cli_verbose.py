"""Tests for verbose reasoning helper behavior in the TUI CLI."""

from __future__ import annotations

from datetime import date
import unittest
from unittest.mock import patch

from agents import Agent, ModelResponse, SQLiteSession
from agents.usage import Usage
from openai.types.responses.response_usage import InputTokensDetails, OutputTokensDetails

from search_agent.cli import (
    _build_model_settings,
    _collect_turn_metrics,
    _extract_tool_call_name_and_arguments,
    _format_tool_call_preview,
    _format_turn_metrics,
    _new_conversation_session,
    _is_openai_first_party_base_url,
    _parse_system_date_override,
    _parse_verbose_command,
    _start_streamed_turn,
    parse_args,
)
from search_agent.runtime_context import SearchAgentContext


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

    def test_parse_system_date_override_accepts_year_or_full_iso_date(self) -> None:
        self.assertEqual(_parse_system_date_override("1862"), date(1862, 1, 1))
        self.assertEqual(_parse_system_date_override("2029-12-31"), date(2029, 12, 31))

    def test_parse_args_supports_system_date_override(self) -> None:
        args = parse_args(["--system-date", "2029"])

        self.assertEqual(args.system_date, date(2029, 1, 1))

    def test_collect_turn_metrics_prefers_last_response_usage(self) -> None:
        earlier = ModelResponse(
            output=[],
            usage=Usage(
                input_tokens=100,
                input_tokens_details=InputTokensDetails(cached_tokens=0),
                output_tokens=20,
                output_tokens_details=OutputTokensDetails(reasoning_tokens=0),
                total_tokens=120,
            ),
            response_id="resp_1",
        )
        later = ModelResponse(
            output=[],
            usage=Usage(
                input_tokens=4567,
                input_tokens_details=InputTokensDetails(cached_tokens=123),
                output_tokens=89,
                output_tokens_details=OutputTokensDetails(reasoning_tokens=45),
                total_tokens=4656,
            ),
            response_id="resp_2",
        )

        metrics = _collect_turn_metrics(
            [earlier, later],
            elapsed_seconds=1.234,
        )

        self.assertEqual(metrics.conversation_tokens, 4567)
        self.assertEqual(metrics.cached_tokens, 123)
        self.assertEqual(metrics.output_tokens, 89)
        self.assertEqual(metrics.reasoning_tokens, 45)
        self.assertEqual(metrics.total_tokens, 4656)
        self.assertAlmostEqual(metrics.elapsed_seconds, 1.234)

    def test_collect_turn_metrics_handles_missing_usage_gracefully(self) -> None:
        metrics = _collect_turn_metrics([], elapsed_seconds=0.5)

        self.assertEqual(metrics.elapsed_seconds, 0.5)
        self.assertIsNone(metrics.conversation_tokens)
        self.assertIsNone(metrics.total_tokens)

    def test_format_turn_metrics(self) -> None:
        metrics = _collect_turn_metrics(
            [
                ModelResponse(
                    output=[],
                    usage=Usage(
                        input_tokens=12345,
                        input_tokens_details=InputTokensDetails(cached_tokens=2000),
                        output_tokens=321,
                        output_tokens_details=OutputTokensDetails(reasoning_tokens=111),
                        total_tokens=12666,
                    ),
                    response_id="resp_3",
                )
            ],
            elapsed_seconds=2.5,
        )

        self.assertEqual(
            _format_turn_metrics(metrics),
            "turn 2.50s | context 12,345 tok | cached 2,000 | output 321 | reasoning 111 | total 12,666",
        )

    def test_format_tool_call_preview_for_single_fetch_stories_query(self) -> None:
        preview = _format_tool_call_preview(
            "fetch_stories",
            '{"query":"OpenAI Deep Research","limit":8}',
        )

        self.assertEqual(preview, 'search query: "OpenAI Deep Research"')

    def test_format_tool_call_preview_for_batched_fetch_stories_queries(self) -> None:
        preview = _format_tool_call_preview(
            "fetch_stories",
            '{"query":["OpenAI Deep Research","Deep Research","OpenAI research"],"limit":8}',
        )

        self.assertEqual(
            preview,
            'search queries (3): "OpenAI Deep Research", "Deep Research", "OpenAI research"',
        )

    def test_format_tool_call_preview_ignores_other_tools_or_bad_json(self) -> None:
        self.assertIsNone(_format_tool_call_preview("fetch_top_comments", '{"story_id":123}'))
        self.assertIsNone(_format_tool_call_preview("fetch_stories", "not-json"))

    def test_extract_tool_call_name_and_arguments_from_dict_raw_item(self) -> None:
        class FakeToolCallItem:
            type = "tool_call_item"

            def __init__(self) -> None:
                self.raw_item = {
                    "name": "fetch_stories",
                    "arguments": '{"query":"hhkb","limit":8}',
                }

        tool_name, arguments = _extract_tool_call_name_and_arguments(FakeToolCallItem())

        self.assertEqual(tool_name, "fetch_stories")
        self.assertEqual(arguments, '{"query":"hhkb","limit":8}')

    def test_extract_tool_call_name_and_arguments_from_object_raw_item(self) -> None:
        class RawToolCall:
            name = "fetch_stories"
            params = {"query": ["hhkb", "Happy Hacking Keyboard"], "limit": 8}

        class FakeToolCallItem:
            type = "tool_call_item"

            def __init__(self) -> None:
                self.raw_item = RawToolCall()

        tool_name, arguments = _extract_tool_call_name_and_arguments(FakeToolCallItem())

        self.assertEqual(tool_name, "fetch_stories")
        self.assertEqual(
            arguments,
            '{"query": ["hhkb", "Happy Hacking Keyboard"], "limit": 8}',
        )

    def test_new_conversation_session_returns_fresh_sqlite_sessions(self) -> None:
        first = _new_conversation_session()
        second = _new_conversation_session()

        try:
            self.assertIsInstance(first, SQLiteSession)
            self.assertIsInstance(second, SQLiteSession)
            self.assertNotEqual(first.session_id, second.session_id)
        finally:
            first.close()
            second.close()

    def test_start_streamed_turn_uses_sdk_session_memory(self) -> None:
        agent: Agent[SearchAgentContext] = Agent(name="Assistant")
        context = SearchAgentContext(repository=object())
        session = _new_conversation_session()

        try:
            with patch("search_agent.cli.Runner.run_streamed", return_value="stream-result") as mock_run:
                result = _start_streamed_turn(
                    agent=agent,
                    user_text="What state is it in?",
                    agent_context=context,
                    hooks=None,
                    verbose=False,
                    base_url="http://localhost:8000/v1",
                    conversation_session=session,
                )

            self.assertEqual(result, "stream-result")
            mock_run.assert_called_once_with(
                agent,
                input="What state is it in?",
                context=context,
                hooks=None,
                max_turns=10,
                session=session,
            )
        finally:
            session.close()


if __name__ == "__main__":
    unittest.main()
