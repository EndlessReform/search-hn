"""Core/presentation isolation and durable interruption behavior."""

import json
import subprocess
import sys
from unittest.mock import patch

from agents import Agent, ModelSettings
from search_agent.journal import Journal
from search_agent.runtime import start_turn
from search_agent.runtime_context import SearchAgentContext


def test_headless_import_does_not_load_textual():
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import search_agent.cli; import search_agent.headless; import sys; assert 'textual' not in sys.modules",
        ],
        check=True,
    )


def test_core_resets_state_and_preserves_resource_settings():
    context = SearchAgentContext(repository=object())
    context.turn_state.no_results_guidance_emitted = True
    agent = Agent(name="test")
    with patch("search_agent.runtime.Runner.run_streamed") as run:
        start_turn(
            agent=agent,
            context=context,
            prompt="question",
            base_url="http://localhost",
            model_settings=ModelSettings(max_tokens=1000),
            max_turns=4,
        )
    assert not context.turn_state.no_results_guidance_emitted
    assert agent.model_settings.max_tokens == 1000
    assert run.call_args.kwargs["max_turns"] == 4


def test_journal_durable_boundary(tmp_path):
    path = tmp_path / "events.jsonl"
    journal = Journal(path)
    with patch("search_agent.journal.os.fsync") as sync:
        journal.write("start", prompt="hello")
        assert json.loads(path.read_text())["prompt"] == "hello"
        sync.assert_called_once()
    journal.close()


def test_resume_preserves_torn_tail_and_complete_records(tmp_path):
    path = tmp_path / "events.jsonl"
    path.write_bytes(b'{"event":"first"}\n{"event":"tor')
    journal = Journal(path)
    journal.write("second")
    journal.close()
    assert [json.loads(line)["event"] for line in path.read_text().splitlines()] == [
        "first",
        "second",
    ]
    assert next(tmp_path.glob("*.partial")).read_bytes() == b'{"event":"tor'
