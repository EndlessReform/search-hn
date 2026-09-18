"""Textual pilot coverage for the model picker's critical keyboard paths."""

from __future__ import annotations

import asyncio
import os
from unittest.mock import patch

from textual.app import App
from textual.widgets import Input, Select, Static

from search_agent.model_config import (
    ModelSelection,
    PresetConfig,
    ProviderConfig,
    SearchAgentModelConfig,
)
from search_agent.model_picker import ModelPickerModal


def _config() -> SearchAgentModelConfig:
    return SearchAgentModelConfig(
        providers={
            "local": ProviderConfig(
                name="Local",
                base_url="http://localhost:1234/v1",
                models=["configured-model"],
            )
        },
        presets={"local": PresetConfig(provider="local", model="configured-model")},
    )


class _PickerHarness(App[None]):
    """Small host app that records one modal result for assertions."""

    def __init__(self, current: ModelSelection) -> None:
        super().__init__()
        self.current = current
        self.result: ModelSelection | None = None

    def on_mount(self) -> None:
        self.push_screen(ModelPickerModal(_config(), self.current), self._record)

    def _record(self, result: ModelSelection | None) -> None:
        self.result = result


def test_current_model_does_not_auto_dismiss_and_free_form_is_accepted() -> None:
    async def exercise(app: _PickerHarness) -> None:
        async with app.run_test() as pilot:
            await pilot.pause()
            assert isinstance(app.screen, ModelPickerModal)
            assert app.focused is app.screen.query_one("#model-select", Select)
            warning = str(app.screen.query_one("#picker-health", Static).render())
            assert "Model discovery unavailable" in warning

            custom = app.screen.query_one("#custom-model", Input)
            custom.focus()
            custom.value = "completely-arbitrary-model"
            await pilot.press("enter")
            await pilot.pause()

    app = _PickerHarness(ModelSelection("local", "configured-model"))
    with patch(
        "search_agent.model_picker.discover_models",
        side_effect=ConnectionError("server unavailable"),
    ):
        asyncio.run(exercise(app))

    assert app.result == ModelSelection("local", "completely-arbitrary-model")


def test_missing_openai_key_warns_without_dismissing_modal() -> None:
    async def exercise(app: _PickerHarness) -> None:
        async with app.run_test() as pilot:
            await pilot.pause()
            custom = app.screen.query_one("#custom-model", Input)
            custom.focus()
            custom.value = "gpt-5.6-sol"
            await pilot.press("enter")
            await pilot.pause()

            assert isinstance(app.screen, ModelPickerModal)
            warning = str(app.screen.query_one("#picker-health", Static).render())
            assert "OPENAI_API_KEY" in warning
            assert app.result is None

    app = _PickerHarness(ModelSelection("openai", "gpt-5.6-luna"))
    with patch.dict(os.environ, {}, clear=True):
        asyncio.run(exercise(app))


def test_enter_accepts_the_initially_highlighted_model() -> None:
    async def exercise(app: _PickerHarness) -> None:
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.press("enter", "enter")
            await pilot.pause()

    app = _PickerHarness(ModelSelection("openai", "gpt-5.6-luna"))
    with patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}, clear=True):
        asyncio.run(exercise(app))

    assert app.result == ModelSelection("openai", "gpt-5.6-luna")


def test_model_command_switches_scoped_transport_and_opens_picker():
    """The merged TUI must change the run's client, not only its model label."""
    from unittest.mock import MagicMock
    from search_agent.model_config import ModelRuntime
    from search_agent.runtime import SearchRuntime
    from search_agent.tui import SearchAgentApp

    async def exercise():
        config = _config()
        models = ModelRuntime(config, config.default_selection())
        runtime = SearchRuntime(
            model=models.selection.model,
            base_url=models.provider.base_url,
            model_runtime=models,
            repository=MagicMock(),
        )
        app = SearchAgentApp(
            agent=runtime.agent,
            agent_context=runtime.context,
            base_url=runtime.base_url,
            runtime=runtime,
            model_runtime=models,
        )
        old_client = models.client
        try:
            async with app.run_test() as pilot:
                await app._set_model_from_command("arbitrary-model")
                assert runtime.agent.model == "arbitrary-model"
                assert models.client is not old_client
                assert old_client.is_closed
                with patch("search_agent.model_config.OpenAIProvider") as provider:
                    active = app._active_run_config()
                    assert active.model_provider is provider.return_value
                    assert provider.call_args.kwargs["openai_client"] is models.client
                prompt = app.query_one("#prompt-input", Input)
                prompt.value = "/model"
                await pilot.press("enter")
                await pilot.pause()
                assert isinstance(app.screen, ModelPickerModal)
        finally:
            app.close_conversation_session()
            await runtime.close()

    with patch(
        "search_agent.model_picker.discover_models",
        side_effect=ConnectionError("offline"),
    ):
        asyncio.run(exercise())
