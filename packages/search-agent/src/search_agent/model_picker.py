"""Codex-style provider and model picker for the Textual application."""

from __future__ import annotations

from collections.abc import Iterable

import httpx
from pydantic import BaseModel, ConfigDict
from textual import work
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Input, Label, Select, Static

from search_agent.model_config import (
    OPENAI_PROVIDER_ID,
    ModelOption,
    ModelSelection,
    SearchAgentModelConfig,
    provider_is_available,
)


class _ModelsResponse(BaseModel):
    """The minimal standard ``GET /models`` response consumed by discovery."""

    model_config = ConfigDict(extra="ignore")

    data: list[ModelOption]


def _merge_models(
    configured: Iterable[ModelOption], discovered: Iterable[ModelOption]
) -> tuple[ModelOption, ...]:
    """Merge catalogs by exact wire ID while preserving useful display order."""

    merged: dict[str, ModelOption] = {}
    for model in (*tuple(configured), *tuple(discovered)):
        merged.setdefault(model.id, model)
    return tuple(merged.values())


async def discover_models(base_url: str) -> tuple[ModelOption, ...]:
    """Fetch a provider's standard model catalog with a short health-check timeout."""

    url = f"{base_url.rstrip('/')}/models"
    timeout = httpx.Timeout(3.0, connect=1.5)
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(url)
        response.raise_for_status()
    parsed = _ModelsResponse.model_validate(response.json())
    return tuple(parsed.data)


class ModelPickerModal(ModalScreen[ModelSelection | None]):
    """Select a provider/model pair without ever accepting credentials.

    The model dropdown receives initial focus, matching the quick Codex flow.
    Users can tab back to the provider dropdown or tab forward to a free-form
    model field.  Discovery is advisory: configured choices and free-form entry
    continue working when ``/models`` is unavailable or malformed.
    """

    CSS = """
    ModelPickerModal {
        align: center middle;
    }
    #model-picker {
        width: 72;
        height: auto;
        max-height: 85%;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
    }
    #model-picker-title {
        text-style: bold;
        margin-bottom: 1;
    }
    .picker-label {
        color: $text-muted;
        margin-top: 1;
    }
    #picker-health {
        min-height: 2;
        color: $text-muted;
        margin-top: 1;
    }
    #picker-help {
        color: $text-muted;
        margin-top: 1;
    }
    """

    def __init__(
        self,
        config: SearchAgentModelConfig,
        current: ModelSelection,
    ) -> None:
        super().__init__()
        self._config = config
        self._current = current
        self._catalogs: dict[str, tuple[ModelOption, ...]] = {
            provider_id: provider.models
            for provider_id, provider in config.provider_items()
        }
        self._ready = False

    def compose(self) -> ComposeResult:
        provider_options = [
            (provider.name, provider_id)
            for provider_id, provider in self._config.provider_items()
        ]
        with Vertical(id="model-picker"):
            yield Label("Select provider and model", id="model-picker-title")
            yield Label("Provider", classes="picker-label")
            yield Select(
                provider_options,
                value=self._current.provider_id,
                allow_blank=False,
                id="provider-select",
            )
            yield Label("Model", classes="picker-label")
            yield Select(
                self._model_options(self._current.provider_id),
                value=self._initial_model_value(self._current.provider_id),
                prompt="Choose a model",
                id="model-select",
            )
            yield Label("Or enter any model ID", classes="picker-label")
            yield Input(
                placeholder="Exact model ID (press Enter to use)",
                id="custom-model",
            )
            yield Static("", id="picker-health")
            yield Static(
                "Enter choose · Tab change focus · Ctrl+R refresh · Esc cancel",
                id="picker-help",
            )

    def on_mount(self) -> None:
        self.title = "Model"
        self.query_one("#model-select", Select).focus()
        self._describe_provider(self._current.provider_id)
        self._poll_models(self._current.provider_id)
        # Initial Select values emit Changed messages. Enabling after the first
        # refresh prevents the current model from dismissing the modal as if
        # the user had just selected it.
        self.call_after_refresh(self._enable_select_events)

    def _enable_select_events(self) -> None:
        self._ready = True

    def _model_options(self, provider_id: str) -> list[tuple[str, str]]:
        return [(model.display_name, model.id) for model in self._catalogs[provider_id]]

    def _initial_model_value(self, provider_id: str):
        known_ids = {model.id for model in self._catalogs[provider_id]}
        if (
            provider_id == self._current.provider_id
            and self._current.model in known_ids
        ):
            return self._current.model
        return Select.NULL

    def _describe_provider(self, provider_id: str) -> None:
        provider = self._config.provider(provider_id)
        available, reason = provider_is_available(provider)
        health = self.query_one("#picker-health", Static)
        if not available:
            health.update(f"[yellow]Unavailable:[/] {reason}")
        elif provider_id == OPENAI_PROVIDER_ID:
            health.update("[green]Ready:[/] OPENAI_API_KEY found in the environment.")
        else:
            health.update(f"[dim]Checking {provider.base_url}/models…[/]")

    def on_select_changed(self, event: Select.Changed) -> None:
        if not self._ready:
            return
        if event.select.id == "provider-select":
            provider_id = str(event.value)
            model_select = self.query_one("#model-select", Select)
            with self.prevent(Select.Changed):
                model_select.set_options(self._model_options(provider_id))
                model_select.value = self._initial_model_value(provider_id)
            self._describe_provider(provider_id)
            self._poll_models(provider_id)
            return
        if event.select.id == "model-select" and event.value is not Select.NULL:
            self._try_dismiss(str(event.value))

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "custom-model":
            return
        model = event.value.strip()
        if not model:
            self.query_one("#picker-health", Static).update(
                "[yellow]Enter a non-empty model ID.[/]"
            )
            return
        self._try_dismiss(model)

    def _try_dismiss(self, model: str) -> None:
        provider_id = str(self.query_one("#provider-select", Select).value)
        provider = self._config.provider(provider_id)
        available, reason = provider_is_available(provider)
        if not available:
            self.query_one("#picker-health", Static).update(
                f"[yellow]Unavailable:[/] {reason}"
            )
            return
        self.dismiss(ModelSelection(provider_id=provider_id, model=model))

    def on_key(self, event) -> None:
        if event.key == "escape":
            event.stop()
            self.dismiss(None)
        elif (
            event.key == "enter"
            and isinstance(self.focused, Select)
            and self.focused.id == "model-select"
            and not self.focused.expanded
            and self.focused.value is not Select.NULL
        ):
            # Select.Changed is not emitted when the user explicitly chooses
            # the already-current value. Treat closing the dropdown with Enter
            # as confirmation so the shortest keyboard path still works.
            event.stop()
            self._try_dismiss(str(self.focused.value))
        elif event.key == "ctrl+r":
            event.stop()
            provider_id = str(self.query_one("#provider-select", Select).value)
            self._describe_provider(provider_id)
            self._poll_models(provider_id)

    @work(exclusive=True, group="model-discovery", thread=False)
    async def _poll_models(self, provider_id: str) -> None:
        """Refresh a local catalog; render failures as warnings, never crashes."""

        if provider_id == OPENAI_PROVIDER_ID:
            return
        provider = self._config.provider(provider_id)
        try:
            discovered = await discover_models(provider.base_url)
        except Exception as exc:  # noqa: BLE001 - discovery is deliberately best-effort
            if self.is_mounted and self._selected_provider_id() == provider_id:
                self.query_one("#picker-health", Static).update(
                    "[yellow]Model discovery unavailable:[/] "
                    f"{exc}. Configured and custom model IDs still work."
                )
            return

        self._catalogs[provider_id] = _merge_models(provider.models, discovered)
        if not self.is_mounted or self._selected_provider_id() != provider_id:
            return
        model_select = self.query_one("#model-select", Select)
        previous = model_select.value
        with self.prevent(Select.Changed):
            model_select.set_options(self._model_options(provider_id))
            known_ids = {model.id for model in self._catalogs[provider_id]}
            if previous is not Select.NULL and str(previous) in known_ids:
                model_select.value = previous
        self.query_one("#picker-health", Static).update(
            f"[green]Online:[/] {len(discovered)} models returned by /models."
        )

    def _selected_provider_id(self) -> str:
        """Return the provider currently displayed by the modal."""

        value = self.query_one("#provider-select", Select).value
        assert value is not Select.NULL, "provider selection cannot be blank"
        return str(value)
