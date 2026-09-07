"""Tests for provider configuration, aliases, and model discovery."""

from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from search_agent.cli import _resolve_startup_model
from search_agent.model_config import (
    ModelOption,
    ModelSelection,
    ProviderConfig,
    load_model_config,
    provider_is_available,
)
from search_agent.model_picker import _merge_models


def _write_config(path: Path) -> None:
    path.write_text(
        """
default_preset = "melchior"

[providers.melchior]
name = "Melchior"
base_url = "http://melchior-1:5000/v1/"
models = ["qwen-3.6-27b"]

[providers.mac]
name = "This Mac"
base_url = "http://localhost:1234/v1"
models = [{ id = "gemma-4-26b-a4b", label = "Gemma 4 26B QAT" }]

[presets.melchior]
provider = "melchior"
model = "qwen-3.6-27b"

[presets.gemma]
provider = "mac"
model = "gemma-4-26b-a4b"

[presets.luna]
provider = "openai"
model = "gpt-5.6-luna"
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_loads_providers_and_case_insensitive_presets() -> None:
    with TemporaryDirectory() as directory:
        path = Path(directory) / "config.toml"
        _write_config(path)

        config = load_model_config(path)

    assert config.provider("melchior").base_url == "http://melchior-1:5000/v1"
    assert config.resolve_preset("GEMMA") == ModelSelection("mac", "gemma-4-26b-a4b")
    assert config.resolve_preset("luna") == ModelSelection("openai", "gpt-5.6-luna")
    assert config.provider("openai").api_key_env == "OPENAI_API_KEY"


def test_openai_provider_cannot_be_redirected() -> None:
    with TemporaryDirectory() as directory:
        path = Path(directory) / "config.toml"
        path.write_text(
            """
default_preset = "bad"
[providers.openai]
name = "Definitely OpenAI"
base_url = "http://example.test/v1"
[presets.bad]
provider = "openai"
model = "gpt-5.6-luna"
""",
            encoding="utf-8",
        )

        with pytest.raises(ValidationError, match="reserved"):
            load_model_config(path)


def test_openai_availability_reads_only_the_standard_environment_name() -> None:
    with TemporaryDirectory() as directory:
        path = Path(directory) / "config.toml"
        _write_config(path)
        config = load_model_config(path)

    with patch.dict(os.environ, {}, clear=True):
        assert provider_is_available(config.provider("openai")) == (
            False,
            "Set OPENAI_API_KEY in .env, then restart.",
        )
    with patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}, clear=True):
        assert provider_is_available(config.provider("openai")) == (True, None)


def test_startup_alias_switches_provider_and_unmatched_url_is_visible() -> None:
    with TemporaryDirectory() as directory:
        path = Path(directory) / "config.toml"
        _write_config(path)
        config = load_model_config(path)

    _, alias_selection = _resolve_startup_model(
        config,
        model_override="luna",
        base_url_override=None,
    )
    assert alias_selection == ModelSelection("openai", "gpt-5.6-luna")

    overridden, override_selection = _resolve_startup_model(
        config,
        model_override="some-new-model",
        base_url_override="http://other-host:9000/v1/",
    )
    assert override_selection == ModelSelection("override", "some-new-model")
    assert overridden.provider("override").name == "Current override"


def test_discovered_models_extend_config_without_replacing_labels() -> None:
    merged = _merge_models(
        [ModelOption(id="configured", label="Friendly")],
        [ModelOption(id="configured"), ModelOption(id="discovered")],
    )

    assert [(model.id, model.display_name) for model in merged] == [
        ("configured", "Friendly"),
        ("discovered", "discovered"),
    ]


def test_provider_accepts_bare_model_strings() -> None:
    provider = ProviderConfig(
        name="Local",
        base_url="http://localhost:1234/v1",
        models=["one", "two"],
    )

    assert [model.id for model in provider.models] == ["one", "two"]
