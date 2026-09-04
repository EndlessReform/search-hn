"""Typed model-provider configuration and live client switching.

The TUI treats a model selection as the pair ``(provider, model)``.  Keeping
that pair explicit is important: a friendly alias such as ``luna`` must switch
the HTTP client as well as the string stored on the agent.

Secrets are deliberately absent from the TOML schema.  Providers may name an
environment variable, but the value is resolved only when a provider is
activated.  The built-in OpenAI provider is more restrictive still: its URL
and conventional ``OPENAI_API_KEY`` credential source cannot be overridden.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from agents import set_default_openai_client
from openai import AsyncOpenAI
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

OPENAI_PROVIDER_ID = "openai"
OPENAI_BASE_URL = "https://api.openai.com/v1"
OPENAI_API_KEY_ENV = "OPENAI_API_KEY"


class ModelOption(BaseModel):
    """One model advertised in a provider's initial picker catalog."""

    model_config = ConfigDict(frozen=True)

    id: str = Field(min_length=1)
    label: str | None = None

    @field_validator("id")
    @classmethod
    def _strip_id(cls, value: str) -> str:
        clean = value.strip()
        assert clean, "model id must not be blank"
        return clean

    @property
    def display_name(self) -> str:
        """Return the human label while retaining the exact wire model ID."""

        return self.label or self.id


class ProviderConfig(BaseModel):
    """A non-OpenAI, Responses-compatible provider declared by the user."""

    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1)
    base_url: str = Field(min_length=1)
    models: tuple[ModelOption, ...] = ()
    api_key_env: str | None = None

    @field_validator("models", mode="before")
    @classmethod
    def _accept_short_model_names(cls, value):
        """Allow both ``["model-id"]`` and labeled inline TOML tables."""

        if value is None:
            return ()
        return tuple({"id": item} if isinstance(item, str) else item for item in value)

    @field_validator("base_url")
    @classmethod
    def _validate_base_url(cls, value: str) -> str:
        clean = value.strip().rstrip("/")
        parsed = urlparse(clean)
        assert parsed.scheme in {"http", "https"} and parsed.netloc, (
            "provider base_url must be an absolute http(s) URL"
        )
        return clean

    @field_validator("api_key_env")
    @classmethod
    def _validate_api_key_env(cls, value: str | None) -> str | None:
        if value is None:
            return None
        clean = value.strip()
        assert clean and clean.isidentifier(), (
            "api_key_env must be an environment-variable name"
        )
        return clean


class PresetConfig(BaseModel):
    """A slash-command alias resolving to an atomic provider/model pair."""

    model_config = ConfigDict(frozen=True)

    provider: str = Field(min_length=1)
    model: str = Field(min_length=1)


class SearchAgentModelConfig(BaseModel):
    """Validated contents of ``config.toml`` plus built-in OpenAI metadata."""

    model_config = ConfigDict(frozen=True)

    default_preset: str = "local"
    providers: dict[str, ProviderConfig]
    presets: dict[str, PresetConfig]

    @model_validator(mode="after")
    def _validate_references(self) -> SearchAgentModelConfig:
        assert OPENAI_PROVIDER_ID not in self.providers, (
            "provider id 'openai' is reserved; its URL and credentials are built in"
        )
        provider_ids = {*self.providers, OPENAI_PROVIDER_ID}
        folded_aliases: set[str] = set()
        for alias, preset in self.presets.items():
            assert alias.strip() and not any(
                character.isspace() for character in alias
            ), "preset aliases must be non-empty and contain no whitespace"
            folded = alias.casefold()
            assert folded not in folded_aliases, (
                f"preset aliases differ only by case: {alias!r}"
            )
            folded_aliases.add(folded)
            assert preset.provider in provider_ids, (
                f"preset {alias!r} references unknown provider {preset.provider!r}"
            )
        assert self.default_preset in self.presets, (
            f"default_preset {self.default_preset!r} is not defined in [presets]"
        )
        return self

    def provider(self, provider_id: str) -> ProviderConfig:
        """Return a configured provider or the non-overridable OpenAI provider."""

        if provider_id == OPENAI_PROVIDER_ID:
            return _openai_provider()
        return self.providers[provider_id]

    def provider_items(self) -> tuple[tuple[str, ProviderConfig], ...]:
        """Return configured providers followed by the first-party option."""

        return (*self.providers.items(), (OPENAI_PROVIDER_ID, _openai_provider()))

    def resolve_preset(self, alias: str) -> ModelSelection | None:
        """Resolve an alias case-insensitively, preserving model ID casing."""

        folded = alias.casefold()
        for configured_alias, preset in self.presets.items():
            if configured_alias.casefold() == folded:
                return ModelSelection(preset.provider, preset.model)
        return None

    def default_selection(self) -> ModelSelection:
        """Return the pair named by ``default_preset``."""

        preset = self.presets[self.default_preset]
        return ModelSelection(preset.provider, preset.model)


@dataclass(frozen=True, slots=True)
class ModelSelection:
    """The provider and wire model ID applied together by the picker."""

    provider_id: str
    model: str


def _openai_provider() -> ProviderConfig:
    """Build the protected first-party provider and its small curated catalog."""

    return ProviderConfig(
        name="OpenAI",
        base_url=OPENAI_BASE_URL,
        api_key_env=OPENAI_API_KEY_ENV,
        models=(
            ModelOption(id="gpt-5.6-sol", label="GPT-5.6 Sol"),
            ModelOption(id="gpt-5.6-terra", label="GPT-5.6 Terra"),
            ModelOption(id="gpt-5.6-luna", label="GPT-5.6 Luna"),
        ),
    )


def _fallback_config() -> SearchAgentModelConfig:
    """Retain the historical Melchior defaults when no config file exists."""

    return SearchAgentModelConfig(
        providers={
            "local": ProviderConfig(
                name="Local (Melchior)",
                base_url="http://melchior-1:5000/v1",
                models=(ModelOption(id="qwen-3.6-27b"),),
            )
        },
        presets={
            "local": PresetConfig(provider="local", model="qwen-3.6-27b"),
        },
    )


def default_config_path() -> Path:
    """Return the XDG user configuration path used by the search agent."""

    xdg_root = os.getenv("XDG_CONFIG_HOME")
    root = Path(xdg_root).expanduser() if xdg_root else Path.home() / ".config"
    return root / "search-agent" / "config.toml"


def load_model_config(path: Path | None = None) -> SearchAgentModelConfig:
    """Load and validate TOML, falling back only when the default path is absent.

    An explicitly requested missing file is an error.  Likewise malformed or
    internally inconsistent config is allowed to raise with Pydantic's useful
    field context instead of being silently ignored.
    """

    config_path = path or default_config_path()
    if not config_path.exists():
        if path is not None:
            raise FileNotFoundError(config_path)
        return _fallback_config()
    with config_path.open("rb") as config_file:
        return SearchAgentModelConfig.model_validate(tomllib.load(config_file))


def provider_is_available(provider: ProviderConfig) -> tuple[bool, str | None]:
    """Report whether credentials required by a provider are present."""

    if provider.api_key_env and not os.getenv(provider.api_key_env):
        return False, f"Set {provider.api_key_env} in .env, then restart."
    return True, None


def provider_api_key(provider: ProviderConfig) -> str:
    """Resolve a provider credential or a harmless local compatibility value."""

    if provider.api_key_env:
        api_key = os.getenv(provider.api_key_env)
        assert api_key, f"{provider.api_key_env} is required for {provider.name}"
        return api_key
    return "local-openai-compatible-no-key"


class ModelRuntime:
    """Own the active OpenAI client and switch it without leaking old clients."""

    def __init__(
        self,
        config: SearchAgentModelConfig,
        selection: ModelSelection,
        *,
        api_key_override: str | None = None,
    ) -> None:
        self.config = config
        self.selection = selection
        self._api_key_override = api_key_override
        self._client = self._new_client(selection)
        set_default_openai_client(self._client)

    @property
    def provider(self) -> ProviderConfig:
        """Return metadata for the active provider."""

        return self.config.provider(self.selection.provider_id)

    def _new_client(self, selection: ModelSelection) -> AsyncOpenAI:
        provider = self.config.provider(selection.provider_id)
        if self._api_key_override is None:
            available, reason = provider_is_available(provider)
            assert available, reason
        api_key = self._api_key_override or provider_api_key(provider)
        return AsyncOpenAI(base_url=provider.base_url, api_key=api_key)

    async def activate(self, selection: ModelSelection) -> None:
        """Atomically install a new provider client, then close the old client."""

        assert selection.model.strip(), "model name must not be blank"
        new_client = self._new_client(selection)
        old_client = self._client
        self._client = new_client
        self.selection = selection
        self._api_key_override = None
        set_default_openai_client(new_client)
        await old_client.close()

    async def close(self) -> None:
        """Close the currently active HTTP client during application shutdown."""

        await self._client.close()
