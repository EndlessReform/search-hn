"""Pinned local Defuddle subprocess adapter."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from threading import Condition

DEFUDDLE_VERSION = "0.18.1"
DEFUDDLE_PACKAGE = f"defuddle@{DEFUDDLE_VERSION}"
_NODE_VERSION = re.compile(r"^v\d+\.\d+\.\d+(?:[-+].+)?$")


class ExtractionError(RuntimeError):
    """Defuddle could not produce a valid extraction."""


@dataclass(frozen=True)
class ExtractedDocument:
    """Clean Markdown and useful metadata returned by an extractor."""

    markdown: str
    title: str | None
    author: str | None
    published: str | None


@dataclass(frozen=True)
class NpxRuntime:
    """A concrete command prefix for invoking npx without shell evaluation."""

    command_prefix: tuple[str, ...]
    source: str


class LocalDefuddleExtractor:
    """Run a pinned Defuddle CLI against HTML fetched by this application.

    Defuddle 0.18.1's published CLI requires a path even though newer upstream
    sources support stdin.  A private temporary directory preserves our fetch
    controls while avoiding Defuddle's URL-fetch mode and its retry behavior.
    """

    def __init__(
        self,
        runtime: NpxRuntime,
        *,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._runtime = runtime
        self._timeout_seconds = timeout_seconds
        self.name = f"defuddle-local@{DEFUDDLE_VERSION}"

    @property
    def runtime_source(self) -> str:
        """Return a concise description suitable for startup diagnostics."""

        return self._runtime.source

    def health_check(self) -> None:
        """Warm the npx cache and verify the exact requested version."""

        completed = self._run(
            ["--version"], timeout_seconds=max(self._timeout_seconds, 45.0)
        )
        actual = completed.stdout.strip()
        if actual != DEFUDDLE_VERSION:
            raise ExtractionError(
                f"expected Defuddle {DEFUDDLE_VERSION}, got {actual or 'no version output'}"
            )

    def extract(self, html: str) -> ExtractedDocument:
        """Extract Markdown and metadata from one bounded HTML document."""

        with tempfile.TemporaryDirectory(
            prefix="search-agent-defuddle-"
        ) as temp_directory:
            source_path = Path(temp_directory) / "page.html"
            source_path.write_text(html, encoding="utf-8")
            completed = self._run(
                ["parse", str(source_path), "--json", "--markdown"],
                timeout_seconds=self._timeout_seconds,
            )
        try:
            payload = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise ExtractionError("Defuddle returned invalid JSON") from exc

        raw_markdown = payload.get("contentMarkdown") or payload.get("content")
        if not isinstance(raw_markdown, str) or not raw_markdown.strip():
            raise ExtractionError("Defuddle returned no readable content")
        return ExtractedDocument(
            markdown=raw_markdown.strip(),
            title=_optional_string(payload.get("title")),
            author=_optional_string(payload.get("author")),
            published=_optional_string(payload.get("published")),
        )

    def _run(
        self,
        arguments: list[str],
        *,
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]:
        command = [
            *self._runtime.command_prefix,
            "--yes",
            DEFUDDLE_PACKAGE,
            *arguments,
        ]
        try:
            return subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise ExtractionError("Defuddle timed out") from exc
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or exc.stdout or "Defuddle failed").strip()
            raise ExtractionError(detail[:500]) from exc
        except OSError as exc:
            raise ExtractionError(f"could not start Defuddle runtime: {exc}") from exc


def _optional_string(value: object) -> str | None:
    """Normalize blank or non-string metadata to ``None``."""

    return value.strip() if isinstance(value, str) and value.strip() else None


_bootstrap_condition = Condition()
_bootstrap_in_progress = False
_bootstrap_result: tuple[LocalDefuddleExtractor | None, str | None] | None = None


def _discover_npx_runtime() -> tuple[NpxRuntime | None, str | None]:
    """Find direct Node/npm tooling, then an already-installed fnm default.

    ``fnm exec`` is used as an argv prefix rather than evaluating shell output.
    This avoids shell injection and does not install or switch Node versions.
    """

    node_path = shutil.which("node")
    npx_path = shutil.which("npx")
    if node_path is not None and npx_path is not None:
        return NpxRuntime((npx_path,), f"PATH ({node_path})"), None

    fnm_path = shutil.which("fnm")
    if fnm_path is None:
        return None, "working node/npx or an fnm-managed default runtime is required"

    try:
        completed = subprocess.run(
            [fnm_path, "default", "--log-level", "quiet"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5.0,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return None, f"could not resolve fnm default runtime: {exc}"

    version = completed.stdout.strip()
    if not _NODE_VERSION.fullmatch(version):
        return (
            None,
            f"fnm returned an invalid default Node version: {version or 'empty'}",
        )
    return (
        NpxRuntime(
            (fnm_path, "exec", f"--using={version}", "--", "npx"),
            f"fnm {version}",
        ),
        None,
    )


def _select_and_warm_local_extractor() -> tuple[
    LocalDefuddleExtractor | None, str | None
]:
    """Resolve one local runtime and perform its bounded Defuddle health check."""

    runtime, runtime_error = _discover_npx_runtime()
    if runtime is None:
        return None, runtime_error
    extractor = LocalDefuddleExtractor(runtime)
    try:
        extractor.health_check()
    except ExtractionError as exc:
        return None, str(exc)
    return extractor, None


def build_local_defuddle_extractor() -> tuple[
    LocalDefuddleExtractor | None, str | None
]:
    """Select and warm one process-wide local Defuddle provider.

    Concurrent context construction waits for the first health check instead of
    racing multiple ``npx`` cache fills. The result—success or a clear startup
    error—is fixed for the process, matching the provider-selection contract.
    """

    global _bootstrap_in_progress, _bootstrap_result

    with _bootstrap_condition:
        while _bootstrap_in_progress:
            _bootstrap_condition.wait()
        if _bootstrap_result is not None:
            return _bootstrap_result
        _bootstrap_in_progress = True

    try:
        result = _select_and_warm_local_extractor()
    except BaseException:
        with _bootstrap_condition:
            _bootstrap_in_progress = False
            _bootstrap_condition.notify_all()
        raise

    with _bootstrap_condition:
        _bootstrap_result = result
        _bootstrap_in_progress = False
        _bootstrap_condition.notify_all()
        return result


def _reset_defuddle_bootstrap_for_tests() -> None:
    """Clear process-wide bootstrap state between isolated unit-test scenarios."""

    global _bootstrap_in_progress, _bootstrap_result

    with _bootstrap_condition:
        assert not _bootstrap_in_progress, "cannot reset an active Defuddle bootstrap"
        _bootstrap_result = None
