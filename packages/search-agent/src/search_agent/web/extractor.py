"""Pinned local Defuddle subprocess adapter."""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

DEFUDDLE_VERSION = "0.18.1"
DEFUDDLE_PACKAGE = f"defuddle@{DEFUDDLE_VERSION}"


class ExtractionError(RuntimeError):
    """Defuddle could not produce a valid extraction."""


@dataclass(frozen=True)
class ExtractedDocument:
    """Clean Markdown and useful metadata returned by an extractor."""

    markdown: str
    title: str | None
    author: str | None
    published: str | None


class LocalDefuddleExtractor:
    """Run a pinned Defuddle CLI against HTML fetched by this application.

    Defuddle 0.18.1's published CLI requires a path even though newer upstream
    sources support stdin.  A private temporary directory preserves our fetch
    controls while avoiding Defuddle's URL-fetch mode and its retry behavior.
    """

    def __init__(self, npx_path: str, *, timeout_seconds: float = 30.0) -> None:
        self._npx_path = npx_path
        self._timeout_seconds = timeout_seconds
        self.name = f"defuddle-local@{DEFUDDLE_VERSION}"

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
            self._npx_path,
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


def _optional_string(value: object) -> str | None:
    """Normalize blank or non-string metadata to ``None``."""

    return value.strip() if isinstance(value, str) and value.strip() else None


def build_local_defuddle_extractor() -> tuple[
    LocalDefuddleExtractor | None, str | None
]:
    """Select and warm the tranche-two local runtime.

    Direct ``node`` and ``npx`` executables are required in this tranche.  fnm
    discovery and the hosted provider are intentionally reserved for the final
    runtime-hardening tranche.
    """

    node_path = shutil.which("node")
    npx_path = shutil.which("npx")
    if node_path is None or npx_path is None:
        return None, "working node and npx executables are required"
    extractor = LocalDefuddleExtractor(npx_path)
    try:
        extractor.health_check()
    except ExtractionError as exc:
        return None, str(exc)
    return extractor, None
