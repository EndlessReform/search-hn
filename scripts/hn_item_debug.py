#!/usr/bin/env python3
"""Fetch and inspect Hacker News item payloads by ID.

This script is intentionally lightweight for recurring debugging: it performs an
HTTP GET to `/v0/item/<id>.json`, pretty-prints the JSON payload, and reports
string fields that contain suspicious characters (for example the NUL byte,
which can trigger Postgres `invalid byte sequence for encoding "UTF8": 0x00`
errors when inserted into text columns).

Examples:
    uv run python scripts/hn_item_debug.py 8073000
    uv run python scripts/hn_item_debug.py 8863 2921983 --base-url https://hacker-news.firebaseio.com/v0
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

DEFAULT_BASE_URL = "https://hacker-news.firebaseio.com/v0"


@dataclass(frozen=True)
class StringIssue:
    """Describes a suspicious string discovered inside the payload."""

    path: str
    issue: str


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for one or more item IDs."""
    parser = argparse.ArgumentParser(
        description=(
            "Fetch and pretty-print Hacker News item payloads by ID, with a "
            "scan for NUL/control characters in string fields."
        )
    )
    parser.add_argument(
        "item_ids",
        metavar="ITEM_ID",
        nargs="+",
        type=int,
        help="One or more Hacker News item IDs.",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="HN API base URL (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Print compact JSON instead of pretty indented JSON.",
    )
    parser.add_argument(
        "--fail-on-missing",
        action="store_true",
        help="Exit non-zero if any requested item returns JSON null.",
    )
    return parser.parse_args()


def item_url(base_url: str, item_id: int) -> str:
    """Build a normalized item endpoint URL."""
    normalized = base_url.rstrip("/")
    return f"{normalized}/item/{item_id}.json"


def fetch_item(base_url: str, item_id: int, timeout_seconds: float) -> Any:
    """Fetch and decode one item payload as JSON.

    Raises:
        RuntimeError: if the request fails, returns non-200, or JSON is invalid.
    """
    url = item_url(base_url, item_id)
    request = urllib.request.Request(url=url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status = response.getcode()
            body = response.read()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP error for item {item_id}: status={exc.code} url={url}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"network error for item {item_id}: {exc.reason}") from exc

    if status != 200:
        raise RuntimeError(f"unexpected status for item {item_id}: status={status} url={url}")

    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid JSON for item {item_id}: {exc}") from exc


def find_string_issues(value: Any, path: str = "$") -> list[StringIssue]:
    """Recursively scan a JSON-compatible object for suspicious string content."""
    issues: list[StringIssue] = []

    if isinstance(value, dict):
        for key, nested in value.items():
            child_path = f"{path}.{key}"
            issues.extend(find_string_issues(nested, child_path))
        return issues

    if isinstance(value, list):
        for idx, nested in enumerate(value):
            child_path = f"{path}[{idx}]"
            issues.extend(find_string_issues(nested, child_path))
        return issues

    if isinstance(value, str):
        if "\x00" in value:
            issues.append(StringIssue(path=path, issue="contains NUL byte (\\x00)"))

        for idx, ch in enumerate(value):
            codepoint = ord(ch)
            # Report most control chars except common whitespace escapes.
            if codepoint < 32 and ch not in ("\n", "\r", "\t"):
                issues.append(
                    StringIssue(
                        path=path,
                        issue=f"contains control char U+{codepoint:04X} at char index {idx}",
                    )
                )
                break

    return issues


def render_json(value: Any, compact: bool) -> str:
    """Serialize value in either compact or indented form."""
    if compact:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def main() -> int:
    """Entrypoint for CLI execution."""
    args = parse_args()

    missing_ids: list[int] = []
    any_errors = False

    for idx, item_id in enumerate(args.item_ids):
        if idx > 0:
            print("\n" + "=" * 80 + "\n")

        print(f"item_id: {item_id}")
        print(f"url: {item_url(args.base_url, item_id)}")

        try:
            payload = fetch_item(args.base_url, item_id, args.timeout_seconds)
        except RuntimeError as exc:
            any_errors = True
            print(f"error: {exc}", file=sys.stderr)
            continue

        if payload is None:
            missing_ids.append(item_id)
            print("result: null (item missing)")
            continue

        if not isinstance(payload, dict):
            print(f"warning: expected object payload, got {type(payload).__name__}")

        print("payload:")
        print(render_json(payload, compact=args.compact))

        issues = find_string_issues(payload)
        if issues:
            print("\nstring_issues:")
            for issue in issues:
                print(f"- {issue.path}: {issue.issue}")
        else:
            print("\nstring_issues: none")

    if args.fail_on_missing and missing_ids:
        print(f"missing item IDs: {missing_ids}", file=sys.stderr)
        return 2

    if any_errors:
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
