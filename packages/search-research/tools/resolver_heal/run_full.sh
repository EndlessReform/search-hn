#!/usr/bin/env bash
# Pass explicit yearly inputs and spending controls to the resumable runner.
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)
uv run --no-sync --project "$repo" --package search-research python \
  "$repo/packages/search-research/tools/resolver_heal/run.py" "$@"
