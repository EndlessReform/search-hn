#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_SCRIPT="${SCRIPT_DIR}/build-catchup-only-debian13.sh"
DOCKERFILE="${SCRIPT_DIR}/debian13-catchup-only.Dockerfile"

[[ -x "${BUILD_SCRIPT}" ]] || {
    echo "expected executable build script at ${BUILD_SCRIPT}" >&2
    exit 1
}

[[ -f "${DOCKERFILE}" ]] || {
    echo "expected Dockerfile at ${DOCKERFILE}" >&2
    exit 1
}

rg -q '^FROM debian:trixie-slim$' "${DOCKERFILE}"
rg -q 'libssl-dev' "${DOCKERFILE}"
rg -q 'libpq-dev' "${DOCKERFILE}"

OUT_DIR="/tmp/search-hn-build-test-$$"
OUTPUT_FILE="/tmp/search-hn-build-test-output-$$.txt"
trap 'rm -rf "${OUT_DIR}" "${OUTPUT_FILE}"' EXIT

"${BUILD_SCRIPT}" --dry-run --out-dir "${OUT_DIR}" >"${OUTPUT_FILE}"

rg -q 'Resolved SOURCE_COMMIT_HASH:' "${OUTPUT_FILE}"
rg -q 'SOURCE_COMMIT_HASH=' "${OUTPUT_FILE}"
rg -q 'debian13-catchup-only.Dockerfile' "${OUTPUT_FILE}"
rg -q 'CARGO_TARGET_DIR=/tmp/cargo-target' "${OUTPUT_FILE}"
rg -q 'catchup_worker' "${OUTPUT_FILE}"
rg -q -- '--bins' "${OUTPUT_FILE}"
rg -q 'catchup_worker' "${OUTPUT_FILE}"
rg -q 'catchup_only' "${OUTPUT_FILE}"
rg -q 'story_id_backfill' "${OUTPUT_FILE}"
rg -q -- '--release' "${OUTPUT_FILE}"
rg -q -- '--locked' "${OUTPUT_FILE}"
rg -q 'for\\ bin\\ in\\ catchup_worker\\ catchup_only\\ story_id_backfill' "${OUTPUT_FILE}"
rg -Fq 'release/\$bin' "${OUTPUT_FILE}"

echo "PASS: Debian 13 builder smoke test"
