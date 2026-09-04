#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INIT_SCRIPT="${SCRIPT_DIR}/init-openai-agent-sdk-docs.sh"

[[ -x "${INIT_SCRIPT}" ]] || {
    echo "expected executable init script at ${INIT_SCRIPT}" >&2
    exit 1
}

TEST_ROOT="$(mktemp -d)"
trap 'rm -rf "${TEST_ROOT}"' EXIT
SOURCE_REPO="${TEST_ROOT}/source"
DESTINATION="${TEST_ROOT}/installed-docs"

git init --quiet --initial-branch=main "${SOURCE_REPO}"
git -C "${SOURCE_REPO}" config user.name "Docs bootstrap test"
git -C "${SOURCE_REPO}" config user.email "docs-bootstrap@example.invalid"
mkdir -p "${SOURCE_REPO}/docs/assets"
printf '%s\n' '# Fixture docs' >"${SOURCE_REPO}/docs/index.md"
printf '%s\n' 'fixture asset' >"${SOURCE_REPO}/docs/assets/example.txt"
git -C "${SOURCE_REPO}" add docs
git -C "${SOURCE_REPO}" commit --quiet -m "Add fixture docs"

"${INIT_SCRIPT}" \
    --repo "${SOURCE_REPO}" \
    --ref main \
    --destination "${DESTINATION}"

test "$(<"${DESTINATION}/index.md")" = "# Fixture docs"
test "$(<"${DESTINATION}/assets/example.txt")" = "fixture asset"
test "$(<"${DESTINATION}/.source-revision")" = \
    "$(git -C "${SOURCE_REPO}" rev-parse HEAD)"

if "${INIT_SCRIPT}" \
    --repo "${SOURCE_REPO}" \
    --ref main \
    --destination "${DESTINATION}" >/dev/null 2>&1; then
    echo "expected existing destination to require --force" >&2
    exit 1
fi

"${INIT_SCRIPT}" \
    --repo "${SOURCE_REPO}" \
    --ref main \
    --destination "${DESTINATION}" \
    --force >/dev/null

echo "PASS: OpenAI Agents SDK docs bootstrap"
