#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

DEFAULT_SOURCE_REPO="https://github.com/openai/openai-agents-python.git"
DEFAULT_DESTINATION="${REPO_ROOT}/docs/openai_agent_sdk_docs"

locked_openai_agents_ref() {
    local version
    version="$(awk '
        $0 == "name = \"openai-agents\"" { in_package = 1; next }
        in_package && $1 == "version" { gsub(/"/, "", $3); print "v" $3; exit }
    ' "${REPO_ROOT}/uv.lock")"
    if [[ -z "${version}" ]]; then
        echo "could not resolve openai-agents from ${REPO_ROOT}/uv.lock" >&2
        exit 1
    fi
    printf '%s\n' "${version}"
}

DEFAULT_SOURCE_REF="$(locked_openai_agents_ref)"

usage() {
    cat <<USAGE
Populate the ignored local OpenAI Agents SDK documentation snapshot.

The script sparse-clones only the upstream docs/ tree, installs it at the
requested destination, and records the exact source commit in .source-revision.
By default, the Git tag is derived from the openai-agents version in uv.lock.

Usage:
  $(basename "$0") [--ref REF] [--repo URL_OR_PATH] [--destination DIR] [--force]

Options:
  --ref REF          Git branch or tag (default: ${DEFAULT_SOURCE_REF})
  --repo SOURCE      Upstream Git URL or local repository (default: ${DEFAULT_SOURCE_REPO})
  --destination DIR  Documentation destination (default: ${DEFAULT_DESTINATION})
  --force            Replace an existing destination after the clone succeeds
  -h, --help         Show this help message
USAGE
}

SOURCE_REPO="${DEFAULT_SOURCE_REPO}"
SOURCE_REF="${DEFAULT_SOURCE_REF}"
DESTINATION="${DEFAULT_DESTINATION}"
FORCE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ref)
            SOURCE_REF="$2"
            shift 2
            ;;
        --repo)
            SOURCE_REPO="$2"
            shift 2
            ;;
        --destination)
            DESTINATION="$2"
            shift 2
            ;;
        --force)
            FORCE=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

command -v git >/dev/null 2>&1 || {
    echo "git is required" >&2
    exit 127
}
command -v rsync >/dev/null 2>&1 || {
    echo "rsync is required" >&2
    exit 127
}

DESTINATION="$(realpath -m "${DESTINATION}")"
if [[ "${DESTINATION}" == "/" || "${DESTINATION}" == "${REPO_ROOT}" ]]; then
    echo "refusing unsafe documentation destination: ${DESTINATION}" >&2
    exit 2
fi
if [[ -e "${DESTINATION}" && "${FORCE}" -ne 1 ]]; then
    echo "destination already exists: ${DESTINATION}" >&2
    echo "rerun with --force to replace it" >&2
    exit 2
fi

TEMP_ROOT="$(mktemp -d)"
trap 'rm -rf "${TEMP_ROOT}"' EXIT
CHECKOUT="${TEMP_ROOT}/openai-agents-python"
STAGED_DOCS="${TEMP_ROOT}/openai_agent_sdk_docs"

git clone \
    --depth 1 \
    --filter=blob:none \
    --sparse \
    --branch "${SOURCE_REF}" \
    "${SOURCE_REPO}" \
    "${CHECKOUT}"
git -C "${CHECKOUT}" sparse-checkout set docs

[[ -f "${CHECKOUT}/docs/index.md" ]] || {
    echo "upstream checkout did not contain docs/index.md" >&2
    exit 1
}

mkdir -p "${STAGED_DOCS}"
rsync -a --delete "${CHECKOUT}/docs/" "${STAGED_DOCS}/"
git -C "${CHECKOUT}" rev-parse HEAD >"${STAGED_DOCS}/.source-revision"

if [[ -e "${DESTINATION}" ]]; then
    rm -rf "${DESTINATION}"
fi
mkdir -p "$(dirname "${DESTINATION}")"
mv "${STAGED_DOCS}" "${DESTINATION}"

echo "Installed OpenAI Agents SDK docs at ${DESTINATION}"
echo "Source revision: $(<"${DESTINATION}/.source-revision")"
