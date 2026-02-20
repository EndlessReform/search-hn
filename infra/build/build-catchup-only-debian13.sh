#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DOCKERFILE="${SCRIPT_DIR}/debian13-catchup-only.Dockerfile"

DEFAULT_OUT_DIR="${REPO_ROOT}/dist/debian13"
DEFAULT_IMAGE_TAG="search-hn/catchup-builder:debian13"

usage() {
    cat <<USAGE
Build all catchup_worker binaries in a Debian 13 (trixie) container and export them.

Usage:
  $(basename "$0") [--out-dir DIR] [--image-tag TAG] [--jobs N] [--no-pull] [--dry-run]

Options:
  --out-dir DIR    Output directory for the built binary (default: ${DEFAULT_OUT_DIR})
  --image-tag TAG  Docker image tag to build/run (default: ${DEFAULT_IMAGE_TAG})
  --jobs N         Cargo parallel jobs (default: detected host core count)
  --no-pull        Skip --pull when building the image
  --dry-run        Print commands and exit without running Docker
  -h, --help       Show this help message
USAGE
}

host_cores() {
    if command -v nproc >/dev/null 2>&1; then
        nproc
        return
    fi

    if command -v getconf >/dev/null 2>&1; then
        getconf _NPROCESSORS_ONLN
        return
    fi

    if command -v sysctl >/dev/null 2>&1; then
        sysctl -n hw.ncpu
        return
    fi

    echo 4
}

resolve_source_commit_hash() {
    if hash_value="$(git -C "${REPO_ROOT}" rev-parse --verify HEAD 2>/dev/null)"; then
        printf '%s\n' "${hash_value}"
        return
    fi

    if [[ -n "${SOURCE_COMMIT_HASH:-}" ]]; then
        printf '%s\n' "${SOURCE_COMMIT_HASH}"
        return
    fi

    printf '%s\n' "unknown"
}

OUT_DIR="${DEFAULT_OUT_DIR}"
IMAGE_TAG="${DEFAULT_IMAGE_TAG}"
JOBS="$(host_cores)"
DO_PULL=1
DRY_RUN=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --out-dir)
            OUT_DIR="$2"
            shift 2
            ;;
        --image-tag)
            IMAGE_TAG="$2"
            shift 2
            ;;
        --jobs)
            JOBS="$2"
            shift 2
            ;;
        --no-pull)
            DO_PULL=0
            shift
            ;;
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            exit 2
            ;;
    esac
done

if [[ ! "$JOBS" =~ ^[0-9]+$ ]] || [[ "$JOBS" -le 0 ]]; then
    echo "--jobs must be a positive integer, got: ${JOBS}" >&2
    exit 2
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required on the build host" >&2
    exit 127
fi

SOURCE_COMMIT_HASH="$(resolve_source_commit_hash)"
mkdir -p "${OUT_DIR}"

BUILD_CMD=(docker build -f "${DOCKERFILE}" -t "${IMAGE_TAG}" "${REPO_ROOT}")
if [[ "${DO_PULL}" -eq 1 ]]; then
    BUILD_CMD=(docker build --pull -f "${DOCKERFILE}" -t "${IMAGE_TAG}" "${REPO_ROOT}")
fi

RUN_CMD=(
    docker run --rm
    -e CARGO_BUILD_JOBS="${JOBS}"
    -e CARGO_TARGET_DIR="/tmp/cargo-target"
    -e SOURCE_COMMIT_HASH="${SOURCE_COMMIT_HASH}"
    -v "${REPO_ROOT}:/src:ro"
    -v "${OUT_DIR}:/out"
    -w /src/crates
    "${IMAGE_TAG}"
    bash -lc 'set -euo pipefail; cargo build --release --locked -p catchup_worker --bins; for bin in catchup_worker catchup_only story_id_backfill; do cp "$CARGO_TARGET_DIR/release/$bin" "/out/$bin"; done'
)

if [[ "${DRY_RUN}" -eq 1 ]]; then
    printf 'Resolved SOURCE_COMMIT_HASH: %s\n\n' "${SOURCE_COMMIT_HASH}"
    printf 'Would run build command:\n  %q' "${BUILD_CMD[0]}"
    for ((i = 1; i < ${#BUILD_CMD[@]}; i++)); do
        printf ' %q' "${BUILD_CMD[$i]}"
    done
    printf '\n\nWould run container command:\n  %q' "${RUN_CMD[0]}"
    for ((i = 1; i < ${#RUN_CMD[@]}; i++)); do
        printf ' %q' "${RUN_CMD[$i]}"
    done
    printf '\n'
    exit 0
fi

"${BUILD_CMD[@]}"
"${RUN_CMD[@]}"

echo "Built binaries at ${OUT_DIR}/: catchup_worker, catchup_only, story_id_backfill"
