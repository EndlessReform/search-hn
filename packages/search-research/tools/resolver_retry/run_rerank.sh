#!/usr/bin/env bash
# Use the previously verified FP8 image; leave other GPU services running.
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)
cd "$repo"
stage=${1:-first}
docker run --rm --name "searchhn-resolver-retry-$stage" --gpus all --ipc=host \
  --user "$(id -u):$(id -g)" -e HOME=/tmp -e USER=ritsuko -e LOGNAME=ritsuko \
  -e RESOLVER_RUN_ROOT="${RESOLVER_RUN_ROOT:-data/research/books-resolver-retry-v1}" -e RESOLVER_BASELINE="${RESOLVER_BASELINE:-}" -e PYTHONPATH=/workspace/packages/search-research/src -e UV_CACHE_DIR=/tmp/uv-cache -e HF_HOME=/hf -e HF_HUB_OFFLINE=1 \
  -v "$repo":/workspace \
  -v "${HF_HOME:-$HOME/.cache/huggingface}":/hf:ro -w /workspace --entrypoint uv \
  sha256:f37691f675bb82f734f606de8af90e777d3f80a20b120e699fd43fd10e60b8d7 \
  run --no-project --no-sync --python /usr/bin/python3 \
  /workspace/packages/search-research/tools/resolver_retry/rerank.py --stage "$stage"
