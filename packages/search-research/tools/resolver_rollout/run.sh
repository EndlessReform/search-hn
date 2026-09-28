#!/usr/bin/env bash
# Resume both selectors and the GPU worker; create a verified snapshot on exit.
set -u
cd /home/ritsuko/projects/data/search-hn
run=data/research/books-resolver-2025-v1
tools=packages/search-research/tools/resolver_rollout
docker run --rm --name searchhn-resolver-2025-rerank --gpus all --ipc=host \
  --user "$(id -u):$(id -g)" -e HOME=/tmp -e USER=ritsuko -e LOGNAME=ritsuko -e UV_CACHE_DIR=/tmp/uv-cache -e HF_HOME=/hf -e HF_HUB_OFFLINE=1 \
  -v /home/ritsuko/projects/data/search-hn:/workspace \
  -v /home/ritsuko/.cache/huggingface:/hf:ro -w /workspace \
  --entrypoint uv \
  sha256:f37691f675bb82f734f606de8af90e777d3f80a20b120e699fd43fd10e60b8d7 \
  run --no-project --no-sync --python /usr/bin/python3 \
  "/workspace/$tools/rerank.py" >> "$run/rerank.log" 2>&1 &
gpu_pid=$!
if ! wait "$gpu_pid"; then
  uv run --no-sync --package search-research python "$tools/snapshot.py" --backup >> "$run/snapshot.log" 2>&1
  exit 1
fi
uv run --no-sync --package search-research python "$tools/selector.py" luna --concurrency 128 >> "$run/luna.log" 2>&1 &
luna_pid=$!
uv run --no-sync --package search-research python "$tools/selector.py" deepseek-native --native --concurrency 128 >> "$run/deepseek-native.log" 2>&1 &
ds_pid=$!
printf '%s\n' "$gpu_pid $luna_pid $ds_pid" > "$run/worker-pids.txt"
result=0
wait "$luna_pid" || result=1
wait "$ds_pid" || result=1
uv run --no-sync --package search-research python "$tools/snapshot.py" --backup >> "$run/snapshot.log" 2>&1 || result=1
exit "$result"
