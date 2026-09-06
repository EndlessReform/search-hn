#!/usr/bin/env bash
# Run only on maya@magi06-inference. Native vLLM; no patched model/server code.
set -euo pipefail
precision=${1:?Pass float32 or bfloat16}
backend=${2:-FLEX_ATTENTION}
case "$precision" in float32|bfloat16) ;; *) exit 2 ;; esac
case "$backend" in FLEX_ATTENTION|FLASH_ATTN) ;; *) exit 2 ;; esac
image_ref=${VLLM_IMAGE:?Set the pinned Docker image digest}
docker run -d --name "searchhn-pplx-vllm-${precision}-${backend}-20260905" \
  --user 1000:1000 --gpus all --shm-size 1g \
  -p 127.0.0.1:8080:8000 \
  -v /opt/searchhn-embeddings/hf-cache:/hf-cache \
  -v /opt/searchhn-embeddings/vllm:/cache \
  -e HF_HUB_CACHE=/hf-cache -e HF_HOME=/cache/hf-home \
  -e VLLM_CACHE_ROOT=/cache/vllm-cache -e XDG_CACHE_HOME=/cache/xdg \
  -e TRITON_CACHE_DIR=/cache/triton -e VLLM_NO_USAGE_STATS=1 \
  "$image_ref" perplexity-ai/pplx-embed-v1-0.6b \
  --revision 2c4d510dd4a732063c31a0f70193e35067b51fd8 \
  --trust-remote-code --model-impl vllm --runner pooling --convert embed \
  --hf-overrides '{"architectures":["Qwen3Model"],"is_causal":false}' \
  --pooler-config '{"pooling_type":"MEAN","use_activation":false}' \
  --dtype "$precision" --attention-backend "$backend" \
  --max-model-len 2048 --max-num-seqs 64 --max-num-batched-tokens 8192 \
  --gpu-memory-utilization 0.35 --kv-cache-memory-bytes 0 \
  --no-enable-prefix-caching --no-enable-chunked-prefill --enforce-eager \
  --host 0.0.0.0 --port 8000 --served-model-name pplx-embed-v1-0.6b
