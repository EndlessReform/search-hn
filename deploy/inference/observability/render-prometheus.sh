#!/usr/bin/env bash
# Render a scrape_configs list entry from local deployment settings. Requires jq.
set -euo pipefail
: "${EMBEDDING_METRICS_TARGET:?Set EMBEDDING_METRICS_TARGET to the full certificate DNS name:port (not a short hostname or IP)}"
target_json=$(jq -cn --arg target "$EMBEDDING_METRICS_TARGET" '$target')
cat <<YAML
- job_name: homelab-embeddings-vllm
  scheme: https
  metrics_path: /vllm/embeddings/metrics
  scrape_interval: 15s
  scrape_timeout: 10s
  static_configs:
    - targets: [$target_json]
      labels:
        service: embeddings
YAML
