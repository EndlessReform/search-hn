#!/usr/bin/env bash
# Retired: the selected model is shared infrastructure, not a research-owned container.
set -euo pipefail
cat >&2 <<'MESSAGE'
The Pplx service is now managed by deploy/inference/compose.yaml.
Use deploy/inference/README.md for operator instructions, or the shared raw endpoint
for historical research clients. Do not start a duplicate model on the shared GPU.
The historical launcher remains available in Git history for reproduction elsewhere.
MESSAGE
exit 1
