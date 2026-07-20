#!/usr/bin/env bash

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

need_command kubectl
need_command python3
require_namespace

RUN_ID="${1:-}"
[[ "$RUN_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] ||
  die "Usage: $0 RUN_ID"

code_path="$ROOT_DIR/runtime/geoflow"
[[ -d "$code_path" ]] ||
  die "Run ./scripts/prepare-workflow.sh first"

prom_url="${PROM_URL:-http://127.0.0.1:19090}"
cached_args=()
if [[ "${INCLUDE_CACHED_ORIGIN_METRICS:-0}" == "1" ]]; then
  cached_args+=(--include-cached-origin-metrics)
fi

python3 "$ROOT_DIR/collector/collect_geoflow_workflow_metadata.py" \
  --namespace "$NS" \
  --driver-pod "$DRIVER_POD" \
  --trace-remote "/workspace/results/trace-$RUN_ID.txt" \
  --console-log-remote "/workspace/results/nextflow-$RUN_ID.log" \
  --debug-log-remote /workspace/geoflow/.nextflow.log \
  --trace-timezone UTC \
  --code-path "$code_path" \
  --prom-url "$prom_url" \
  --carbon-intensity-source co2map \
  --co2map-state DE \
  --co2map-country DE \
  --co2map-data-status preliminary \
  --output-dir "$ROOT_DIR/metadata/generated" \
  "${cached_args[@]}"
