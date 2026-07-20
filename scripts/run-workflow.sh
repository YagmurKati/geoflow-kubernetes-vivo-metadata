#!/usr/bin/env bash

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

need_command kubectl
require_namespace

RUN_ID="${1:-}"
[[ "$RUN_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] ||
  die "Usage: $0 RUN_ID (letters, digits, dot, underscore, and hyphen only)"

resume_arg=""
if [[ "${RESUME:-0}" == "1" ]]; then
  resume_arg="-resume"
fi

wait_for_pod "$DRIVER_POD"

printf 'Starting run %s in namespace %s.\n' "$RUN_ID" "$NS"
printf 'This command remains attached until Nextflow completes.\n'

kubectl -n "$NS" exec -i "$DRIVER_POD" -- bash -lc "
  set -o pipefail
  cd /workspace/geoflow
  export GEOFLOW_NAMESPACE='$NS'
  export GEOFLOW_PVC='$PVC_NAME'
  export NXF_ANSI_LOG=false
  nextflow run main.nf \
    -c nextflow.k8s.config \
    $resume_arg \
    --input_dirP '/workspace/input/$INPUT_TILE/*{BOA,QAI}.tif' \
    --output_dir_indices /workspace/results \
    --n_cpus_indices 1 \
    -with-report '/workspace/results/report-$RUN_ID.html' \
    -with-trace '/workspace/results/trace-$RUN_ID.txt' \
    -with-timeline '/workspace/results/timeline-$RUN_ID.html' \
    2>&1 | tee '/workspace/results/nextflow-$RUN_ID.log'
"
