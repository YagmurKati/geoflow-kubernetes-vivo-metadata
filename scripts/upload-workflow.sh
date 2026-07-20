#!/usr/bin/env bash

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

need_command kubectl
need_command tar
require_namespace

workflow_dir="$ROOT_DIR/runtime/geoflow"
[[ -d "$workflow_dir/.git" ]] ||
  die "Run ./scripts/prepare-workflow.sh first"

wait_for_pod "$UPLOADER_POD"
kubectl -n "$NS" exec "$UPLOADER_POD" -- \
  mkdir -p /workspace/geoflow /workspace/input /workspace/results /workspace/work

# The Nextflow driver image has no tar binary. The Alpine uploader shares the
# same PVC and is intentionally used for all archive extraction.
tar --exclude=.git -C "$workflow_dir" -cf - . |
  kubectl -n "$NS" exec -i "$UPLOADER_POD" -- \
    tar -xf - -C /workspace/geoflow

kubectl -n "$NS" exec "$UPLOADER_POD" -- \
  test -f /workspace/geoflow/main.nf
printf 'Uploaded patched workflow to %s:/workspace/geoflow\n' "$UPLOADER_POD"
