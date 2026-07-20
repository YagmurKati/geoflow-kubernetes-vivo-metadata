#!/usr/bin/env bash

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

need_command kubectl
require_namespace

RUN_ID="${1:-}"
[[ "$RUN_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] ||
  die "Usage: $0 RUN_ID"

printf '%s\n' '--- Nextflow pods ---'
kubectl -n "$NS" get pods -o wide

printf '%s\n' '--- Last 40 console-log lines ---'
kubectl -n "$NS" exec "$DRIVER_POD" -- \
  tail -n 40 "/workspace/results/nextflow-$RUN_ID.log"
