#!/usr/bin/env bash

set -euo pipefail

MONITORING_NS="${MONITORING_NS:-monitoring}"
PROMETHEUS_POD="${PROMETHEUS_POD:-prometheus-prometheus-kube-prometheus-prometheus-0}"
LOCAL_PROM_PORT="${LOCAL_PROM_PORT:-19090}"

command -v kubectl >/dev/null 2>&1 || {
  printf 'ERROR: Required command not found: kubectl\n' >&2
  exit 1
}

printf 'Prometheus will be available at http://127.0.0.1:%s\n' \
  "$LOCAL_PROM_PORT"
printf 'Leave this command running while metadata is collected.\n'
kubectl -n "$MONITORING_NS" port-forward \
  "pod/$PROMETHEUS_POD" "$LOCAL_PROM_PORT:9090"
