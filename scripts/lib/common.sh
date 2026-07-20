#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NS="${NS:-geoflow}"
PVC_NAME="${PVC_NAME:-geoflow-pvc}"
DRIVER_POD="${DRIVER_POD:-nextflow-driver}"
UPLOADER_POD="${UPLOADER_POD:-geoflow-uploader}"
INPUT_TILE="${INPUT_TILE:-X0103_Y0103}"

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

need_command() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

validate_dns_label() {
  local value="$1"
  local label="$2"
  [[ "$value" =~ ^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$ ]] ||
    die "$label contains unsupported characters: $value"
}

require_namespace() {
  validate_dns_label "$NS" "NS"
  kubectl get namespace "$NS" >/dev/null 2>&1 ||
    die "Kubernetes namespace '$NS' does not exist or is not accessible"
}

wait_for_pod() {
  local pod="$1"
  kubectl -n "$NS" wait --for=condition=Ready "pod/$pod" --timeout=180s
}
