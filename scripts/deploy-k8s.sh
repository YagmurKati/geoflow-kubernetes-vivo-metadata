#!/usr/bin/env bash

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

need_command kubectl
need_command sed
require_namespace

STORAGE_CLASS="${STORAGE_CLASS:-cephfs}"
[[ "$STORAGE_CLASS" =~ ^[A-Za-z0-9._-]+$ ]] ||
  die "STORAGE_CLASS contains unsupported characters"
validate_dns_label "$PVC_NAME" "PVC_NAME"
validate_dns_label "$DRIVER_POD" "DRIVER_POD"
validate_dns_label "$UPLOADER_POD" "UPLOADER_POD"

kubectl -n "$NS" apply -f "$ROOT_DIR/k8s/rbac.yaml"
sed \
  -e "s/name: geoflow-pvc/name: ${PVC_NAME}/" \
  -e "s/storageClassName: cephfs/storageClassName: ${STORAGE_CLASS}/" \
  "$ROOT_DIR/k8s/pvc.yaml" |
  kubectl -n "$NS" apply -f -

kubectl -n "$NS" wait \
  --for=jsonpath='{.status.phase}'=Bound \
  "pvc/$PVC_NAME" --timeout=180s

sed \
  -e "s/name: nextflow-driver/name: ${DRIVER_POD}/" \
  -e "s/claimName: geoflow-pvc/claimName: ${PVC_NAME}/" \
  "$ROOT_DIR/k8s/nextflow-driver.yaml" |
  kubectl -n "$NS" apply -f -
sed \
  -e "s/name: geoflow-uploader/name: ${UPLOADER_POD}/" \
  -e "s/claimName: geoflow-pvc/claimName: ${PVC_NAME}/" \
  "$ROOT_DIR/k8s/uploader.yaml" |
  kubectl -n "$NS" apply -f -

wait_for_pod "$DRIVER_POD"
wait_for_pod "$UPLOADER_POD"

kubectl -n "$NS" get pvc "$PVC_NAME"
kubectl -n "$NS" get pods "$DRIVER_POD" "$UPLOADER_POD" -o wide
