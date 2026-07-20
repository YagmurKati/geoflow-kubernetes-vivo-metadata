#!/usr/bin/env bash

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

for command in git kubectl tar python3; do
  need_command "$command"
done

require_namespace

for resource in pods persistentvolumeclaims serviceaccounts rolebindings; do
  if ! kubectl auth can-i create "$resource" -n "$NS" | grep -qx yes; then
    die "Current Kubernetes identity cannot create $resource in namespace $NS"
  fi
done

kubectl auth can-i create pods/exec -n "$NS" | grep -qx yes ||
  die "Current Kubernetes identity cannot exec into pods in namespace $NS"

printf 'Prerequisites look good.\n'
printf 'namespace=%s\n' "$NS"
printf 'kubectl_context=%s\n' "$(kubectl config current-context)"
