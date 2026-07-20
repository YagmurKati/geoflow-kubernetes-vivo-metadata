#!/usr/bin/env bash

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

need_command kubectl
need_command tar
require_namespace

"$ROOT_DIR/scripts/verify-input.sh"
wait_for_pod "$UPLOADER_POD"

kubectl -n "$NS" exec "$UPLOADER_POD" -- \
  mkdir -p /workspace/input

tar -C "$ROOT_DIR/data" -cf - "$INPUT_TILE" |
  kubectl -n "$NS" exec -i "$UPLOADER_POD" -- \
    tar -xf - -C /workspace/input

printf 'Uploaded data/%s to /workspace/input/%s\n' \
  "$INPUT_TILE" "$INPUT_TILE"
