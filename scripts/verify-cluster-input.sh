#!/usr/bin/env bash

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

need_command kubectl
require_namespace
wait_for_pod "$UPLOADER_POD"

kubectl -n "$NS" exec "$UPLOADER_POD" -- sh -lc \
  "cd '/workspace/input/$INPUT_TILE' && sha256sum -c SHA256SUMS"

printf 'Cluster input checksums match the repository manifest.\n'
