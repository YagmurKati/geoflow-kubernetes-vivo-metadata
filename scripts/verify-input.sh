#!/usr/bin/env bash

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

need_command python3

python3 "$ROOT_DIR/scripts/validate-repository.py" --input-only
