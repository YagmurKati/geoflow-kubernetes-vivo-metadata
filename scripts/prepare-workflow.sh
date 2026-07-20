#!/usr/bin/env bash

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

need_command git

upstream_url="https://github.com/CRC-FONDA/geoflow.git"
upstream_commit="$(tr -d '[:space:]' < "$ROOT_DIR/workflow/UPSTREAM_COMMIT")"
workflow_dir="$ROOT_DIR/runtime/geoflow"
patch_file="$ROOT_DIR/patches/geoflow-kubernetes.patch"

if [[ ! -d "$workflow_dir/.git" ]]; then
  mkdir -p "$ROOT_DIR/runtime"
  git clone "$upstream_url" "$workflow_dir"
fi

current_commit="$(git -C "$workflow_dir" rev-parse HEAD)"
if [[ "$current_commit" != "$upstream_commit" ]]; then
  if ! git -C "$workflow_dir" diff --quiet ||
    ! git -C "$workflow_dir" diff --cached --quiet; then
    die "$workflow_dir has changes on a different commit; preserve it manually"
  fi
  git -C "$workflow_dir" fetch --quiet origin "$upstream_commit"
  git -C "$workflow_dir" checkout --detach "$upstream_commit"
fi

if git -C "$workflow_dir" apply --unidiff-zero --check \
  "$patch_file" 2>/dev/null; then
  git -C "$workflow_dir" apply --unidiff-zero "$patch_file"
elif git -C "$workflow_dir" apply --unidiff-zero --reverse --check \
  "$patch_file" 2>/dev/null; then
  printf 'Compatibility patch is already applied.\n'
else
  die "Compatibility patch does not apply to pinned upstream commit"
fi

cp "$ROOT_DIR/workflow/nextflow.k8s.config" \
  "$workflow_dir/nextflow.k8s.config"

printf 'Prepared Geoflow at %s\n' "$workflow_dir"
printf 'upstream_commit=%s\n' "$upstream_commit"
git -C "$workflow_dir" status --short
