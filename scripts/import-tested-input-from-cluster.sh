#!/usr/bin/env bash

# Maintainer utility: copy the documented input from an existing workspace
# without changing any source file. Normal repository users do not need this
# script because the verified tile is included under data/.

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

need_command kubectl
need_command shasum
require_namespace

SOURCE_NAMESPACE="${SOURCE_NAMESPACE:-$NS}"
SOURCE_POD="${SOURCE_POD:-geoflow-uploader}"
SOURCE_DIR="${SOURCE_DIR:-/workspace/input/$INPUT_TILE}"
destination="$ROOT_DIR/data/$INPUT_TILE"

mkdir -p "$destination"

file_list="$(
  kubectl -n "$SOURCE_NAMESPACE" exec "$SOURCE_POD" -- \
    find "$SOURCE_DIR" -maxdepth 1 -type f -print |
    sed 's|.*/||' |
    LC_ALL=C sort
)"

[[ -n "$file_list" ]] || die "No source files found in $SOURCE_DIR"

while IFS= read -r name; do
  [[ "$name" =~ ^[A-Za-z0-9._-]+$ ]] ||
    die "Refusing unexpected source filename: $name"
  [[ "$name" != "SHA256SUMS" ]] || continue

  printf 'Copying %s\n' "$name"
  part_file="$destination/$name.part"
  kubectl -n "$SOURCE_NAMESPACE" exec "$SOURCE_POD" -- \
    cat "$SOURCE_DIR/$name" > "$part_file"
  mv "$part_file" "$destination/$name"
done <<< "$file_list"

(
  cd "$destination"
  for file in *; do
    [[ -f "$file" && "$file" != "SHA256SUMS" ]] || continue
    shasum -a 256 "$file"
  done
) | LC_ALL=C sort > "$destination/SHA256SUMS"

"$ROOT_DIR/scripts/verify-input.sh"
