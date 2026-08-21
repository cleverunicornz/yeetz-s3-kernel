#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

fixture_root="tools/testdata/storage-boundary"
if output="$(tools/check_storage_boundaries.sh "$fixture_root" 2>&1)"; then
  echo "storage-boundary test: aliased adapter fixtures passed" >&2
  exit 1
fi
for expected in aliased_aws.rs aliased_object_store.rs; do
  printf '%s\n' "$output" | grep -qF "$expected" || {
    echo "storage-boundary test: fixture was not detected: $expected" >&2
    printf '%s\n' "$output" >&2
    exit 1
  }
done

echo "storage boundary tests: clean"
