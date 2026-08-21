#!/usr/bin/env bash
# Mechanical enforcement of the storage-boundary invariant:
# the kernel closure is the ONLY
# code that touches object storage. Adapter calls anywhere else fail.
#
# Known legacy debt is ratcheted: files in the allowlist are exempt
# until they are migrated onto kernel APIs; removing a file from the
# allowlist without removing its adapter calls fails CI.
set -euo pipefail

cd "$(dirname "$0")/.."

ALLOWLIST="tools/storage-boundary-allowlist"

# Adapter-surface identifiers: near-zero false-positive names that every
# bypass must use to reach storage (the adapter type, its import path,
# or its method names).
PATTERN='ObjectStoreClient|yeetz_sdk_s3|object_store|aws_sdk_s3|AmazonS3Builder|list_prefix|upload_conditional|download_if_changed|get_object|put_object|delete_object|list_objects_v2'

is_sanctioned() {
  case "$1" in
    crates/yeetz-s3-kernel/*|crates/yeetz-sdk-core/*|crates/yeetz-sdk-s3/*) return 0 ;;
    *) return 1 ;;
  esac
}

count=0
violations=""
if [ "$#" -eq 0 ]; then
  source_roots=(crates rigs)
else
  source_roots=("$@")
fi
for root in "${source_roots[@]}"; do
  [ -e "$root" ] || {
    echo "storage boundary: source root does not exist: $root" >&2
    exit 1
  }
done
while IFS= read -r f; do
  is_sanctioned "$f" && continue
  count=$((count + 1))
  grep -qE "$PATTERN" "$f" || continue
  if [ -f "$ALLOWLIST" ] && grep -qxF "$f" "$ALLOWLIST"; then continue; fi
  violations="$violations$f
"
done < <(find "${source_roots[@]}" -type f -name '*.rs' | sort)

[ "$count" -gt 0 ] || { echo "no application sources found?"; exit 1; }

if [ -n "$violations" ]; then
  echo "STORAGE BOUNDARY VIOLATIONS (adapter access outside the kernel closure):"
  printf '  %s' "$violations"
  echo ""
  echo "The kernel closure is the only S3 client (see .agents/skills/state-kernel)."
  echo "A missing kernel capability is a BLOCKING escalation to the human —"
  echo "never a raw-adapter workaround. If this is pre-approved migration debt,"
  echo "the file must be in tools/storage-boundary-allowlist."
  exit 1
fi

echo "storage boundary: clean ($count application sources checked)"
