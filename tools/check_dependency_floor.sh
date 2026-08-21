#!/usr/bin/env bash
# Every workspace crate outside the storage closure may reach an object-storage
# adapter only through yeetz-s3-kernel. Cargo's resolved package names defeat
# dependency aliases; the whole-workspace, all-features, all-targets tree
# includes optional and target-specific routes plus future workspace members
# without a hand-maintained root list.
set -euo pipefail

cd "$(dirname "$0")/.."

tree_file="$(mktemp)"
trap 'rm -f "$tree_file"' EXIT

"${CARGO:-cargo}" tree \
  --locked \
  --workspace \
  --all-features \
  --target all \
  --edges normal,build,dev \
  --no-dedupe \
  --prefix depth \
  --format '|{p}' >"$tree_file"

path=()
root=""
root_is_sanctioned=false
application_roots=0
violations=""

while IFS= read -r line; do
  [ -n "$line" ] || continue
  depth="${line%%|*}"
  package_and_version="${line#*|}"
  case "$depth" in
    ''|*[!0-9]*)
      echo "dependency floor: unparseable cargo tree row: $line" >&2
      exit 1
      ;;
  esac
  [ "$package_and_version" != "$line" ] && [ -n "$package_and_version" ] || {
    echo "dependency floor: unparseable cargo tree row: $line" >&2
    exit 1
  }
  package="${package_and_version%% *}"
  path[$depth]="$package"

  if [ "$depth" -eq 0 ]; then
    root="$package"
    case "$root" in
      yeetz-s3-kernel|yeetz-sdk-core|yeetz-sdk-s3) root_is_sanctioned=true ;;
      *)
        root_is_sanctioned=false
        application_roots=$((application_roots + 1))
        ;;
    esac
    continue
  fi

  $root_is_sanctioned && continue
  case "$package" in
    yeetz-sdk-core|yeetz-sdk-s3|object_store|aws-sdk-s3) ;;
    *) continue ;;
  esac

  behind_floor=false
  for ((index = 0; index < depth; index++)); do
    if [ "${path[$index]-}" = "yeetz-s3-kernel" ]; then
      behind_floor=true
      break
    fi
  done
  $behind_floor && continue

  offending_path="$root"
  for ((index = 1; index <= depth; index++)); do
    [ -n "${path[$index]-}" ] || {
      echo "dependency floor: cargo tree skipped an ancestor before: $line" >&2
      exit 1
    }
    offending_path="$offending_path -> ${path[$index]}"
  done
  case "$violations" in
    *"  $offending_path"*) ;;
    *) violations="$violations  $offending_path
" ;;
  esac
done <"$tree_file"

[ "$application_roots" -gt 0 ] || {
  echo "dependency floor: no application workspace roots found" >&2
  exit 1
}

if [ -n "$violations" ]; then
  echo "DEPENDENCY FLOOR VIOLATIONS (object storage reachable before yeetz-s3-kernel):"
  printf '%s' "$violations"
  echo ""
  echo "Application crates must receive opaque kernel handles. Remove the"
  echo "offending manifest edge or route the dependency through yeetz-s3-kernel."
  exit 1
fi

echo "dependency floor: clean ($application_roots application workspace roots checked)"
