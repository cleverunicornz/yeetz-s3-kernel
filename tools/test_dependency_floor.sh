#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fake_cargo="$tmp/cargo"

cat >"$fake_cargo" <<'FAKE_CARGO'
#!/usr/bin/env bash
set -euo pipefail
args=" $* "
for required in tree --locked --workspace --all-features --no-dedupe; do
  case "$args" in
    *" $required "*) ;;
    *) echo "dependency-floor test: cargo invocation omitted $required" >&2; exit 90 ;;
  esac
done
case "$args" in
  *" --target all "*) ;;
  *) echo "dependency-floor test: cargo invocation omitted --target all" >&2; exit 92 ;;
esac
case "$args" in
  *" --package "*) echo "dependency-floor test: graph was narrowed by --package" >&2; exit 91 ;;
esac
cat "${DEPENDENCY_FLOOR_FIXTURE:?}"
FAKE_CARGO
chmod +x "$fake_cargo"

run_pass() {
  fixture="$1"
  CARGO="$fake_cargo" DEPENDENCY_FLOOR_FIXTURE="$fixture" \
    tools/check_dependency_floor.sh >/dev/null
}

run_fail() {
  fixture="$1"
  expected="$2"
  if output="$(CARGO="$fake_cargo" DEPENDENCY_FLOOR_FIXTURE="$fixture" \
    tools/check_dependency_floor.sh 2>&1)"; then
    echo "dependency-floor test: forbidden fixture passed: $fixture" >&2
    exit 1
  fi
  printf '%s\n' "$output" | grep -qF "$expected" || {
    echo "dependency-floor test: missing path $expected" >&2
    printf '%s\n' "$output" >&2
    exit 1
  }
}

fixtures="tools/testdata/dependency-floor"
run_pass "$fixtures/behind-kernel.tree"
run_fail "$fixtures/aliased-direct-aws.tree" \
  "future-app -> aws-sdk-s3"
run_fail "$fixtures/transitive-object-store.tree" \
  "future-app -> neutral-helper -> object_store"
run_fail "$fixtures/optional-workspace-member.tree" \
  "yeetz-s3-streams -> optional-bridge -> yeetz-sdk-s3"
run_fail "$fixtures/target-specific-aws.tree" \
  "yeetz-runner -> aws-sdk-s3"

echo "dependency floor tests: clean"
