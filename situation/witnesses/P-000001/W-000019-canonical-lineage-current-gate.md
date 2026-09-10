# W-000019 — Canonical lineage current-head gate observation

## Promise

`situation/promises/P-000001-canonical-lineage-state.md`

## Oracle

`situation/oracles/O-000001-canonical-lineage-state.md`

## Result

PASS

## Head

`43fb1f661ad91363fc99ae257ed4e62ea8207303`

## Observed

2026-09-10

## Evidence

- Run: `https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34421839069`
  — ci-dev workflow dispatched with task `gates`; run checkout SHA
  `43fb1f661ad91363fc99ae257ed4e62ea8207303`.
- Job:
  `https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34421839069/job/102698755081`
  — `gates (full set)`, conclusion success, runner label `cvu-test-runner-x64`,
  runner name `cvu-test-runner-de1-2ee94c92`, started 2026-09-10T00:36:41Z,
  completed 2026-09-10T00:40:47Z.
- `.github/workflows/ci-dev.yml` at this head defines `gates (full set)` to run
  `cargo fmt --all --check`, `cargo clippy --workspace --all-targets
  --all-features -- -D warnings`, `cargo build --workspace --locked`, then
  `cargo nextest run --workspace --no-fail-fast`. The job log records nextest
  run ID `bc89783a-f8e9-4779-8fac-2915b740cfdc`, 287 tests across 19 binaries,
  and the closing summary at 2026-09-10T00:40:43.1555626Z: `287 tests run: 287
  passed, 4 skipped`. The four skipped tests are not individually enumerated in
  the run log; no oracle-named case is among them, because every case named
  below carries its own PASS line in the same log.
- Identity precondition: the run's checkout SHA equals the observation head;
  the named K1/K2/K7 cases live in
  `crates/yeetz-s3-kernel/src/state_kernel.rs` (nextest binary `yeetz-s3-kernel`,
  module `state_kernel::gateway_state_contract`) at this head.
- This witness retains already-executed regression assurance at the current
  head for the coordinated 0.5 closure. It is not an expansion of the promise
  scope and not a new current-data guarantee.

## Oracle legs

| Leg | Evidence |
|---|---|
| P1 | `PASS` (26/287) `yeetz-s3-kernel::state_kernel::gateway_state_contract::k1_immutable_append_positive` at 2026-09-10T00:39:50.3062152Z and `PASS` (25/287) `…::k1_immutable_append_negative` at 2026-09-10T00:39:50.2321008Z in job 102698755081. |
| P2 | `PASS` (27/287) `…::k2_canonical_head_cas_positive` at 2026-09-10T00:39:50.4794917Z and `PASS` (30/287) `…::k2_canonical_head_cas_negative` at 2026-09-10T00:39:50.9234640Z in job 102698755081. |
| P3 | `PASS` (38/287) `…::k7_record_history_integrity_positive` at 2026-09-10T00:39:53.3892240Z and `PASS` (36/287) `…::k7_record_history_integrity_negative` at 2026-09-10T00:39:53.1627736Z in job 102698755081. |
