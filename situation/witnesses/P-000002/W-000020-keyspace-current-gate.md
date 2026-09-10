# W-000020 — Conditional keyspace current-head gate observation

## Promise

`situation/promises/P-000002-conditional-keyspace-operations.md`

## Oracle

`situation/oracles/O-000002-conditional-keyspace-operations.md`

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
  at this head A1, A2, and A11 live in
  `crates/yeetz-s3-kernel/tests/atomic_contract.rs` (nextest binary
  `yeetz-s3-kernel::atomic_contract`) and A19–A23 live in
  `crates/yeetz-s3-kernel/src/state_kernel.rs` (module
  `state_kernel::gateway_state_contract`).
- This witness retains already-executed regression assurance at the current
  head for the coordinated 0.5 closure. It is not an expansion of the promise
  scope and not a new current-data guarantee.

## Oracle legs

| Leg | Evidence |
|---|---|
| P1 | `PASS` (85/287) `yeetz-s3-kernel::atomic_contract::a1_create_exclusivity_one_winner_typed_conflict` at 2026-09-10T00:40:18.6107072Z in job 102698755081. |
| P2 | `PASS` (86/287) `yeetz-s3-kernel::atomic_contract::a2_cas_match_mismatch_and_concurrent_exchange` at 2026-09-10T00:40:18.6146939Z and `PASS` (80/287) `…::a11_versioned_aba_cycle_rejects_recycled_era_etag` at 2026-09-10T00:40:18.5896221Z in job 102698755081. |
| P3 | `PASS` (14/287) `yeetz-s3-kernel::state_kernel::gateway_state_contract::a19_conditional_delete_match_deletes_on_the_wire` at 2026-09-10T00:39:48.9457718Z; `PASS` (15/287) `…::a20_conditional_delete_mismatch_names_the_era` at 2026-09-10T00:39:49.1639149Z; `PASS` (16/287) `…::a21_conditional_delete_absent_key_taxonomy` at 2026-09-10T00:39:49.1808688Z; `PASS` (17/287) `…::a22_conditional_delete_incarnation_composition` at 2026-09-10T00:39:49.4280758Z; `PASS` (18/287) `…::a23_concurrent_delete_vs_cas_exactly_one_winner` at 2026-09-10T00:39:49.5737666Z; all in job 102698755081. |
