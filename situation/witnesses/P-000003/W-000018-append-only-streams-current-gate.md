# W-000018 — Append-only streams current-head gate observation

## Promise

`situation/promises/P-000003-append-only-streams.md`

## Oracle

`situation/oracles/O-000003-append-only-streams.md`

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
  the named S-suite and G130 cases live under
  `crates/yeetz-s3-streams/tests/` at this head (nextest binary
  `yeetz-s3-streams`, modules `streams_contract` and `streams_loopback`).
- This witness retains already-executed regression assurance at the current
  head for the coordinated 0.5 closure. It is not an expansion of the promise
  scope and not a new current-data guarantee.

## Oracle legs

| Leg | Evidence |
|---|---|
| P1 | `PASS` (182/287) `yeetz-s3-streams::streams_contract::s1_contiguity_one_winner_per_seq` at 2026-09-10T00:40:26.8357360Z in job 102698755081. |
| P2 | `PASS` (186/287) `yeetz-s3-streams::streams_contract::s3_idempotent_reappend_converges` at 2026-09-10T00:40:26.8720397Z and `PASS` (199/287) `yeetz-s3-streams::streams_loopback::s3_lost_response_converges_on_retry` at 2026-09-10T00:40:28.5243889Z in job 102698755081. |
| P3 | `PASS` (187/287) `yeetz-s3-streams::streams_contract::s4_damage_loud_and_named` at 2026-09-10T00:40:26.8783916Z; `PASS` (201/287) `yeetz-s3-streams::streams_loopback::s4_loopback_damage_is_named` at 2026-09-10T00:40:28.8453001Z; `PASS` (195/287) `…::g130_frozen_list_without_witness_withholds_completeness` at 2026-09-10T00:40:27.5653535Z; `PASS` (196/287) `…::g130_lagging_witness_cannot_certify_hidden_suffix` at 2026-09-10T00:40:27.8738387Z; all in job 102698755081. |
