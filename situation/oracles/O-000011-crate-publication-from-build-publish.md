# O-000011 — Crate publication from the build-publish runner

## State

designed

## Judges

`situation/promises/P-000011-crate-publication-from-build-publish.md`

## Inputs

- `.github/workflows/publish.yml` at the judged revision.
- For the step's own behavior: the `run` script of the step
  `publish crates to crates.io`, executed under `bash -e` with the token path
  substituted by a temporary path and `cargo` on `PATH` replaced by a stub that
  reports its arguments and whether `CARGO_REGISTRY_TOKEN` equals a sentinel
  value, in three cases: file missing, file empty, file holding the sentinel.
- When runtime execution is judged: the Actions run log of a manual dispatch
  and its checkout revision. A missing run is neither PASS nor FAIL. A run
  whose checkout differs from the judged source is INVALID for this oracle.

## Pass

- P1: `workflow_dispatch` is the workflow's only trigger.
- P2: The `publish` job declares runner group `publish` and the single label
  `build-publish`; it uses `actions/checkout@v7`, configures Rust 1.96.0, and
  has exactly one step named `publish crates to crates.io`.
- P3: The file contains no `secrets.CARGO_REGISTRY_TOKEN`, and no workflow,
  job, or step `env` declares `CARGO_REGISTRY_TOKEN`.
- P4: In the substituted execution, the missing-file and empty-file cases
  exit nonzero before the stub runs, with output containing
  `this job must run on build-publish`; the sentinel case runs the stub once
  with exactly the P-000011 literal Cargo arguments, the stub sees the
  sentinel, the shell after the stub does not have `CARGO_REGISTRY_TOKEN`
  set, and no case's output contains the sentinel.
- P5: When a run is supplied, its checkout matches the judged source, it ran
  on a runner of group `publish` with label `build-publish`, its log records
  the named step beginning the configured command, and the log does not
  contain the token value.

## Fail

- F1: Another trigger is present, or `workflow_dispatch` is absent.
- F2: The runner group or label differs, the checkout or toolchain step
  differs, or there are zero or several steps named
  `publish crates to crates.io`.
- F3: A `secrets.CARGO_REGISTRY_TOKEN` reference or an `env` declaration of
  `CARGO_REGISTRY_TOKEN` is present.
- F4: In the substituted execution, a missing or empty file reaches the stub
  or exits without the message; the stub receives other arguments or no
  sentinel; the token remains set in the shell after the command; or any
  output contains the sentinel.
- F5: A supplied run whose checkout matches the judged source fails before
  the named step begins, runs on another runner, or its log contains the
  token value. A Cargo or registry failure after the step begins does not
  refute P-000011.

## Implementation coverage

| Leg | Decision | Coverage |
|---|---|---|
| P1 | Only the manual trigger | manual |
| P2 | Runner group and label, checkout, toolchain, one named step | manual |
| P3 | No secret reference, no `env` token | manual |
| P4 | Missing/empty file refused; token reaches only Cargo, never printed | manual (substituted execution) |
| P5 | Dispatched run on build-publish, token absent from log | manual (Actions run log) |
| F1 | Trigger differs | manual |
| F2 | Runner or step shape differs | manual |
| F3 | Secret or env token present | manual |
| F4 | Substituted execution violates the token handling | manual (substituted execution) |
| F5 | Dispatched run fails early, runs elsewhere, or logs the token | manual (Actions run log) |
