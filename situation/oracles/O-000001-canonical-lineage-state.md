# O-000001 — Canonical lineage-state contracts

## State

implemented

## Judges

`situation/promises/P-000001-canonical-lineage-state.md`

## Inputs

The K1, K2, and K7 tests in `crates/yeetz-s3-kernel/src/state_kernel.rs`, their
`cargo nextest run --workspace` execution in the `gates` task, and the exact
source/test/workflow identity between that execution head and the observation
head. If the identity precondition does not hold, the historical execution is
INVALID for this oracle rather than a judgment about changed bytes.

## Pass

- P1: `k1_immutable_append_positive` and `k1_immutable_append_negative`
  establish immutable append behavior and reject a conflicting append.
- P2: `k2_canonical_head_cas_positive` and
  `k2_canonical_head_cas_negative` establish conditional canonical-head
  advancement and reject an invalid/stale transition.
- P3: `k7_record_history_integrity_positive` and
  `k7_record_history_integrity_negative` establish that broken record history
  is surfaced as an integrity outcome.

## Fail

- F1: either named K1 case fails.
- F2: either named K2 case fails.
- F3: either named K7 case fails.

## Implementation

`49ba2ced98831d192f6a2371b90aec8e81a081fd:.github/workflows/ci-dev.yml`
defines `gates` to execute `cargo nextest run --workspace`; the named tests are
in `crates/yeetz-s3-kernel/src/state_kernel.rs`.

## Implementation coverage

| Leg | Decision | Coverage |
|---|---|---|
| P1 | K1 immutable append cases succeed | `cargo nextest run --workspace` |
| F1 | K1 detects conflicting/non-immutable append | `cargo nextest run --workspace` |
| P2 | K2 canonical-head CAS case succeeds | `cargo nextest run --workspace` |
| F2 | K2 detects invalid or stale head transition | `cargo nextest run --workspace` |
| P3 | K7 reports valid versus incomplete history | `cargo nextest run --workspace` |
| F3 | K7 detects incomplete history | `cargo nextest run --workspace` |
