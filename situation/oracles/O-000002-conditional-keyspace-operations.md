# O-000002 — Conditional keyspace-operation contracts

## State

implemented

## Judges

`situation/promises/P-000002-conditional-keyspace-operations.md`

## Inputs

The A1, A2, A11, and A19–A23 contracts in
`crates/yeetz-s3-kernel/tests/atomic_contract.rs` and
`crates/yeetz-s3-kernel/src/state_kernel.rs`, their `cargo nextest run
--workspace` execution in `gates`, and source/test/workflow identity between
the execution and observation heads. Changed bytes make a historical execution
INVALID for this oracle rather than a judgment about changed behavior.

## Pass

- P1: `a1_create_exclusivity_one_winner_typed_conflict` establishes that create
  has one winner and reports a collision.
- P2: `a2_cas_match_mismatch_and_concurrent_exchange` and
  `a11_versioned_aba_cycle_rejects_recycled_era_etag` establish ETag- and
  era-aware compare-and-swap behavior.
- P3: A19–A23 establish that conditional deletion uses a matching ETag,
  reports mismatch/absence taxonomy, and admits one winner against CAS.

## Fail

- F1: the A1 exclusivity/conflict case fails.
- F2: either named compare-and-swap/ABA case fails.
- F3: any A19–A23 conditional-delete case fails.

## Implementation

`49ba2ced98831d192f6a2371b90aec8e81a081fd:.github/workflows/ci-dev.yml`
defines `gates` to execute `cargo nextest run --workspace`; the named A-suite
contracts are in the listed current kernel test sources.

## Implementation coverage

| Leg | Decision | Coverage |
|---|---|---|
| P1 | A1 create exclusivity succeeds | `cargo nextest run --workspace` |
| F1 | A1 detects a conflicting create | `cargo nextest run --workspace` |
| P2 | A2/A11 conditional CAS succeeds | `cargo nextest run --workspace` |
| F2 | A2/A11 reject stale or recycled-era CAS | `cargo nextest run --workspace` |
| P3 | A19–A23 conditional delete succeeds | `cargo nextest run --workspace` |
| F3 | A19–A23 report failed conditional deletion | `cargo nextest run --workspace` |
