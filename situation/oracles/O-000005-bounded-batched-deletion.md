# O-000005 — Bounded typed batched-deletion contracts

## State

implemented

## Judges

`situation/promises/P-000005-bounded-batched-deletion.md`

## Inputs

A36–A45 in `crates/yeetz-s3-kernel/src/deletion_contract.rs`, their
`cargo nextest run --workspace` execution in `gates`, and source/test/workflow
identity between the execution and observation heads. A changed input set makes
a historical execution INVALID for this oracle rather than a judgment about
changed bytes.

## Pass

- P1: `a36_delete_objects_input_preflight_is_side_effect_free` establishes
  complete admission before a storage request.
- P2: `a37_delete_objects_chunks_exactly_at_1000` establishes bounded,
  sequential request chunking.
- P3: A38, A40, A41, and A42 establish typed per-key partial outcomes,
  unconfirmed-stop handling, fail-closed response reconciliation, and typed
  unsupported behavior.

## Fail

- F1: A36 observes an effect before input admission completes.
- F2: A37 observes a request with more than 1,000 keys or non-sequential
  chunking.
- F3: any named A38/A40/A41/A42 case defaults uncertain response data to
  success or loses the required typed outcome.

## Implementation

`49ba2ced98831d192f6a2371b90aec8e81a081fd:.github/workflows/ci-dev.yml`
defines `gates` to execute `cargo nextest run --workspace`; the named A-suite
contracts are in `crates/yeetz-s3-kernel/src/deletion_contract.rs`.

## Implementation coverage

| Leg | Decision | Coverage |
|---|---|---|
| P1 | A36 preflight is effect-free | `cargo nextest run --workspace` |
| F1 | A36 detects a premature effect | `cargo nextest run --workspace` |
| P2 | A37 enforces 1,000-key chunks | `cargo nextest run --workspace` |
| F2 | A37 detects oversized or non-sequential chunks | `cargo nextest run --workspace` |
| P3 | A38/A40/A41/A42 preserve typed outcomes | `cargo nextest run --workspace` |
| F3 | A38/A40/A41/A42 detect uncertain-response success | `cargo nextest run --workspace` |
