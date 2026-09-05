# O-000003 — Append-only stream contracts

## State

implemented

## Judges

`situation/promises/P-000003-append-only-streams.md`

## Inputs

The S1–S4 and G130 tests in `crates/yeetz-s3-streams/tests/`, their
`cargo nextest run --workspace` execution in `gates`, and source/test/workflow
identity between the execution and observation heads. A changed input set makes
a historical execution INVALID for this oracle rather than a judgment about
changed bytes.

## Pass

- P1: `s1_contiguity_one_winner_per_seq` establishes one conditional-create
  winner per sequence.
- P2: `s3_idempotent_reappend_converges` and
  `s3_lost_response_converges_on_retry` establish bounded retry convergence.
- P3: `s4_damage_loud_and_named`, `s4_loopback_damage_is_named`, and the G130
  frozen-LIST cases establish named damage and withheld completeness.

## Fail

- F1: the S1 allocation case fails.
- F2: either named S3 retry-convergence case fails.
- F3: any named S4 or G130 damage/completeness case fails.

## Implementation

`49ba2ced98831d192f6a2371b90aec8e81a081fd:.github/workflows/ci-dev.yml`
defines `gates` to execute `cargo nextest run --workspace`; the named S-suite
and G130 contracts are in `crates/yeetz-s3-streams/tests/`.

## Implementation coverage

| Leg | Decision | Coverage |
|---|---|---|
| P1 | S1 one-winner allocation succeeds | `cargo nextest run --workspace` |
| F1 | S1 detects allocation collision behavior | `cargo nextest run --workspace` |
| P2 | S3 retry-convergence cases succeed | `cargo nextest run --workspace` |
| F2 | S3 detects a non-convergent retry | `cargo nextest run --workspace` |
| P3 | S4/G130 name damage and withhold completion | `cargo nextest run --workspace` |
| F3 | S4/G130 detect damage or false completeness | `cargo nextest run --workspace` |
