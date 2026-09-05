# O-000004 — Manifest-committed streamed-value contracts

## State

implemented

## Judges

`situation/promises/P-000004-manifest-committed-streamed-values.md`

## Inputs

A24, A25, A26, and A29 in
`crates/yeetz-s3-kernel/src/streaming_contract.rs`, their `cargo nextest run
--workspace` execution in `gates`, and source/test/workflow identity between
the execution and observation heads. A changed input set makes a historical
execution INVALID for this oracle rather than a claim about changed behavior.

## Pass

- P1: `a24_manifest_only_visibility_and_control_cuts` establishes that only the
  control manifest makes completed streamed data visible.
- P2: `a25_whole_stream_equivalence_across_transitions` and
  `a26_concurrent_writers_distinct_and_identical_matrix` establish complete
  logical transitions under the manifest publication boundary.
- P3: `a29_missing_truncated_swapped_and_bad_root_taxonomy` establishes typed
  rejection of invalid chunk state.

## Fail

- F1: A24 exposes completed chunks as a logical value before control publication.
- F2: A25 or A26 produces an incomplete or invalid logical transition.
- F3: A29 accepts missing, truncated, swapped, or invalid-root chunk state as a
  logical value.

## Implementation

`49ba2ced98831d192f6a2371b90aec8e81a081fd:.github/workflows/ci-dev.yml`
defines `gates` to execute `cargo nextest run --workspace`; the named
streaming contracts are in `crates/yeetz-s3-kernel/src/streaming_contract.rs`.

## Implementation coverage

| Leg | Decision | Coverage |
|---|---|---|
| P1 | A24 manifest-only visibility succeeds | `cargo nextest run --workspace` |
| F1 | A24 detects pre-manifest visibility | `cargo nextest run --workspace` |
| P2 | A25/A26 complete logical transitions | `cargo nextest run --workspace` |
| F2 | A25/A26 detect incomplete transition behavior | `cargo nextest run --workspace` |
| P3 | A29 returns typed corruption taxonomy | `cargo nextest run --workspace` |
| F3 | A29 detects invalid chunk state | `cargo nextest run --workspace` |
