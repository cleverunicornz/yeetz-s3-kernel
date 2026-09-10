# O-000009 — Manual crate-publication workflow

## State

designed

## Judges

`situation/promises/P-000009-manual-crate-publication.md`

## Inputs

The selected revision of `.github/workflows/publish.yml`; and, when runtime
execution is being judged, the GitHub Actions run log for a manual dispatch and
its checkout revision. A missing run is neither PASS nor FAIL: it leaves
P-000009 implemented without a witness. A run whose checkout differs from the
workflow source under judgment is INVALID for this oracle.

## Pass

- P1: The workflow source declares `workflow_dispatch` as its only trigger.
- P2: The source declares the `publish` job on `cvu-native-builder-x64`; within
  that job it uses `actions/checkout@v7`, configures Rust 1.96.0, and declares
  exactly one step named `publish crates to crates.io` with
  `CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}` and a `run` value
  exactly equal to the P-000009 literal Cargo command, with no extra, missing,
  or reordered tokens.
- P3: When a run is supplied, its checkout matches the judged source and its
  log records the named publish step beginning the configured command.

## Fail

- F1: The workflow source has a non-manual trigger or lacks the declared manual
  dispatch trigger.
- F2: The `publish` job's runner differs from `cvu-native-builder-x64`; its
  checkout action is not `actions/checkout@v7`; its Rust 1.96.0 configuration
  or `CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}` reference
  differs; it has zero or more than one step named `publish crates to crates.io`;
  or that step's `run` command has an extra, missing, or reordered token relative
  to the P-000009 literal Cargo command.
- F3: A supplied run whose checkout matches the judged source fails before the
  named publish step begins; a Cargo failure after the step begins does not refute
  P-000009 because that promise does not claim registry success.
