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
- P2: The source declares the `publish` job on `cvu-native-builder-x64`, its
  `actions/checkout@v7` and Rust 1.96.0 configuration, and the named publish
  step with `CARGO_REGISTRY_TOKEN` from the Actions secret and the exact locked
  four-package Cargo command stated by P-000009.
- P3: When a run is supplied, its checkout matches the judged source and its
  log records the named publish step beginning the configured command.

## Fail

- F1: The workflow source has a non-manual trigger or lacks the declared manual
  dispatch trigger.
- F2: The runner, checkout, toolchain, secret reference, `--locked` flag, or
  any of the four package selectors differs from the P-000009 configuration.
- F3: A supplied run checks out different source or fails before the named
  publish step begins; a Cargo failure after the step begins does not refute
  P-000009 because that promise does not claim registry success.
