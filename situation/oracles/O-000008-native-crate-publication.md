# O-000008 — Native crate publication contracts

## State

implemented

## Judges

`situation/promises/P-000008-native-crate-publication.md`

## Inputs

The GitHub Actions run of `.github/workflows/ci-dev.yml` dispatched with
`task=package` or `task=publish`, a full 40-hex SHA `ref`, and
`release_version`; the workflow and `tools/release_crates.py` source at
that ref; the complete repository tree at that ref as the bounded manual
input for the finite P11/F11 no-other-in-repository-publication-surface
check; the run's retained logs; the output directory's archives and
emitted SHA-256 checksums; the four sparse-index endpoints
(`https://index.crates.io/ye/et/yeetz-sdk-core`,
`https://index.crates.io/ye/et/yeetz-sdk-s3`,
`https://index.crates.io/ye/et/yeetz-s3-kernel`,
`https://index.crates.io/ye/et/yeetz-s3-streams`); the `v0.5.0` annotated
tag and GitHub release. A run whose workflow or script source differs from
the SHA under judgment is INVALID for this oracle rather than a judgment
about changed bytes.

## Pass

- P1: The retained package-run log shows the preflight passed: the checkout
  resolves to exactly the dispatched full SHA at a location outside the
  output directory, the tree is clean, and the workspace and all four crate
  versions equal `release_version`.
- P2: Exactly four archives named `<crate>-<V>.crate` appear in the
  output directory, one per crate, each with an emitted SHA-256 checksum.
- P3: Each archive carries the root `LICENSE`; its `Cargo.toml` states
  version V with the workspace license metadata; every other carried file
  is byte-identical to the pinned tree, with the crate's README and test
  files present; and its `.cargo_vcs_info.json` records the exact pinned
  SHA, a `dirty` status that is absent or false, and `path_in_vcs`
  exactly `crates/<crate name>` — an omitted or wrong package path is not
  accepted.
- P4: Each packaged `Cargo.toml` is registry-normalized: no `path =` or
  `workspace =` dependency references remain, each of the four internal
  dependencies is a registry requirement on V, and every external dependency
  requirement equals its corresponding workspace dependency pin.
- P5: The retained run log shows the exact cargo packaging invocations
  carrying `--locked` and neither `--no-verify` nor `--allow-dirty`; and
  manual source inspection of the workflow and script shows no path that
  bypasses verification or admits dirty state.
- P6: The retained publish-run log shows the merged-into-`main` ancestry
  check and the workspace-version equality check passed before any registry
  upload was attempted.
- P7: Sparse-index adjudication observed before each upload in the order
  `yeetz-sdk-core`, `yeetz-sdk-s3`, `yeetz-s3-kernel`, `yeetz-s3-streams`,
  with the observed action per crate: V absent → upload; V present
  exactly once with an index checksum equal to the local artifact's
  SHA-256 → skip reported as complete; duplicate V records are an
  invalid state.
- P8: After each upload, the crate's sparse-index entry for V carries a
  checksum equal to the actually uploaded artifact before the next crate
  begins, and the confirmed status and receipt are persisted to the run
  output before any fallible artifact-copy step.
- P9: An exercised partial failure reports exactly: the failing run's output
  names which crates are confirmed at V and which are not, and a retention
  failure is reported as retention, never as a change to publication status.
- P10: The four-before-release rule holds — the `v0.5.0` tag is
  annotated, resolves to exactly the published SHA, and the GitHub
  release attaching the four archives and checksums exists only after
  all four crates are confirmed, with attached asset checksums matching
  the published artifacts.
- P11: Manual source inspection of `.github/workflows/ci-dev.yml`,
  `tools/release_crates.py`, and the complete repository tree at the judged
  SHA establishes the clause-6 authority boundary: `package` and `publish`
  are dispatch-only and require a full 40-hex `ref`; the native `release`
  job runs only on `cvu-native-builder-x64` while the existing `run` gate job
  skips the native tasks; `CARGO_REGISTRY_TOKEN` is present only in the
  publish step's environment; packaging subprocesses carry no Cargo
  credential environment; the crate list is the fixed four; and no other
  in-repository crate-publication surface exists.

## Fail

- F1: a precondition violation (SHA, cleanliness, version, path relation)
  is accepted rather than failing the run closed before an archive is
  produced (P1).
- F2: an archive is missing, an extra archive appears, or a checksum is
  missing (P2).
- F3: an archive lacks `LICENSE`, misstates its version or license
  metadata, omits README or test files, byte-drifts from the pinned tree,
  or its `.cargo_vcs_info.json` names a wrong SHA, records `dirty` true,
  or omits or misstates `path_in_vcs` (P3).
- F4: a packaged manifest retains a path or workspace reference, an
  internal requirement off V, or an external requirement off its
  corresponding workspace dependency pin (P4).
- F5: packaging is invoked with `--no-verify` or `--allow-dirty`, without
  `--locked`, or a bypass path exists in source (P5).
- F6: publish proceeds from a SHA unmerged into `main` or a version
  unequal to the workspace version rather than refusing before registry
  contact (P6).
- F7: an upload is attempted against a present-but-mismatched version or
  against duplicate V records, an overwrite is attempted, or adjudication
  is skipped (P7).
- F8: a dependent crate is attempted before its dependency's index
  confirmation, an index checksum disagrees with the uploaded artifact,
  or a confirmed status is not persisted before a fallible
  artifact-copy step (P8).
- F9: an exercised partial failure is reported incompletely or wrongly,
  or a retention failure is reported as a publication-status change (P9).
- F10: the tag/release appears before all four confirmations, at a
  different SHA, non-annotated, or with non-matching assets (P10).
- F11: source inspection finds a non-dispatch or non-full-SHA native task,
  a native job on another label, a native task executed by the `run` gate
  job, `CARGO_REGISTRY_TOKEN` outside the publish-step environment, a
  packaging subprocess with a Cargo credential environment, a crate list
  other than the fixed four, or another in-repository crate-publication
  surface (P11).

## Implementation

The `release` job of `.github/workflows/ci-dev.yml` (dispatch-only input and
host-tool validation; pinned checkout at the dispatched full SHA; the
secret-free workflow-owned guard; the allowlisted immutable toolchain pin
with Rust 1.96.0; one mode-specific script step; and partial-on-failure
artifact upload) together with `tools/release_crates.py` is the executable
surface. Retained package and publish Actions-run logs and artifacts, the
sparse-index and release receipts, and manual source inspection supply the
evidence mechanisms named below.

## Implementation coverage

Every independently decidable Pass and Fail leg appears exactly once below.
The table names decision mechanisms, not outcomes; run logs, artifacts,
sparse-index and release receipts, and manual inspections are recorded by
witnesses, not here. A leg with both executable and manual mechanisms names
both.

| Leg | Decision | Coverage |
|---|---|---|
| P1 | Package preflight holds at the pinned SHA/version | retained package Actions-run preflight log |
| P2 | Four archives plus emitted checksums appear in the output directory | `tools/release_crates.py` package-mode check |
| P3 | Archive contents carry LICENSE, version, byte-identity, and exact VCS fields | `tools/release_crates.py` post-package archive check |
| P4 | Packaged manifest has no path/workspace references, internal V requirements, and external workspace dependency pins | `tools/release_crates.py` manifest check |
| P5 | Invocations carry `--locked`, no bypass flags; no bypass path exists | retained package Actions-run log for flags; manual (source inspection for absent bypass paths) |
| P6 | Publish preflight holds before registry contact | retained publish Actions-run preflight log |
| P7 | Adjudication occurs before each upload in order; duplicate V records are invalid | retained publish Actions-run adjudication log |
| P8 | Per-crate checksum is confirmed and persisted before fallible artifact copies and the next crate | retained publish Actions-run post-upload output plus the sparse index |
| P9 | An exercised partial failure reports exactly | retained failing-publish summary output; manual (source inspection of the failure-summary path) |
| P10 | Tag/release follows four confirmations at the exact SHA with matching assets | manual (receipt reconciliation: tag object type, resolved SHA, release asset checksums versus published receipts) |
| P11 | Dispatch/full-SHA, runner, gate-job, token, credential-free packaging, fixed-four, and no-other-surface authority boundary | manual (source inspection of `.github/workflows/ci-dev.yml`, `tools/release_crates.py`, and the complete repository tree at the judged SHA) |
| F1 | Preflight fails closed on a violation | manual (source inspection of the preflight); an exercised refusal case supplies direct run evidence |
| F2 | Package mode detects a missing or extra archive, or a missing checksum | `tools/release_crates.py` package-mode check |
| F3 | Post-package check detects an archive defect, including a wrong SHA, `dirty` not absent or false, or omitted/wrong `path_in_vcs` | `tools/release_crates.py` post-package archive check |
| F4 | Manifest check detects a path/workspace leak, internal V mismatch, or external workspace dependency-pin mismatch | `tools/release_crates.py` manifest check |
| F5 | A bypass flag, missing `--locked`, or source bypass path is detected | retained package Actions-run log; manual (source inspection) |
| F6 | Publish preflight refuses unmerged or misversioned source | manual (source inspection of the publish preflight); an exercised refusal case supplies direct run evidence |
| F7 | Adjudication blocks mismatched or duplicate-V uploads and cannot be skipped | manual (source inspection of the adjudication path); an exercised mismatch, duplicate-record, or idempotent-skip case supplies direct run evidence |
| F8 | Confirmation detects an out-of-order attempt, checksum disagreement, or unpersisted confirmation | retained publish Actions-run post-upload output plus the sparse index |
| F9 | A failing run misreports partial state or conflates retention with publication status | retained failing-publish summary output; manual (source inspection of the failure-summary path) |
| F10 | Tag/release violates the four-before-release rule, the SHA, or asset equality | manual (receipt reconciliation) |
| F11 | A clause-6 authority-boundary violation is found | manual (source inspection of `.github/workflows/ci-dev.yml`, `tools/release_crates.py`, and the complete repository tree at the judged SHA) |
