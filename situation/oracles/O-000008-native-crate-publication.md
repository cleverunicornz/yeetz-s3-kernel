# O-000008 — Native crate publication contracts

## State

implemented

## Judges

`situation/promises/P-000008-native-crate-publication.md`

## Inputs

The GitHub Actions run of `.github/workflows/ci-dev.yml` dispatched with
`task=package` or `task=publish`, a full 40-hex SHA `ref`, and
`release_version`; the workflow and `tools/release_crates.py` source at
that ref; the run's retained logs; the output directory's archives and
emitted SHA-256 checksums; the four sparse-index endpoints
(`https://index.crates.io/ye/et/yeetz-sdk-core`,
`https://index.crates.io/ye/et/yeetz-sdk-s3`,
`https://index.crates.io/ye/et/yeetz-s3-kernel`,
`https://index.crates.io/ye/et/yeetz-s3-streams`); the `v0.5.0` annotated
tag and GitHub release. A run whose
workflow or script source differs from the head under judgment is INVALID
for this oracle rather than a judgment about changed bytes. The
executable surface — the native `ci-dev` tasks and
`tools/release_crates.py` — has landed on the release branch, and the
package route has now executed for real: `package` run `34459982826` at
`c7ef1eee845f322dcfc009885b3e6b999712c2c1` passed, exercising the
P1–P5 executable decisions. No `publish` run has executed and no tag or
release was created, so P6–P10 remain uncredited; P11 awaits its
direct source evidence.

## Pass

- P1: Package preflight observed to hold — the retained run log shows the
  preflight executed and passed: the checkout resolves to exactly the
  dispatched full SHA at a location outside the output directory, the
  tree is clean, and the workspace and all four crate versions equal
  `release_version`.
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
  dependencies is a registry requirement on V, and external dependency
  requirements equal the workspace pins.
- P5: The retained run log shows the exact cargo packaging invocations
  carrying `--locked` and neither `--no-verify` nor `--allow-dirty`; and
  manual source inspection of the workflow and script shows no path that
  bypasses verification or admits dirty state.
- P6: Publish preflight observed to hold — the retained run log shows the
  merged-into-`main` ancestry check and the workspace-version equality
  check executed and passed before any registry upload was attempted.
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
- P9: An exercised partial failure reports exactly — the failing run's
  output names which crates are confirmed at V and which are not, and a
  retention failure is reported as retention, never as a change to
  publication status; until such a run exists, the failure-summary path
  is credited only by manual source inspection.
- P10: The four-before-release rule holds — the `v0.5.0` tag is
  annotated, resolves to exactly the published SHA, and the GitHub
  release attaching the four archives and checksums exists only after
  all four crates are confirmed, with attached asset checksums matching
  the published artifacts.
- P11: Authority and secret guards, by manual source inspection of the
  workflow and script at the run head: the registry token
  (`CARGO_REGISTRY_TOKEN`) is referenced only in the publish step's
  environment and is never echoed or persisted; packaging subprocesses
  carry no credential environment; native tasks run only on
  `cvu-native-builder-x64` and the existing gate job skips them; the
  trigger remains dispatch-only; the crate list is the fixed four; and no
  other in-repo publication surface exists.

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
  internal requirement off V, or a non-pinned external requirement (P4).
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
- F11: the token is present outside the publish step or echoed anywhere,
  packaging subprocesses carry credentials, a native task runs on another
  label, the gate job executes a native task, a non-dispatch trigger
  exists, a fifth crate is packaged, or another in-repo publication
  surface exists (P11).

## Implementation

The `release` job of `.github/workflows/ci-dev.yml` (dispatch-only;
input and host-tool validation; pinned checkout at the dispatched full
SHA; the secret-free workflow-owned guard — HEAD equals the dispatch
SHA, clean tree, `publish` additionally requires `origin/main` ancestry
and the workflow definition from `refs/heads/main`, and the checked-out
release sources must be blob-identical to the trusted workflow commit;
the allowlisted immutable toolchain pin with Rust 1.96.0; one
mode-specific script step; and the partial-on-failure artifact upload)
together with `tools/release_crates.py` is the executable surface. First
real execution: `package` mode, Actions run `34459982826` at
`c7ef1eee845f322dcfc009885b3e6b999712c2c1`, passed, artifact
`crate-release-0.5.0-package-1` — the P1–P5 executable legs are
exercised by it. `publish` mode has not executed; the workflow guard's
refusal legs and the script's failure paths remain covered by source
inspection until exercised.

## Implementation coverage

A leg is credited only by its named evidence. A successful run credits
only the positive observations its log actually carries: refusal and
secrecy guarantees — that a violated precondition would fail closed, that
a mismatched version would never upload, that no token leaks anywhere —
are negative claims credited only by an exercised refusal case in a
retained run or by the named manual source leg; the passing package
pilot (run 34459982826) credits exactly its P1–P5 executable
observations and none of the negative claims. Every Pass and Fail leg
has exactly one row;
where one leg combines an executable and a manual decision, the row
names both.

| Leg | Decision | Coverage |
|---|---|---|
| P1 | Preflight executed and held at the pinned SHA/version | `tools/release_crates.py` preflight output in the real Actions run |
| P2 | Four archives plus emitted checksums in the output directory | `tools/release_crates.py` package mode |
| P3 | Archive contents: LICENSE, version, byte-identity, exact VCS fields | `tools/release_crates.py` post-package archive check |
| P4 | Packaged manifest is registry-normalized | `tools/release_crates.py` manifest check |
| P5 | Invocations carry `--locked`, no bypass flags; no bypass path exists | run log of the real Actions run for the flags; manual (source inspection for absent bypass paths) |
| P6 | Publish preflight executed and held before registry contact | `tools/release_crates.py` publish preflight output in the real Actions run |
| P7 | Adjudication observed before each upload, in order; duplicate V records treated as invalid | `tools/release_crates.py` publish adjudication in the real Actions run |
| P8 | Per-crate checksum confirmed and persisted before fallible artifact copies, before the next crate | `tools/release_crates.py` post-upload confirmation plus the sparse index |
| P9 | An exercised partial failure reports exactly | an exercised failing run's summary output; manual (source inspection of the failure-summary path) until one exists |
| P10 | Tag/release only after four confirmations, exact SHA, matching assets | manual (receipt reconciliation: tag object type, resolved SHA, release asset checksums vs published receipts) |
| P11 | Token step-scoping, credential-stripped packaging, runner label, gate-job skip, dispatch-only, fixed four, no other surface | manual (source inspection of the workflow and `tools/` at the run head) |
| F1 | Preflight fails closed on a violation | manual (source inspection of the preflight); an exercised refusal case in a retained run upgrades it to executable |
| F2 | Package mode detects a missing or extra archive, or a missing checksum | `tools/release_crates.py` package-mode check |
| F3 | Post-package check detects an archive defect, including a wrong SHA, `dirty` not absent or false, or omitted/wrong `path_in_vcs` | `tools/release_crates.py` post-package archive check |
| F4 | Manifest check detects a leak or an off-requirement dependency | `tools/release_crates.py` manifest check |
| F5 | Run log shows a bypass flag or missing `--locked`; source shows a bypass path | run log of the real Actions run; manual (source inspection) |
| F6 | Publish preflight refuses unmerged or misversioned source | manual (source inspection of the publish preflight); an exercised refusal case upgrades it |
| F7 | Adjudication blocks mismatched or duplicate-V uploads and cannot be skipped | manual (source inspection of the adjudication path); an exercised mismatch, duplicate-record, or idempotent-skip case upgrades it |
| F8 | Confirmation detects an out-of-order attempt, a checksum disagreement, or an unpersisted confirmation | `tools/release_crates.py` post-upload confirmation in the real Actions run |
| F9 | A failing run misreports the partial state or conflates retention with publication status | an exercised failing run's summary output; manual (source inspection of the failure-summary path) until one exists |
| F10 | Tag/release violates the four-before-release rule, the SHA, or asset equality | manual (receipt reconciliation) |
| F11 | A guard violation exists in source or in the run | manual (source inspection of the workflow and `tools/` at the run head) |
