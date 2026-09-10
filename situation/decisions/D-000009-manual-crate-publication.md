# D-000009 — Manual crate publication workflow

## Status

accepted

## Date

2026-09-10

## Supersedes

`f13635dab992eb93ecfa55ffd8b59b8209fdc55b:situation/decisions/D-000008-native-crate-publication.md`

## Context

The admitted DELTA at `c733386f3577fd6c10322d14dc9c5f07baa6f667` removes the
custom native crate-release route: the `release` job and `package`/`publish`
inputs in `.github/workflows/ci-dev.yml`, `tools/release_crates.py`, and the
current P-000008, O-000008, D-000008, and P-000008 reference records. It adds
`.github/workflows/publish.yml` instead. PR #50 identifies this as an
owner-directed cleanup and retains the removed records only as historical Git
bytes at the prior closure checkpoint.

## Evidence

- https://github.com/cleverunicornz/yeetz-s3-kernel/pull/50 — the admitted
  owner-directed scope names the removed release machinery and the replacement
  manual workflow.
- `f13635dab992eb93ecfa55ffd8b59b8209fdc55b:situation/decisions/D-000008-native-crate-publication.md`
  — the prior native-route decision.
- `f13635dab992eb93ecfa55ffd8b59b8209fdc55b:situation/promises/P-000008-native-crate-publication.md`,
  `f13635dab992eb93ecfa55ffd8b59b8209fdc55b:situation/oracles/O-000008-native-crate-publication.md`,
  and `f13635dab992eb93ecfa55ffd8b59b8209fdc55b:situation/references/P-000008/crate-publication.md`
  — the retired current records and procedure.
- `c733386f3577fd6c10322d14dc9c5f07baa6f667:.github/workflows/ci-dev.yml`
  and `c733386f3577fd6c10322d14dc9c5f07baa6f667:.github/workflows/publish.yml`
  — the selected workflow surface.

## Decision

Use `.github/workflows/publish.yml` as the current crate-publication entry
point. It is manually dispatched and delegates publication to its ordinary
Cargo command. Treat the P-000008/O-000008/D-000008 route and its procedure as
retired historical material at the prior closure checkpoint, not as current
records.

## Why

The owner-directed change explicitly selects one simple manually invoked
workflow in place of the custom helper, release job, guards, artifact handling,
and state tracking. The admitted source contains that direct workflow and no
replacement wrapper or release-state mechanism.

## Rejected alternatives

- Retaining the `ci-dev` release job and `tools/release_crates.py`: rejected by
  the owner-directed cleanup and removed in the admitted delta.
- Treating P-000008's native-route assertions as current behavior: rejected
  because the workflow and helper that implemented them are removed.
- Inventing replacement packaging, registry, artifact, or release guarantees:
  rejected because the selected workflow contains none and no publication run
  is retained as current evidence.

## Consequences

`ci-dev` again provides only its manual verification tasks, while publication
has its own manual workflow. P-000009 and O-000009 record the limited current
workflow behavior; G-000003 retains the missing PASS witness. Historical
P-000008/O-000008/D-000008 bytes remain addressable through the evidence
coordinates above.

## Revisit when

The owner directs another publication route, `.github/workflows/publish.yml`
changes, or a real manually dispatched run is retained for the current route.
