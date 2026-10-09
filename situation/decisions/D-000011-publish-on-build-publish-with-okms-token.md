# D-000011 — Crate publication on the build-publish runner with the OKMS-held token

## Status

accepted

## Date

2026-10-09

## Supersedes

`situation/decisions/D-000009-manual-crate-publication.md`

## Context

- At `226238fd4fefb889b493773872da1b8d2b4fdb2f:.github/workflows/publish.yml`
  the `publish` job runs on `{group: ci, labels: [build-native]}` and its step
  `publish crates to crates.io` takes the crates.io token from the repository
  Actions secret through
  `CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}`.
- Operator decision, 2026-10-09: no secrets are held in GitHub. The crates.io
  token lives in OKMS, and only the dedicated runner `build-publish` (runner
  group `publish`) receives it, as the mounted file
  `/etc/cvu/crates-io/token`.
- P-000009 still names the retired runner label `cvu-native-builder-x64`,
  although `91c47d25868df1ff4445ca57ae881cd2e23d13ad` already moved the job to
  `build-native`; its oracle O-000009 judges the secret reference this change
  removes.

## Evidence

- `226238fd4fefb889b493773872da1b8d2b4fdb2f:.github/workflows/publish.yml` —
  the workflow before this change.
- `895145941170c8f7b90336c30761e7723b6a780a:.github/workflows/publish.yml` —
  the workflow after this change.
- `situation/promises/P-000009-manual-crate-publication.md` and
  `situation/oracles/O-000009-manual-crate-publication.md` — the superseded
  workflow contract and its judgment rule.
- The operator's instruction of 2026-10-09 (no secrets in GitHub; token in
  OKMS; only `build-publish` receives it). This is the decision's authority;
  the runner and its mount are provisioned outside this repository.

## Decision

1. `.github/workflows/publish.yml` keeps `workflow_dispatch` as its only
   trigger, its checkout and Rust 1.96.0 steps, its one step
   `publish crates to crates.io`, and its literal Cargo command.
2. The `publish` job runs on `{group: publish, labels: [build-publish]}`.
3. The crates.io token is held in OKMS and reaches only the `build-publish`
   runner, mounted at `/etc/cvu/crates-io/token`. No GitHub secret carries it:
   the workflow references no `secrets.CARGO_REGISTRY_TOKEN`, and the
   repository Actions secret of that name is not used.
4. The publish step reads that file into `CARGO_REGISTRY_TOKEN` as an
   environment prefix of the Cargo command alone, never prints it, and fails
   before Cargo runs, naming `this job must run on build-publish`, when the
   file is missing or empty.
5. Token location throughout the crate-publication records is as stated here.
   The earlier statements that the token is a GitHub Actions secret are
   history, not current behavior:
   - `situation/decisions/D-000008-native-crate-publication.md` Decision
     clause 5 (the temporary repository Actions secret `CARGO_REGISTRY_TOKEN`)
     and its rejection of organization credential surfaces;
   - `situation/promises/P-000008-native-crate-publication.md` clauses 3 and 6
     and `situation/oracles/O-000008-native-crate-publication.md` legs P11 and
     F11;
   - `situation/references/P-000008/crate-publication.md` route component 5,
     publish-mode step 2, the Credential lifecycle section, and execution
     steps 3 and 5;
   - `situation/promises/P-000009-manual-crate-publication.md` and
     `situation/oracles/O-000009-manual-crate-publication.md` P2 and F2.
   Those records are frozen and stay unchanged; P-000011 and O-000011 carry
   the current contract.

## Why

- A GitHub secret is readable by any workflow a writer of this public
  repository can change; a file mounted only on a dedicated runner is
  available only to jobs scheduled on that runner.
- Prefixing the assignment to the Cargo command keeps the token out of the
  job, step, and shell environments; no later command in the step sees it.
- An explicit check before Cargo turns a job scheduled anywhere else into a
  clear failure instead of an unauthenticated Cargo error.

## Rejected alternatives

- Keep the repository Actions secret: contradicts the operator decision.
- Export the token for the whole step or job (`env:` or `export`): widens its
  exposure beyond the one command that needs it.
- Mask the token with `::add-mask::`: requires writing the token to the step's
  output stream; the step never prints it instead.
- Trusted publishing: not selected by the operator for this change.

## Consequences

- `situation/promises/P-000011-crate-publication-from-build-publish.md` and
  `situation/oracles/O-000011-crate-publication-from-build-publish.md` state
  and judge the workflow; P-000009 and O-000009 are superseded.
- `situation/gaps/G-000003-manual-crate-publication-witness.md` remains open:
  no dispatched run is retained, and this change dispatches none, because
  dispatching publishes crates.
- A dispatch succeeds only when a `build-publish` runner with the mounted
  token is online; otherwise the job waits for one.

## Revisit when

- The token's home, the runner that receives it, or the mount path changes.
- The organization adopts a registry mechanism without a long-lived token.
