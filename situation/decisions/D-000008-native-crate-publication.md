# D-000008 — Native crate publication route for the 0.5.0 release

## Status

accepted

## Date

2026-09-10

## Context

The 0.5.0 closure is merged at `2eb823c43f07a512128f4466f36fa5fbc3247c3b`
(PR #47) and source-built and source-tested by the assured `gates` route,
but the four sparse indexes end at 0.4.2 (published 2026-08-24): no 0.5.0
artifact has been built or published. The repository has no publication
route at all: `.github/workflows/ci-dev.yml` at that tip carries only
verification tasks (`check`, `clippy`, `build`, `nextest`, `gates`,
`qualify`,
`kernel-rigs`, `real-s3`) on `cvu-test-runner-x64`. The historical 0.4.2
publication (PR #37) ran bottom-up with `cargo publish --no-verify`,
justified by source-branch gates, and retained no upload host, no auth
mechanism, and no run evidence — nothing to inherit and nothing a witness
could cite.

Source builds are not package verification. `cargo package` verification
builds the packaged file set, which is exactly how the missing
distribution license surfaced: `cargo package --list --locked -p
yeetz-sdk-core` showed no LICENSE file until
`56df6eb8f2bc6ad37da47dbfd1cc89961cdb1547` added `license-file =
"LICENSE"` to the workspace package table and `license-file.workspace =
true` to the four crate manifests. Distribution metadata needed no Rust or
dependency change; publication is an operational slice, not a storage
feature. This slice is directly authorized work (release 0.5.0 plus the
necessary publication work); no Candidate preceded it, and Main owns
credentials, CI dispatch, and the GitHub release.

## Evidence

- Verification-only workflow at the merged 0.5.0 tip:
  `2eb823c43f07a512128f4466f36fa5fbc3247c3b:.github/workflows/ci-dev.yml`
- Merged 0.5.0 source: `2eb823c43f07a512128f4466f36fa5fbc3247c3b`
- Distribution-license fix on the release branch:
  `56df6eb8f2bc6ad37da47dbfd1cc89961cdb1547`
- Workspace 0.5.0 and license-file inheritance: `Cargo.toml`,
  `crates/yeetz-sdk-core/Cargo.toml`, `crates/yeetz-sdk-s3/Cargo.toml`,
  `crates/yeetz-s3-kernel/Cargo.toml`, `crates/yeetz-s3-streams/Cargo.toml`
- Sparse indexes ending at 0.4.2 — internal dependencies as caret
  requirements, external pins carried, per-version checksums recorded:
  https://index.crates.io/ye/et/yeetz-sdk-core
  https://index.crates.io/ye/et/yeetz-sdk-s3
  https://index.crates.io/ye/et/yeetz-s3-kernel
  https://index.crates.io/ye/et/yeetz-s3-streams
- Historical ad-hoc publication:
  https://github.com/cleverunicornz/yeetz-s3-kernel/pull/37
- Runner-label and CI law (five logical Linux labels, dispatch-only gate
  runner, run-URL witness rule): `AGENTS.md` `bedrock-organization` and
  `bedrock-repository` blocks, and the `ci-dev` header comment
- Current gate witnesses, whose source builds are not package
  verification: `situation/witnesses/P-000001/W-000019-canonical-lineage-current-gate.md`
  through `situation/witnesses/P-000007/W-000017-conditional-stream-writes-gate.md`
- Assured behavior the route must publish unchanged: P-000001 through
  P-000007

## Decision

Adopt a retained native publication route as the only mechanism that
publishes repository crates, and release 0.5.0 through it:

1. `ci-dev` gains task values `package` and `publish` and a
   `release_version` input. Both run only from a dispatched full 40-hex
   SHA `ref` and route to a new native job on `cvu-native-builder-x64`
   pinned to Rust 1.96.0 through the approved immutable toolchain-action
   pin. The existing `run` job is unchanged except that it skips the two
   tasks.
2. `tools/release_crates.py` provides modes `package` and `publish`
   taking `--source-sha SHA --version V --output DIR`. It operates on a
   clean checkout of the exact SHA located outside the output directory,
   verifies the checkout resolves to that SHA, and fails closed on any
   version mismatch, dirty state, or path violation.
3. Exactly four crates, in dependency order: `yeetz-sdk-core`,
   `yeetz-sdk-s3`, `yeetz-s3-kernel`, `yeetz-s3-streams`. `yeetz-rigs`
   stays unpublished. Packaging is plain `cargo package --locked` with
   cargo's own verification and each crate's default library feature set;
   `--no-verify` and `--allow-dirty` are never passed.
4. Each archive must carry the root LICENSE, a `Cargo.toml` stating
   `release_version` with the workspace license metadata, source, README,
   and test bytes derived from the pinned tree, cargo VCS metadata
   recording the pinned SHA, `dirty` absent or false, and `path_in_vcs`
   exactly `crates/<name>`, internal dependencies rewritten to registry
   requirements with no path or workspace references, and a SHA-256
   checksum emitted per archive into the output directory.
5. Publishing accepts only source merged into `main`. The registry token
   — the temporary repository Actions secret `CARGO_REGISTRY_TOKEN` — is
   present only in the publish step's environment. Per crate, in order,
   the sparse index is
   adjudicated first: `release_version` absent means upload; present with
   an index checksum equal to the local artifact's SHA-256 means skip as
   already complete; any other state fails closed with no upload. An
   immutable registry version is never overwritten, and each upload is
   confirmed in the sparse index with a checksum equal to the actual
   uploaded artifact before the next crate proceeds.
6. Publication is honestly non-atomic: a partial failure reports exactly
   which crates are confirmed and which are not, with no blind reupload.
   Main creates the annotated `v0.5.0` tag and the GitHub release at
   exactly the published source, attaching the four archives and
   checksums, only after all four crates are confirmed.

## Why

The dependency boundary is the assurance boundary: a published 0.5.0 must
be the assured bytes, and assurance here has three distinct subjects the
0.4.2 route collapsed into one. `gates` proves the source tree; `cargo
package` verification proves the shipped file set (the missing LICENSE is
precisely what source gates could not see); sparse-index receipts prove
the registry state. Workstation publication with `--no-verify` retained
no evidence for the second and third subjects. Fail-closed index
adjudication is the only honest idempotence test because registry
versions are immutable: a checksum equal to the local artifact proves a
prior partial run completed; anything else is divergence no reupload can
repair, so the route stops rather than retries blind. Scoping the token
to the publish step alone minimizes its exposure window on a shared
native builder, and a fixed four-crate list with a fixed runner label
keeps the publication surface exactly as decided rather than as
discovered.

## Rejected alternatives

- Repeat the PR-37 workstation route (`--no-verify`, source gates standing
  in for package verification): the missing LICENSE is the counterexample;
  no runner witness exists; organization law routes agent-driven build
  and test work through the fleet's Linux runners.
- A dedicated release workflow: duplicates the dispatch and witness
  route; `ci-dev` is the repository's single manual gate runner and its
  run URL is the witness currency the workflow header and the root
  repository block already name.
- Blind reupload on publish failure: registry versions are immutable;
  only checksum-verified idempotent completion or a fail-closed stop is
  honest.
- Staged atomic multi-crate publication: crates.io has no cross-crate
  transaction; the truthful model is dependency order plus honest partial
  reporting.
- Packaging with `--all-features` or feature reshaping: the release ships
  each crate's default library feature set; `test-support` stays a
  non-default dev feature, as in the 0.4.x lineage.
- Publishing `yeetz-rigs`: dev-only verification rigs, excluded since
  PR #37.
- Replacing token auth with an unqualified mechanism (trusted publishing,
  organization credential surfaces): none is qualified for this
  organization today; the temporary-secret lifecycle in the reference is
  the retained mechanism, and no credential material ever enters the
  repository.

## Consequences

`ci-dev` gains two task values and a `release_version` input, and a
`cvu-native-builder-x64` job joins the existing test-runner job, which
skips the native tasks and is otherwise untouched. No Rust code, public
API, dependency pin, or downstream migration changes (I-000002); beyond
the route's own workflow and tool files, the only source-tree change it
needs is the distribution-license metadata already on the release branch.
P-000008 records the falsifiable artifact
and publication behavior, O-000008 the judgment rule with its manual
authority, secret, and runner legs, and
`situation/references/P-000008/crate-publication.md` the procedure and
credential lifecycle Main executes. Cargo mechanics that are doubtful
today — single repeated-`-p` invocation, the exact emitted archive file
set, cross-run archive byte determinism — are execution qualifications
owned by the reference, not promised behavior.

## Revisit when

A qualified registry mechanism (trusted publishing or a transactional
multi-crate upload) changes the auth or atomicity model; the runner fleet
labels change; or a later release needs behavior this route excludes.
