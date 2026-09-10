# Crate publication procedure

Owned by `situation/promises/P-000008-native-crate-publication.md`.
Depth and procedure, not law: it never overrides P-000008,
O-000008, or D-000008. It does not restate organization rules — runner
labels, pull-request merge law, scratch and tool-skill rules live in the
root `AGENTS.md` blocks and named skills — it inherits them.

## Route components

### `ci-dev` workflow (`.github/workflows/ci-dev.yml`)

- The `task` input gains the values `package` and `publish`; a new
  `release_version` input is required for both. The workflow description
  states that native tasks require `ref` to be a full 40-hex SHA; the
  script enforces it. The dispatch-only trigger is unchanged, validation
  tasks keep their historical cancellation semantics, and a native task
  never cancels an in-flight publish: the native route holds its own
  serialization group with `cancel-in-progress: false`, so a re-dispatch
  queues instead of canceling.
- The existing `run` job gains one job-level guard —
  `if: inputs.task != 'package' && inputs.task != 'publish'` — and is
  otherwise untouched.
- A new native job runs when `inputs.task` is `package` or `publish`:
  `runs-on: cvu-native-builder-x64` (the organization's native
  compilation/packaging label), `actions/checkout` at `inputs.ref`,
  `dtolnay/rust-toolchain@stable` with `toolchain: "1.96.0"` mirroring
  the test job, then:
  - `package` step (both tasks):
    `python3 tools/release_crates.py package --source-sha <ref> --version <release_version> --output dist`
  - `publish` step (publish task only):
    `python3 tools/release_crates.py publish --source-sha <ref> --version <release_version> --output dist`
    with `env: CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}`
    scoped to that step alone.
  - The `dist/` output is uploaded as an Actions artifact for retention.

### `tools/release_crates.py`

Fixed crate list in dependency order: `yeetz-sdk-core`, `yeetz-sdk-s3`,
`yeetz-s3-kernel`, `yeetz-s3-streams`. `yeetz-rigs` is never packaged.

`package` mode:

1. Preflight (fail closed): the working tree resolves to exactly
   `--source-sha` with clean status; the checkout lies outside the output
   directory and the output outside the checkout; the workspace and all
   four crate versions equal `--version`; `--source-sha` is a full 40-hex
   SHA.
2. Package the four crates in one invocation —
   `cargo package --locked --registry crates-io` with repeated `-p` in
   dependency order and a build target directory under the output.
   Cargo's own verification stays enabled; `--no-verify` and
   `--allow-dirty` do not exist in the tool.
3. Post-package check per archive (fail closed): extract the `.crate`;
   the root `LICENSE` is present; `Cargo.toml` states the version and
   license metadata; every other carried file is byte-identical to the
   checkout, with README and test files present; `.cargo_vcs_info.json`
   records the exact pinned SHA, `dirty` absent or false, and
   `path_in_vcs` exactly `crates/<crate name>` — an omitted or wrong
   package path fails; the dependency table has no `path =` or
   `workspace =` references, internal dependencies are registry
   requirements on the version, external requirements equal the workspace
   pins.
4. Emit a SHA-256 checksum record per archive and a summary manifest to
   the output directory and run log.

`publish` mode: run the package preflight and packaging exactly as
`package` mode, record each artifact's SHA-256, then per crate in order:

1. Adjudicate the sparse index for `<crate>` at `V`: absent → upload;
   present with index checksum equal to the recorded SHA-256 → skip,
   reported as already complete; anything else → fail closed, no upload.
2. Upload with `cargo publish --locked --registry crates-io -p <crate>`
   (cargo's own verification enabled; registry auth from the step's
   `CARGO_REGISTRY_TOKEN` environment, never a flag or a file).
3. Confirm the sparse-index entry for `V` carries a checksum equal to the
   recorded artifact SHA-256 before proceeding to the next crate. An
   upload whose registry checksum disagrees with the artifact fails
   closed.

On any failure, print a per-crate summary — confirmed at V / not
published — and exit nonzero. No reupload occurs outside adjudication.

## Credential lifecycle (never tokens)

The token value never appears in repository files, situation records,
transcripts, run logs, or agent chat; only the secret name is recorded
here.

1. Immediately before the publish dispatch, Main creates the temporary
   repository Actions secret `CARGO_REGISTRY_TOKEN` from the existing
   crates.io credential.
2. The secret is referenced only by the publish step's `env`.
3. Main removes the secret immediately after the publish task completes,
   whether it succeeded or failed.

Workers never inspect credentials; there is nothing for them to read.
The script strips `CARGO_REGISTRY_TOKEN` and its credential-carrying kin
from every packaging subprocess, so packaging never sees a token.

## Human approval gate

The preparation pull request (release branch into `main`) carrying the
workflow tasks, `tools/release_crates.py`, the distribution-license
metadata fix, and the P-000008/O-000008/D-000008 records merges only
with explicit human approval under the root organization rules. The
working agent opens and updates the pull request and does not merge it.
Per organization law the merge is a merge commit, never a squash or
rebase.

## Execution order for the 0.5.0 release

1. Preparation PR merges with human approval; `main` sits at the release
   SHA M.
2. Main creates the temporary secret.
3. Dispatch `gh workflow run ci-dev.yml -f ref=M -f task=package -f
   release_version=0.5.0`. Retain the Actions run URL; download the
   `dist` artifact; judge O-000008 P1–P5 on it.
4. Dispatch `gh workflow run ci-dev.yml -f ref=M -f task=publish -f
   release_version=0.5.0`. The script packages, adjudicates, uploads in
   order, and confirms per-crate index checksums; judge O-000008 P6–P8
   on the run and the sparse index, and P9 if the run fails.
5. Main removes the temporary secret.
6. Main creates the annotated tag `v0.5.0` at exactly M and the GitHub
   release attaching the four archives and their checksum files.
7. Witness retention: the witness cites the two Actions run URLs and the
   four sparse-index receipts (version, checksum, publication time), plus
   the manual-leg evidence for P10's tag/release reconciliation,
   P11, and P12.

## Partial-failure handling

Do not reupload blindly. Re-dispatching `publish` is safe: adjudication
completes crates whose index checksums match and fails closed on any
divergence. Report the exact confirmed/not-confirmed state in the pull
request. Genuine divergence (a published V whose bytes differ from the
built artifact) is immutable registry history; resolution — a new version
or a yank — is a separate human decision outside this procedure.

## Evidence rules

Package, publish, tag, and release claims cite the real Actions run URLs
and the sparse-index receipts. No witness is written before the run it
records exists; no record claims completion ahead of its receipts.

## Execution qualifications (not promised)

These are decided by the first real execution and recorded then; P-000008
promises none of them:

- Single repeated-`-p` invocation versus per-crate `cargo package`
  invocations — either is allowed; artifacts must be identical.
- The exact emitted archive file set beyond the named invariants (for
  example `Cargo.lock` presence) — observed at first run and folded into
  the P3 check.
- Cross-run `.crate` byte determinism — not claimed; the checksum
  equality checks reconcile idempotently or fail closed.
- Sparse-index propagation delay before confirmation is observable — the
  script retries within a bounded window and fails closed on timeout;
  registry-side timing is residual in P-000008.
