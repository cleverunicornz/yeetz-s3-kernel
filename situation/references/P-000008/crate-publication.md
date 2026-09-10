# Crate publication procedure

Owned by `situation/promises/P-000008-native-crate-publication.md`.
Depth and procedure, not law: it never overrides P-000008,
O-000008, or D-000008. It does not restate organization rules — runner
labels, pull-request merge law, scratch and tool-skill rules live in the
root `AGENTS.md` blocks and named skills — it inherits them.

## Route components

### `ci-dev` workflow (`.github/workflows/ci-dev.yml`)

- The `task` input gains the values `package` and `publish`; a new
  `release_version` input is mandatory for both, the `ref` must be a
  full 40-hex SHA, and a validation step fails the job on a missing
  `release_version`, a non-SHA `ref`, or a missing host tool (`git`,
  `cargo`, `rustup`, `python3`). The dispatch-only trigger is unchanged,
  validation tasks keep their historical cancellation semantics, and a
  native task never cancels an in-flight native run: the `release` job
  holds its own serialization group (`ci-dev-release-<task>`,
  `cancel-in-progress: false`), so a re-dispatch queues instead of
  canceling.
- The existing `run` job gains one job-level guard —
  `if: inputs.task != 'package' && inputs.task != 'publish'` — and is
  otherwise untouched.
- The `release` job (`cvu-native-builder-x64`, `permissions: contents:
  read`, 60-minute timeout) carries job env `RELEASE_SOURCE_SHA`
  (the dispatch `ref`), `WORKFLOW_SHA`/`WORKFLOW_REF` (the trusted
  identity of the executing workflow definition), `RELEASE_VERSION`,
  and `RELEASE_DIR: release-<run_id>-<run_attempt>` — release output
  basename, unique per run and attempt so stale artifacts cannot
  contaminate the exact-four release set. Steps:
  1. Input and host-tool validation (above).
  2. Pinned checkout at `inputs.ref`, `fetch-depth: 0`.
  3. A secret-free, workflow-owned guard, run from the trusted workflow
     definition before the checked-out helper executes or any token is
     granted: HEAD must equal the dispatch SHA and the tree be clean;
     `publish` additionally requires `origin/main` ancestry and the
     workflow definition from `refs/heads/main`; in both modes the
     checked-out release sources (`tools/release_crates.py`,
     `.github/workflows/ci-dev.yml`) must be blob-identical to the
     trusted workflow commit's blobs.
  4. The immutable toolchain action pinned at
     `dtolnay/rust-toolchain@29eef336d9b2848a0b548edc03f92a220660cdb8`
     with explicit `toolchain: "1.96.0"` — the organization's action
     allowlist rejected a `6bed…` pin for this job, so the approved
     immutable pin was selected with no policy change.
  5. Exactly one mode step runs per dispatch — the workflow chooses the
     mode by task, and `publish` mode performs its own packaging before
     any upload:
     - `package crates` (task `package` only):
       `python3 tools/release_crates.py package --source-sha "$RELEASE_SOURCE_SHA" --version "$RELEASE_VERSION" --output "$RUNNER_TEMP/$RELEASE_DIR"`
     - `publish crates` (task `publish` only): the same command with
       `publish`, and `env: CARGO_REGISTRY_TOKEN:
       ${{ secrets.CARGO_REGISTRY_TOKEN }}` scoped to that step alone.
     The output lives outside the checkout under `$RUNNER_TEMP` — an
     in-checkout output such as `dist` would fail the script's
     path-relationship preflight.
  6. Artifact upload runs `if: always()` (partial on failure), named
     `crate-release-<version>-<task>-<run_attempt>`: reruns share a
     run's artifact namespace, so the task and attempt suffixes keep
     every attempt's artifacts distinct — nothing is overwritten. It
     carries `*.crate`, `release-manifest.json`, and `SHA256SUMS` from
     the release directory.

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
   present exactly once with an index checksum equal to the recorded
   SHA-256 → skip, reported as already complete; duplicate `V` records
   are an invalid state; anything else → fail closed, no upload.
2. Upload with `cargo publish --locked --registry crates-io -p <crate>`
   (cargo's own verification enabled; registry auth from the step's
   `CARGO_REGISTRY_TOKEN` environment, never a flag or a file).
3. Confirm the sparse-index entry for `V` carries a checksum equal to the
   recorded artifact SHA-256 before proceeding to the next crate. An
   upload whose registry checksum disagrees with the artifact fails
   closed.

Per-crate status (`not-attempted`, `unconfirmed`, `published`,
`verified-already-present`, `verified-after-ambiguous-error`) is
persisted to `release-manifest.json` before and after every attempt and
on the failure path. A confirmed registry receipt and its status are
persisted before any fallible artifact-copy step, so a retention failure
after confirmation is reported separately and never changes publication
status. On any failure, print a per-crate summary and exit nonzero. No
reupload occurs outside adjudication.

## Credential lifecycle (never tokens)

The token value never appears in repository files, situation records,
transcripts, run logs, or agent chat; only the secret name is recorded
here. The package route runs entirely secret-free; only the publish
step consumes the secret.

1. Immediately before the publish dispatch, Main creates the temporary
   repository Actions secret `CARGO_REGISTRY_TOKEN` from the existing
   crates.io credential.
2. The secret is referenced only by the publish step's `env`.
3. Main removes the secret immediately after the publish task completes,
   whether it succeeded or failed.

Workers never inspect credentials; there is nothing for them to read.
The script strips `CARGO_REGISTRY_TOKEN` and its credential-carrying kin
from every packaging subprocess, so packaging never sees a token.

## Execution order for the 0.5.0 release

1. After PR #48 is merged into `main`, that branch sits at release SHA M.
2. Dispatch the package verification at exactly M, running the workflow
   definition from `main`:
   `gh workflow run ci-dev.yml --ref main -f ref=M -f task=package -f release_version=0.5.0`.
   Retain the Actions run URL; download the
   `crate-release-0.5.0-package-<attempt>` artifact; judge O-000008
   P1–P5 on it. This step is secret-free.
3. Immediately before the publish dispatch, Main creates the temporary
   secret `CARGO_REGISTRY_TOKEN`.
4. Dispatch publish at exactly M from `main`:
   `gh workflow run ci-dev.yml --ref main -f ref=M -f task=publish -f release_version=0.5.0`
   — the workflow guard requires `origin/main` ancestry and the
   workflow definition from `refs/heads/main`. The script packages,
   adjudicates, uploads in order, and confirms per-crate index
   checksums; judge O-000008 P6–P8 on the run and the sparse index, and
   P9 if the run fails.
5. Main removes the temporary secret immediately after the publish task
   completes, success or failure.
6. Main creates the annotated tag `v0.5.0` at exactly M and the GitHub
   release attaching the four archives and their checksum files.
7. Witness retention: the witness cites the real Actions run URLs and
   the four sparse-index receipts (version, checksum, publication time),
plus the manual-leg evidence for P10's tag/release reconciliation and
P11.

Current position: the package route is already exercised — pilot
`package` run `34459982826` at the PR-48 head
`c7ef1eee845f322dcfc009885b3e6b999712c2c1`
(https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34459982826)
passed with exactly four verified archives — LICENSE, normalized
manifest, lock, and VCS-source checks, Cargo 1.96 `--locked`
verification retained — artifact `crate-release-0.5.0-package-1`
(https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34459982826/artifacts/10145210146).
Package mode permits the unmerged pilot source; publish does not, so
steps 1 and 3–7 remain and the publish dispatch waits for the merge. A
package-only run cannot fulfill the publish or release legs, so no PASS
witness exists.

## Partial-failure handling

Do not reupload blindly. Re-dispatching `publish` is safe: adjudication
completes crates whose index checksums match and fails closed on any
divergence. Report the exact confirmed/not-confirmed state in the
pull request; a retention or artifact-upload failure after
confirmation is a retention defect reported as such, never a change
to publication status. Genuine divergence (a published V whose bytes
differ from the built artifact) is immutable registry history;
resolution — a new version or a yank — is a separate human decision
outside this procedure.

## Evidence rules

Package, publish, tag, and release claims cite the real Actions run URLs
and the sparse-index receipts. No witness is written before the run it
records exists; no record claims completion ahead of its receipts.

## Execution qualifications (not promised)

- Single repeated-`-p` invocation — exercised and passing in pilot run
  `34459982826`; retained as the route's shape.
- The exact emitted archive file set beyond the named invariants — the
  pilot's post-package checks passed on the emitted set; anything beyond
  the named invariants stays observed per run rather than promised.
- Cross-run `.crate` byte determinism — still unqualified on a single
  run; the checksum equality checks reconcile idempotently or fail
  closed.
- Sparse-index propagation delay before confirmation is observable —
  still unqualified; the script retries within a bounded window and
  fails closed on timeout; registry-side timing is residual in P-000008.
