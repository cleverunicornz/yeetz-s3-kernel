# P-000008 — Native crate publication

## State

implemented

## Promise

The repository publishes crate releases only through the `package` and
`publish` tasks of the `ci-dev` workflow. For a dispatched full-SHA `ref`
and `release_version` V:

1. The `package` task produces exactly four `.crate` archives —
   `yeetz-sdk-core-V`, `yeetz-sdk-s3-V`, `yeetz-s3-kernel-V`,
   `yeetz-s3-streams-V` — in the run's output directory, built by
   `cargo package --locked` with cargo's own verification enabled (never
   `--no-verify`, never `--allow-dirty`) from a clean checkout located
   outside the output directory whose resolved commit is exactly the
   dispatched SHA and whose workspace and crate versions are exactly V.
   Any SHA, version, cleanliness, or path violation fails closed with no
   archive retained as passing.
2. Each archive carries the root `LICENSE` file and a `Cargo.toml`
   stating version V with the workspace license metadata; carries the
   crate's README and test files alongside its source, each carried file
   byte-identical to the pinned tree; carries `.cargo_vcs_info.json`
   recording the exact pinned SHA, a `dirty` status that is absent or
   false, and `path_in_vcs` exactly `crates/<crate name>` — an omitted or
   wrong package path is not accepted; and carries a
   registry-normalized dependency table — no path or workspace references,
   with each of the four internal dependencies rewritten to a registry
   requirement on V. A SHA-256 checksum is emitted per archive alongside
   it.
3. The `publish` task refuses any source SHA not merged into `main`. The
   registry token is present only in the publish step's environment. For
   each crate in the order `yeetz-sdk-core`, `yeetz-sdk-s3`,
   `yeetz-s3-kernel`, `yeetz-s3-streams`, the sparse index is adjudicated
   before any upload: V absent means upload; V present exactly once with
   an index checksum equal to the packaged artifact's SHA-256 means skip,
   reported as already complete; any other state — duplicate V records
   included — fails closed with no upload of that crate. No attempt is
   made to overwrite an existing registry version. After each upload, the
   crate's sparse-index entry for V shows a checksum equal to the actual
   uploaded artifact before the next crate is attempted.
4. A partial publish failure is reported exactly: the run output names
   which crates are confirmed published at V and which are not, and no
   crate is re-uploaded except through the clause-3 checksum
   adjudication. Each confirmed crate's status is persisted to the run
   output before any fallible artifact-retention step, and a retention
   failure is reported separately from, and never as a change to,
   publication status.
5. The annotated `v0.5.0` tag and the GitHub release are created only
   after clause 3 confirms all four crates at V. The tag resolves to
   exactly the published SHA, and the release attaches the four archives
   and their checksums, matching what was published.

## Scope

The `package` and `publish` tasks of `.github/workflows/ci-dev.yml`,
`tools/release_crates.py`, the four named crates' release artifacts and
sparse-index outcomes, and the `v0.5.0` tag and release publication.
Excludes any Rust code, public API, or dependency change; downstream
migration; crates beyond the four (`yeetz-rigs` stays unpublished);
cross-crate atomicity (explicitly absent — clause 4); registry-side
behavior beyond the observed sparse-index state; and any publication not
initiated through these tasks.

## Oracle

`situation/oracles/O-000008-native-crate-publication.md`

## State evidence

- `situation/decisions/D-000008-native-crate-publication.md` — the
  accepted route authorizing this work.
- Implementation landed through PR #48
  (https://github.com/cleverunicornz/yeetz-s3-kernel/pull/48, branch
  `release-0.5.0`): `e40d1fe` adds the route, `cc916b1` fixes runner
  paths, `c7ef1eee845f322dcfc009885b3e6b999712c2c1` closes receipt and
  provenance gaps, atop the distribution-license fix `56df6eb`.
- The package route is exercised and passing: `ci-dev` `package` run
  `34459982826` at
  `c7ef1eee845f322dcfc009885b3e6b999712c2c1`
  (https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34459982826)
  produced exactly four verified archives — LICENSE, normalized
  manifest, lock, and VCS-source checks passed with Cargo 1.96
  `--locked` verification retained — artifact
  `crate-release-0.5.0-package-1`. The branch is not yet merged to
  `main` (human-authorized merge pending gates and the Bedrock closure),
  no `publish` run exists, and the four sparse indexes end at 0.4.2.
- No PASS witness is created from the pilot: a package-only run cannot
  fulfill the publish clauses (3–4) or the release clause (5).
  `situation/references/P-000008/crate-publication.md` remains the
  procedure of record.

State advances to `assured` only on a witness retaining real package
and publish Actions runs, registry receipts, and the tag/release
evidence judged by O-000008.

## Residual

- Publication is not atomic across crates: any intermediate state between
  zero and four confirmed crates can be observed externally; the route
  reports it honestly but cannot roll it back. Yanking or superseding a
  partially published version is a separate human decision outside this
  promise.
- Sparse-index propagation and registry availability between upload and
  confirmation are registry-side; the route fails closed when
  confirmation is not observable and promises no consistency window.
- Non-disclosure of the token beyond the reviewed step scoping, and
  absence of unauthorized runner or repository access, are negative
  guarantees no runtime success can prove; they are covered only by the
  manual source legs of O-000008 and remain otherwise unassured.
- Archives are built on the `cvu-native-builder-x64` Linux runner; no
  other platform's packaging behavior is claimed.
- Cross-run archive byte determinism is not claimed: the clause-3
  checksum equality either reconciles idempotently or fails closed.

## References

- `situation/decisions/D-000008-native-crate-publication.md`
- `situation/references/P-000008/crate-publication.md`
