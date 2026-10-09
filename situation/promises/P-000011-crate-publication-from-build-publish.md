# P-000011 — Crate publication from the build-publish runner

## State

implemented

## Promise

At its selected revision, `.github/workflows/publish.yml`:

1. Exposes only a manual `workflow_dispatch` entry point.
2. Runs its `publish` job on `{group: publish, labels: [build-publish]}`,
   checks out source with `actions/checkout@v7`, configures Rust 1.96.0, and
   executes exactly one step named `publish crates to crates.io`.
3. In that step, takes the crates.io token only from the file
   `/etc/cvu/crates-io/token` and passes it as `CARGO_REGISTRY_TOKEN` only in
   the environment of this literal command:

   ```text
   cargo publish --locked -p yeetz-sdk-core -p yeetz-sdk-s3 -p yeetz-s3-kernel -p yeetz-s3-streams
   ```

4. Fails the step before Cargo runs, with a message containing
   `this job must run on build-publish`, when that file is missing or empty.
5. Never writes the token to the run log, and references no GitHub secret for
   it: the file contains no `secrets.CARGO_REGISTRY_TOKEN`.

## Scope

The trigger, the `publish` job's runner group and label, its checkout and
toolchain steps, the one named publication step and its token handling, and
the literal Cargo command in `.github/workflows/publish.yml`. It excludes how
OKMS and the runner provision the mounted file, which runners carry the label,
dispatch authorization, Cargo's packaging and upload semantics, registry
outcomes, tags and releases, and publication behavior outside this workflow.

## Oracle

`situation/oracles/O-000011-crate-publication-from-build-publish.md`

## State evidence

- Implementation commit
  `895145941170c8f7b90336c30761e7723b6a780a:.github/workflows/publish.yml`.
- Decision: `situation/decisions/D-000011-publish-on-build-publish-with-okms-token.md`.
- Supersedes `situation/promises/P-000009-manual-crate-publication.md`.

## Residual

- No dispatched run is retained: this promise does not assure that a
  `build-publish` runner picks up the job, that the mounted file is present
  there, or that any registry publication succeeded
  (`situation/gaps/G-000003-manual-crate-publication-witness.md`).
- Item 5 bounds what this workflow's source writes; it does not assure what
  Cargo or the runner itself log.

## References

- `situation/decisions/D-000011-publish-on-build-publish-with-okms-token.md`
- `situation/gaps/G-000003-manual-crate-publication-witness.md`
