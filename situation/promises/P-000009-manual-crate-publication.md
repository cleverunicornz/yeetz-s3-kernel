# P-000009 — Manual crate publication

## State

implemented

## Promise

At its selected revision, `.github/workflows/publish.yml` exposes only a manual
`workflow_dispatch` entry point. Its `publish` job runs on
`cvu-native-builder-x64`, checks out source with `actions/checkout@v7`,
configures Rust 1.96.0, and executes exactly one step named
`publish crates to crates.io` with `CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}`
and this literal command:

```text
cargo publish --locked -p yeetz-sdk-core -p yeetz-sdk-s3 -p yeetz-s3-kernel -p yeetz-s3-streams
```

## Scope

This promise covers the trigger, publish job, runner label, exact checkout and
toolchain steps, exactly one named publication step, secret reference, and the
complete literal Cargo command (including its token order) in
`.github/workflows/publish.yml`. It excludes Cargo's packaging semantics;
dispatch authorization and secret lifecycle; registry availability,
authentication, acceptance, and resulting crate versions; artifact retention;
tags and GitHub releases; and any publication behavior outside this workflow.

## Oracle

`situation/oracles/O-000009-manual-crate-publication.md`

## State evidence

- Implementation commit
  `c733386f3577fd6c10322d14dc9c5f07baa6f667:.github/workflows/publish.yml`.
- `situation/decisions/D-000009-manual-crate-publication.md`.

## Residual

No PASS witness has applied O-000009 to a manually dispatched run, so this
promise does not assure that the configured Cargo command has executed or that
any registry publication succeeded. That bounded evidence absence is
`situation/gaps/G-000003-manual-crate-publication-witness.md`.

## References

- `situation/decisions/D-000009-manual-crate-publication.md`
- `situation/gaps/G-000003-manual-crate-publication-witness.md`
