# Repository context

## Identity

`yeetz-s3-kernel` is the Rust source of record for an S3-native storage
kernel and its closure: `yeetz-s3-kernel`, `yeetz-s3-streams`,
`yeetz-sdk-s3`, and `yeetz-sdk-core`. The workspace membership and package
metadata are declared in `Cargo.toml`; the crates' public surfaces are
introduced in their package READMEs.

## Classification

- Phase: `EVOLUTION`
- Ownership: `OWNED`
- Upstream coordinate: none
- Current Bedrock operation: `DELTA`
- Completed adoption operation: `BACKPORT`
- Adoption run: `20260905T005131Z-96a05336c850895143c297fb47ffb55227b0c4fb`
- Opening checkpoint: `94c39fecb90ca998156078c7532ebab45927d934`
- Trigger tree: `96a05336c850895143c297fb47ffb55227b0c4fb`

## Current implementation map

- `crates/yeetz-s3-kernel/` owns the canonical state kernel and the
  `AtomicKeyspace` surface.
- `crates/yeetz-s3-streams/` provides append-only event logs over the kernel.
- `crates/yeetz-sdk-s3/` provides request-scoped S3-compatible mechanics.
- `crates/yeetz-sdk-core/` provides provider-neutral request-scoped HTTP
  primitives.
- `rigs/` contains durable executable witnesses; `rigs/INDEX.md` identifies
  their historical execution evidence.
- `tools/check_storage_boundaries.sh` mechanically checks the storage-access
  boundary for repository Rust sources.

## Canonical knowledge

Repository behavior, decisions, invariants, evidence, and future work are
recorded under the current `situation/` namespaces. `situation/promises/`,
`situation/oracles/`, and `situation/witnesses/` carry behavior and assurance;
`situation/decisions/` and `situation/invariants/` carry collapsed choices and
binding rules. `README.md` is human orientation only.

## Historical donor boundary

The graph-era material present in
`96a05336c850895143c297fb47ffb55227b0c4fb:situation/` and the former
repository-local skill surface at
`96a05336c850895143c297fb47ffb55227b0c4fb:.agents/` are BACKPORT donor
material. It informs the current records but is not current operational
authority. Git retains those bytes under the stated trigger commit.
