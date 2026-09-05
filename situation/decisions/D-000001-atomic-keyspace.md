# D-000001 — Atomic keyspace and conditional lineage state

## Status

accepted

## Date

2026-08-21

## Context

The extracted storage kernel needed a keyed S3 surface without exposing raw
adapter access to application code. The donor design also had to prevent an
S3 ETag ABA cycle from making a stale compare-and-swap appear current and had
to distinguish a missing lineage history from a never-created lineage.

## Evidence

- Historical decision: `96a05336c850895143c297fb47ffb55227b0c4fb:situation/record/decision-0001-atomic-keyspace.yamlld`
- Current implementation source: `96a05336c850895143c297fb47ffb55227b0c4fb:crates/yeetz-s3-kernel/src/atomic_keyspace.rs`
- Current lineage source: `96a05336c850895143c297fb47ffb55227b0c4fb:crates/yeetz-s3-kernel/src/state_kernel.rs`
- Current boundary checker: `96a05336c850895143c297fb47ffb55227b0c4fb:tools/check_storage_boundaries.sh`

## Decision

Keep `AtomicKeyspace` in `yeetz-s3-kernel` as the kernel-owned keyed-I/O
surface. Creates use conditional create, compare-and-swap carries an observed
ETag, and conditional deletion is the deletion-side counterpart. Lineage
records remain immutable and canonical heads advance conditionally. The
kernel's public state taxonomy keeps unavailable or incomplete history distinct
from an absent lineage.

## Why

A single kernel-owned surface preserves one storage authority and makes the
conditional-write rule structural rather than dependent on callers remembering
it. Version and incarnation data prevent a stale token from silently crossing
a deletion/recreation era. Distinct history and absence outcomes preserve the
information a caller needs to decide whether repair is required.

## Rejected alternatives

- Unconditional object overwrite: it reopens stale-write and ABA failure modes.
- Placing the keyed surface in `yeetz-sdk-core`: it dilutes the assured storage
  boundary.
- Lexical policy or caller discipline alone: neither makes raw adapter access
  unavailable to application code.

## Consequences

`AtomicKeyspace` and `StateKernel` are the repository's canonical durable-state
surfaces. The storage-access rule is recorded as
`situation/invariants/I-000001-kernel-storage-boundary.md`; behavior and
assurance are recorded by P-000001 and P-000002 with their own oracles.

## Revisit when

A backend changes the ETag or conditional-operation properties relied on by the
kernel, or a replacement state model is selected through a new decision and
superseding promises.
