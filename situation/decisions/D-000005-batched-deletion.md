# D-000005 — Bounded typed multi-object deletion

## Status

accepted

## Date

2026-08-23

## Context

The existing `delete_many` surface could not report provider-specific partial
outcomes or efficiently use S3's multi-object deletion shape. A new primitive
had to remain additive, preserve the conditional single-key surface, and avoid
claiming a transaction where the backend has none.

## Evidence

- Historical decision: `96a05336c850895143c297fb47ffb55227b0c4fb:situation/record/decision-0005-batched-deletion.yamlld`
- Historical completed plan: `96a05336c850895143c297fb47ffb55227b0c4fb:situation/plan/batched-deletion-primitive.yamlld`
- Current implementation: `96a05336c850895143c297fb47ffb55227b0c4fb:crates/yeetz-s3-kernel/src/atomic_keyspace.rs`
- Current contracts: `96a05336c850895143c297fb47ffb55227b0c4fb:crates/yeetz-s3-kernel/src/deletion_contract.rs`
- Dependency decisions: `situation/decisions/D-000001-atomic-keyspace.md` and
  `situation/decisions/D-000004-streaming-value-v2.md`

## Decision

Provide `AtomicKeyspace::delete_objects` as an additive raw deletion primitive.
It admits a bounded unique input, sends sequential S3 multi-object requests
with bounded chunks, requires an exact verbose response reconciliation, and
returns a typed outcome for every admitted key. It does not replace or wrap the
older `delete_many` behavior.

## Why

The new API can preserve partial-failure information and avoid treating omitted
provider response members as success. A distinct method protects published
behavior and makes its weaker legacy response-trust behavior explicit rather
than silently changing it.

## Rejected alternatives

- Mutating or wrapping `delete_many`: it changes established behavior and
  response-trust semantics.
- Read-then-delete or rollback/compensation: neither supplies an S3
  multi-object conditional transaction.
- A batched conditional delete: the conditional operation remains a per-key
  surface.
- A wholesale top-level error after admission: it loses per-key progress and
  recovery information.

## Consequences

The method is transport, not a cross-key transaction; it writes neither
lifecycle tombstones nor incarnation counters. Its relation to streamed chunk
collection remains a declared residual in D-000004 rather than an implied
safety guarantee. P-000005 and O-000005 record the method's assured boundary.

## Revisit when

A portable standards-level multi-object conditional operation becomes available
and is qualified through a new decision. Cross-key transactionality remains out
of this decision's scope.
