# D-000002 — Append-only streams over AtomicKeyspace

## Status

accepted

## Date

2026-08-21

## Context

Consumers needed durable event logs without coupling the crate to forge types,
a delivery broker, or a shared lineage-head contention point. The design had to
make per-sequence damage and uncertain backend listings visible rather than
silently treating them as a complete replay.

## Evidence

- Historical decision: `96a05336c850895143c297fb47ffb55227b0c4fb:situation/record/decision-0002-streams.yamlld`
- Current implementation: `96a05336c850895143c297fb47ffb55227b0c4fb:crates/yeetz-s3-streams/src/lib.rs`
- Current contract tests: `96a05336c850895143c297fb47ffb55227b0c4fb:crates/yeetz-s3-streams/tests/streams_contract.rs`
- Dependency decision: `situation/decisions/D-000001-atomic-keyspace.md`

## Decision

Represent each stream event as an immutable keyspace object. A conditional
create at a sequence is the allocation operation; colliding appenders inspect
the landed value and retry a later sequence. Replay is pull-based, verifies
key/envelope agreement and payload integrity, and withholds completeness unless
it has both a verified tail witness and an ordered probe.

## Why

Conditional event creation separates contention by stream sequence and avoids
a shared head pointer. Opaque identifiers preserve a reusable crate boundary.
Explicit typed replay results make missing, malformed, trimmed, and
backend-unqualified conditions observable to callers.

## Rejected alternatives

- A single lineage-backed event log: it serializes appenders and couples damage
to an entire lineage.
- A delivery bus, push scheduling, or forge-specific types: these belong above
  this storage crate.
- Treating a LIST response alone as proof of completeness: a stale or incomplete
  listing could hide a suffix.

## Consequences

`yeetz-s3-streams` remains a pull-only, forge-agnostic consumer of the kernel.
Its assured behavior is bounded by P-000003 and O-000003. Retention and
migration rules remain part of this decision's historical source rather than
new live graph-era records.

## Revisit when

A replacement durable-log representation can preserve conditional allocation,
typed damage reporting, and the completeness boundary, or a changed retention
model requires a superseding decision.
