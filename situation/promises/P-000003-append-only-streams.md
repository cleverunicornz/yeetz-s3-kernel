# P-000003 — Append-only streams

## State

assured

## Promise

For a stream, conditional creation allocates one immutable event per sequence,
a byte-identical retry within the bounded idempotency window converges on its
original receipt, and replay reports per-sequence damage or withholds
completeness instead of silently skipping or guessing.

## Scope

The `Streams` append and replay behavior exercised by the S1–S4 and G130
contract cases in `crates/yeetz-s3-streams/tests/`. The promise excludes
application-level delivery, scheduling, name registration, and consumer
business-policy decisions.

## Oracle

`situation/oracles/O-000003-append-only-streams.md`

## State evidence

- `situation/oracles/O-000003-append-only-streams.md`
- `situation/witnesses/P-000003/W-000013-append-only-streams-gate.md`

## Residual

This assurance does not claim that every object-store backend satisfies the
stream's listing assumptions. A backend that cannot establish the required
listing evidence is represented by the typed unqualified outcome within scope.

## References

- `situation/decisions/D-000002-append-only-streams.md`
