# P-000001 — Canonical lineage state

## State

assured

## Promise

For a `StateKernel` lineage, appending creates immutable canonical records,
advancing a successor head requires the observed canonical head, and a broken
record history is reported as an integrity condition rather than as lineage
absence.

## Scope

The `StateKernel` lineage write and read behavior exercised by the K1, K2, and
K7 contract cases in `crates/yeetz-s3-kernel/src/state_kernel.rs`. This does
not claim behavior for application-level projections or an unqualified external
object-store backend.

## Oracle

`situation/oracles/O-000001-canonical-lineage-state.md`

## State evidence

- `situation/oracles/O-000001-canonical-lineage-state.md`
- `situation/witnesses/P-000001/W-000006-canonical-lineage-input-identity.md`

## Residual

The assurance is limited to the named kernel contract scope. Availability,
latency, and application-specific projection policy are not claimed here.

## References

- `situation/decisions/D-000001-atomic-keyspace.md`
