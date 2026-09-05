# P-000002 — Conditional keyspace operations

## State

assured

## Promise

Within `AtomicKeyspace`, create inserts only an absent key, and a
compare-and-swap or conditional delete changes a key only when its observed
ETag matches the current key state. A stale operation returns a typed outcome
rather than changing the value.

## Scope

`AtomicKeyspace::create`, `AtomicKeyspace::compare_exchange`, and
`AtomicKeyspace::delete_if_match` in `crates/yeetz-s3-kernel`. This promise
does not cover unconditional/idempotent delete helpers, multi-object deletion,
or an application's policy after receiving a typed conflict.

## Oracle

`situation/oracles/O-000002-conditional-keyspace-operations.md`

## State evidence

- `situation/oracles/O-000002-conditional-keyspace-operations.md`
- `situation/witnesses/P-000002/W-000012-keyspace-gate.md`

## Residual

The promise does not claim an external backend's availability or application
retry policy. Provider qualification beyond the named loopback contract scope
is outside this assurance.

## References

- `situation/decisions/D-000001-atomic-keyspace.md`
