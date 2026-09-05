# P-000005 — Bounded typed batched deletion

## State

assured

## Promise

`AtomicKeyspace::delete_objects` validates all input before issuing a request,
uses sequential S3 deletion chunks of at most 1,000 keys, and returns one typed
outcome for every admitted input key without defaulting an unconfirmed response
member to success.

## Scope

`AtomicKeyspace::delete_objects` and its `DeleteObjects*` result types in
`crates/yeetz-s3-kernel`. The bound is the public
`DELETE_OBJECTS_MAX_KEYS` value in the implementation. This does not claim
cross-key atomicity, per-key conditional deletion, lifecycle tombstone effects,
or caller retry policy.

## Oracle

`situation/oracles/O-000005-bounded-batched-deletion.md`

## State evidence

- `situation/oracles/O-000005-bounded-batched-deletion.md`
- `situation/witnesses/P-000005/W-000010-batched-deletion-input-identity.md`

## Residual

The method is transport rather than a transaction. It is intentionally raw
relative to lifecycle state and cannot make a caller's concurrent chunk-GC
policy safe; see `situation/gaps/G-000001-streaming-gc-quiescence.md`.

## References

- `situation/decisions/D-000005-batched-deletion.md`
