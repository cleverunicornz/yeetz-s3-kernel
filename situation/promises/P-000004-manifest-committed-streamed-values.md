# P-000004 — Manifest-committed streamed values

## State

assured

## Promise

For a streamed `AtomicKeyspace` value, completed chunks become logically
visible only through a conditional control-manifest publication, and readers
validate the control manifest and report invalid chunk state as a typed error
rather than returning a partial logical value.

## Scope

The manifest/chunk writer and reader paths exercised by A24, A25, A26, and A29
in `crates/yeetz-s3-kernel/src/streaming_contract.rs`. The promise covers
logical publication and read integrity; it does not assure destructive chunk
collection during arbitrary concurrent writes.

## Oracle

`situation/oracles/O-000004-manifest-committed-streamed-values.md`

## State evidence

- `situation/oracles/O-000004-manifest-committed-streamed-values.md`
- `situation/witnesses/P-000004/W-000009-streamed-value-input-identity.md`

## Residual

Destructive collection requires operational writer quiescence that the kernel
cannot prove; see `situation/gaps/G-000001-streaming-gc-quiescence.md`.
Backend-specific multipart capability beyond this manifest representation is
outside the assurance.

## References

- `situation/decisions/D-000004-streaming-value-v2.md`
