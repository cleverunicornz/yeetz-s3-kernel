# I-000002 — Scoped kernel extension

## Priority

standard

## Invariant

A change that extends durable-kernel behavior is introduced only through an
explicitly authorized, narrowly scoped batch with its contract evidence.
Downstream migration or repair is separately orchestrated rather than bundled
into the kernel change.

## Basis

- `situation/decisions/D-000001-atomic-keyspace.md`
- `situation/decisions/D-000004-streaming-value-v2.md`
- `situation/decisions/D-000005-batched-deletion.md`
- Historical source: `96a05336c850895143c297fb47ffb55227b0c4fb:situation/definition/invariant-kernel-extension.yamlld`
