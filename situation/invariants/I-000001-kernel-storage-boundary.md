# I-000001 — Kernel storage boundary

## Priority

critical

## Invariant

All durable object-storage access owned by this repository flows through the
kernel closure: `yeetz-s3-kernel`, `yeetz-sdk-s3`, and `yeetz-sdk-core`.
Application and rig code use kernel surfaces rather than raw object-store or S3
adapter APIs.

## Basis

- `situation/decisions/D-000001-atomic-keyspace.md`
- `tools/check_storage_boundaries.sh`
- Historical source: `96a05336c850895143c297fb47ffb55227b0c4fb:situation/definition/invariant-13-state.yamlld`
