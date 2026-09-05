# G-000001 — Streaming chunk-GC quiescence

## State

accepted

## Gap

The kernel cannot establish that writers are quiescent while destructive
streamed-value chunk collection runs.

## Relevance

P-000004 assures manifest publication and reader integrity for streamed values.
Chunk collection is a related destructive maintenance operation; without a
quiescence boundary, a concurrently prepared or recreated value can make a
chunk look orphaned while it is becoming reachable.

## Evidence

- `situation/decisions/D-000004-streaming-value-v2.md`
- Historical design evidence:
  `96a05336c850895143c297fb47ffb55227b0c4fb:situation/record/decision-0004-streaming-value-io-v2.yamlld`
- Current implementation boundary:
  `96a05336c850895143c297fb47ffb55227b0c4fb:crates/yeetz-s3-kernel/src/value_manifest.rs`

## Impact

The kernel cannot assure destructive chunk sweeping under arbitrary concurrent
writer activity. A caller that violates the maintenance boundary can cause a
streamed logical value to become incomplete.

## Resolution

Accepted by D-000004: destructive collection is quiesced-only and guarded by a
maintenance fence. No kernel-internal proof of global writer quiescence exists.

## References

- `situation/decisions/D-000004-streaming-value-v2.md`
