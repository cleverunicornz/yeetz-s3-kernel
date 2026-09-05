# D-000003 — Initial streaming-value proposal

## Status

superseded

## Date

2026-08-22

## Supersedes

None. This was the initial recorded streaming-value proposal.

## Superseded by

`situation/decisions/D-000004-streaming-value-v2.md`

## Context

Large logical values could not use the existing whole-value conditional-write
surface without a design that kept publication atomic and did not expose a
partially uploaded value as current.

## Evidence

- Historical decision: `96a05336c850895143c297fb47ffb55227b0c4fb:situation/record/decision-0003-streaming-value-io.yamlld`
- Historical successor: `96a05336c850895143c297fb47ffb55227b0c4fb:situation/record/decision-0004-streaming-value-io-v2.yamlld`
- Predecessor decision: `situation/decisions/D-000001-atomic-keyspace.md`

## Decision

The proposal selected a control-manifest plus immutable-chunk representation
for values beyond the inline representation. The conditional control-manifest
write would be the publication point, and readers would validate that control
object before reading chunks.

## Why

The proposal preserved the existing conditional mutation boundary while
allowing chunked transfer. It deliberately remained a design artifact until
review resolved its stale-era, cleanup, range, and backend-capability edges.

## Rejected alternatives

- Publishing chunks directly as the logical value: it has no single
  conditional commit point.
- Exposing partial upload state to normal readers: it turns an interrupted
  write into apparent current state.

## Consequences

This record is retained only to preserve the rejected/changed design path.
D-000004 carries the accepted amended representation and the associated live
behavior record P-000004.

## Revisit when

Never in place. Any reconsideration begins from D-000004 or a later
superseding decision.
