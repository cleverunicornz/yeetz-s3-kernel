# PLAN-000001 — Streaming representation revisit

## Candidates

- `situation/candidates/C-000001-part-addressed-streaming-read.md`

## Promises

- `situation/promises/P-000004-manifest-committed-streamed-values.md`

## Dependencies

C-000001 requires a capability witness before any change to P-000004 is
considered. A representation change requires a superseding decision, promise,
and oracle.

## Completion

Completes when C-000001 reaches a terminal Candidate state: `promoted`,
`rejected`, `merged`, or `superseded`. If promoted, the atomic promotion links
C-000001, its Decision, new Promise, and new Oracle; the new Promise is
`qualified`, and this Plan links those records in the same promotion
transaction. For an additive selection, P-000004 remains `assured`; for a
replacement selection, P-000004 is `superseded` by the new Promise.
