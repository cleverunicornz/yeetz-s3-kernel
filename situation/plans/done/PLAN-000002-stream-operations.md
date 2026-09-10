# PLAN-000002 — Stream operations

## Promises

- `situation/promises/P-000006-strict-stream-reads.md`
- `situation/promises/P-000007-conditional-stream-writes.md`

## Dependencies

- P-000006's immutable `Envelope`/`EventRef` surface is a prerequisite of
  P-000007: `append_expected` consumes `EventRef` and the immutable
  getters. P-000006 lands first; P-000007's assurance follows
  P-000006's.
- Both promises leave P-000003's assured replay, append, and
  idempotency-window semantics undisturbed; neither supersedes it.

## Completion

Completes when P-000006 and P-000007 are each `assured` — each oracle
applied to a passing witness — with P-000003 not superseded.
