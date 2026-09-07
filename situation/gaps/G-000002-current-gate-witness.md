# G-000002 — Current gate-witness applicability

## State

open

## Gap

No PASS witness currently establishes the applicability of O-000001 through
O-000005 to this opening tree after a required gate input changed.

## Relevance

P-000001 through P-000005 retain historical `assured` state from their named
PASS witnesses. Their Oracles require exact source, test, and workflow identity
between an execution and the observation head. The root
`<bedrock-repository>` Verification bullet therefore cannot claim an assured
current gate route until current applicability is witnessed.

## Evidence

- `situation/witnesses/P-000001/W-000011-canonical-lineage-gate.md`,
  `situation/witnesses/P-000002/W-000012-keyspace-gate.md`,
  `situation/witnesses/P-000003/W-000013-append-only-streams-gate.md`,
  `situation/witnesses/P-000004/W-000014-manifest-committed-streamed-values-gate.md`,
  and `situation/witnesses/P-000005/W-000015-batched-deletion-gate.md` record
  PASS at `49ba2ced98831d192f6a2371b90aec8e81a081fd` and source identity only
  through `94c39fecb90ca998156078c7532ebab45927d934`.
- O-000001 through O-000005 each define a changed source, test, or workflow
  input set as INVALID for a historical execution.
- `49ba2ced98831d192f6a2371b90aec8e81a081fd:.github/workflows/ci-dev.yml`
  differs from `.github/workflows/ci-dev.yml`; the current workflow fixes the
  runner label and removes the former runner input. The current
  `tools/check_storage_boundaries.sh` also differs from
  `49ba2ced98831d192f6a2371b90aec8e81a081fd:tools/check_storage_boundaries.sh`.

## Impact

No current gate claim may treat W-000011 through W-000015 as an assured
observation of this opening tree. This absence does not refute the historical
PASS observations or broaden any Promise scope.

## Resolution

None. The absence can close only with an actual `gates` execution for a head
whose Oracle inputs match the observation and a PASS witness that evidences
each Pass leg. No Candidate is proposed: collecting a current witness is not a
behavioral selection.

## References

- `.github/workflows/ci-dev.yml`
- `situation/oracles/O-000001-canonical-lineage-state.md`
- `situation/oracles/O-000002-conditional-keyspace-operations.md`
- `situation/oracles/O-000003-append-only-streams.md`
- `situation/oracles/O-000004-manifest-committed-streamed-values.md`
- `situation/oracles/O-000005-bounded-batched-deletion.md`
