# G-000002 — Current gate-witness applicability

## State

closed

## Gap

The former absence of a PASS witness establishing the applicability of
O-000001 through O-000005 after a required gate input changed.

## Relevance

P-000001 through P-000005 retain `assured` state from their named PASS
witnesses. Their Oracles require exact source, test, and workflow identity
between an execution and the observation head. The current-head PASS witnesses
below establish that identity for the existing promises' scopes and restore a
recorded assured gate route.

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
- `situation/witnesses/P-000001/W-000019-canonical-lineage-current-gate.md`,
  `situation/witnesses/P-000002/W-000020-keyspace-current-gate.md`,
  `situation/witnesses/P-000003/W-000018-append-only-streams-current-gate.md`,
  `situation/witnesses/P-000004/W-000021-streamed-values-current-gate.md`,
  and `situation/witnesses/P-000005/W-000022-batched-deletion-current-gate.md`
  each record a PASS from `gates` run `34421839069` at
  `43fb1f661ad91363fc99ae257ed4e62ea8207303`, with an Oracle-leg table
  evidencing every Pass leg.

## Impact

The former absence prevented the root Verification bullet from naming an
assured current gate route. It did not refute the historical PASS observations
or broaden any Promise scope.

## Resolution

Closed by W-000018, W-000019, W-000020, W-000021, and W-000022: each
observes a `gates` execution whose source, test, and workflow inputs match its
observation head and names the evidence for its Oracle's Pass legs. No
Candidate was proposed because collecting current evidence selected no new
behavior.

## References

- `.github/workflows/ci-dev.yml`
- `situation/oracles/O-000001-canonical-lineage-state.md`
- `situation/oracles/O-000002-conditional-keyspace-operations.md`
- `situation/oracles/O-000003-append-only-streams.md`
- `situation/oracles/O-000004-manifest-committed-streamed-values.md`
- `situation/oracles/O-000005-bounded-batched-deletion.md`
