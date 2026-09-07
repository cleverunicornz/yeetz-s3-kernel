# G-000002 — Purpose-profile gate evidence

## State

open

## Gap

No current-head PASS witness in `situation/witnesses/` observes the
post-cutover purpose-profile workflow running the complete CI gate suite.

## Relevance

D-000006 changes the runner context for both tracked CI workflows. The existing
kernel and stream assurance oracles require source, test, and workflow identity
between a gate execution and the observation head; their retained PASS witnesses
identify the historical `ci-dev.yml` source at
`49ba2ced98831d192f6a2371b90aec8e81a081fd`. A workflow definition alone does
not establish a current-head gate claim after that runner-profile change.

## Evidence

- `situation/decisions/D-000006-ci-purpose-profile-runners.md`
- `.github/workflows/ci.yml`
- `.github/workflows/ci-dev.yml`
- `situation/oracles/O-000001-canonical-lineage-state.md`
- `situation/oracles/O-000002-conditional-keyspace-operations.md`
- `situation/oracles/O-000003-append-only-streams.md`
- `situation/oracles/O-000004-manifest-committed-streamed-values.md`
- `situation/oracles/O-000005-bounded-batched-deletion.md`
- `https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34117710250`
  completed `cancelled` on 2026-09-07 at opening checkpoint
  `89d9891c958adc12a1ac669c57374de5a668b96b`; its `check` job has no PASS
  result and supplies no retained gate witness.

## Impact

The repository block cannot name an assured current-head verification route for
the runner cutover based only on the workflow definitions.

## Resolution

None. `situation/candidates/C-000002-current-head-purpose-profile-gate.md` is
proposed, not an active resolution.

## References

- `situation/decisions/D-000006-ci-purpose-profile-runners.md`
- `situation/candidates/C-000002-current-head-purpose-profile-gate.md`
