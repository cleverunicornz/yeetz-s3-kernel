# C-000002 — Current-head purpose-profile gate

## State

proposed

## Candidate

Obtain a gate observation at the final pull-request head by dispatching
`ci-dev.yml` with `task=gates` and `runner=cvu-test-runner-x64`, then retain a
separate PASS, FAIL, INVALID, or BLOCKED witness for each affected
Promise/Oracle pair.

## Origin

- `situation/gaps/G-000002-purpose-profile-gate-evidence.md`
- `situation/decisions/D-000006-ci-purpose-profile-runners.md`

## Why consider it

The runner-profile cutover is implemented, but its current workflow source has
no retained gate witness. An observation at the final head could establish or
refute the gate route without inferring assurance from configuration alone.

## Qualification questions

- Does `gates` complete at the exact final pull-request head on
  `cvu-test-runner-x64` without the retired cache helper?
- Does the resulting run URL and its retained job evidence decide every Pass leg
  for each affected Oracle without using the workflow artifact as its own
  provenance?
- If the dispatch cannot complete, does its witness accurately retain the
  failure, invalidity, or blocking condition instead of asserting a PASS?

## Candidate approaches

- Dispatch `.github/workflows/ci-dev.yml` with the exact final head,
  `task=gates`, and `runner=cvu-test-runner-x64`; use the resulting run URL only
  after each affected Oracle's legs are independently mapped to its witness.
- No alternate route is currently evidenced. An automatic `ci.yml` run is not
  assumed equivalent without an Oracle and witness review.

## Disposition

None. This candidate remains proposed; no Decision promotes it, no Promise or
Oracle is created, and G-000002 remains open.
