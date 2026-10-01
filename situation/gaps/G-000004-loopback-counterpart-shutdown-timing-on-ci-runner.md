# G-000004 — Loopback S3 counterpart shutdown timing fails a contract test on the CI runner

## State

open

## Gap

`streaming_contract::a33_crash_after_every_storage_request` failed in the
`ci` workflow on the in-cluster `automation-test-s` runner because its loopback
S3 counterpart did not exit within the fixture's deadlines and was killed.
Whether this is timing sensitivity of the fixture on a slow or loaded runner,
an effect of the runner image change, or a defect in the counterpart is not
established.

## Relevance

The `ci` workflow's `cargo nextest run --workspace` step decides the pull
request check; the test belongs to the streaming contract
(`crates/yeetz-s3-kernel/src/streaming_contract.rs`). It arose while observing
`situation/promises/P-000010-ci-supersedes-only-pull-request-runs.md` on pull
request https://github.com/cleverunicornz/yeetz-s3-kernel/pull/54, whose
change touches only the workflow's `concurrency` block and path filters.

## Evidence

Observed:

- Run https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/36865743508,
  job https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/36865743508/job/110381091254
  (event `pull_request`, head `47b8f993a03c70cd8a665948c4c3035aecc83c21`,
  2026-10-01): `FAIL [11.934s] (65/287) yeetz-s3-kernel
  streaming_contract::a33_crash_after_every_storage_request`, panic at
  `crates/yeetz-s3-kernel/src/state_kernel.rs:4537:17`: "loopback S3
  counterpart exited unsuccessfully after graceful=false: signal: 9
  (SIGKILL)"; nextest stopped after the failure (65 passed, 1 failed, 221 not
  run).
- At that head `shutdown()` sends the counterpart's shutdown request with a
  1 s timeout, then waits `T001_COUNTERPART_TIMEOUT` (5 s,
  `state_kernel.rs:2877`) and kills the child when it has not exited
  (`wait_for_exit` → `kill_and_wait`, `state_kernel.rs:4143-4168`).
  `graceful=false` and `SIGKILL` match that path.
- The previous `ci` runs passed: push to `main`
  https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/36331280528
  (2026-09-27) and pull request run 36330918221.

Interpretation (unconfirmed): the CI runner sets moved to a new image and the
runner's per-core speed is about a third of a desktop CPU, so a 1 s / 5 s
deadline may be too short under load. No experiment isolates the cause.

## Impact

A pull request check can fail for reasons unrelated to its change, and nextest
stops the run at the first failure, so later tests go unobserved in that run.

## Resolution

none

## References

- `situation/witnesses/P-000010/W-000023-ci-supersedes-only-pull-request-runs.md`
