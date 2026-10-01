# W-000023 — CI supersedes only pull-request runs (push pair)

## Promise

`situation/promises/P-000010-ci-supersedes-only-pull-request-runs.md`

## Oracle

`situation/oracles/O-000010-ci-supersedes-only-pull-request-runs.md`

## Result

PASS

## Head

`47b8f993a03c70cd8a665948c4c3035aecc83c21`

## Observed

2026-10-01

## Evidence

All runs read with the github MCP `actions_read` `list_runs` (branch
`ci/cancel-superseded-pr-runs`, pagination complete) on pull request
https://github.com/cleverunicornz/yeetz-s3-kernel/pull/54.

- `47b8f993a03c70cd8a665948c4c3035aecc83c21:.github/workflows/ci.yml` is
  byte-identical to the implementation commit
  `e11276fe94688e744127413df116c4eff597600b:.github/workflows/ci.yml`; the two
  commits between them are empty proof commits.
- While the `opened` run
  https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/36865602974
  (head `e11276fe…`) was running, two normal commits (no `[skip ci]`) were
  pushed 8 s apart: head A `0810f1faa3e73700d092a68f58e2743981121ce7`
  (13:01:05Z) and head B `47b8f993a03c70cd8a665948c4c3035aecc83c21`
  (13:01:13Z).
  - https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/36865728953 —
    event `pull_request`, head A, conclusion `cancelled`.
  - https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/36865743508 —
    event `pull_request`, head B, ran to conclusion `failure`: every step
    through `cargo build --workspace --locked` succeeded, and
    `cargo nextest run --workspace` failed on
    `streaming_contract::a33_crash_after_every_storage_request` (loopback
    counterpart killed after the shutdown deadline). That failure is owned by
    `situation/gaps/G-000004-loopback-counterpart-shutdown-timing-on-ci-runner.md`;
    the change under observation touches only `concurrency` and path filters.
  - The `opened` run 36865602974 also ended `cancelled`, superseded by a newer
    run of the same pull request.
- These are the only three runs on the branch; no further run exists for A or
  B. No `main` push occurred in the window, and no run of another event was
  cancelled.

## Oracle legs

| Leg | Evidence |
|---|---|
| P1 | PASS — the workflow-level block at head B is exactly D-000010's. |
| P2 | PASS — both `paths-ignore` lists at head B are `"**.md"`, `"docs/**"`, `".agents/**"`, `"LICENSE"`; none matches `.github/workflows/ci.yml`. |
| P3 | PASS — run 36865728953 (head A) `cancelled`; run 36865743508 (head B) concluded `failure`, not `cancelled`; no third run for A or B. |
