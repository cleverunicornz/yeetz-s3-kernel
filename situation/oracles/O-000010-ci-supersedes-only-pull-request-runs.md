# O-000010 — CI supersedes only pull-request runs; a workflow change runs the workflow

## State

designed

## Judges

`situation/promises/P-000010-ci-supersedes-only-pull-request-runs.md`

## Inputs

- `.github/workflows/ci.yml` at the judged head.
- The workflow runs of a test pull request, read with the github MCP
  `actions_read` `list_runs` (name, event, head_sha, status, conclusion, URL),
  after two normal commits (no `[skip ci]`) are pushed to it a few seconds
  apart: push 1 (head A) and push 2 (head B).

## Pass

- P1: The workflow carries, at workflow level, exactly the block of
  `situation/decisions/D-000010-ci-supersedes-only-pull-request-runs.md`:
  group `${{ github.workflow }}-${{ github.event_name == 'pull_request' && github.event.pull_request.number || github.run_id }}`
  and `cancel-in-progress: ${{ github.event_name == 'pull_request' }}`. This
  leg decides Promise item 2.
- P2: Neither `paths-ignore` list contains a pattern matching
  `.github/workflows/ci.yml`.
- P3: The `pull_request` `ci` run with head_sha A ends `cancelled`; the
  `pull_request` `ci` run with head_sha B ends with a conclusion other than
  `cancelled`; there is no further `ci` run for A or B.

## Fail

- F1: The block is absent, differs, or sits at job level only.
- F2: A `paths-ignore` pattern matches `.github/workflows/ci.yml`.
- F3: The run for A is not `cancelled`, the run for B is `cancelled`, or a
  further run exists for A or B.
- F4: Any non-`pull_request` `ci` run in the observed window ends `cancelled`
  through concurrency.

## Implementation coverage

| Leg | Decision | Coverage |
|---|---|---|
| P1 | Block present and exact at workflow level | manual |
| P2 | No ignore pattern matches the workflow file | manual |
| P3 | Run for A cancelled, run for B concluded, no third | manual (`actions_read` `list_runs`) |
| F1 | Block absent or different | manual |
| F2 | Ignore pattern matches the workflow file | manual |
| F3 | Live supersede does not hold | manual (`actions_read` `list_runs`) |
| F4 | A non-pull_request run cancelled | manual (`actions_read` `list_runs`) |
