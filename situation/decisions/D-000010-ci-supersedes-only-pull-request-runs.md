# D-000010 — CI supersedes only pull-request runs; a workflow change runs the workflow

## Status

accepted

## Date

2026-10-01

## Context

- At `b91e90c5a5ce98baef9b7b35b69cc32c8db7bcb7:.github/workflows/ci.yml` the
  workflow declared `concurrency: {group: ci-${{ github.ref }},
  cancel-in-progress: true}`. The group is keyed on the ref and the cancel is
  unconditional, so a later push to `main` cancels an in-progress `main` run.
- The same file listed `.github/**` in both `paths-ignore` filters, so a pull
  request that changes only this workflow starts no run of it, and a merged
  change to it starts no `main` run.
- The organization rule (PLATFORM-PLAN P6, 2026-10-01) is that every
  pull-request-triggered workflow cancels a superseded run of the same pull
  request and workflow, while pushes to `main` and publish runs are never
  cancelled half-way. It is decided once for the organization in
  `Private: cleverunicornz/infra-v2@ci/cancel-superseded-pr-runs#situation/decisions/D-000203-pull-request-runs-supersede-only-their-own.md`
  (private repository; cleverunicornz/infra-v2 pull request #488), and stated
  as the composing contract
  `Private: cleverunicornz/infra-v2@ci/cancel-superseded-pr-runs#situation/promises/P-000087-pull-request-runs-supersede-only-their-own.md`.

## Evidence

- `b91e90c5a5ce98baef9b7b35b69cc32c8db7bcb7:.github/workflows/ci.yml`
  (concurrency block and both path filters).
- `.github/` at that commit holds only `workflows/`
  (`git ls-tree -d b91e90c5a5ce98baef9b7b35b69cc32c8db7bcb7 .github`).
- GitHub concurrency semantics:
  https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#concurrency
  (a group may fall back to `github.run_id`, which is unique per run).
- The live observation on this change's pull request:
  `situation/witnesses/P-000010/W-000023-ci-supersedes-only-pull-request-runs.md`.

## Decision

1. `.github/workflows/ci.yml` carries, at workflow level, exactly:

   ```yaml
   concurrency:
     group: ${{ github.workflow }}-${{ github.event_name == 'pull_request' && github.event.pull_request.number || github.run_id }}
     cancel-in-progress: ${{ github.event_name == 'pull_request' }}
   ```

2. `.github/**` is removed from both `paths-ignore` filters; the other entries
   stay as they are.
3. `ci-dev.yml` and `publish.yml` are unchanged: both are dispatch-only and
   carry no pull-request trigger.

## Why

- A `pull_request` run gets the group `ci-<PR number>` and cancels the older
  run of the same pull request, which no one will merge.
- A push to `main` gets a group keyed on its own `run_id`, which no other run
  shares, and `cancel-in-progress` is false for it, so `main` work is never cut
  off.
- With `.github/` holding only workflows, removing `.github/**` is the
  narrowest filter under which a change to this workflow runs it.

## Rejected alternatives

- Keep `ci-${{ github.ref }}` with an unconditional cancel: two `main` pushes
  cancel each other.
- Replace `paths-ignore` with a `paths` list using a negated
  `!.github/workflows/ci.yml`: equivalent today, harder to read, and diverges
  from the filter shape the other entries already use.
- Add `situation/**` to the filters: outside this change; record-only pull
  requests keep their current behaviour.

## Consequences

- `situation/promises/P-000010-ci-supersedes-only-pull-request-runs.md` and
  `situation/oracles/O-000010-ci-supersedes-only-pull-request-runs.md` state
  and judge the behaviour.
- A pull request's checks show `cancelled` for each run superseded by a newer
  push; only the newest head's run counts. No ruleset requires a status check.

## Revisit when

- A pull-request-triggered workflow in this repository must publish on the
  pull-request event itself.
- The organization changes the block decided in infra-v2 D-000203.
