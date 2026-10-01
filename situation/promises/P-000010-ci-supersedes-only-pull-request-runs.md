# P-000010 — CI supersedes only pull-request runs; a workflow change runs the workflow

## State

assured

## Promise

For `.github/workflows/ci.yml`:

1. When a `pull_request` event starts a `ci` run for a pull request while an
   earlier `pull_request` `ci` run of the same pull request is queued or
   running, the earlier run ends `cancelled`, and the newer run runs to its own
   conclusion.
2. A `ci` run of any other event (push to `main`) is never cancelled or
   replaced through concurrency, and a `pull_request` run never cancels a run
   of another pull request.
3. Neither path filter ignores `.github/workflows/ci.yml`, so a change to that
   file starts the workflow.

## Scope

The workflow-level `concurrency` block and both `paths-ignore` filters of
`.github/workflows/ci.yml`. This is the local Promise for this repository's
part of the organization contract
`Private: cleverunicornz/infra-v2@ci/cancel-superseded-pr-runs#situation/promises/P-000087-pull-request-runs-supersede-only-their-own.md`
(private repository; cleverunicornz/infra-v2 pull request #488). It does not
cover `ci-dev.yml` or `publish.yml`, the `check` job's steps or outcome, or
which other paths start a run.

## Oracle

`situation/oracles/O-000010-ci-supersedes-only-pull-request-runs.md`

## State evidence

- Implementation: commit `e11276fe94688e744127413df116c4eff597600b`
  (pull request https://github.com/cleverunicornz/yeetz-s3-kernel/pull/54).
- Decision: `situation/decisions/D-000010-ci-supersedes-only-pull-request-runs.md`.
- `assured`: O-000010 passes on
  `situation/witnesses/P-000010/W-000023-ci-supersedes-only-pull-request-runs.md`.

## Residual

- A run that has already finished is not affected; only queued or running runs
  are cancelled.
- No `main` push was observed in the witness window; item 2 is decided by the
  block itself (a group keyed on `run_id` with `cancel-in-progress` false),
  not by a live `main` observation.

## References

none
