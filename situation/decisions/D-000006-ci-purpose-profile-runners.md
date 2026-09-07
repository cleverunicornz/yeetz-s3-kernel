# D-000006 — CI purpose-profile runners

## Status

accepted

## Date

2026-09-07

## Context

The tracked CI workflows still named the retired `org-ci-linux-x64` label,
exposed host-specific `runner-<host>-<nn>` choices in the manual workflow, and
invoked `/opt/gh-runners/bin/configure-rust-local-cache`. That helper is not
available on one-job ephemeral runner images.

## Evidence

- `de741852455d3b8ce5add0da40bf668d7a205ee2:.github/workflows/ci.yml`
- `de741852455d3b8ce5add0da40bf668d7a205ee2:.github/workflows/ci-dev.yml`
- `AGENTS.md` requires Linux and platform-neutral jobs to use owned-fleet
  logical labels and to run the real suite.
- Historical donor only:
  `96a05336c850895143c297fb47ffb55227b0c4fb:situation/definition/invariant-11-ci-rules.yamlld`

## Decision

Run `ci.yml` on `cvu-test-runner-x64`. Make `ci-dev.yml` default to that
profile and limit its selectable runner values to `cvu-test-runner-x64`,
`cvu-native-builder-x64`, `cvu-docker-builder-x64`, `cvu-agent-code-x64`, and
`cvu-deploy-x64`. Remove the retired host-label choices and the unavailable
local-cache helper from both workflows.

## Why

Purpose profiles select the owned ephemeral fleet by workload rather than by a
specific bare-metal host. The resulting workflows comply with the current
logical-label rule and do not rely on a helper absent from ephemeral images.

## Rejected alternatives

- Retain `org-ci-linux-x64`: it is a decommissioned label and cannot select the
  intended current fleet.
- Retain the host-specific choice list: it exposes retired bare-metal routing
  as a workflow interface rather than selecting by workload.
- Retain the local-cache helper: it is unavailable on the runner images the
  workflows now select.

## Consequences

The `ci` and `ci-dev` workflows retain their real-suite commands while their
execution environment is selected through purpose profiles. A configuration
change alone does not establish a current-head gate result.

## Revisit when

A selected purpose profile is retired or cannot provide a required CI tool, or
a verified runner observation shows that a different profile is required.
