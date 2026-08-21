---
name: ci
description: Use when running gates, verifying a branch, citing gate evidence, or deciding where builds/tests run. Owns the remote verification plane — WarpBuild runners, the ci-dev dispatch workflow, the witness rule, and the no-local-builds default.
metadata:
  short-description: Remote verification plane and witness rule
---

# ci

Verification runs on WarpBuild, not on local machines. Local Rust builds
by agents are prohibited except for throwaway editor-loop `cargo check`
— they eat the human's disk, spawn process storms that crash the
preference (measured on the parent yeetz workspace, which subsumes
this one): warm 16x full gates = 4:17 wall (~8¢); clippy-only = 32s
wall (~2¢); the old path was 40 minutes and a crashed Mac.

## The two workflows

- **`ci.yml`** — automatic on PRs and main, path-filtered: markdown,
  `docs/`, `.agents/`, `.github/`, LICENSE changes never trigger it.
  If your PR is Rust-silent and you still need a run, dispatch ci-dev
  manually. Runner: `warp-ubuntu-latest-x64-16x`.
- **`ci-dev.yml`** — manual dispatch only, for iteration and evidence:

```bash
gh workflow run ci-dev.yml --ref <branch> -f ref=<branch> -f task=<task> [-f runner=<label>]
gh run watch  # or poll gh run list --workflow=ci-dev.yml
```

  Tasks: `clippy` (default; the iteration gate — `cargo clippy
  --workspace --all-targets --all-features -- -D warnings`), `build`,
  `nextest`, `gates` (fmt + clippy + locked build + full suite),
  `kernel-rigs` (durable streams witness), `real-s3` (live Exoscale
  ABA probe; needs the `EXO_S3_*` secrets). Runner defaults to 16x;
  8x is ~20% cheaper per run for penny-shaving on clippy iterations.

## The witness rule

Gate claims cite a run URL, never a local attestation. A PR Claims
section saying "gates green" without a run link is incomplete. Local
runs are iteration aids, not evidence — any validator (or human) must
be able to open the cited run, read the log, and confirm the gates and
tests actually executed and passed. The run log is the witness.

## Cache

`WarpBuilds/rust-cache@v2` with `cache-provider: warpbuild` and
`shared-key: yeetz-s3-kernel-ci` — one cache pool shared by both workflows and
every branch. Warm restore is seconds. If a run recompiles the world,
suspect a key change (toolchain bump, Cargo.lock) — not the weather.

## Escalation tools (available, not default)

WarpBuild provides an action-debugger (SSH into a live run) and
Tailscale-connectable runners for interactive debugging of flaky
steps. Use only when a log genuinely cannot explain a failure; note
the debug session in the PR.

## Cost reference (16x, $0.032/min)

- clippy iteration: ~2¢ (32s wall)
- full gates: ~8¢ (4:17 wall)
- 100 gates/day ≈ $8/day. Do not optimize below this without a human
  asking.
