---
name: ci
description: Use when running gates, verifying a branch, citing gate evidence, or deciding where builds/tests run. Owns the remote verification plane — the org's self-hosted bare-metal runners, the ci-dev dispatch workflow, the witness rule, and the no-local-builds default.
metadata:
  short-description: Remote verification plane and witness rule
---

# ci

Verification runs on the organization's self-hosted bare-metal
runners, not on local machines. Local Rust builds by agents are
prohibited except for throwaway editor-loop `cargo check` — they eat
the human's disk, spawn process storms that crash the harness, and
produce no citable evidence.

## The two workflows

- **`ci.yml`** — automatic on PRs and main, path-filtered: markdown,
  `docs/`, `.agents/`, `.github/`, LICENSE changes never trigger it.
  If your PR is Rust-silent and you still need a run, dispatch ci-dev
  manually. Runner: `org-ci-linux-x64`.
- **`ci-dev.yml`** — manual dispatch only, for iteration and evidence:

```bash
gh workflow run ci-dev.yml --ref <branch> -f ref=<branch> -f task=<task> [-f runner=<label>]
gh run watch  # or poll gh run list --workflow=ci-dev.yml
```

  Tasks: `clippy` (default; the iteration gate — `cargo clippy
  --workspace --all-targets --all-features -- -D warnings`), `build`,
  `nextest`, `gates` (fmt + clippy + locked build + full suite),
  `kernel-rigs` (durable streams witness), `real-s3` (live Exoscale
  ABA probe; needs the `EXO_S3_*` secrets). Runner defaults to
  `org-ci-linux-x64`; `runner-<host>-<nn>` pins a specific bare-metal
  box (cache-hot iteration, or reproducing a host-specific failure).

## Fork-PR guard

Both workflows refuse to run untrusted code: `ci.yml` skips
fork PRs (same-repo branches and main pushes only), and `ci-dev`
dispatch already requires write access. Untrusted code never executes
on our runners.

## The witness rule

Gate claims cite a run URL, never a local attestation. A PR Claims
section saying "gates green" without a run link is incomplete. Local
runs are iteration aids, not evidence — any validator (or human) must
be able to open the cited run, read the log, and confirm the gates and
tests actually executed and passed. The run log is the witness.

## Cache

`/opt/gh-runners/bin/configure-rust-local-cache` — the rust build
cache lives on each runner's local SSD. No cold-start penalty, no
external cache service, nothing to restore over the network: every
run on a box finds that box's cache already hot. If a run recompiles
the world, suspect a key change (toolchain bump, `Cargo.lock`) — not
the weather. Pinning `runner-<host>-<nn>` keeps you on one box's
cache.

## Cost

Self-hosted: no per-minute cost — the machines are ours. There is no
budget reason to batch or skimp on gate dispatches; run them whenever
evidence is needed.
