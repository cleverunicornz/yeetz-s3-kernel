# W-000008 — Append-only streams input-identity correction

## Promise

`situation/promises/P-000003-append-only-streams.md`

## Oracle

`situation/oracles/O-000003-append-only-streams.md`

## Result

PASS

## Head

`94c39fecb90ca998156078c7532ebab45927d934`

## Observed

2026-09-05

## Correction

This observation replaces W-000003 as the promise's state evidence. It retains
the same independent gate result and adds every package manifest to the
source-identity comparison. W-000003 remains immutable historical evidence.

## Evidence

- `https://api.github.com/repos/cleverunicornz/yeetz-s3-kernel/actions/runs/32736208926/jobs`
  records successful execution of `gates (full set)` at
  `49ba2ced98831d192f6a2371b90aec8e81a081fd`.
- `49ba2ced98831d192f6a2371b90aec8e81a081fd:.github/workflows/ci-dev.yml`
  defines `gates` to execute `cargo nextest run --workspace`.
- At this witness head, `git diff --name-only 49ba2ced98831d192f6a2371b90aec8e81a081fd..94c39fecb90ca998156078c7532ebab45927d934 -- Cargo.toml Cargo.lock rust-toolchain.toml crates/yeetz-s3-kernel/Cargo.toml crates/yeetz-s3-kernel/src crates/yeetz-s3-kernel/tests crates/yeetz-s3-streams/Cargo.toml crates/yeetz-s3-streams/src crates/yeetz-s3-streams/tests crates/yeetz-sdk-s3/Cargo.toml crates/yeetz-sdk-s3/src crates/yeetz-sdk-s3/tests crates/yeetz-sdk-core/Cargo.toml crates/yeetz-sdk-core/src crates/yeetz-sdk-core/tests rigs/Cargo.toml rigs/src .github/workflows/ci-dev.yml tools/check_storage_boundaries.sh tools/check_dependency_floor.sh` produced no paths. The committed Git history is the retained source-identity evidence, not this witness text.

## Oracle legs

| Leg | Evidence |
|---|---|
| P1 | Successful historical `gates` execution and unchanged oracle inputs |
| P2 | Successful historical `gates` execution and unchanged oracle inputs |
| P3 | Successful historical `gates` execution and unchanged oracle inputs |
