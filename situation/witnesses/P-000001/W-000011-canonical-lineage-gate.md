# W-000011 — Canonical lineage historical gate

## Promise

`situation/promises/P-000001-canonical-lineage-state.md`

## Oracle

`situation/oracles/O-000001-canonical-lineage-state.md`

## Result

PASS

## Head

`49ba2ced98831d192f6a2371b90aec8e81a081fd`

## Observed

2026-08-24

## Correction

This is the historical gate execution that supplies current state evidence for
the promise. It replaces W-000006 as state evidence because its Head and
Observed fields name the exact source and date of the executable observation.
W-000006 remains an immutable source-identity observation.

## Evidence

- `https://api.github.com/repos/cleverunicornz/yeetz-s3-kernel/actions/runs/32736208926/jobs`
  records the successful `gates (full set)` job at this head on 2026-08-24.
- `49ba2ced98831d192f6a2371b90aec8e81a081fd:.github/workflows/ci-dev.yml`
  defines `gates` to execute `cargo nextest run --workspace`.
- For current applicability, `git diff --name-only 49ba2ced98831d192f6a2371b90aec8e81a081fd..94c39fecb90ca998156078c7532ebab45927d934 -- Cargo.toml Cargo.lock rust-toolchain.toml crates/yeetz-s3-kernel/Cargo.toml crates/yeetz-s3-kernel/src crates/yeetz-s3-kernel/tests crates/yeetz-s3-streams/Cargo.toml crates/yeetz-s3-streams/src crates/yeetz-s3-streams/tests crates/yeetz-sdk-s3/Cargo.toml crates/yeetz-sdk-s3/src crates/yeetz-sdk-s3/tests crates/yeetz-sdk-core/Cargo.toml crates/yeetz-sdk-core/src crates/yeetz-sdk-core/tests rigs/Cargo.toml rigs/src .github/workflows/ci-dev.yml tools/check_storage_boundaries.sh tools/check_dependency_floor.sh` produced no paths at the opening checkpoint. The committed Git history, not this witness text, retains that identity evidence.

## Oracle legs

| Leg | Evidence |
|---|---|
| P1 | Successful historical `gates` execution; opening inputs identical |
| P2 | Successful historical `gates` execution; opening inputs identical |
| P3 | Successful historical `gates` execution; opening inputs identical |
