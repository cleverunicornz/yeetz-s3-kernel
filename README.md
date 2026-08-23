# yeetz-s3-kernel

[![CI](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/workflows/ci.yml/badge.svg)](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

An S3-native storage kernel in Rust: the single atomic point through
which durable state is written. Extracted from the
[yeetz forge](https://github.com/cleverunicornz/yeetz) because it is
infrastructure consumed by multiple products — this repository is the
kernel's source of record.

## The idea

Object storage gives you PUT/GET/LIST and almost nothing else. Real
state needs more: writes that can't clobber, histories that can't lie,
and reads that can't mistake damage for absence. The kernel builds
those on S3's conditional primitives — `If-None-Match` create and
`If-Match` compare-and-swap — and enforces one non-negotiable rule:
**one storage truth**. All durable reads and writes flow through the
kernel closure below; nothing writes or reads objects around it, in
any direction.

The laws the closure enforces:

1. **Records are immutable, created by conditional write.** New state
   is a new record; put-if-absent, never in-place mutation.
2. **Pointers move by CAS.** Heads advance via `If-Match`, git-ref
   semantics; a failed CAS is contention — re-read, re-derive, retry.
3. **Every projection is rebuildable from records.** Derived indexes
   are disposable; repair is lineage replay, never surgery.
4. **Reads are fenced.** Generations and incarnations keep stale
   readers from acting on superseded state.
5. **Integrity failures are never absence.** `StateHistoryIncomplete`
   is distinct from never-existed and from empty — always.

## The crates

| Crate | On crates.io | Role |
| --- | --- | --- |
| [`yeetz-s3-kernel`](./crates/yeetz-s3-kernel) | [![crates.io](https://img.shields.io/crates/v/yeetz-s3-kernel.svg)](https://crates.io/crates/yeetz-s3-kernel) | assured state layer: append-only lineages, `AtomicKeyspace`, streaming large values |
| [`yeetz-s3-streams`](./crates/yeetz-s3-streams) | [![crates.io](https://img.shields.io/crates/v/yeetz-s3-streams.svg)](https://crates.io/crates/yeetz-s3-streams) | append-only event logs on the keyspace (ADR 0002) |
| [`yeetz-sdk-s3`](./crates/yeetz-sdk-s3) | [![crates.io](https://img.shields.io/crates/v/yeetz-sdk-s3.svg)](https://crates.io/crates/yeetz-sdk-s3) | request-scoped S3-compatible client mechanics |
| [`yeetz-sdk-core`](./crates/yeetz-sdk-core) | [![crates.io](https://img.shields.io/crates/v/yeetz-sdk-core.svg)](https://crates.io/crates/yeetz-sdk-core) | provider-neutral HTTP foundation (retry, rate limits, raw-body errors) |

## Repository layout

- `crates/` — the four published crates above
- `rigs/` — durable verification rigs: executable witnesses proving
  kernel claims against real backends (streams contracts, live-S3 ABA
  probe); indexed in `rigs/INDEX.md`
- `docs/decisions/` — ADRs: [0001 AtomicKeyspace](./docs/decisions/0001-atomic-keyspace.md),
  [0002 Streams](./docs/decisions/0002-streams.md),
  [0003/0004 Streaming value I/O](./docs/decisions/0004-streaming-value-io-v2.md)
- `tools/` — storage-boundary and dependency-floor enforcement (CI gates)

## Verification

Behavior is pinned by named contract suites (K/A/G for the kernel, S
for streams, R for trim) run against a fault-injecting loopback S3
counterpart, plus durable rigs against real S3 in CI. Gate claims cite
CI run URLs — see the checks on any PR or on `main`.

Rust toolchain: 1.96 (pinned in `rust-toolchain.toml`).

## License

MIT — see [LICENSE](LICENSE).
