# yeetz-s3-kernel

[![Crates.io](https://img.shields.io/crates/v/yeetz-s3-kernel.svg)](https://crates.io/crates/yeetz-s3-kernel)
[![Docs.rs](https://docs.rs/yeetz-s3-kernel/badge.svg)](https://docs.rs/yeetz-s3-kernel)
[![CI](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/workflows/ci.yml/badge.svg)](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/cleverunicornz/yeetz-s3-kernel/blob/main/LICENSE)

An **assured state layer over S3-compatible object storage**. The kernel
gives you an append-only lineage of immutable, digest-chained records
whose canonical current state is named by a single conditionally-written
head object — plus a validated `AtomicKeyspace` with put-if-absent
creates, `If-Match` compare-and-swap, and etag-guarded deletes.

Its defining property is not what it does but what it makes
impossible: **two storage truths**. The kernel is the *only* holder of
the S3 client. Applications construct a [`KernelHandle`] from an
[`S3Config`] and receive opaque kernel surfaces; the underlying adapter
type is structurally unreachable, and **no unconditional overwrite
exists anywhere on the API surface**. Every write is a conditional
create (`If-None-Match`) or a compare-and-swap (`If-Match`) — git-ref
semantics, but the refs live in your bucket.

[`KernelHandle`]: https://docs.rs/yeetz-s3-kernel/latest/yeetz_s3_kernel/struct.KernelHandle.html
[`S3Config`]: https://docs.rs/yeetz-sdk-s3/latest/yeetz_sdk_s3/struct.S3Config.html

## Two surfaces

**`StateKernel` — lineages.** One kernel per [`KernelLineage`]:
immutable `CanonicalRecord`s, a CAS-guarded head pointer, deterministic
[`fold`] replay from checkpoints, rebuildable projections, and
tombstoned destroy. Errors never conflate integrity with absence: a
broken chain is `StateHistoryIncomplete`, a never-created lineage is
`LineageHeadState::Absent`, a deliberate delete is
`LineageHeadState::Destroyed` — three different answers, never
flattened into `None` or empty.

**`AtomicKeyspace` — shared keyspace.** Namespaced key layout
(`keyspace/{namespace}/{key}`) with `create` (put-if-absent),
`get`/`get_with_etag`, `compare_exchange` (If-Match CAS),
`list_after`, idempotent `delete`, etag-guarded `delete_if_match`,
per-key-outcome `delete_many`, tombstoned `destroy`, certified trim
with a resumable GC sweeper, and streaming large-value I/O
(`ValueWriter`/`ValueReader` over chunked manifests, with maintenance
fences and chunk sweeps).

[`KernelLineage`]: https://docs.rs/yeetz-s3-kernel/latest/yeetz_s3_kernel/state_kernel/struct.KernelLineage.html
[`fold`]: https://docs.rs/yeetz-s3-kernel/latest/yeetz_s3_kernel/state_kernel/struct.StateKernel.html#method.fold

## Example

```rust
use yeetz_s3_kernel::state_kernel::{
    CanonicalRecord, KernelLineage, SuccessorPolicy,
};
use yeetz_s3_kernel::{KernelHandle, LineageHeadState};

# async fn run(config: yeetz_s3_kernel::S3Config) -> Result<(), Box<dyn std::error::Error>> {
let handle = KernelHandle::from_s3_config(&config)?;

let lineage = KernelLineage::new("issue/demo/1", SuccessorPolicy::SuccessorCapable)?;
let kernel = handle.state_kernel(lineage.clone());

// Genesis — a conditional create: the record and the first head land
// atomically. Lost races leave nothing partial behind.
let genesis = CanonicalRecord::new(
    &lineage,
    0,
    None,
    "issue.created",
    "issue.v1",
    br#"{"title":"hello"}"#.to_vec(),
    "op-1",
    "actor-alice",
    "cause-1",
)?;
let head = kernel.append_genesis(&genesis).await?;

// Successors — CAS against the head you observed. A failed CAS is
// contention: re-read, re-derive, retry.
let successor = CanonicalRecord::new(
    &lineage,
    1,
    Some(head.record_position()),
    "issue.renamed",
    "issue.v1",
    br#"{"title":"hi"}"#.to_vec(),
    "op-2",
    "actor-alice",
    "cause-2",
)?;
let head = kernel.append_successor(&successor, &head).await?;

// Terminal read — O(1): head + terminal record, no history walk.
match kernel.read_head_state().await? {
    LineageHeadState::Present(read) => {
        println!("generation {}", read.generation());
    }
    LineageHeadState::Absent => { /* never created */ }
    LineageHeadState::Destroyed(_) => { /* tombstoned */ }
}
# Ok(())
# }
```

## The laws

1. **Records are immutable, created by conditional write.** New state
   is a new record; put-if-absent, never in-place mutation.
2. **Pointers move by CAS.** Heads advance via `If-Match`, git-ref
   semantics. A failed CAS means contention: re-read, re-derive, retry.
3. **Every projection is rebuildable from records.** Derived state is
   disposable by design; repair is lineage replay, never surgery.
4. **Reads are fenced.** Heads carry generations and incarnations;
   stale readers never act on superseded state.
5. **Integrity failures are never absence.** `StateHistoryIncomplete`
   is distinct from never-existed and from empty, always.

Full rationale: [D-000001 — AtomicKeyspace](../../situation/decisions/D-000001-atomic-keyspace.md),
[D-000003 — initial streaming proposal](../../situation/decisions/D-000003-streaming-value-v1.md), and
[D-000004 — selected streamed-value design](../../situation/decisions/D-000004-streaming-value-v2.md).

## Features

- `test-support` — in-memory store handle
  (`KernelHandle::with_in_memory_store`), in-kernel damage helpers, and
  the live-backend ABA probe engine (`run_real_s3_aba_probe`) for
  exercising conditional-write behavior against a real S3 endpoint.

## Assurance

Behavior is pinned by named contract suites, not prose: the **K-suite**
(K1–K7 core storage laws), the **A-suite** (A1–A34 keyspace and
streaming-value extensions), and the **G-suite** (GC resumability,
weak-cursor recovery), all run against a loopback S3 counterpart with
injectable faults — plus `rigs/` executable witnesses replaying the
durable legs against real backends in CI.

Full rationale: [D-000001 — AtomicKeyspace](https://github.com/cleverunicornz/yeetz-s3-kernel/blob/main/situation/decisions/D-000001-atomic-keyspace.md),
[D-000003 — initial streaming proposal](https://github.com/cleverunicornz/yeetz-s3-kernel/blob/main/situation/decisions/D-000003-streaming-value-v1.md), and
[D-000004 — selected streamed-value design](https://github.com/cleverunicornz/yeetz-s3-kernel/blob/main/situation/decisions/D-000004-streaming-value-v2.md).

## The closure

| Crate | Role |
| --- | --- |
| [`yeetz-s3-kernel`](https://crates.io/crates/yeetz-s3-kernel) | this crate — lineages + atomic keyspace |
| [`yeetz-s3-streams`](https://crates.io/crates/yeetz-s3-streams) | append-only event logs on the keyspace |
| [`yeetz-sdk-s3`](https://crates.io/crates/yeetz-sdk-s3) | request-scoped S3 client mechanics |
| [`yeetz-sdk-core`](https://crates.io/crates/yeetz-sdk-core) | provider-neutral HTTP foundation |

## License

MIT.
