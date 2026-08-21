# AGENTS.md — yeetz-s3-kernel

S3-native storage kernel in Rust: the single atomic point through
which durable state is written. Extracted from the yeetz forge
(parent: `cleverunicornz/yeetz`) because it is infrastructure
consumed by multiple products, not just yeetz. This repo is the
kernel's source of record.

## Invariants — breaking any of these is wrong, whatever else is right

1. **One storage truth.** All durable reads and writes flow through
   `crates/yeetz-s3-kernel` (+ closure `yeetz-sdk-s3`,
   `yeetz-sdk-core`, `yeetz-s3-streams`). Nothing writes or reads
   objects around it — in any direction.
2. **Missing capability is BLOCKING, not a workaround.** If the kernel
   does not expose what a consumer needs, stop and escalate to the
   human. Raw-adapter access from application code is the
   disqualifying failure this repo exists to prevent.
3. **Records are immutable, created by conditional write.** New state
   is a new record; put-if-absent semantics, never in-place mutation.
4. **Pointers move by CAS.** Heads/canonical references advance via
   compare-and-swap (`If-Match`), git-ref semantics. A failed CAS
   means contention: re-read, re-derive, retry.
5. **Every projection is rebuildable from records.** Derived indexes
   are disposable by design; repair is lineage replay, never surgery.
6. **Reads are fenced.** Projections carry generation stamps; stale
   readers must not act on superseded state.
7. **Integrity failures are never absence.** A missing head or record
   is `StateHistoryIncomplete` — distinct from never-existed and from
   empty, never translated.
8. **Kernel extension is human-gated.** One tightly scoped batch: the
   change, its contract suite, its proof — nothing else. Downstream
   breakage breaks definitively; repairs are separately orchestrated.
9. **Pins are assured versions.** Do not bump the dependency pins
   casually (root `Cargo.toml`); the kernel is assured at these
   versions.
10. **PRs always; a human merges.** CI (fmt / clippy `-D warnings` /
    locked build / nextest / boundary checks) is the only standing
    gate. No lanes, closures, or delegates.
11. **ADRs are immutable.** `docs/decisions/` owns the why; supersede,
    never edit.
12. **Verification runs remotely.** WarpBuild 16x, shared cache
    `yeetz-s3-kernel-ci`; gate claims cite a ci-dev run URL, never a
    local attestation (`.agents/skills/ci`).

## Where things live

- `crates/yeetz-s3-kernel` — lineage state kernel, `AtomicKeyspace`,
  terminal reads, the real-S3 ABA probe engine
- `crates/yeetz-s3-streams` — append-only event logs (ADR 0002)
- `crates/yeetz-sdk-s3`, `crates/yeetz-sdk-core` — the storage
  adapter closure; sanctioned S3 clients
- `rigs/` — durable verification rigs (streams contracts, real-S3
  ABA probe); index in `rigs/INDEX.md`
- `docs/decisions/` — ADR 0001 (atomic keyspace), ADR 0002 (streams)
- `tools/` — storage-boundary and dependency-floor enforcement (CI
  gates)
- `.agents/skills/` — the process surface (state-kernel, ci)
