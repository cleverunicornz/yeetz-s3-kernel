---
name: state-kernel
description: Use when writing or reviewing any code that persists state, touches S3, creates records, moves pointers/refs, or maintains derived projections. Owns the kernel's usage law.
metadata:
  short-description: Storage kernel usage law
---

# state-kernel

`crates/yeetz-s3-kernel` (+ closure `yeetz-sdk-s3`, `yeetz-sdk-core`,
`yeetz-s3-streams`) is the single atomic point through which ALL
storage access flows — reads, writes, deletes, listings. This repo is
the kernel's source of record (extracted from the yeetz forge); the
contract suites — kernel K/A/H/G, streams S — are the standing
regression net, and every extension adds its own contract suite
alongside them.

## The law

1. **Nothing bypasses the kernel — in any direction.** No direct
   object writes, no raw-adapter reads, no private-key-layout parsing,
   no out-of-band listings from application code. The kernel closure
   and its assured extension modules are the only S3 clients. Two
   storage truths is the disqualifying failure this crate exists to
   prevent.
2. **Missing capability is BLOCKING, not a workaround.** If the kernel
   does not expose what a slice needs, the agent stops and escalates
   to the orchestrator, who escalates to the human. Kernel extension
   is human-adjudicated (small input or full design). Reaching for the
   raw adapter because "the kernel can't do this" is the canonical
   failure mode this law exists to prevent.
3. **Records are immutable, created by conditional write.** New state is
   a new record; put-if-absent semantics, never in-place mutation.
4. **Pointers move by CAS.** Heads/canonical references advance via
   compare-and-swap (`If-Match`), git-ref semantics. A failed CAS means
   contention: re-read, re-derive, retry.
5. **Every projection is rebuildable from records.** Derived indexes are
   disposable by design; repair is lineage replay, never surgery.
6. **Reads are fenced.** Projections carry generation stamps; stale
   readers must not act on superseded state.
7. **Integrity failures are never absence.** A missing head or record is
   `StateHistoryIncomplete` — it must surface as an error distinct
   from never-existed and from empty. Translating integrity failure
   into empty/None is a defect of the same class as bypassing.

## Review posture

When reviewing a change that persists anything, check the laws above
before anything else. Wrong regardless of what else it gets right:
writes objects directly; READS via the raw adapter; parses the
kernel's private key layout (`/objects/`, `/head`, `/checkpoints/`);
mutates a record in place; moves a pointer without CAS; builds a
projection that cannot replay from records; translates an integrity
error into an empty/absent result.

The boundary is mechanically enforced: `tools/check_storage_boundaries.sh`
runs in CI (adapter calls outside sanctioned crates fail the build,
with a ratchet allowlist for documented debt).

## Kernel extension protocol (human-gated)

- A kernel change ships as ONE tightly scoped batch: the change, its
  contract suite, its proof — nothing else. Downstream breakage breaks
  definitively; repairs are separately orchestrated afterward.
- Dispatches that touch the kernel must state their human
  authorization explicitly and where the decision lives.
- Do not bump the kernel's dependency pins casually — assured versions
  (see root `Cargo.toml`).
