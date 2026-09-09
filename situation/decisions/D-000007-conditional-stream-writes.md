# D-000007 — Conditional stream writes: caller-ID creation and exact-successor append

## Status

accepted

## Date

2026-09-09

## Context

The write side of `yeetz-s3-streams` has two gaps that the strict reads of
D-000006 made conspicuous.

Creation: `Streams::create_stream` (`crates/yeetz-s3-streams/src/lib.rs`)
mints the stream's identity itself and returns it only in the success
response. A caller whose response is lost cannot retry: a retry mints a
different id and strands the first genesis object. No surface accepts a
caller-chosen `StreamId`, so creation is not idempotent from the caller's
side.

Append: `Streams::append` allocates at the first free sequence implied by
event evidence (LIST max, verified tail hint, trim clamp), tolerates
conflicts through a budget/backoff loop that advances the slot, and bounds
idempotency to a pre-scan window. A caller holding a predecessor
`EventRef` and demanding the exact successor has no surface: ordinary
append interleaves at whatever slot is free and never reports "the
demanded position is held by a different event" as its own typed outcome.

Neither surface can state effect certainty. `AtomicKeyspace::create`
(`crates/yeetz-s3-kernel/src/atomic_keyspace.rs`) is put-if-absent, and
its stale-incarnation cleanup can return `AlreadyExists` after the upload
applied — an aggregate error can conceal a successful PUT. Retention
races the write path: a certified trim floor may advance while an
append is in flight, and GC later sweeps below the floor, so "landed"
and "retained" are separately decidable and must be reported separately.

This slice is the authorized scoped batch under
`situation/invariants/I-000002-scoped-kernel-extension.md`. The accepted
validator contract bounds it to three effect certainties (`NotAttempted`,
`PossiblyCommitted`, `Committed`), requires every error to carry its
effect, exempts the immortal genesis from floor-based refusal, and grants
no atomic append+trim fence and no write authority.

## Evidence

- Ordinary creation, the append allocation loop, idempotency window,
  trim floor, ordered probe, and tail-hint accelerator:
  `crates/yeetz-s3-streams/src/lib.rs`
- Immutable envelope, canonical deterministic encoding, `EventRef`, and
  the genesis envelope: `crates/yeetz-s3-streams/src/envelope.rs`
- Existing typed boundary variants reused by the failure taxonomy:
  `crates/yeetz-s3-streams/src/error.rs`
- Put-if-absent create and the stale-incarnation
  `AlreadyExists`-after-upload path:
  `crates/yeetz-s3-kernel/src/atomic_keyspace.rs`
- Reserved-key admission guard (`tombstones/`, `incarnations/`,
  `fences/`, and the structural `trims` path segment) and its typed
  variants: `crates/yeetz-s3-kernel/src/atomic_keyspace.rs`
  (`ensure_not_reserved_key`)
- Strict exact-position reads this slice mirrors on the write side:
  `crates/yeetz-s3-streams/src/bounded.rs`
- Deterministic fault/pause instrumentation, request log, and LIST
  freeze/hide controls:
  `crates/yeetz-s3-streams/tests/support/loopback.rs`
- Stream design boundary:
  `situation/decisions/D-000002-append-only-streams.md`
- `EventRef` and immutable-envelope basis:
  `situation/decisions/D-000006-strict-stream-reads.md`
- Assured semantics this slice must not disturb:
  `situation/promises/P-000003-append-only-streams.md`
- Batch and storage-boundary law:
  `situation/invariants/I-000002-scoped-kernel-extension.md` and
  `situation/invariants/I-000001-kernel-storage-boundary.md`

## Decision

Add a conditional-write surface to `Streams` in a new
`crates/yeetz-s3-streams/src/conditional.rs`, wired at the crate root:

1. `create_stream_with_id(stream: &StreamId, config: &[u8]) ->
   Result<CreateStreamOutcome, CreateStreamError>`. The caller supplies
   the identity through the existing `StreamId::new` — no new generator —
   and the canonical genesis is encoded through the existing
   `Envelope::genesis` before any I/O, preserving the encoded-size bound.
   The create is one conditional create at seq 0: `Created` on success;
   `Existing` only when the incumbent genesis is the byte-identical
   canonical genesis (an exact retry); a valid different genesis is
   `ConfigurationConflict`; a malformed incumbent is `Storage(Corrupt)`;
   an incumbent that conflicts and is then absent on readback is
   `Storage(BackendUnqualified)`; any other storage failure is
   `Storage(Unavailable)`, and a retry with the same id and bytes
   converges. A caller id whose keyspace key falls under the kernel's
   reserved-key guard — the `tombstones/`, `incarnations/`, and
   `fences/` roots and the structural `trims` path segment, so for
   example `StreamId::new("trims")` whose genesis key is structurally a
   certificate key — is a permanent `Storage(InvalidArgument)` with
   zero storage requests: the pure guard precedes the store call in
   `AtomicKeyspace::create`, the existing variant mapping already
   classifies it, and no reserved-name policy is duplicated in streams
   and no kernel change is made. It is never a retryable
   `Storage(Unavailable)`. No path overwrites an incumbent or writes a
   second genesis identity. `Existing` asserts byte-identity only, not
   a writer grant.
   Ordinary `create_stream` collision semantics are unchanged.

2. `append_expected(predecessor: &EventRef, schema_id: &SchemaId,
   stable_event_id: &StableEventId, payload: &[u8]) ->
   Result<AppendReceipt, AppendExpectedError>`. Every failure carries its
   effect: `AppendExpectedError { kind, effect }` with `effect` one of
   `NotAttempted`, `PossiblyCommitted`, `Committed(AppendReceipt)` — no
   fourth certainty exists. `kind` reuses the existing `StreamsError`
   variants inside `Storage` and adds `PredecessorMismatch`,
   `PositionConflict`, `HoleWitnessed`, and `Expired` with subject
   `Predecessor` or `Target`.
   The cold failure kind is boxed — `kind:
   Box<AppendExpectedFailure>` — so the failure payload does not widen
   the error value carried on the hot success path, and a successful
   adjudication returns the receipt derived from the envelope identity
   without cloning it. The result signature and the persisted wire
   format are unchanged.

   Adjudication order, each step observable from the request log: pure
   admission validation before any request (identifiers, predecessor
   digest form, successor seq, encoded size); genesis verification; the
   exact target GET first, before predecessor, suffix, or floor-driven
   steps. A byte-identical canonical target reconciles to `Committed`
   without reading the predecessor or suffix and without writing — it
   proves the target's bytes, not that the supplied predecessor was
   examined, and it works after later appends and after the predecessor
   was collected while the target remains retained. A target occupied by
   different bytes is judged after a floor observation (an expired target
   overrides a zombie occupant; the same stable id with different
   canonical bytes or schema is the existing `IdempotencyConflict`; a
   different verified event is `PositionConflict`; malformed is
   `Corrupt`) and never advances the slot. An absent target observes the
   floor while maintaining the maximum ever observed — a lower later
   observation is `BackendUnqualified` — applies target and predecessor
   expiry with the genesis exemption (the immortal seq 0 means a floor
   of 1 at target 1 never refuses a fresh append from a verified
   genesis), then verifies the predecessor against all four `EventRef`
   fields, reusing the already-verified genesis when the predecessor is
   seq 0: absent is `EventMissing` after one floor reread to distinguish
   concurrent expiry, a field mismatch is `PredecessorMismatch`, and
   malformed is `Corrupt` — all `NotAttempted`. Before any create it
   probes the ordered next log key after the target and GET-verifies a
   listed witness: a verified later event is `HoleWitnessed` and the
   hole is never repaired; LIST-present/GET-absent is
   `BackendUnqualified`; a malformed witness is `Corrupt`; a verified
   tail hint may supply additional later-record evidence, unverified
   hints grant nothing, and no tail write is ever made.

   The attempt is exactly one `AtomicKeyspace::create` at the exact
   target. From its invocation the effect is at least `PossiblyCommitted`;
   any create error — including `AlreadyExists` — can conceal a prior
   successful PUT and is resolved only by an exact-target readback:
   byte-identical canonical bytes upgrade to `Committed`; a conflicting
   or corrupt readback preserves `PossiblyCommitted` alongside its typed
   kind; an absent or unavailable readback retains `PossiblyCommitted`.
   A floor observation is mandatory before adjudicating success, and it
   precedes interpreting a possibly expired occupant:
   committed-and-expired is `Expired(Target, Committed)` carrying the
   receipt; a failed observation is `Storage(Unavailable)` carrying the
   current effect; otherwise committed returns the receipt;
   unresolved-possibly-committed with observed expiry is
   `Expired(Target, PossiblyCommitted)`, otherwise
   `Storage(Unavailable, PossiblyCommitted)`. No failure after the
   attempt is downgraded to `NotAttempted`, and no `Rejected` certainty
   is inferred from error wording. The floor never clamps or moves the
   target; there is no budget/backoff loop, no slot increment, no
   bounded id pre-scan, no tail write, no new lease/coordinator/head
   CAS, and no persisted format change.

`Ok` means the requested canonical envelope was confirmed at the exact
successor and a later successful qualified floor observation did not
retire the target. It is not a retention pin, a future-retention
guarantee, an ownership grant, or an atomic append+trim fence. Bytes can
physically land below a concurrently advanced floor and be collected by
later GC; a silently stale certificate LIST can miss a new floor; a
target already collected cannot reconstruct an earlier caller's lost
receipt or uncertainty.

## Why

Caller-supplied identity plus canonical deterministic encoding makes
creation retry-safe without a second consensus point: the retry meets
its own first attempt byte-for-byte, and a conflict against a different
genesis is exactly the cross-wiring the typed `ConfigurationConflict`
names. A reserved-key collision classifies with admission rather than
availability: the kernel's guard is pure and precedes the store call,
so the refusal is a permanent typed argument error, never a retryable
unavailable. The exact-target-first order converts the dominant retry case
into a pure read that survives suffix advance and predecessor
collection — predecessor identity cannot be validated after its
collection, but the target's canonical bytes remain self-verifying. The
three-effect taxonomy is forced by the keyspace: `create` is a consumed
operation whose aggregate error can conceal an applied PUT, and absence
of evidence is not evidence of absence, so the honest certainties are
exactly attempt-not-yet-made, uncertain, and byte-confirmed. Floor
observation bounds every occupancy and success judgment because
retention races the write path; the genesis exemption follows from
trim's own law (floors retain seq 0), so floor 1 at target 1 is the
normal first-successor case, not an expiry. Hole witnessing reuses the
read side's discipline: only a GET-verified record is a witness, LIST
alone is qualification evidence, and a contradiction fails closed. Slot
advancement, budget/backoff, id pre-scan, tail writes, and any
coordinator or head CAS are rejected because a predecessor reference is
identifying data, not a whole-prefix density proof, and a second
authority point would reintroduce exactly the contention D-000002
removed.

## Rejected alternatives

- Retry creation through ordinary `create_stream`: a lost response
  strands the first genesis and mints a new identity; not idempotent.
- Deterministic name-derived ids or a registry publication transaction:
  name→id mapping is the application's boundary law (D-000002); a
  registry transaction is a second consensus point; derivation across
  different configs needs exactly the conflict taxonomy built here while
  adding registry divergence as a new failure mode.
- `append` plus caller-side read-back heuristics: the position is not
  pinned, the loop interleaves at any free slot, the idempotency window
  is evidence-bounded, and errors carry no effect certainty.
- A `Rejected` ("definitely not landed") effect: not derivable from the
  keyspace's aggregate error; absence and error wording prove nothing,
  and exposing it would let callers delete or reissue on a false
  negative.
- Clamping the target up to the floor as ordinary append does: the
  floor is a retention boundary, not event evidence; silently moving a
  demanded position is the dishonest answer `PositionConflict` and
  `Expired` exist to prevent.
- Repairing a witnessed hole by writing the missing seq: it cannot
  distinguish a hole from a position about to be taken by its rightful
  writer, and writing below a certified floor is resurrection the
  certificate rejects.
- An append+trim fence, retention pin, or write authority attached to
  success: no such primitive exists at this backend; the promise would
  be false, so the boundary is stated as a residual instead.
- Tail-hint writes or a head/coordinator CAS to cheapen suffix-density
  proofs: hints are accelerators, never authority (D-000002); the
  conditional path writes exactly one object.

## Consequences

`yeetz-s3-streams` gains a second write surface beside the ordinary
ones; P-000003 replay, multiwriter append, and idempotency-window
semantics and P-000006 strict reads are unchanged and unconstrained by
this slice. `append_expected` follows a distinct target-first,
retention-aware read sequence — exact-target reconciliation, floor
observations, predecessor verification, hole probe — rather than
ordinary append's evidence scan; request counts depend on the path and
the retention window. It makes exactly one logical keyspace create per
attempt — one create call landing at most one object, where the
consumed kernel may retry its incarnation mechanics inside that call
without a second creation effect; its request shape is
contract evidence. The residual boundaries — a stale certificate LIST
can miss a new floor; bytes below a later floor can be swept after
success; a swept target cannot reconstruct an earlier caller's lost
receipt; a hidden unwitnessed suffix under a stale LIST during hole
adjudication — are recorded in P-000007 rather than repaired. Behavior
and assurance land as P-000007/O-000007 under I-000002's scoped-batch
rule; assurance rides the `gates` task with a new
`crates/yeetz-s3-streams/tests/streams_conditional.rs`, while the
`kernel-rigs` route continues to prove standalone keyspace behavior and
is not the fault route for these contracts. Implementation rides the
kernel keyspace only, respecting I-000001.

## Revisit when

P-000003 is superseded; a qualified backend capability (strongly
consistent LIST, an atomic multi-object fence, retention pins) changes
what floor observation or success adjudication can honestly claim; or a
deliberate whole-prefix-proof or write-authority surface supersedes the
bounded `Ok`.
