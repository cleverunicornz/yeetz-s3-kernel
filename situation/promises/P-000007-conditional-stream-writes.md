# P-000007 — Conditional stream writes

## State

implementing

## Promise

`yeetz-s3-streams` provides two conditional-write surfaces alongside the
ordinary ones:

1. `Streams::create_stream_with_id(stream: &StreamId, config: &[u8]) ->
   Result<CreateStreamOutcome, CreateStreamError>` creates a stream under
   caller-supplied identity. Caller-id validity and encoded-genesis
   validity (including the encoded-size bound) are checked before any
   storage request; a violation is a typed error with zero storage
   requests. A caller id whose genesis key falls under the kernel's
   reserved-key namespaces is likewise refused before any storage
   request, as a permanent typed invalid-argument error — never a
   retryable storage-unavailable error. The create is one conditional
   create of the canonical
   genesis at seq 0: success returns `Created`; an incumbent
   byte-identical canonical genesis returns `Existing` with no second
   creation effect — the incumbent's bytes are unchanged and no second
   genesis object appears; a valid incumbent genesis with different
   bytes returns
   `ConfigurationConflict`; a malformed incumbent returns a corrupt
   storage error; an incumbent that conflicts and is then absent on
   readback returns a backend-unqualified storage error; any other
   storage failure returns an unavailable storage error, and a retry
   with the same id and bytes converges on one outcome. No path
   overwrites an incumbent or writes a second genesis identity, and
   `Existing` asserts byte-identity only — not a writer grant. Ordinary
   `create_stream` semantics are unchanged.

2. `Streams::append_expected(predecessor: &EventRef, schema_id: &SchemaId,
   stable_event_id: &StableEventId, payload: &[u8]) ->
   Result<AppendReceipt, AppendExpectedError>` lands an event at exactly
   `predecessor.seq + 1` or fails typed. Every failure carries its
   effect — `AppendExpectedError { kind, effect }` with `effect` one of
   `NotAttempted`, `PossiblyCommitted`, `Committed(AppendReceipt)` — and
   no other certainty is returned:

   - Admission (identifier and predecessor-digest validity, successor
     seq, encoded size) is checked before any storage request;
     violations are `NotAttempted` with zero storage requests, including
     a predecessor at `u64::MAX`, which is `SeqExhausted`.
   - The genesis is verified, then the exact target is read before any
     predecessor, suffix, or floor-driven step. A byte-identical
     canonical target reconciles to its original receipt without
     reading the predecessor or suffix and without writing; this path
     works after later appends and after the predecessor was collected
     while the target remains retained, proves the target's bytes only,
     and claims nothing about the supplied predecessor.
   - A target occupied by different bytes is judged after a floor
     observation: an expired target yields `Expired(Target, ...)` even
     over a zombie occupant; the same stable event id with different
     canonical bytes or schema yields the existing
     `IdempotencyConflict`; a different verified event yields
     `PositionConflict`; a malformed occupant yields `Corrupt`. No path
     advances to another slot.
   - An absent target applies expiry — `Expired(Target, NotAttempted)`
     for a target below the floor, `Expired(Predecessor,
     NotAttempted)` for a nonzero predecessor below the floor — with
     the genesis exemption: the seq-0 genesis is immortal, so a floor
     of 1 at target 1 never refuses a fresh append from a verified
     genesis. The predecessor is then GET-verified against all four
     `EventRef` fields, reusing the verified genesis when the
     predecessor is seq 0: absent is `EventMissing` after one floor
     reread to distinguish concurrent expiry — a reread whose floor
     retires the target prioritizes `Expired(Target, NotAttempted)`
     over any predecessor verdict — a mismatch is
     `PredecessorMismatch`, malformed is `Corrupt` — all
     `NotAttempted`.
   - Before any create, the ordered next log key after the target is
     probed and a listed witness GET-verified: a verified later event is
     `HoleWitnessed` and the hole is never repaired;
     LIST-present/GET-absent is `BackendUnqualified`; a malformed
     witness is `Corrupt`. Unverified evidence grants nothing.
   - The attempt is exactly one conditional create at the exact target.
     From its invocation the effect is at least `PossiblyCommitted`.
     Any create error — including already-exists — is resolved only by
     an exact-target readback: byte-identical canonical bytes upgrade to
     `Committed`; a conflicting or corrupt readback preserves
     `PossiblyCommitted` with its typed kind; an absent or unavailable
     readback retains `PossiblyCommitted`. No failure after the attempt
     is reported as `NotAttempted`.
   - A floor observation is mandatory before adjudicating an attempt
     and precedes interpreting a possibly expired occupant: confirmed
     and not expired returns `Ok(receipt)`; confirmed and expired
     returns `Expired(Target, Committed)` carrying the receipt; a failed
     observation returns `Storage(Unavailable)` carrying the current
     effect; unresolved-possibly-committed with observed expiry returns
     `Expired(Target, PossiblyCommitted)`, otherwise
     `Storage(Unavailable, PossiblyCommitted)`. A floor observation
     lower than one already seen is `BackendUnqualified` carrying the
     current effect; the floor never clamps or moves the target.

   `Ok(receipt)` means the requested canonical envelope was confirmed at
   the exact successor and a later successful qualified floor observation
   did not retire the target — nothing more: it is not a retention pin,
   a future-retention guarantee, an ownership grant, or an atomic
   append+trim fence. The call issues no tail write and no write at any
   position other than the exact target, and the persisted envelope wire
   format is unchanged.

## Scope

`Streams::create_stream_with_id`, `CreateStreamOutcome`,
`CreateStreamError`, `Streams::append_expected`, `AppendExpectedEffect`,
`ExpiredSubject`, `AppendExpectedError`, and `AppendExpectedFailure` in
`crates/yeetz-s3-streams`, and the storage-request shape of those two
calls. Excludes ordinary `create_stream`, `append`, replay, and
strict-read semantics (P-000003, P-000006); cross-call retention
stability; GC and trim scheduling; whole-prefix-proof or authorization
meaning for `EventRef`; and any backend qualification beyond the typed
outcomes inside this scope.

## Oracle

`situation/oracles/O-000007-conditional-stream-writes.md`

## State evidence

- `situation/decisions/D-000007-conditional-stream-writes.md` — accepted
  design authorizing this slice as its scoped batch.
- `situation/oracles/O-000007-conditional-stream-writes.md` —
  `implemented` judgment rule, name-aligned to the complete
  `crates/yeetz-s3-streams/tests/streams_conditional.rs` suite
  (`c1`–`c12`, `e1`–`e34`; 46 tests); no execution has been recorded,
  and the oracle claims executability only.
- Implementation source is complete on this branch
  (`crates/yeetz-s3-streams/src/conditional.rs` and the test suite);
  the state remains `implementing`: the published candidate `7c40b58`
  failed compilation before any test executed, so it is not a valid
  implementation citation and no witness exists. No assurance is
  claimed; the transition to `implemented` will cite a compiling
  containing commit, and `assured` will cite a passing witness on the
  `gates` route.

## Residual

- A silently stale trim-certificate LIST can miss a concurrently
  advanced floor: adjudication stands on the qualified observation it
  obtained. This is an existing qualified-backend limitation, shared
  with the read path, and is explicitly not repaired here.
- `Ok` is not a retention pin: bytes that land below a concurrently
  advanced floor can be collected by later GC after success was
  observed.
- A target already collected cannot reconstruct an earlier caller's
  lost receipt or uncertainty; the earlier caller retains its own prior
  result.
- A hidden suffix invisible to both LIST and verified evidence during
  hole adjudication is unwitnessed; the `HoleWitnessed` and
  `BackendUnqualified` decisions are bounded by witnessed evidence.
- Assurance is pending (state `implementing`): no witness claims any
  leg has passed.

## References

- `situation/decisions/D-000007-conditional-stream-writes.md`
