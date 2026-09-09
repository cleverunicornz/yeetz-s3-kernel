# D-000006 — Strict stream reads and an immutable Envelope

## Status

accepted

## Date

2026-09-09

## Context

`Streams::read` (`crates/yeetz-s3-streams/src/lib.rs`) answers a live question —
which events follow `after_seq` up to the current end — and pays for that
liveness: a genesis GET, a trim-floor lookup, a computed-key GET walk, an
ordered LIST probe, tail-hint consultation, and read-path accelerator
maintenance that writes (`heal_tail_hint`, `recover_tail_hint`). Its
`complete=true` is witness-bounded live EOF, not a statement about a caller's
demanded window. Callers needing one exact historical event, or one exact
contiguous window, have no surface that returns the demanded bytes without
importing LIST qualification, hint witnesses, or read-path writes.
Separately, the exported `Envelope`
(`crates/yeetz-s3-streams/src/envelope.rs`) exposes public mutable fields:
construction outside the crate is already blocked by the private `encoded`
field, but any caller can mutate a previously verified value's public fields,
invalidating the claim its verification carried;
`decode_and_verify` checks `payload_sha256` and then drops it, while
`digest_hex` recomputes SHA-256 over the encoded bytes on every call.

## Evidence

- Live replay read path, LIST/hint machinery, read-path writes, and
  `AppendReceipt` public fields: `crates/yeetz-s3-streams/src/lib.rs`
- Envelope public mutable fields, per-call digest recompute, and the
  verified-then-dropped payload digest (the file's state as examined on this
  decision's date, 2026-09-09; the implemented tree has since adopted the
  private getters): `crates/yeetz-s3-streams/src/envelope.rs`
- Existing typed boundary variants (`InvalidArgument`, `StreamNotFound`,
  `EventMissing`, `OffsetExpired`, `Corrupt`, `Unavailable`) already serving
  the cursor and replay paths: `crates/yeetz-s3-streams/src/error.rs`
- Request-shape witnesses that make "which storage requests ran" decidable:
  `crates/yeetz-s3-streams/tests/support/loopback.rs` (`Loopback::start`,
  `request_log`, freeze/hide/fault controls) and
  `crates/yeetz-s3-streams/tests/support/mod.rs`
  (`streams_on_in_memory_store`, `hand_envelope`, `streams_keyspace`)
- Assured live-replay semantics this slice must not disturb:
  `situation/promises/P-000003-append-only-streams.md` and
  `situation/oracles/O-000003-append-only-streams.md`
- Stream design boundary: `situation/decisions/D-000002-append-only-streams.md`
- Batch discipline and storage boundary:
  `situation/invariants/I-000002-scoped-kernel-extension.md` and
  `situation/invariants/I-000001-kernel-storage-boundary.md`

## Decision

Add strict historical reads to `Streams` as a separate GET-only surface:
`read_event(&StreamId, Seq) -> Result<Envelope, StreamsError>` returning one
verified event, and
`read_range(&StreamId, after_seq, through_seq, limit) -> Result<RangePage, StreamsError>`
returning the complete contiguous demanded subwindow `(after, through]` or a
typed error — never a partial success with missing history, never a live-EOF
claim. Both validate arguments before any storage request, verify the genesis,
and issue no log LIST, no tail-hint read, and no write of any kind; only the
genesis, the demanded target or page window, and permitted retention-control
lookups are read, and an unavailable retention lookup fails closed. Make the
exported `Envelope` immutable by privacy behind getters — including
`payload_sha256()` backed by the digest `decode_and_verify` already verified,
retained on the value instead of dropped and recomputed — and add a
serializable `EventRef` with public fields `stream_id`, `seq`,
`stable_event_id`, `payload_sha256`, carried by `Envelope::event_ref()` and
`AppendReceipt::event_ref()`. The encoded-envelope digest that tail witnesses
compare stays distinct from `payload_sha256`; the two are not interchangeable.
The source-API break is deliberate; the persisted wire format
(`WireEnvelope`, `ENVELOPE_FORMAT_VERSION`) is unchanged and no persisted
migration occurs.

## Why

A strict read's answer is fully determined by append-only allocation: the
demanded seq or window either has verified bytes or has a typed reason not to.
There is no EOF to bound and no suffix to certify, so the LIST probe, the
tail-hint witness, and accelerator maintenance that `read` carries are not
merely wasted — their failure modes (`BackendUnqualified`, `complete: false`)
would be false answers to a question the caller did not ask. A distinct typed
surface keeps P-000003's live replay intact and makes each read's request
shape provable from the loopback request log. The scope stays consumer-agnostic
on D-000002's boundary: opaque ids, opaque payloads, no delivery or consumer
policy; an `EventRef` identifies a record for revalidation — it is neither
authorization nor whole-prefix proof. Verified construction is the envelope's
contract (only `encode` and `decode_and_verify` build one); public mutable
fields let any caller mutate a previously verified value, voiding that
contract at every callsite, and privacy makes the compiler enforce it.
Retaining the verified `payload_sha256` preserves the value the read path
actually checked without paying SHA-256 on every access. Recomputation from
verified immutable contents is cryptographically sound; its defects are the
repeated wasted work and, while fields are mutable, ambiguity about which
contents a recomputed digest names. Keeping the encoded-envelope digest
separate prevents a payload digest from masquerading as a tail-witness record
digest.

## Rejected alternatives

- Parameterizing `Streams::read` with a `through_seq`: conflates live-EOF
  completeness with strict-window satisfaction; the LIST/hint machinery still
  runs or needs branch suppression, and it perturbs an assured surface
  (P-000003) without supersession.
- Serving strict reads through cursors (`advance_cursor`/`read_cursor`):
  cursors are consumer-state writes under a monotonicity policy; a strict read
  is a pure read at an arbitrary window.
- Returning `Replay` from the new reads: `Replay::Empty` and `complete` encode
  live-replay meaning that is undefined or false for a demanded window, and
  its limit-0 answer is a backend-qualification failure — wrong for caller
  argument validation, which is an `InvalidArgument`.
- Keeping public fields with a documented convention: a convention is not
  enforcement.
- Recomputing the payload digest inside the accessor: sound against verified
  immutable contents, but it repeats the work on every call and stays
  ambiguous while fields are mutable — no behavioral gain.

## Consequences

Every `Envelope` field consumer migrates to getters — a deliberate source
break with no compat shim; persisted objects are untouched. `read_event` and
`read_range` add GET-only traffic plus permitted retention-control lookups;
P-000003 live replay, multiwriter append, and idempotency-window semantics
are unchanged by this slice. Retention between pages of one walk is not
pinned: a trim landing between pages surfaces as the typed `OffsetExpired`
boundary on the next page. Expected append and retry-safe stream creation are
excluded here and will receive separate records. Contract evidence lands as
P-000006/O-000006 under I-000002's scoped-batch rule; implementation rides
the kernel keyspace only, respecting I-000001.

## Revisit when

P-000003 is superseded, or a shared witness model serves both live and strict
reads without weakening either; a qualified backend capability (cf.
`situation/candidates/C-000001-part-addressed-streaming-read.md`) changes what
bounded reads must cost; or a superseding retention model removes the
certified-floor assumption.
