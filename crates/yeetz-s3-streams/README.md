# yeetz-s3-streams

[![Crates.io](https://img.shields.io/crates/v/yeetz-s3-streams.svg)](https://crates.io/crates/yeetz-s3-streams)
[![Docs.rs](https://docs.rs/yeetz-s3-streams/badge.svg)](https://docs.rs/yeetz-s3-streams)
[![CI](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/workflows/ci.yml/badge.svg)](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/cleverunicornz/yeetz-s3-kernel/blob/main/LICENSE)

**Append-only event logs on S3, without a broker.**
[`yeetz-s3-streams`] implements durable event logs on top of the
[`yeetz-s3-kernel`] `AtomicKeyspace` ([D-000002]): one immutable
object per event at `streams/v1/<id>/log/<seq>`, where the object's
*conditional create* **is** the sequence allocation — no counters, no
coordination service, no leader. Concurrent writers race the create;
one lands, losers advance `+1` and retry.

Design boundaries, on purpose:

- **Forge-agnostic.** Opaque `StreamId`s, opaque payloads, no
  application types anywhere.
- **Pull-only.** No delivery bus, no push, no scheduling — consumers
  replay and advance cursors.
- **Damage is loud, named, and per-seq.** Every read verifies
  key↔envelope agreement and payload digests; decode failure is an
  error, never a skip.

[`yeetz-s3-streams`]: https://docs.rs/yeetz-s3-streams/latest/yeetz_s3_streams/
[`yeetz-s3-kernel`]: https://crates.io/crates/yeetz-s3-kernel
[D-000002]: https://github.com/cleverunicornz/yeetz-s3-kernel/blob/main/situation/decisions/D-000002-append-only-streams.md
[D-000006]: https://github.com/cleverunicornz/yeetz-s3-kernel/blob/main/situation/decisions/D-000006-strict-stream-reads.md
[P-000006]: https://github.com/cleverunicornz/yeetz-s3-kernel/blob/main/situation/promises/P-000006-strict-stream-reads.md

## Example

```rust
use yeetz_s3_streams::{
    AppendExpectedError, CreateStreamError, Replay, SchemaId, StableEventId, StreamId, Streams,
};
use yeetz_s3_kernel::KernelHandle;

# async fn run(config: yeetz_s3_kernel::S3Config) -> Result<(), Box<dyn std::error::Error>> {
let handle = KernelHandle::from_s3_config(&config)?;
let streams = Streams::new(&handle)?;

// Create: mints an opaque id and writes the immutable genesis record
// (seq 0). The conditional create defines existence.
let stream = streams.create_stream(br#"{"repo":"demo/hello"}"#).await?;

// Append: caller-chosen StableEventId dedupes retries within the
// bounded idempotency window. The receipt names the landed seq.
let receipt = streams
    .append(
        &stream,
        &SchemaId::new("issue.comment.v1")?,
        &StableEventId::new("c-42")?,
        br#"{"body":"hi"}"#,
    )
    .await?;

// Replay: after-exclusive, bounded page, typed outcome — never a
// blanket error. `complete` is witness-bounded and withheld, not
// guessed.
match streams.read(&stream, 0, 100).await {
    Replay::Page { events, complete } => {
        let _ = complete;
        for event in &events { /* … */ }
        // resume with after_seq = events.last().map(|e| e.seq())
    }
    Replay::Empty => {}
    other => { /* NotFound, Corrupt { .. }, OffsetExpired { .. }, … */ }
}

// Consumer position: a CAS'd, monotone-only cursor.
let cursor = streams
    .advance_cursor(&stream, "projector", receipt.seq)
    .await?;
# let _ = cursor;

// Caller-owned identity: you mint the id. Identical id + config
// bytes converge — Created on the first create, Existing on the
// lost-response retry; a different config under the same id is a
// typed conflict, never an overwrite.
let owned = StreamId::new("orders-eu-west-1")?;
match streams.create_stream_with_id(&owned, br#"{"shard":"eu"}"#).await {
    Ok(_) => { /* Created, or Existing: converged either way */ }
    Err(CreateStreamError::ConfigurationConflict { .. }) => {
        // same id, different config — the incumbent is untouched
    }
    Err(CreateStreamError::Storage(_)) => {
        // typed StreamsError underneath: retry only Unavailable, and
        // with the SAME id and bytes. InvalidArgument (an id that
        // never passed validation) and Corrupt (a malformed incumbent
        // genesis) are permanent — no retry converges them.
    }
}

// Expected append: name the predecessor (an EventRef from a receipt
// or a verified envelope). The event lands at predecessor.seq + 1 or
// fails typed — never at another slot, never filling a hole.
match streams
    .append_expected(
        &receipt.event_ref(),
        &SchemaId::new("issue.comment.v1")?,
        &StableEventId::new("c-43")?,
        br#"{"body":"next"}"#,
    )
    .await
{
    Ok(next) => { /* a receipt at exactly receipt.seq + 1 */ }
    Err(AppendExpectedError { kind, effect }) => {
        // kind: PredecessorMismatch | PositionConflict | HoleWitnessed
        //       | Expired | Storage(..) — why it failed
        // effect: NotAttempted | PossiblyCommitted | Committed(..)
        //       — what may have landed; on PossiblyCommitted, retry
        //         the identical call
    }
}

// Strict historical reads are read-only and serve exactly what was
// demanded: no event-log LIST probe, no tail-hint read, no write.
// (Permitted trim-certificate lookups may LIST certificate keys.)
let envelope = streams.read_event(&stream, receipt.seq).await?;
let digest: &str = envelope.payload_sha256();
# let _ = digest;

// One exact contiguous window: (after_seq, through_seq], never partial.
let page = streams.read_range(&stream, 0, receipt.seq, 100).await?;
for event in &page.events { /* the complete demanded subwindow, in order */ }
// page.reached_end: served through through_seq — a window boundary,
// not a live-EOF claim like Replay::Page.complete.
# Ok(())
# }
```

## Semantics worth knowing

- **Durability is the create.** There is no flush API — the S3
  conditional create is the linearization point. Retries converge via
  the stable event id (`IdempotencyConflict` is typed for same-id /
  different-content within the window; beyond it, at-least-once —
  consumers dedupe).
- **Caller-owned ids converge or conflict — never overwrite.**
  `create_stream_with_id(&id, &config)` writes the canonical genesis at
  seq 0 under *your* id: `Created` on the first write, `Existing` when
  a byte-identical genesis (same id and config) already landed — a
  lost create response retried with the same id and bytes converges.
  A valid *different* config under the same id is a typed
  `CreateStreamError::ConfigurationConflict`; the incumbent is never
  overwritten or adopted. Failures wrap the typed `StreamsError` as
  `CreateStreamError::Storage(..)`: retry the *same* id and bytes only
  for `Unavailable`; `InvalidArgument` (an id that never passed
  validation) and `Corrupt` (a malformed incumbent genesis) are
  permanent — no retry converges them. `Existing` is existence
  convergence, not a writer grant; ordinary `create_stream` keeps its
  minted-id semantics.
- **`append_expected` lands at the exact successor or fails typed.**
  Given the predecessor's `EventRef` (from a receipt or a verified
  envelope), the event lands at `predecessor.seq + 1` and nowhere
  else: it never advances to another slot on conflict and never fills
  a hole (`HoleWitnessed` when a verified later event proves the
  target seq is a gap). Exact retry — same predecessor, stable id,
  schema, and payload — converges to the original receipt, including
  after later appends. Same stable id with different bytes or schema
  at the target is `Storage(IdempotencyConflict)`; a different
  verified event already at the target is `PositionConflict` naming
  the incumbent; a predecessor that does not verify is
  `PredecessorMismatch`, absent (`Storage(EventMissing)`), or corrupt
  (`Storage(Corrupt)`) — adjudicated before any write.
- **Every `append_expected` failure carries its effect.**
  `AppendExpectedError { kind, effect }`: `kind` is the typed failure;
  `effect` records what may have landed — `NotAttempted` (nothing was
  written), `PossiblyCommitted` (a create was invoked and its outcome
  is unconfirmed; retry the identical call), or `Committed(receipt)`
  (confirmed durable, e.g. an expiry observed after a confirmed
  write). Once an attempt is made, failures never downgrade to
  `NotAttempted`, and a conflicting or corrupt readback preserves the
  uncertainty it must.
- **`Ok` is exact-successor confirmation, not a retention pin.**
  `Ok(receipt)` confirms the canonical envelope at the exact successor
  under a later successful qualified floor observation. It is not a
  future-retention guarantee, an ownership grant, or an atomic
  append+trim fence. Retention residuals are typed and qualified: a
  floor that has reached the target surfaces as
  `Expired { subject: Target, .. }` carrying the preserved effect
  (`Committed` or `PossiblyCommitted`); a collected predecessor is
  `Expired { subject: Predecessor, .. }` — except the genesis (seq 0),
  which is immortal and always a valid predecessor. Bytes can land
  below a concurrently advanced floor and be collected by later GC,
  and a silently stale certificate LIST can miss a new floor — the
  qualified-backend limitation already named for live-replay
  `complete`. A swept target cannot reconstruct an earlier caller's
  lost receipt or uncertainty; the caller retains its prior result.
- **The conditional surface adds no persisted state.**
  `create_stream_with_id` and `append_expected` persist only the
  ordinary genesis and per-event objects: no tail-hint writes, no new
  object kinds, no persisted format change. The wire format is
  unchanged and no persisted migration occurs; the only API break
  remains the deliberate `Envelope` source break documented below.
- **Cursors are monotone pointers.** `advance_cursor` validates the
  target event exists and moves by CAS; it never rewinds.
- **Retention is certified trim.** `trim` writes an immutable
  create-once certificate bounding the retained prefix; reads below
  the floor are a typed `OffsetExpired` — never `Corrupt`, never
  `Empty` — and `gc` is an idempotent, resumable sweeper that deletes
  only below the certificate. The genesis record is immortal.
- **`read` returns a `Replay`, not a `Result`.** Seven states —
  `NotFound`, `Empty`, `Page { events, complete }`, `Corrupt
  { missing_or_mismatched }`, `OffsetExpired { first_retained }`,
  `Unavailable`, `BackendUnqualified` — because in a distributed log
  the outcome *is* the information. `complete = true` requires a
  verified tail witness plus an empty ordered probe past it;
  otherwise completeness is withheld rather than guessed.
- **Strict historical reads are read-only and exact.** `read_event(stream, seq)`
  returns the one verified `Envelope` at a demanded seq;
  `read_range(stream, after_seq, through_seq, limit)` returns
  `RangePage.events` carrying the complete contiguous demanded subwindow
  `(after_seq, through_seq]` — or a typed error, never a partial page
  with silent holes ([D-000006], [P-000006]). Arguments are validated as
  `InvalidArgument` before any storage request. Both surfaces are
  read-only: they verify the genesis and read only the demanded target or
  page window plus permitted retention controls — no event-log LIST, no
  tail-hint read, no write of any kind. The trim-floor lookup lists
  trim-certificate keys and is an allowed retention control; an
  unavailable retention lookup fails closed as an error. Seq 0 serves the
  verified
  genesis without a trim-floor lookup; a nonzero seq below the certified
  floor is `OffsetExpired`, an absent seq at or above it is
  `EventMissing`, and malformed or key-mismatched bytes are `Corrupt`.
- **`reached_end` is a window boundary, not EOF.** `RangePage.reached_end`
  is true exactly when the last returned seq equals `through_seq`; it says
  nothing about events beyond the window. That is deliberately different
  from `Replay::Page.complete`, a witness-bounded live-EOF claim about the
  log's tail. Resume a walk with `after_seq` set to the last returned seq;
  a window ending at `u64::MAX` is served without overflow. Retention is
  not pinned across pages: a trim landing mid-walk surfaces as
  `OffsetExpired` on the next page.
- **`Envelope` is immutable — a deliberate source break.** Its fields are
  private behind getters — `format_version()`, `stream_id()`, `seq()`,
  `stable_event_id()`, `schema_id()`, `payload()`, `payload_sha256()`,
  `event_ref()` — so a value that passed verification cannot be mutated
  after the fact. `payload_sha256()` returns the digest the read path
  verified, retained on the value rather than recomputed; the
  encoded-envelope digest that tail witnesses carry is a distinct value no
  `payload_sha256` substitutes for. The break is source-only: the
  persisted wire format is unchanged and no persisted migration occurs —
  existing objects read as they are.
- **`EventRef` names a record for revalidation.** `Envelope::event_ref()`
  and `AppendReceipt::event_ref()` return a serializable reference with
  public fields `stream_id`, `seq`, `stable_event_id`, `payload_sha256`.
  A reference identifies one record; it is neither authorization nor
  whole-prefix proof.
- **Migration is a first-class surface.** `migration::migrate_log`
  copies a verified history into dense seqs 1..n and seals it with an
  immutable `MigrationSeal` (source lineage, digests, event count).

## Assurance

The S contract (S1–S11: contiguity, one-winner-per-seq, replay order,
typed damage, envelope bounds), the trim contract (R2/R6/R7/R8 —
including *trim-to-end is logically empty with a floor*), and the G130
completeness regression are all named tests against an in-memory
kernel, a fault-injecting loopback S3 wire counterpart
(`freeze_list` / `hide_key` / `arm_fault`), and durable CI rigs
against real backends.

The strict historical-read surface — `read_event`, `read_range`, the
immutable `Envelope`, `EventRef` ([D-000006], [P-000006]) — is not
included in the assurance above; no witness claims it yet. The blanket
claim here covers the live append, replay, trim, and migration surfaces
named above only.

The conditional-write surface — `create_stream_with_id`,
`append_expected`, and their outcomes, errors, and effects — is
likewise not included; no witness claims it yet. Its fault and
retention races (paused PUTs, trims racing appends, stale LIST
certificates) belong to the loopback tests, not to the durable rig.

MSRV: Rust 1.96 (pinned by the workspace `rust-toolchain.toml`).

## The closure

| Crate | Role |
| --- | --- |
| [`yeetz-s3-kernel`](https://crates.io/crates/yeetz-s3-kernel) | lineages + atomic keyspace |
| [`yeetz-s3-streams`](https://crates.io/crates/yeetz-s3-streams) | this crate — append-only event logs |
| [`yeetz-sdk-s3`](https://crates.io/crates/yeetz-sdk-s3) | request-scoped S3 client mechanics |
| [`yeetz-sdk-core`](https://crates.io/crates/yeetz-sdk-core) | provider-neutral HTTP foundation |

## License

MIT.
