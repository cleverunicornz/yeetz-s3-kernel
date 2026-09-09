# P-000006 — Strict stream reads

## State

implementing

## Promise

`yeetz-s3-streams` provides strict historical reads as a GET-only surface
alongside live replay:

1. `Streams::read_event(&StreamId, Seq) -> Result<Envelope, StreamsError>`
   validates caller arguments before any storage request; on a valid call it
   verifies the genesis and returns the verified `Envelope` at the demanded
   seq. Seq 0 returns the verified genesis without a trim-floor lookup. A
   nonzero target below the certified trim floor returns `OffsetExpired`; an
   absent target at or above the floor returns `EventMissing`; a malformed or
   key-mismatched object returns `Corrupt`; a store failure returns a distinct
   error rather than any boundary outcome. The call issues no event-object GET
   other than the genesis and the target, reads no tail-hint object, and
   issues no log LIST. Retention-control lookups are permitted, and their
   failure is an error.
2. `Streams::read_range(&StreamId, after_seq, through_seq, limit) ->
   Result<RangePage, StreamsError>` serves the strict window
   `(after_seq, through_seq]` with `limit > 0` and `after_seq < through_seq`,
   both validated before any storage request. A successful page carries the
   complete contiguous demanded subwindow in `RangePage.events`, or the call
   returns a typed error — never a partial successful page with missing
   history. `RangePage.reached_end` is true exactly when the last returned seq
   equals `through_seq`; it is a window boundary, never a live-EOF claim.
   Paging resumes with `after_seq` set to the last returned seq; a window
   ending at `u64::MAX` is served without overflow. The read issues no event
   GET beyond the current demanded page, no log LIST, no tail-hint read, no
   read-path healing, and no write; the per-page event GET set and fetch
   parallelism stay bounded. Genesis verification and trim-control lookups are
   permitted; an unavailable trim lookup fails closed. Retention may change
   between pages; no snapshot or retention pin is promised.
3. The exported `Envelope` is immutable through getters —
   `format_version() -> u32`, `stream_id() -> &StreamId`, `seq() -> Seq`,
   `stable_event_id() -> &StableEventId`, `schema_id() -> &SchemaId`,
   `payload() -> &bytes::Bytes`, `payload_sha256() -> &str`,
   `event_ref() -> &EventRef` — where `payload_sha256()` returns the digest
   verified on decode, retained on the value rather than recomputed, and the
   encoded-envelope digest used by tail witnesses remains a distinct value no
   `payload_sha256` accessor substitutes for. `EventRef` is a serializable
   value with public fields `stream_id`, `seq`, `stable_event_id`,
   `payload_sha256`; `Envelope::event_ref()` and `AppendReceipt::event_ref()`
   return the reference identifying the record. A reference identifies a
   record for revalidation; it is neither authorization nor whole-prefix
   proof. The persisted envelope wire format is unchanged, no persisted
   migration occurs, and the source-API break is deliberate.

## Scope

`Streams::read_event`, `Streams::read_range`, `RangePage`, `EventRef`, the
exported `Envelope` accessor surface, `AppendReceipt::event_ref`, and the
storage-request shape of these reads in `crates/yeetz-s3-streams`. Excludes
live replay (`Streams::read`, `Replay`) semantics, which remain P-000003's;
append, idempotency-window, and retry-safe creation semantics; cross-page
retention stability; and any authorization, delivery, or whole-prefix-proof
meaning for `EventRef`.

## Oracle

`situation/oracles/O-000006-strict-stream-reads.md`

## State evidence

- `situation/decisions/D-000006-strict-stream-reads.md` — accepted design
  authorizing this slice as its scoped batch; source implementation is the
  next unit of work on this pull request.
- `situation/oracles/O-000006-strict-stream-reads.md` — designed judgment
  rule with predeclared legs.
- No implementation commit or witness exists yet; the transition to
  `implemented` will cite the implementation commit, and `assured` will cite
  a passing witness.

## Residual

- No retention snapshot or pin across the pages of one logical walk: a trim
  landing between pages surfaces as `OffsetExpired` on the next page; the
  caller owns that recovery.
- Verification is per-envelope: a served window is verified event by event and
  is not a chain or whole-prefix proof; `EventRef` authorizes nothing.
- `reached_end` deliberately claims nothing about events beyond `through_seq`.
- Assurance is pending (state `implementing`): no witness claims any leg has
  passed.

## References

- `situation/decisions/D-000006-strict-stream-reads.md`
