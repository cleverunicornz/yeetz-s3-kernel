# O-000006 — Strict stream-read contracts

## State

implemented

## Judges

`situation/promises/P-000006-strict-stream-reads.md`

## Inputs

The r-suite tests in `crates/yeetz-s3-streams/tests/streams_read.rs`
(r1–r10), run against the existing in-memory harness
(`crates/yeetz-s3-streams/tests/support/mod.rs`: `streams_on_in_memory_store`,
`hand_envelope`, `streams_keyspace`) and the loopback S3 counterpart
(`crates/yeetz-s3-streams/tests/support/loopback.rs`: `Loopback::start`,
`request_log`, freeze/hide/fault controls), with
`cargo nextest run --workspace --no-fail-fast` in the `gates` task of the
`.github/workflows/ci-dev.yml` on the dispatched candidate head as the
execution route; the witness retains the exact executed source and workflow
identities. A changed input set makes a historical execution INVALID for
this oracle rather than a judgment about changed bytes.

## Pass

- P1: `r1_read_event_returns_verified_envelope` establishes that `read_event`
  returns accessor-complete verified envelopes for appended seqs and for
  seq 0 under a certified floor above 1 — the genesis is served without a
  trim-floor boundary.
- P2: `r2_read_event_request_shape_is_exact` establishes, from
  `Loopback::request_log`, that `read_event` issues no log-event GET other
  than the genesis and the target, no tail-hint read, and no log LIST; the
  trim-certificate LIST a retention-control lookup requires is expressly
  permitted, and no other LIST is issued.
- P3: `r3_read_event_typed_boundaries` and
  `r10_corrupt_outer_kernel_envelope_names_seq_on_both_strict_reads`
  establish each boundary outcome and its distinctness: invalid id →
  `InvalidArgument` with zero storage requests; absent genesis →
  `StreamNotFound`; below-floor target → `OffsetExpired`; absent target at or
  above the floor → `EventMissing`; a seeded malformed or key-mismatched
  inner object, and outer kernel storage corruption seeded by raw byte
  replacement over the wire (a corrupted genesis included), → `Corrupt`
  naming the seq; cut target GET → the store-failure error; cut retention
  lookup → an error, never a silent success.
- P4: `r4_read_range_argument_preflight_is_side_effect_free` establishes that
  `limit == 0` and `after_seq >= through_seq` are refused before any storage
  request.
- P5: `r5_read_range_complete_window_or_typed_error`, with the range leg of
  `r10_corrupt_outer_kernel_envelope_names_seq_on_both_strict_reads`,
  establishes that a dense window is served complete and contiguous in
  order, that an interior hole yields a typed error naming the missing seq
  rather than a partial success, that outer kernel storage corruption is
  `Corrupt` naming the damaged seq, that a limit-cut page serves exactly its
  subwindow with `reached_end == false`, and that a page ending exactly at
  `through_seq` reports `reached_end == true`.
- P6: `r6_read_range_reached_end_is_window_not_eof` establishes that
  `reached_end` tracks the window boundary, not the live tail: true at
  `through_seq` with events existing beyond it, false on a limit cut before
  it.
- P7: `r7_read_range_resume_and_seq_max` establishes that resuming with
  `after_seq` set to the last returned seq walks the whole window without gap
  or duplication, and that a window ending at `u64::MAX` is served without
  overflow.
- P8: `r8_read_range_request_shape_no_log_list_no_tail_no_writes`
  establishes, from `Loopback::request_log` across a multi-page walk, that
  `read_range` issues no log LIST, no tail-hint read, no write of any kind,
  and no event GET beyond the current demanded page; the trim-certificate
  LIST a retention-control lookup requires is expressly permitted; the
  per-page request set stays bounded by the demanded subwindow and the page
  limit.
- P9: `r9_envelope_immutable_surface_and_event_ref` establishes that a
  `hand_envelope`-encoded object (the independent wire encoder) still decodes
  and verifies through the strict read path; that `payload_sha256()` equals
  the SHA-256 of `payload()`, equals the digest persisted in the object, and
  is stable across calls; that the tail-witness encoded-envelope digest
  remains a different value; that `EventRef` round-trips through
  serialization with exactly `stream_id`, `seq`, `stable_event_id`,
  `payload_sha256`; and that `AppendReceipt::event_ref()` equals the landed
  envelope's `event_ref()`. Field privacy itself is compiler-enforced and
  decided by source inspection, not by a runtime leg.

## Fail

- F1: an argument-invalid input produces any storage request, or an
  argument-valid input is refused as an argument error (r4; r3 invalid-id
  leg).
- F2: any strict read issues an extra event GET, a tail-hint read, a log
  LIST, or any write (r2, r8).
- F3: a boundary or corruption input returns an outcome of the wrong type or
  `Ok` — an expired, missing, corrupt, or store-failure condition answered by
  another variant (a stored-integrity failure accusing the caller as
  `InvalidArgument` included), or a failed retention lookup served as
  success (r3, r10).
- F4: an interior hole yields a successful page with missing history, or a
  dense window yields a non-contiguous or short page without a typed error
  (r5).
- F5: `reached_end` is true with the last returned seq not equal to
  `through_seq`, or false with it equal (r5, r6).
- F6: pagination across a resume loses or duplicates an event, or a
  `u64::MAX` window overflows or panics (r7).
- F7: `payload_sha256()` disagrees with the payload digest or the persisted
  digest; the tail-witness digest equals or stands in for `payload_sha256`;
  a wire-format-preserved object fails verification; `EventRef` loses or
  renames a field in round-trip; or `AppendReceipt::event_ref()` disagrees
  with the envelope's (r9).

## Implementation

`crates/yeetz-s3-streams/tests/streams_read.rs` carries the r-suite (r1–r10)
against the in-memory and loopback harnesses; the `gates` task of the current
`.github/workflows/ci-dev.yml` executes
`cargo nextest run --workspace --no-fail-fast` (identical suite and
criteria; all outcomes collected even when a test fails) on
the dispatched ref. The strict surfaces under judgment live in
`crates/yeetz-s3-streams/src/bounded.rs` (`read_event`, `read_range`,
`RangePage`) and `crates/yeetz-s3-streams/src/envelope.rs`. The field-privacy
clause of P9 is decided by source inspection: every `Envelope` field is
private, the getters are public, and `encode`/`decode_and_verify` are
crate-private constructors; it remains the one manual leg. This section

## Implementation coverage

Every leg except the P9 field-privacy clause is decided by the named r-suite
tests under `cargo nextest run --workspace --no-fail-fast` in the `gates`
task of the current `.github/workflows/ci-dev.yml`. Coverage names the
deciding executable only; outcomes are recorded by witnesses, not here.

| Leg | Decision | Coverage |
|---|---|---|
| P1 | r1 serves verified envelopes incl. seq 0 under a floor | `cargo nextest run --workspace --no-fail-fast` |
| P2 | r2 request shape is exact (read_event) | `cargo nextest run --workspace --no-fail-fast` |
| P3 | r3/r10 boundaries and stored-integrity corruption are typed and distinct | `cargo nextest run --workspace --no-fail-fast` |
| P4 | r4 argument preflight is effect-free | `cargo nextest run --workspace --no-fail-fast` |
| P5 | r5/r10 window served complete or typed error | `cargo nextest run --workspace --no-fail-fast` |
| P6 | r6 reached_end is a window boundary, not EOF | `cargo nextest run --workspace --no-fail-fast` |
| P7 | r7 resume walks exactly; u64::MAX without overflow | `cargo nextest run --workspace --no-fail-fast` |
| P8 | r8 request shape: no log LIST, tail, or writes | `cargo nextest run --workspace --no-fail-fast` |
| P9 | r9 envelope surface, digests, EventRef agree | `cargo nextest run --workspace --no-fail-fast`; field privacy manual (source inspection of `crates/yeetz-s3-streams/src/envelope.rs`) |
| F1 | r4/r3 detect a premature effect | `cargo nextest run --workspace --no-fail-fast` |
| F2 | r2/r8 detect an out-of-shape request | `cargo nextest run --workspace --no-fail-fast` |
| F3 | r3/r10 detect a mistyped outcome | `cargo nextest run --workspace --no-fail-fast` |
| F4 | r5 detects missing-history success | `cargo nextest run --workspace --no-fail-fast` |
| F5 | r5/r6 detect a wrong boundary claim | `cargo nextest run --workspace --no-fail-fast` |
| F6 | r7 detects gap/duplicate; MAX detects overflow | `cargo nextest run --workspace --no-fail-fast` |
| F7 | r9 detects digest/ref interchange or loss | `cargo nextest run --workspace --no-fail-fast` |
