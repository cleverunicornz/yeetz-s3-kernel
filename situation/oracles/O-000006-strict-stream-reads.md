# O-000006 — Strict stream-read contracts

## State

designed

## Judges

`situation/promises/P-000006-strict-stream-reads.md`

## Inputs

The r-suite tests in `crates/yeetz-s3-streams/tests/streams_read.rs` (a new
file whose identifiers are predeclared below; it does not exist at this
record's creation), run against the existing in-memory harness
(`crates/yeetz-s3-streams/tests/support/mod.rs`: `streams_on_in_memory_store`,
`hand_envelope`, `streams_keyspace`) and the loopback S3 counterpart
(`crates/yeetz-s3-streams/tests/support/loopback.rs`: `Loopback::start`,
`request_log`, freeze/hide/fault controls), with
`cargo nextest run --workspace` in the `gates` task of the current
`.github/workflows/ci-dev.yml` on the candidate head as the execution route;
the future witness retains the exact executed source and workflow identities,
and no historical workflow or source is claimed to have run tests that did
not yet exist. A changed input set makes a historical execution INVALID for
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
- P3: `r3_read_event_typed_boundaries` establishes each boundary outcome and
  its distinctness: invalid id → `InvalidArgument` with zero storage requests;
  absent genesis → `StreamNotFound`; below-floor target → `OffsetExpired`;
  absent target at or above the floor → `EventMissing`; seeded malformed or
  key-mismatched object → `Corrupt`; cut target GET → the store-failure
  error; cut retention lookup → an error, never a silent success.
- P4: `r4_read_range_argument_preflight_is_side_effect_free` establishes that
  `limit == 0` and `after_seq >= through_seq` are refused before any storage
  request.
- P5: `r5_read_range_complete_window_or_typed_error` establishes that a dense
  window is served complete and contiguous in order, that an interior hole
  yields a typed error naming the missing seq rather than a partial success,
  that a limit-cut page serves exactly its subwindow with `reached_end ==
  false`, and that a page ending exactly at `through_seq` reports
  `reached_end == true`.
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
- F3: a boundary input returns an outcome of the wrong type or `Ok` — an
  expired, missing, corrupt, or store-failure condition answered by another
  variant, or a failed retention lookup served as success (r3).
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

## Implementation coverage

All legs are manual at design time; each row names the intended executable
that will decide it once `crates/yeetz-s3-streams/tests/streams_read.rs`
lands and the oracle moves to `implemented`.

| Leg | Decision | Coverage |
|---|---|---|
| P1 | read_event serves verified envelopes incl. seq 0 under a floor | manual (intended: r1 via `cargo nextest run --workspace`) |
| P2 | read_event request shape is exact | manual (intended: r2 via `cargo nextest run --workspace`) |
| P3 | read_event boundaries are typed and distinct | manual (intended: r3 via `cargo nextest run --workspace`) |
| P4 | read_range argument preflight is effect-free | manual (intended: r4 via `cargo nextest run --workspace`) |
| P5 | read_range serves the window or a typed error | manual (intended: r5 via `cargo nextest run --workspace`) |
| P6 | reached_end is a window boundary, not EOF | manual (intended: r6 via `cargo nextest run --workspace`) |
| P7 | resume walks exactly; u64::MAX without overflow | manual (intended: r7 via `cargo nextest run --workspace`) |
| P8 | read_range request shape: no log LIST, tail, or writes | manual (intended: r8 via `cargo nextest run --workspace`) |
| P9 | envelope surface, digests, EventRef agree | manual (intended: r9 via `cargo nextest run --workspace`; privacy leg manual by source inspection) |
| F1 | preflight detects a premature effect | manual (intended: r4/r3 via `cargo nextest run --workspace`) |
| F2 | request log detects an out-of-shape request | manual (intended: r2/r8 via `cargo nextest run --workspace`) |
| F3 | boundaries detect a mistyped outcome | manual (intended: r3 via `cargo nextest run --workspace`) |
| F4 | window detects missing-history success | manual (intended: r5 via `cargo nextest run --workspace`) |
| F5 | reached_end detects a wrong boundary claim | manual (intended: r5/r6 via `cargo nextest run --workspace`) |
| F6 | pagination detects gap/duplicate; MAX detects overflow | manual (intended: r7 via `cargo nextest run --workspace`) |
| F7 | digest/ref legs detect interchange or loss | manual (intended: r9 via `cargo nextest run --workspace`) |
