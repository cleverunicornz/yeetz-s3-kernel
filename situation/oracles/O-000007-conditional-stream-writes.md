# O-000007 — Conditional stream-write contracts

## State

implemented

## Judges

`situation/promises/P-000007-conditional-stream-writes.md`

## Inputs

The c-suite (creation) and e-suite (expected append) tests in
`crates/yeetz-s3-streams/tests/streams_conditional.rs` (`c1`–`c12`,
`e1`–`e36`, name-aligned below), run against the existing in-memory
harness (`crates/yeetz-s3-streams/tests/support/mod.rs`:
`streams_on_in_memory_store`, `streams_on_store`, `hand_envelope`,
`streams_keyspace`) and the loopback S3 counterpart
(`crates/yeetz-s3-streams/tests/support/loopback.rs`: `Loopback::start`,
`request_log`, `pause_next` with `StorageOp`/`FaultPhase`,
`RequestPause::wait_reached`/`release`, freeze/hide/fault controls, and
the per-key stale-LIST pair `omit_from_list`/`restore_to_list`), with
`cargo nextest run --workspace --no-fail-fast` inside the `gates` task
of the current `.github/workflows/ci-dev.yml` on the candidate head as
the execution route — `--no-fail-fast` so a failing case is judged
without cancelling the remaining cases' judgments. The deterministic
race legs arm pauses sequentially and time out their waits so an
unexpected request shape fails rather than hangs; no
test orders a race with a sleep. A future witness retains the exact
executed source and workflow identities, and no workflow or source is
claimed to have run tests before they existed. A changed input set makes
a historical execution INVALID for this oracle rather than a judgment
about changed bytes. The `kernel-rigs` route of the same workflow
separately proves standalone keyspace behavior; it is not an input to
this oracle, and its runs are not witnesses for these legs.

Request-trace attribution: every write-shape judgment below examines
only writes attributable to the conditional call itself — the
mark-partitioned `Loopback::request_log()` window the call's own
operations produce. Writes the test fixture deliberately injects into
the same trace (trim-certificate proposals at the reserved `trims`
scope, GC/bulk deletes, seeded or damage-overwriting CAS including raw
object replacement for outer-corruption legs, fixture-driven
cursor/tail operations, omitted/restored LIST state) are excluded from
the product-write judgment. The suite source is complete and landed;
this record claims the existence of the executable legs, not any
outcome — only a witness claims an outcome.

## Pass

Creation:

- P1: `c1_fresh_create_writes_only_the_genesis_identity` establishes
  that a fresh caller id returns `Created`, lands exactly one genesis
  object at seq 0 whose bytes are the canonical genesis, and leaves the
  stream verifiable through the existing read path.
- P2: `c2_identical_retry_returns_existing_and_retains_config`
  establishes that retrying the same id and config returns `Existing`
  with no second creation effect: the incumbent's bytes are unchanged
  and no second genesis object exists. A rejected conditional create
  against the occupied slot is the selected algorithm's expected step,
  not an effect — the retry is judged on creation outcomes, not on raw
  write counts.
- P3: `c5_concurrent_same_id_yields_one_created_one_existing`
  establishes that concurrent creations under one id converge on one
  genesis identity — exactly one `Created`, the other `Existing` — with
  no second genesis object and no overwrite, regardless of
  interleaving order.
- P4: `c6_lost_create_response_then_same_id_retry_returns_existing`
  establishes that an applied-then-lost seq-0 create response (After
  fault) surfaces as a storage error and that the same-id, same-bytes
  retry returns `Existing` without writing a second identity.
- P5: `c3_different_config_conflicts_and_never_overwrites`,
  `c4_corrupt_incumbent_is_typed_storage_corrupt`,
  `c10_absent_incumbent_after_conflict_is_backend_unqualified`, and
  `c11_outer_genesis_corruption_maps_to_corrupt` establish the
  incumbent-conflict family: a valid different genesis is
  `ConfigurationConflict` with incumbent bytes unchanged; an incumbent
  failing envelope verification is `Storage(Corrupt)` naming seq 0 —
  never adopted, never overwritten — and so is corruption of the OUTER
  kernel stored bytes of the genesis (raw object replacement), which
  maps through the shared error mapper to `Storage(Corrupt)`, never
  `InvalidArgument`; a losing conditional create whose incumbent
  readback comes back absent (hidden from GET while the losing PUT
  parks after its effect) contradicts the conflict and is
  `Storage(BackendUnqualified)`, with the incumbent untouched.
- P6: `c7_invalid_stream_id_rejected_before_any_io`,
  `c8_oversized_genesis_rejected_before_any_io`,
  `c9_kernel_reserved_scope_id_rejected_before_any_io`, and
  `c12_hierarchical_ids_are_valid_through_create_append_and_reads`
  establish the admission boundary: an invalid id — invalid under the
  existing grammar, where slash-joined components each satisfying the
  segment rule are VALID, so invalid means a malformed component, a
  leading or trailing slash, or an empty component — including one
  bypassing the constructor by deserialization, an oversized encoded
  genesis, and a kernel-reserved logical key (a stream id naming the
  `trims` certificate scope) are typed errors issued before any
  storage request, with zero recorded requests; the reserved case is
  specifically `InvalidArgument` — permanent, mapped from the kernel
  guard rather than name-listed — and distinct from a cut-store
  `Unavailable`; and hierarchical ids for the stream, the schema, and
  the stable event id alike are accepted through creation, expected
  append, exact retry, and verified reads, carrying hierarchy into the
  physical key layout.

Expected append:

- P7: `e1_exact_successor_from_genesis` establishes that a fresh append
  lands at exactly `predecessor.seq + 1` and returns the receipt
  carrying the landed envelope's identity.
- P8: `e2_exact_retry_after_suffix_advance_writes_nothing` and
  `e3_exact_retry_after_predecessor_trimmed_target_retained` establish
  exact-retry convergence: after later appends, the identical retry
  returns the same receipt and the reconciliation writes nothing — no
  retry PUT, no tail hint, no write at any position; after the
  predecessor is collected while the target remains retained (floor ==
  target), the retry reconciles on the target's canonical bytes alone
  without reading the predecessor.
- P9: `e4_same_id_payload_conflict_is_not_attempted`,
  `e5_same_id_schema_conflict_is_not_attempted`,
  `e6_different_occupant_position_conflict_no_interleave`,
  `e24_malformed_target_occupant_is_corrupt_before_attempt`, and
  `e32_outer_target_corruption_maps_to_corrupt` establish
  occupied-target adjudication: the same stable id with different
  payload, or identical payload under a different schema, is
  `IdempotencyConflict`; a different verified occupant at the target is
  `PositionConflict` naming the occupant; a target occupied by bytes
  failing envelope verification, or by corrupted OUTER kernel stored
  bytes, is `Corrupt` naming the target seq — never `InvalidArgument` —
  each `NotAttempted`, nothing written at the target or any other
  position.
- P10: `e7_missing_predecessor_is_event_missing_not_attempted`,
  `e8_corrupt_predecessor_is_typed_corrupt`,
  `e9_mismatched_predecessor_names_expected_and_observed`, and
  `e33_outer_predecessor_corruption_maps_to_corrupt` establish
  predecessor adjudication: a missing predecessor (floor unchanged on
  the reread) is `EventMissing`, a predecessor failing verification —
  inner envelope or OUTER kernel stored bytes — is `Corrupt` naming the
  predecessor seq, never `InvalidArgument`, and a stable-id or
  digest-disagreeing reference is `PredecessorMismatch` carrying both
  the expected and observed references — each `NotAttempted`, each
  naming the predecessor.
- P11: `e10_verified_later_event_witnesses_hole_without_filling`,
  `e11_list_get_contradiction_fails_closed`, and
  `e26_malformed_later_witness_is_corrupt_without_fill` establish hole
  adjudication: an absent target with a GET-verified later record is
  `HoleWitnessed` naming the later record, with nothing written (the
  hole is never filled); a later record the LIST reports but the GET
  cannot fetch is `BackendUnqualified` — fail closed, not attempted; a
  listed later witness whose bytes fail verification is a `Corrupt`
  witness — the hole is not filled, nothing is written.
- P12: `e12_genesis_predecessor_at_floor_one_still_appends` establishes
  the genesis exemption: the immortal seq 0 means a floor of 1 at
  target 1 does not prevent a fresh expected append from the genesis
  predecessor.
- P13: `e13_predecessor_expiry_at_floor_is_typed`,
  `e14_target_expiry_at_floor_is_typed`,
  `e15_below_floor_zombie_committed_then_swept_target_cannot_recreate`,
  `e25_expired_target_overrides_zombie_conflict`, and
  `e31_missing_predecessor_raced_by_trim_gc_prioritizes_target_expiry`
  establish pre-attempt expiry: a nonzero predecessor below the
  certified floor (target retained) is `Expired` naming the Predecessor
  subject; an absent target below the floor is `Expired` naming the
  Target subject — both `NotAttempted`; an exact below-floor zombie
  (retained bytes under a floor past them, pre-GC) carries its receipt
  as `Expired(Target, Committed)`, and after the sweeper collects the
  target a later retry is `Expired(Target, NotAttempted)` — a swept
  target cannot recreate a receipt; a target holding a verified
  DIFFERENT event below a floor past it is judged by the floor first —
  `Expired(Target)`, not `PositionConflict` — and nothing is attempted;
  the same floor precedence holds over corruption:
  `e36_outer_corrupt_expired_target_is_expired_not_corrupt` establishes
  that a target whose outer kernel stored bytes are corrupted while a
  certified floor already extends past it is judged by the floor FIRST —
  `Expired(Target, NotAttempted)`, not `Corrupt`: the requested
  canonical bytes cannot be verified, so no `Committed` receipt is
  inferred, with no GC run and the corrupt object physically present
  (e32 keeps the retained-target leg); when a missing predecessor
  is raced by a concurrent landing of the target and trim/GC advances
  the floor past it, the floor reread prioritizes the target's own
  expiry — `Expired(Target)`, never a predecessor verdict.
- P14: `e16_seq_max_and_invalid_admission_perform_no_io` establishes
  that a predecessor at `u64::MAX` (`SeqExhausted`),
  non-canonical/invalid predecessor digests, deserialized-invalid ids,
  and an oversized encoded target (`EnvelopeTooLarge`) are rejected
  client-side with `NotAttempted` and zero storage requests.
- P15: `e17_refused_target_put_retains_possibly_committed`,
  `e18_lost_target_put_unavailable_readback_retains_possibly_committed`,
  `e27_lost_put_conflicting_readback_preserves_possibly_committed`,
  `e28_lost_put_corrupt_readback_preserves_possibly_committed`,
  `e34_lost_put_outer_corrupt_readback_preserves_possibly_committed`,
  and `e35_lost_put_with_exact_readback_returns_committed_receipt`
  establish the attempt boundary: a target PUT refused before its effect
  still reports `PossiblyCommitted` (no `Rejected` certainty is
  exposed); a PUT that applied but lost its response followed by an
  unavailable readback retains `PossiblyCommitted` — never downgraded to
  `NotAttempted`; the same lost PUT read back as a verified different
  event preserves the conflicting witness — `PositionConflict` with
  `PossiblyCommitted`; read back as bytes failing verification — inner
  envelope corruption (e28) or OUTER kernel stored bytes corrupted
  inside the parked window (e34) — preserves `Storage(Corrupt)` with
  `PossiblyCommitted`, surviving floor adjudication and never degrading
  to `Unavailable`; and the same lost PUT whose exact canonical readback
  remains available upgrades WITHIN the same call — `Ok` with the
  verified intended receipt under a valid final floor, the target
  durable exactly once and the byte-identical retry converging with no
  new PUT (e18 cuts the readback, e22 sweeps it, e35 is the upgrade
  leg); neither failing path lands a log write at any other position.
- P16: `e19_postwrite_floor_failure_preserves_committed_receipt`,
  `e20_pause_before_target_put_trim_to_target_retained_success`,
  `e21_pause_after_target_put_trim_beyond_and_gc_expired_committed`, and
  `e22_lost_target_put_gc_before_readback_expired_possibly_committed`
  establish post-attempt adjudication and the retention races under
  deterministic pauses: a failed post-write floor observation returns
  `Storage(Unavailable)` with effect `Committed(receipt)`, never a
  silent downgrade; parking the target PUT before its effect while a
  concurrent trim advances the floor exactly to the target (GC below
  it) leaves the released PUT a retained success — `Ok` is allowed when
  floor == target; parking after the applied PUT while trim advances
  beyond the target and GC collects it yields `Expired(Target,
  Committed)` — the commit is known; an applied-then-lost PUT whose
  floor passes the target and GC collects it before the readback yields
  `Expired(Target, PossiblyCommitted)` — the unknown commit stays
  unknown.
- P17: `e23_frozen_certificate_list_residual_is_honest` establishes the
  stale-floor residual honestly: a frozen certificate LIST hides the
  concurrent trim certificate from the post-write floor observation and
  the append is `Ok` — the explicit qualified-backend residual — while a
  later exact retry under a fresh LIST honestly reports
  `Expired(Target, Committed)` for the same bytes.
- P18: the suite-wide write-shape witness — the mark-partitioned
  `log_put_keys` assertions and the per-path no-PUT assertions
  (`e2`, `e10`, `e11`, `e27`) plus `e20`'s exact-one-log-PUT assertion —
  establishes, from `Loopback::request_log`, that every write
  attributable to `append_expected` targets the exact target log key
  (the method makes one logical keyspace create per attempt) and that no
  tail-hint key, no cursor key, and no key at any other position is
  written by the call. Writes deliberately injected by the test fixture
  into the same trace (trim-certificate proposals, GC/bulk deletes,
  seeded or damage CAS, raw object replacement) are excluded by the
  attribution rule in Inputs. Raw request multiplicity is not the
  criterion: the consumed kernel may retry its incarnation mechanics
  inside one logical create, and no SDK retry prohibition is judged
  here. The exact-target reconciliation path (P8) still issues zero
  writes.
- P19: `e29_floor_regression_before_attempt_is_backend_unqualified` and
  `e30_floor_regression_after_commit_preserves_committed_receipt`
  establish monotone-floor handling: a floor observation that regresses
  below an already observed floor (the higher certificate omitted from
  the LIST inside the parked predecessor-read window) is
  `BackendUnqualified` — the maximum observed floor is never forgotten —
  with the append never attempted; the same regression observed after an
  acknowledged create is `BackendUnqualified` carrying the current
  effect — the committed receipt is preserved, never downgraded.

## Fail

- F1: an admission-invalid input produces any storage request, an
  admission-valid input — including a hierarchical id — is refused as
  an admission error, or a kernel-reserved logical key is answered as
  a retryable `Unavailable` (P6, P14; c7, c8, c9, c12, e16).
- F2: any `append_expected` failure is returned without an effect, any
  post-attempt failure is downgraded to `NotAttempted` or `Unavailable`,
  or any definite-rejected certainty is surfaced — including a
  lost-PUT-with-exact-readback blindly returned `PossiblyCommitted`
  instead of upgrading (P15, P16, P19; e17, e18, e19, e20, e21, e22,
  e27, e28, e30, e34, e35).
- F3: an occupied position is mistyped: a different genesis not
  `ConfigurationConflict`, a malformed incumbent or occupant — inner or
  outer corruption — not `Corrupt` (including answered
  `InvalidArgument`), a conflict-then-absent incumbent not
  `BackendUnqualified`, a same-id-different-content occupant not
  `IdempotencyConflict`, a different verified occupant not
  `PositionConflict`, or an expired zombie occupant answered
  `PositionConflict` (P5, P9, P13; c3, c4, c10, c11, e4, e5, e6, e24,
  e25, e32).
- F4: a slot is advanced, a second landing occurs for a converged
  retry, or a second genesis identity appears: any attributable write
  at a position other than the exact target, a duplicate object for an
  identical retry, or an extra genesis for one id (P2, P3, P4, P8, P9,
  P15, P18; c2, c5, c6, e2, e4, e5, e6, e27, and the suite-wide
  witness).
- F5: a predecessor condition is answered by another variant or `Ok`,
  or the predecessor object is written (P10; e7, e8, e9, e33).
- F6: a witnessed hole is repaired by any write, or a LIST/GET
  contradiction is served as success (P11; e10, e11, e26).
- F7: the genesis exemption is violated — floor 1 at target 1 answered
  `Expired` — or a floor observation clamps or moves the target (P12;
  e12).
- F8: expiry loses its effect or mislabels its subject:
  `Expired(Target, Committed)` returned without the committed receipt,
  an expired predecessor answered as a target expiry, a zombie answered
  without its receipt, a target raced by trim/GC answered with a
  predecessor verdict or `EventMissing` instead of the prioritized
  target expiry, or an outer-corrupt expired target answered `Corrupt`
  instead of the prioritized expiry (P13, P16; e13, e14, e15, e21, e22,
  e25, e31, e36).
- F9: a stale or contradictory floor observation is mishandled — a
  monotone floor regression not answered `BackendUnqualified` with the
  current effect, or the frozen-LIST residual answered with a repair
  write or a spurious failure instead of standing on the qualified
  observation (P11, P17, P19; e11, e23, e29, e30).
- F10: the suite's attributable-write witness shows the call writing
  any key other than the exact target log key — a tail-hint write, a
  cursor write, a non-target or alternate-position log write, or any
  other key — anywhere in the suite, judged under the Inputs attribution
  rule. Raw PUT multiplicity from kernel-internal retries within one
  logical create is not itself a failure (P18; `log_put_keys` and
  no-PUT assertions across the loopback legs).

## Implementation

The executable is `crates/yeetz-s3-streams/tests/streams_conditional.rs`
(`c1`–`c12`, `e1`–`e36`; 48 tests), run by `cargo nextest run
--workspace --no-fail-fast` inside the `gates` task of the current
`.github/workflows/ci-dev.yml`. No case has passed and no witness
exists. Run history so far: the published candidate `7c40b58` failed
compilation before any test ran (exposing c7's original "bad/id"
literal — valid under the grammar's slash-joined components — as a
fixture defect, corrected to leading-slash/empty-component literals
with the source unchanged); a subsequent CI run reached the suite and
failed `e20` on fixture-side trace attribution — the mark was taken
after the parked request was already logged, and a later legacy live
read wrote the tail inside the attributed window — corrected by
opening the window before the spawn and closing it before the legacy
read; the product criteria and the Inputs trace-exclusion rules are
unchanged. Since those runs, `e35_lost_put_with_exact_readback_returns_committed_receipt`
(the same-call Committed/`Ok` upgrade leg) and
`e36_outer_corrupt_expired_target_is_expired_not_corrupt` (the
floor-precedence-over-corruption leg) have landed, their names verified
against the landed source — completing the declared 48-test suite with
no criteria change. The next run rides a new immutable SHA, and a
witness under `situation/witnesses/P-000007/` will record the executed
outcome with the exact source and workflow identities.

## Implementation coverage

Every leg is executable through the named tests via
`cargo nextest run --workspace --no-fail-fast` in the `gates` task; the
table claims executability, not outcome.

| Leg | Decision | Coverage |
|---|---|---|
| P1 | fresh caller-id creation lands one canonical genesis | `c1_fresh_create_writes_only_the_genesis_identity` |
| P2 | identical retry converges with no second creation effect | `c2_identical_retry_returns_existing_and_retains_config` |
| P3 | concurrent same-id creation yields one identity | `c5_concurrent_same_id_yields_one_created_one_existing` |
| P4 | lost create response converges on same-id retry | `c6_lost_create_response_then_same_id_retry_returns_existing` |
| P5 | incumbent conflict family is typed, incl. outer corruption; no overwrite | `c3_different_config_conflicts_and_never_overwrites`, `c4_corrupt_incumbent_is_typed_storage_corrupt`, `c10_absent_incumbent_after_conflict_is_backend_unqualified`, `c11_outer_genesis_corruption_maps_to_corrupt` |
| P6 | admission boundary: invalid/oversized/reserved refused effect-free; hierarchical ids accepted | `c7_invalid_stream_id_rejected_before_any_io`, `c8_oversized_genesis_rejected_before_any_io`, `c9_kernel_reserved_scope_id_rejected_before_any_io`, `c12_hierarchical_ids_are_valid_through_create_append_and_reads` |
| P7 | append lands at the exact successor | `e1_exact_successor_from_genesis` |
| P8 | exact retry converges after suffix advance and predecessor trim | `e2_exact_retry_after_suffix_advance_writes_nothing`, `e3_exact_retry_after_predecessor_trimmed_target_retained` |
| P9 | occupied target conflicts are typed, incl. outer corruption; no interleave | `e4_same_id_payload_conflict_is_not_attempted`, `e5_same_id_schema_conflict_is_not_attempted`, `e6_different_occupant_position_conflict_no_interleave`, `e24_malformed_target_occupant_is_corrupt_before_attempt`, `e32_outer_target_corruption_maps_to_corrupt` |
| P10 | predecessor adjudication is typed, incl. outer corruption, and NotAttempted | `e7_missing_predecessor_is_event_missing_not_attempted`, `e8_corrupt_predecessor_is_typed_corrupt`, `e9_mismatched_predecessor_names_expected_and_observed`, `e33_outer_predecessor_corruption_maps_to_corrupt` |
| P11 | hole witnessed, never repaired; contradiction and malformed witness fail typed | `e10_verified_later_event_witnesses_hole_without_filling`, `e11_list_get_contradiction_fails_closed`, `e26_malformed_later_witness_is_corrupt_without_fill` |
| P12 | genesis successor allowed at floor 1 | `e12_genesis_predecessor_at_floor_one_still_appends` |
| P13 | expiry: subjects typed; zombie receipt; expiry overrides occupant, corruption, and predecessor verdicts | `e13_predecessor_expiry_at_floor_is_typed`, `e14_target_expiry_at_floor_is_typed`, `e15_below_floor_zombie_committed_then_swept_target_cannot_recreate`, `e25_expired_target_overrides_zombie_conflict`, `e31_missing_predecessor_raced_by_trim_gc_prioritizes_target_expiry`, `e36_outer_corrupt_expired_target_is_expired_not_corrupt` |
| P14 | append admission preflight is effect-free incl. SeqExhausted | `e16_seq_max_and_invalid_admission_perform_no_io` |
| P15 | attempt failures preserve PossiblyCommitted; exact readback upgrades to Committed/`Ok` | `e17_refused_target_put_retains_possibly_committed`, `e18_lost_target_put_unavailable_readback_retains_possibly_committed`, `e27_lost_put_conflicting_readback_preserves_possibly_committed`, `e28_lost_put_corrupt_readback_preserves_possibly_committed`, `e34_lost_put_outer_corrupt_readback_preserves_possibly_committed`, `e35_lost_put_with_exact_readback_returns_committed_receipt` |
| P16 | post-attempt floor adjudication and retention races | `e19_postwrite_floor_failure_preserves_committed_receipt`, `e20_pause_before_target_put_trim_to_target_retained_success`, `e21_pause_after_target_put_trim_beyond_and_gc_expired_committed`, `e22_lost_put_gc_before_readback_expired_possibly_committed` |
| P17 | frozen certificate LIST stands on the qualified observation | `e23_frozen_certificate_list_residual_is_honest` |
| P18 | attributable writes target the exact slot; no tail/cursor/alternate writes | suite-wide `log_put_keys`/no-PUT assertions (incl. `e2`, `e10`, `e11`, `e27`) and `e20`'s one-log-PUT assertion |
| P19 | monotone floor regression fails closed with the current effect | `e29_floor_regression_before_attempt_is_backend_unqualified`, `e30_floor_regression_after_commit_preserves_committed_receipt` |
| F1 | preflight detects a premature effect, a mistyped reserved refusal, or a refused valid id | `c7`, `c8`, `c9`, `c12`, `e16` |
| F2 | effect coverage detects a lost, downgraded, degraded, or un-upgraded certainty | `e17`, `e18`, `e19`, `e20`, `e21`, `e22`, `e27`, `e28`, `e30`, `e34`, `e35` |
| F3 | occupancy legs detect a mistyped outcome incl. outer corruption | `c3`, `c4`, `c10`, `c11`, `e4`, `e5`, `e6`, `e24`, `e25`, `e32` |
| F4 | attributable-write witness detects slot advance or duplicate landing | `c2`, `c5`, `c6`, `e2`, `e4`, `e5`, `e6`, `e27`, suite witness |
| F5 | predecessor legs detect a mistyped or written predecessor | `e7`, `e8`, `e9`, `e33` |
| F6 | hole legs detect repair, contradiction-as-success, or skipped corruption | `e10`, `e11`, `e26` |
| F7 | floor legs detect exemption violation or clamping | `e12` |
| F8 | expiry legs detect a lost receipt, wrong subject, or unprioritized verdict | `e13`, `e14`, `e15`, `e21`, `e22`, `e25`, `e31`, `e36` |
| F9 | stale-floor legs detect regression or dishonest repair | `e11`, `e23`, `e29`, `e30` |
| F10 | attributable-write witness detects any non-target-key write | suite-wide `log_put_keys`/no-PUT assertions |
