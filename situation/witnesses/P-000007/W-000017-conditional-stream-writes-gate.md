# W-000017 — Conditional stream writes gate

## Promise

`situation/promises/P-000007-conditional-stream-writes.md`

## Oracle

`situation/oracles/O-000007-conditional-stream-writes.md`

## Result

PASS

## Head

`43fb1f661ad91363fc99ae257ed4e62ea8207303`

## Observed

2026-09-10

## Evidence

- Gates job:
  `https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34421839069/job/102698755081`
  (run `34421839069`, job `102698755081`, conclusion `success`, runner
  label `cvu-test-runner-x64`, runner `cvu-test-runner-de1-2ee94c92`),
  checkout `43fb1f661ad91363fc99ae257ed4e62ea8207303` — the checkout SHA
  equals the head; the orchestrator verified the checkout against actual
  `git log` output.
- The `gates (full set)` step succeeded
  (`2026-09-10T00:37:35Z`–`2026-09-10T00:40:43Z`); at this head
  `.github/workflows/ci-dev.yml` defines it as
  `cargo fmt --all --check`, `cargo clippy --workspace --all-targets
  --all-features -- -D warnings`, `cargo build --workspace --locked`,
  and `cargo nextest run --workspace --no-fail-fast`.
- Nextest summary at `2026-09-10T00:40:43.1555626Z`: `287 tests run:
  287 passed, 4 skipped` (55.880s). The run log carries an individual
  PASS line for every one of the 48 conditional cases — `c1`–`c12`,
  `e1`–`e36` — cited per leg below by test name and `(n/287)` ordinal.
- Supplemental, not fault-leg proof: kernel-rigs run `34421838684`, job
  `https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34421838684/job/102698754400`
  at the same head (runner label `cvu-test-runner-x64`) yielded 13 PASS
  verdicts (S1–S4, cursor, C1, C2, E1–E5, R). That route proves
  standalone behavior on a commissioned runner; the fault legs are
  decided only by the loopback suite inside the gates job.
- Also observed at this head, outside this promise's oracle: the 10
  strict-read cases `r1`–`r10` passed (P-000006's records own them).
- Source review accepted and retained at
  `https://github.com/cleverunicornz/yeetz-s3-kernel/pull/47#issuecomment-5610975924`.
- Provenance: the facts above are transcribed from the GitHub Actions
  run/job records and the retained pull-request comment; no
  machine-local scratch file is cited as evidence.

Scope of the observation: the suite exercises the in-memory kernel and
the loopback S3 counterpart under the qualified-backend contract. This
witness qualifies no external object-store provider. The stale-LIST
residual passed as the bounded honest claim it is (`e23` frozen
certificate LIST: `Ok` on the qualified observation, honest
`Expired(Target, Committed)` on the later fresh-LIST retry), not as
repair; separately, `e17` passed as the effect-uncertainty claim (a
refused target PUT still reports `PossiblyCommitted`). No
retention pin, ownership grant, or future-retention guarantee is
observed; `Ok` is bounded exactly as the promise states. The e20 leg
passed with its corrected mark-partitioned attribution (window opened
before spawn, closed before the legacy read), so its write-shape
evidence is attributable to the call alone.

## Oracle legs

| Leg | Evidence |
|---|---|
| P1 | `c1_fresh_create_writes_only_the_genesis_identity` PASS (126/287) |
| P2 | `c2_identical_retry_returns_existing_and_retains_config` PASS (127/287) |
| P3 | `c5_concurrent_same_id_yields_one_created_one_existing` PASS (130/287) |
| P4 | `c6_lost_create_response_then_same_id_retry_returns_existing` PASS (133/287) |
| P5 | `c3_different_config_conflicts_and_never_overwrites` PASS (128/287); `c4_corrupt_incumbent_is_typed_storage_corrupt` PASS (129/287); `c10_absent_incumbent_after_conflict_is_backend_unqualified` PASS (124/287); `c11_outer_genesis_corruption_maps_to_corrupt` PASS (131/287) |
| P6 | `c7_invalid_stream_id_rejected_before_any_io` PASS (132/287); `c8_oversized_genesis_rejected_before_any_io` PASS (141/287); `c9_kernel_reserved_scope_id_rejected_before_any_io` PASS (134/287); `c12_hierarchical_ids_are_valid_through_create_append_and_reads` PASS (125/287) |
| P7 | `e1_exact_successor_from_genesis` PASS (145/287) |
| P8 | `e2_exact_retry_after_suffix_advance_writes_nothing` PASS (159/287); `e3_exact_retry_after_predecessor_trimmed_target_retained` PASS (164/287) |
| P9 | `e4_same_id_payload_conflict_is_not_attempted` PASS (165/287); `e5_same_id_schema_conflict_is_not_attempted` PASS (166/287); `e6_different_occupant_position_conflict_no_interleave` PASS (167/287); `e24_malformed_target_occupant_is_corrupt_before_attempt` PASS (150/287); `e32_outer_target_corruption_maps_to_corrupt` PASS (161/287) |
| P10 | `e7_missing_predecessor_is_event_missing_not_attempted` PASS (168/287); `e8_corrupt_predecessor_is_typed_corrupt` PASS (169/287); `e9_mismatched_predecessor_names_expected_and_observed` PASS (170/287); `e33_outer_predecessor_corruption_maps_to_corrupt` PASS (162/287) |
| P11 | `e10_verified_later_event_witnesses_hole_without_filling` PASS (135/287); `e11_list_get_contradiction_fails_closed` PASS (136/287); `e26_malformed_later_witness_is_corrupt_without_fill` PASS (152/287) |
| P12 | `e12_genesis_predecessor_at_floor_one_still_appends` PASS (137/287) |
| P13 | `e13_predecessor_expiry_at_floor_is_typed` PASS (138/287); `e14_target_expiry_at_floor_is_typed` PASS (139/287); `e15_below_floor_zombie_committed_then_swept_target_cannot_recreate` PASS (140/287); `e25_expired_target_overrides_zombie_conflict` PASS (151/287); `e31_missing_predecessor_raced_by_trim_gc_prioritizes_target_expiry` PASS (160/287); `e36_outer_corrupt_expired_target_is_expired_not_corrupt` PASS (183/287) |
| P14 | `e16_seq_max_and_invalid_admission_perform_no_io` PASS (146/287) |
| P15 | `e17_refused_target_put_retains_possibly_committed` PASS (142/287); `e18_lost_target_put_unavailable_readback_retains_possibly_committed` PASS (143/287); `e27_lost_put_conflicting_readback_preserves_possibly_committed` PASS (154/287); `e28_lost_put_corrupt_readback_preserves_possibly_committed` PASS (156/287); `e34_lost_put_outer_corrupt_readback_preserves_possibly_committed` PASS (163/287); `e35_lost_put_with_exact_readback_returns_committed_receipt` PASS (189/287) — the positive lost-response same-call upgrade |
| P16 | `e19_postwrite_floor_failure_preserves_committed_receipt` PASS (147/287); `e20_pause_before_target_put_trim_to_target_retained_success` PASS (148/287) with corrected attribution; `e21_pause_after_target_put_trim_beyond_and_gc_expired_committed` PASS (149/287); `e22_lost_target_put_gc_before_readback_expired_possibly_committed` PASS (153/287) |
| P17 | `e23_frozen_certificate_list_residual_is_honest` PASS (155/287) |
| P18 | no-PUT assertions inside `e2` (159/287), `e10` (135/287), `e11` (136/287), `e27` (154/287); `e20`'s exact-one-log-PUT assertion (148/287); the suite-wide mark-partitioned `log_put_keys` witness across the loopback legs — every constituent test PASS at this head |
| P19 | `e29_floor_regression_before_attempt_is_backend_unqualified` PASS (157/287); `e30_floor_regression_after_commit_preserves_committed_receipt` PASS (158/287) |
