# W-000022 — Bounded batched-deletion current gate

## Promise

`situation/promises/P-000005-bounded-batched-deletion.md`

## Oracle

`situation/oracles/O-000005-bounded-batched-deletion.md`

## Result

PASS

## Head

`43fb1f661ad91363fc99ae257ed4e62ea8207303`

## Observed

2026-09-10

## Evidence

- Workflow run `https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34421839069`
  (`gates` dispatch), job
  `https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34421839069/job/102698755081`,
  conclusion `success`, started `2026-09-10T00:36:41Z`, completed
  `2026-09-10T00:40:47Z`, runner group `cvu`, label `cvu-test-runner-x64`,
  runner name `cvu-test-runner-de1-2ee94c92`, toolchain `1.96.0` stable.
- Execution-head identity: the job's checkout step fetched the exact ref and
  printed `git log -1 --format=%H` =
  `43fb1f661ad91363fc99ae257ed4e62ea8207303`, so the oracle's
  source/test/workflow identity between the execution and observation heads
  holds at one commit rather than by comparison between heads.
- `43fb1f661ad91363fc99ae257ed4e62ea8207303:.github/workflows/ci-dev.yml`
  defines the `gates (full set)` step to run `cargo build --workspace --locked`
  then `cargo nextest run --workspace --no-fail-fast`. The oracle
  implementation coordinate
  `49ba2ced98831d192f6a2371b90aec8e81a081fd:.github/workflows/ci-dev.yml`
  ran the same step without `--no-fail-fast`; the flag only changes
  failure-abort behavior, and every Pass leg below is decided by its own
  named PASS line, not by the aggregate summary.
- The oracle input tests exist at the head:
  `43fb1f661ad91363fc99ae257ed4e62ea8207303:crates/yeetz-s3-kernel/src/deletion_contract.rs`
  defines `a36` through `a45`. Beyond the legs, the remaining input set also
  passed in this job: `a39_delete_objects_remainder_crosses_chunks`
  (`3/287`), `a43_delete_objects_stays_below_lifecycle_state` (`54/287`),
  `a44_delete_objects_has_no_condition_or_transaction` (`10/287`), and
  `a45_delete_many_and_delete_if_match_remain_unchanged` (`9/287`).
- The run compiled and tested the coordinated 0.5 crate closure: the job's
  build output checks `yeetz-s3-kernel`, `yeetz-s3-streams`, `yeetz-sdk-s3`,
  and `yeetz-sdk-core` all at `v0.5.0` against workspace members fixed in
  `43fb1f661ad91363fc99ae257ed4e62ea8207303:Cargo.toml`.
- nextest summary: `287 tests run: 287 passed, 4 skipped` (logged
  `2026-09-10T00:40:43.1555626Z`). nextest does not print skipped-test names;
  the count matches the four workspace `#[ignore]` tests at this head —
  `s3_loopback_counterpart_process` in
  `43fb1f661ad91363fc99ae257ed4e62ea8207303:crates/yeetz-s3-kernel/src/state_kernel.rs`
  and `test_hetzner_list_prefix`, `test_real_s3_download_object_store_client`,
  `test_real_s3_download_aws_sdk` in
  `43fb1f661ad91363fc99ae257ed4e62ea8207303:crates/yeetz-sdk-s3/src/store.rs`
  — none of which are oracle inputs.
- Observation boundary: the A36–A45 contracts ran on the repository test
  harnesses those tests stage in-process — in-memory request and response
  doubles carrying the chunking, preflight, lost-response, and
  invalid-response fault states; no standalone rig executable ran in this
  job. The `real-s3 (Exoscale SOS ABA probe)` and `kernel-facing durable
  rigs` steps were skipped in this `gates` job (`task == 'gates'`) and the
  live-S3 probes above stayed ignored, so this witness claims no live
  provider DeleteObjects wire, quorum, or consistency behavior.
- Residuals are unchanged by this observation: `delete_objects` remains
  transport rather than transaction, intentionally raw relative to lifecycle
  state, unable to make a caller's concurrent chunk-GC policy safe
  (`situation/gaps/G-000001-streaming-gc-quiescence.md`).

## Oracle legs

| Leg | Evidence |
|---|---|
| P1 | `PASS [   0.206s] (  1/287) yeetz-s3-kernel deletion_contract::a36_delete_objects_input_preflight_is_side_effect_free` at `2026-09-10T00:39:47.4830515Z` in job 102698755081 |
| P2 | `PASS [   0.562s] (  4/287) yeetz-s3-kernel deletion_contract::a37_delete_objects_chunks_exactly_at_1000` at `2026-09-10T00:39:47.8382248Z` in job 102698755081 |
| P3 | `PASS [   0.221s] (  2/287) yeetz-s3-kernel deletion_contract::a38_delete_objects_partial_batch_is_typed_per_key` at `2026-09-10T00:39:47.4972063Z`, `PASS [   0.705s] (  6/287) yeetz-s3-kernel deletion_contract::a40_delete_objects_lost_response_marks_chunk_unconfirmed` at `2026-09-10T00:39:48.1868449Z`, `PASS [   0.849s] (  7/287) yeetz-s3-kernel deletion_contract::a41_delete_objects_invalid_response_is_never_success` at `2026-09-10T00:39:48.3470499Z`, and `PASS [   0.624s] (  5/287) yeetz-s3-kernel deletion_contract::a42_delete_objects_fails_closed_without_wire_support` at `2026-09-10T00:39:48.1690498Z` in job 102698755081 |
