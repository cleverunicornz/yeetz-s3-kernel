# W-000021 — Manifest-committed streamed-values current gate

## Promise

`situation/promises/P-000004-manifest-committed-streamed-values.md`

## Oracle

`situation/oracles/O-000004-manifest-committed-streamed-values.md`

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
  `43fb1f661ad91363fc99ae257ed4e62ea8207303:crates/yeetz-s3-kernel/src/streaming_contract.rs`
  defines `a24_manifest_only_visibility_and_control_cuts`,
  `a25_whole_stream_equivalence_across_transitions`,
  `a26_concurrent_writers_distinct_and_identical_matrix`, and
  `a29_missing_truncated_swapped_and_bad_root_taxonomy`.
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
- Observation boundary: the A24/A25/A26/A29 contracts ran on the repository
  test harnesses those tests stage in-process — in-memory chunk and manifest
  doubles carrying their own fault states; no standalone rig executable ran
  in this job. The `real-s3 (Exoscale SOS ABA probe)` and `kernel-facing
  durable rigs` steps were skipped in this `gates` job (`task == 'gates'`)
  and the live-S3 probes above stayed ignored, so this witness claims no
  live S3 behavior.
- Residuals are unchanged by this observation: destructive chunk collection
  still requires operational writer quiescence the kernel cannot prove
  (`situation/gaps/G-000001-streaming-gc-quiescence.md`), and
  backend-specific multipart capability beyond the manifest representation
  remains outside the assurance.

## Oracle legs

| Leg | Evidence |
|---|---|
| P1 | `PASS [   5.108s] ( 55/287) yeetz-s3-kernel streaming_contract::a24_manifest_only_visibility_and_control_cuts` at `2026-09-10T00:39:59.6106209Z` in job 102698755081 |
| P2 | `PASS [  12.458s] ( 59/287) yeetz-s3-kernel streaming_contract::a25_whole_stream_equivalence_across_transitions` at `2026-09-10T00:40:07.0849831Z` and `PASS [  10.274s] ( 57/287) yeetz-s3-kernel streaming_contract::a26_concurrent_writers_distinct_and_identical_matrix` at `2026-09-10T00:40:05.0607738Z` in job 102698755081 |
| P3 | `PASS [   5.912s] ( 58/287) yeetz-s3-kernel streaming_contract::a29_missing_truncated_swapped_and_bad_root_taxonomy` at `2026-09-10T00:40:05.5225655Z` in job 102698755081 |
