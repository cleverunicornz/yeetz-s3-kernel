# W-000016 — Strict stream-reads gate

## Promise

`situation/promises/P-000006-strict-stream-reads.md`

## Oracle

`situation/oracles/O-000006-strict-stream-reads.md`

## Result

PASS

## Head

`43fb1f661ad91363fc99ae257ed4e62ea8207303`

## Observed

2026-09-10

## Evidence

- `gates` run 34421839069, job 102698755081, conclusion `success`, runner
  `cvu-test-runner-de1-2ee94c92` (label `cvu-test-runner-x64`), checkout
  `43fb1f661ad91363fc99ae257ed4e62ea8207303`; the `gates (full set)` step ran
  2026-09-10T00:37:35Z–00:40:43Z and its nextest summary reads
  `287 tests run: 287 passed, 4 skipped`:
  https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34421839069/job/102698755081
  The run's 58 new streams tests include the complete r-suite (r1–r10, all
  PASS); every PASS line quoted below is from this job's log.
- Rig run 34421838684, job 102698754400, conclusion `success`, runner
  `cvu-test-runner-de1-0a152ecd` (label `cvu-test-runner-x64`), same head;
  the `kernel-facing durable rigs` step produced 13 PASS verdicts, including
  `R: paginated window (0,9] complete, byte-identical after suffix growth to
  seq 12`:
  https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34421838684/job/102698754400
  The rig harness runs against the kernel's in-memory store; neither run is
  a live external-S3 qualification.
- Manual P9 range-fetch bound and the P10 retained-digest clause: the
  correction source review
  https://github.com/cleverunicornz/yeetz-s3-kernel/pull/47#issuecomment-5611554000
  inspects the immutable source at this head. It records that `read_range`
  limits each fetch chunk to `FETCH_PARALLELISM` and awaits it before the next
  chunk, and that `decode_and_verify` retains the verified wire digest in
  `EventRef` while `payload_sha256()` borrows it without recomputing.
- Manual P10 `Envelope`-privacy clause: retained review comment
  https://github.com/cleverunicornz/yeetz-s3-kernel/pull/47#issuecomment-5610975924
  and the same correction source review — disposition PASS by source
  inspection over the immutable source at this head,
  https://github.com/cleverunicornz/yeetz-s3-kernel/blob/43fb1f661ad91363fc99ae257ed4e62ea8207303/crates/yeetz-s3-streams/src/envelope.rs.
  They record every `Envelope` field private, public access limited to
  value/shared-reference getters, `encode` and `decode_and_verify` as
  crate-private constructors, and forgeable public `EventRef` fields granting
  no mutation of an existing verified `Envelope`. These are retained manual
  reviews, not claims that runtime tests prove the source-only clauses.
- Observation provenance: job metadata and logs were fetched from GitHub
  Actions by the orchestrator against this head on 2026-09-10; the run and
  job URLs above are the canonical evidence, not any local copy.

## Oracle legs

| Leg | Evidence |
|---|---|
| P1 | `PASS [0.008s] (213/287) yeetz-s3-streams::streams_read r1_read_event_returns_verified_envelope` |
| P2 | `PASS [0.227s] (216/287) yeetz-s3-streams::streams_read r2_read_event_request_shape_is_exact` |
| P3 | `PASS [0.256s] (217/287) yeetz-s3-streams::streams_read r3_read_event_typed_boundaries` and `PASS [0.237s] (212/287) yeetz-s3-streams::streams_read r10_corrupt_outer_kernel_envelope_names_seq_on_both_strict_reads` |
| P4 | `PASS [0.290s] (220/287) yeetz-s3-streams::streams_read r4_read_range_argument_preflight_is_side_effect_free` |
| P5 | `PASS [0.356s] (229/287) yeetz-s3-streams::streams_read r5_read_range_complete_window_or_typed_error` plus the r10 range leg; corroborated by rig verdict `R: paginated window (0,9] complete, byte-identical after suffix growth to seq 12` |
| P6 | `PASS [0.012s] (218/287) yeetz-s3-streams::streams_read r6_read_range_reached_end_is_window_not_eof`; rig verdict R re-serves the identical window after suffix growth past its end |
| P7 | `PASS [0.009s] (219/287) yeetz-s3-streams::streams_read r7_read_range_resume_and_seq_max` |
| P8 | `PASS [0.477s] (234/287) yeetz-s3-streams::streams_read r8_read_range_request_shape_no_log_list_no_tail_no_writes` |
| P9 | Manual source inspection in the correction review: `read_range` constructs at most `FETCH_PARALLELISM.min(remaining)` fetches per chunk and awaits `join_all` before the next chunk |
| P10 | `PASS [0.007s] (221/287) yeetz-s3-streams::streams_read r9_envelope_immutable_surface_and_event_ref` for runtime clauses; retained direct source reviews decide digest retention and field privacy |
