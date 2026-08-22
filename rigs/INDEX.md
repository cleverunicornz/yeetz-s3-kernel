# Rigs index

Durable witnesses. Each rig maps to the promise it proves, its
verdict, and how to run it. All rigs are compiled and linted by the
workspace gates. Execution evidence is named per row; a green compile
does not claim that an example ran. The `ci-dev` task `kernel-rigs`
executes the streams witness; the `real-s3` task runs the live
Exoscale probe.

| Rig | Promise (Claim) | Verdict | Run |
|---|---|---|---|
| `streams_contracts` | yeetz-s3-streams (ADR 0002): (1) concurrent appends allocate one winner per seq (S1); (2) replay is dense, ordered, and LIST-qualified complete (S2); (3) a mid-log deletion surfaces Corrupt naming the seq — damage is loud, never skipped (S4); (4) idempotent re-append converges to the original receipt (S3); (5) cursor advance is monotonic and idempotent. | PASS (all five legs green; carried from yeetz run [32460116519](https://github.com/cleverunicornz/yeetz/actions/runs/32460116519)) | `cargo run -p yeetz-rigs --example streams_contracts` |
| `real_s3_aba_probe` | Real-backend kernel capability probe (ADR 0001 ABA addendum; ADR 0004 multipart alternative + streaming legs): measures raw Exoscale etag recurrence/conditional requests, proves versioned `AtomicKeyspace` closure, measures conditional multipart completion, incomplete-upload abort/list visibility, and part-addressed reads; and (ADR 0004) proves the streamed v3 round trip on real S3 — chunked create/CAS with exact whole/reader/range bytes, v3 stale-token conditional delete naming the manifest era, delete-free chunk metering, the maintenance fence, and the quiesced sweep (which doubles as the run's chunk-root cleanup); isolated prefixes and cleanup are asserted. | **PASS; RAW ABA HAZARD + PARTIAL MULTIPART WITNESS**: current-etag conditional completion succeeded, stale completion returned `PreconditionFailed`, incomplete upload was listed then absent after abort, exact multipart bytes landed, but `GetObject partNumber` returned `UnsupportedArgument`; 25 verdict rows and empty object/MPU cleanup (run [32592980637](https://github.com/cleverunicornz/yeetz)). **Streaming legs (batch 10): PASS** — streamed v3 create/CAS byte-exact via whole collect, verified reader, and boundary range; v3 stale-token conditional delete names the manifest era; delete-free meter 6/3/3/0; fence refuses begins; quiesced sweep reclaims 6 with an idempotent re-run; release restores begins; 38 verdict rows and empty object/chunk-root cleanup ([run 32597320572](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/32597320572)). | `cargo run -p yeetz-rigs --example real_s3_aba_probe` |

The ADR 0004 loopback wire rig (A24–A34: manifest-only visibility,
commit oracle, conditional stale-era eviction, corruption taxonomy,
state/deletion composition, inline request profile, lost-response
crash matrix, fence/inventory/sweep, frozen-LIST leak, and the
broken-quiescence demonstration cut) is the kernel's in-crate
`streaming_contract` suite, executed by the standard `gates`/`nextest`
tasks; A28/A32/A35 public-API legs live in
`crates/yeetz-s3-kernel/tests/streaming_contract.rs`, and S11 in
`crates/yeetz-s3-streams/tests/streams_envelope_bound.rs`.

The batch-10 teardown commit-identity witness is
`writer_commit_ids_are_unique_in_large_sample` in `value_manifest`.
The construction now draws 128 bits from the process RNG instead of
clock/PID/process-local state; the witness checks that a 4,096-ID
sample is collision-free.

The forge-facing rigs (connect transport legs, gRPC legs, write-path
concurrency, events migration) stayed in the parent `yeetz` repo —
they prove forge behavior against forge types.
