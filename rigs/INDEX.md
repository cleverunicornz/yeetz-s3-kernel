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
| `real_s3_aba_probe` | Real-backend kernel capability probe (ADR 0001 ABA addendum; ADR 0004 multipart alternative): measures raw Exoscale etag recurrence/conditional requests, proves versioned `AtomicKeyspace` closure, and measures conditional multipart completion, incomplete-upload abort/list visibility, and part-addressed reads; isolated prefixes and cleanup are asserted. | **PASS; RAW ABA HAZARD + PARTIAL MULTIPART WITNESS**: current-etag conditional completion succeeded, stale completion returned `PreconditionFailed`, incomplete upload was listed then absent after abort, exact multipart bytes landed, but `GetObject partNumber` returned `UnsupportedArgument`; 25 verdict rows and empty object/MPU cleanup. ([run 32592980637](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/32592980637). Historical ABA-only witness: [run 32459327751](https://github.com/cleverunicornz/yeetz/actions/runs/32459327751).) | `gh workflow run ci-dev.yml -f ref=<ref> -f task=real-s3` |

The forge-facing rigs (connect transport legs, gRPC legs, write-path
concurrency, events migration) stayed in the parent `yeetz` repo —
they prove forge behavior against forge types.
