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
| `real_s3_aba_probe` | Real-backend ABA probe (ADR 0001 ruled addendum): measures raw Exoscale etag recurrence and conditional requests, then proves the versioned `AtomicKeyspace` A(v0)→B(v1)→A(v2) closure with stale/current tokens; writes under isolated run prefixes and asserts cleanup. | **PASS; RAW HAZARD MEASURED**: raw identical bytes reused an etag and accepted stale `If-Match`; wrapped A(v0)/A(v2) etags differed, the stale v0 token was rejected, and a current-token identical-payload CAS reached v3. ([yeetz run 32459327751](https://github.com/cleverunicornz/yeetz/actions/runs/32459327751), 14 verdict rows, cleanup empty.) | `gh workflow run ci-dev.yml -f ref=<ref> -f task=real-s3` |
| `batch9_adversary` | Kernel batch 9 lineage-head incarnation teardown: public sequential cross-destroy fencing, concurrent counter convergence, and terminal/taxonomy invariance; companion in-source L8-L13 canaries attack the private eviction wire, post-landing failure window, destroy wire, dual decode, counter race, and exact read shape. | PENDING remote adversary witness. The public surface legs hold; L8/L9/L10 are expected to fail against merged batch 9 until adjudicated or fixed. | `cargo run -p yeetz-rigs --example batch9_adversary` (legs also run as rig tests; private canaries run in the workspace suite) |

The forge-facing rigs (connect transport legs, gRPC legs, write-path
concurrency, events migration) stayed in the parent `yeetz` repo —
they prove forge behavior against forge types.
