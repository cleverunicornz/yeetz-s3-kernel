# yeetz-s3-streams

[![Crates.io](https://img.shields.io/crates/v/yeetz-s3-streams.svg)](https://crates.io/crates/yeetz-s3-streams)
[![Docs.rs](https://docs.rs/yeetz-s3-streams/badge.svg)](https://docs.rs/yeetz-s3-streams)
[![CI](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/workflows/ci.yml/badge.svg)](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/cleverunicornz/yeetz-s3-kernel/blob/main/LICENSE)

**Append-only event logs on S3, without a broker.**
[`yeetz-s3-streams`] implements durable event logs on top of the
[`yeetz-s3-kernel`] `AtomicKeyspace` ([ADR 0002]): one immutable
object per event at `streams/v1/<id>/log/<seq>`, where the object's
*conditional create* **is** the sequence allocation — no counters, no
coordination service, no leader. Concurrent writers race the create;
one lands, losers advance `+1` and retry.

Design boundaries, on purpose:

- **Forge-agnostic.** Opaque `StreamId`s, opaque payloads, no
  application types anywhere.
- **Pull-only.** No delivery bus, no push, no scheduling — consumers
  replay and advance cursors.
- **Damage is loud, named, and per-seq.** Every read verifies
  key↔envelope agreement and payload digests; decode failure is an
  error, never a skip.

[`yeetz-s3-streams`]: https://docs.rs/yeetz-s3-streams/latest/yeetz_s3_streams/
[`yeetz-s3-kernel`]: https://crates.io/crates/yeetz-s3-kernel
[ADR 0002]: https://github.com/cleverunicornz/yeetz-s3-kernel/blob/main/situation/record/decision-0002-streams.yamlld

## Example

```rust
use yeetz_s3_streams::{Replay, SchemaId, StableEventId, Streams};
use yeetz_s3_kernel::KernelHandle;

# async fn run(config: yeetz_s3_kernel::S3Config) -> Result<(), Box<dyn std::error::Error>> {
let handle = KernelHandle::from_s3_config(&config)?;
let streams = Streams::new(&handle)?;

// Create: mints an opaque id and writes the immutable genesis record
// (seq 0). The conditional create defines existence.
let stream = streams.create_stream(br#"{"repo":"demo/hello"}"#).await?;

// Append: caller-chosen StableEventId dedupes retries within the
// bounded idempotency window. The receipt names the landed seq.
let receipt = streams
    .append(
        &stream,
        &SchemaId::new("issue.comment.v1")?,
        &StableEventId::new("c-42")?,
        br#"{"body":"hi"}"#,
    )
    .await?;

// Replay: after-exclusive, bounded page, typed outcome — never a
// blanket error. `complete` is witness-bounded and withheld, not
// guessed.
match streams.read(&stream, 0, 100).await {
    Replay::Page { events, complete } => {
        let _ = complete;
        for event in &events { /* … */ }
        // resume with after_seq = events.last().map(|e| e.seq)
    }
    Replay::Empty => {}
    other => { /* NotFound, Corrupt { .. }, OffsetExpired { .. }, … */ }
}

// Consumer position: a CAS'd, monotone-only cursor.
let cursor = streams
    .advance_cursor(&stream, "projector", receipt.seq)
    .await?;
# let _ = cursor;
# Ok(())
# }
```

## Semantics worth knowing

- **Durability is the create.** There is no flush API — the S3
  conditional create is the linearization point. Retries converge via
  the stable event id (`IdempotencyConflict` is typed for same-id /
  different-content within the window; beyond it, at-least-once —
  consumers dedupe).
- **Cursors are monotone pointers.** `advance_cursor` validates the
  target event exists and moves by CAS; it never rewinds.
- **Retention is certified trim.** `trim` writes an immutable
  create-once certificate bounding the retained prefix; reads below
  the floor are a typed `OffsetExpired` — never `Corrupt`, never
  `Empty` — and `gc` is an idempotent, resumable sweeper that deletes
  only below the certificate. The genesis record is immortal.
- **`read` returns a `Replay`, not a `Result`.** Seven states —
  `NotFound`, `Empty`, `Page { events, complete }`, `Corrupt
  { missing_or_mismatched }`, `OffsetExpired { first_retained }`,
  `Unavailable`, `BackendUnqualified` — because in a distributed log
  the outcome *is* the information. `complete = true` requires a
  verified tail witness plus an empty ordered probe past it;
  otherwise completeness is withheld rather than guessed.
- **Migration is a first-class surface.** `migration::migrate_log`
  copies a verified history into dense seqs 1..n and seals it with an
  immutable `MigrationSeal` (source lineage, digests, event count).

## Assurance

The S contract (S1–S11: contiguity, one-winner-per-seq, replay order,
typed damage, envelope bounds), the trim contract (R2/R6/R7/R8 —
including *trim-to-end is logically empty with a floor*), and the G130
completeness regression are all named tests against an in-memory
kernel, a fault-injecting loopback S3 wire counterpart
(`freeze_list` / `hide_key` / `arm_fault`), and durable CI rigs
against real backends.

MSRV: Rust 1.96 (pinned by the workspace `rust-toolchain.toml`).

## The closure

| Crate | Role |
| --- | --- |
| [`yeetz-s3-kernel`](https://crates.io/crates/yeetz-s3-kernel) | lineages + atomic keyspace |
| [`yeetz-s3-streams`](https://crates.io/crates/yeetz-s3-streams) | this crate — append-only event logs |
| [`yeetz-sdk-s3`](https://crates.io/crates/yeetz-sdk-s3) | request-scoped S3 client mechanics |
| [`yeetz-sdk-core`](https://crates.io/crates/yeetz-sdk-core) | provider-neutral HTTP foundation |

## License

MIT.
