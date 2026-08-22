//! S-suite contracts (ADR 0017) — the in-memory leg. Loopback-backed
//! contracts (lost-response cuts, stale LIST, crash matrix) live in
//! `streams_loopback.rs`.

mod support;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use support::streams_on_in_memory_store;
use yeetz_s3_kernel::KernelHandle;
use yeetz_s3_streams::{Replay, SchemaId, StableEventId, StreamId, Streams, StreamsError};

fn schema(value: &str) -> SchemaId {
    SchemaId::new(value).unwrap()
}

fn event(value: &str) -> StableEventId {
    StableEventId::new(value).unwrap()
}

/// S1: one winner per seq under concurrent appends; the log stays
/// dense and contiguous (no gaps, no overwrites).
#[tokio::test]
async fn s1_contiguity_one_winner_per_seq() {
    let streams = std::sync::Arc::new(streams_on_in_memory_store());
    let stream = streams.create_stream(&[]).await.unwrap();
    let mut tasks = Vec::new();
    for index in 1..=8u64 {
        let streams = streams.clone();
        let stream = stream.clone();
        tasks.push(tokio::spawn(async move {
            streams
                .append(
                    &stream,
                    &schema("test.event.v1"),
                    &event(&format!("event-{index}")),
                    format!("payload-{index}").as_bytes(),
                )
                .await
                .unwrap()
        }));
    }
    let receipts = futures::future::join_all(tasks).await;
    // One winner per seq: 8 distinct seqs covering 1..=8 exactly.
    let mut seqs: Vec<u64> = receipts.iter().map(|r| r.as_ref().unwrap().seq).collect();
    seqs.sort_unstable();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    // Dense replay with no corruption.
    match streams.read(&stream, 0, 100).await {
        Replay::Page {
            events, complete, ..
        } => {
            assert_eq!(events.len(), 8);
            assert!(complete);
            let ids: Vec<&str> = events.iter().map(|e| e.stable_event_id.as_str()).collect();
            let mut sorted = ids.clone();
            sorted.sort_unstable();
            assert_eq!(sorted.len(), 8, "eight distinct events");
        }
        other => panic!("expected complete page, got {other:?}"),
    }
}

/// S2: replay order and completeness — a paginated walk advances on
/// `events.last().seq` (the read is after-exclusive; anything beyond
/// the last fetched seq would skip) and `complete=true` only at the
/// witness-bounded qualified end.
#[tokio::test]
async fn s2_replay_order_and_completeness() {
    let streams = streams_on_in_memory_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=5u64 {
        streams
            .append(
                &stream,
                &schema("ordered.v1"),
                &event(&format!("e{index}")),
                &[index as u8],
            )
            .await
            .unwrap();
    }
    let mut collected: Vec<(u64, String)> = Vec::new();
    let mut after = 0u64;
    loop {
        match streams.read(&stream, after, 3).await {
            Replay::Page { events, complete } => {
                assert!(!events.is_empty());
                for envelope in &events {
                    // Order is seq order; payloads verify.
                    assert_eq!(envelope.seq, after + 1);
                    collected.push((envelope.seq, envelope.stable_event_id.as_str().to_string()));
                    after = envelope.seq;
                }
                if complete {
                    break;
                }
            }
            other => panic!("expected page, got {other:?}"),
        }
    }
    let expected: Vec<(u64, String)> = (1..=5).map(|i| (i, format!("e{i}"))).collect();
    assert_eq!(collected, expected);

    // Reading past the end is Empty, and a nonexistent stream is
    // NotFound — distinct typed states.
    assert!(matches!(streams.read(&stream, 5, 3).await, Replay::Empty));
    let ghost = StreamId::new("sghost").unwrap();
    assert!(matches!(
        streams.read(&ghost, 0, 3).await,
        Replay::NotFound { .. }
    ));
}

#[tokio::test]
async fn s2_replay_remains_dense_across_fetch_chunks() {
    let streams = streams_on_in_memory_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=65u64 {
        streams
            .append(
                &stream,
                &schema("chunked.v1"),
                &event(&format!("chunk-{index}")),
                &[],
            )
            .await
            .unwrap();
    }

    match streams.read(&stream, 0, 100).await {
        Replay::Page {
            events, complete, ..
        } => {
            assert!(complete);
            assert_eq!(events.len(), 65);
            assert_eq!(
                events.iter().map(|event| event.seq).collect::<Vec<_>>(),
                (1..=65).collect::<Vec<_>>()
            );
        }
        other => panic!("expected dense chunked replay, got {other:?}"),
    }
}

/// S2 (D2 regression, crate-side witness): paginating with
/// `limit < total` across the internal fetch-chunk boundary (8-seq
/// chunks) while advancing on `events.last().seq` must see every
/// event exactly once, dense. The removed `next_seq` field
/// (= last + 1) resumed one-past-the-end under after-exclusive
/// semantics and skipped an event per page — the yeetz #113 defect;
/// this keeps the regression pinned on the crate side.
#[tokio::test]
async fn s2_paginated_walk_limit_below_total_advances_on_last_seq() {
    let streams = streams_on_in_memory_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    const TOTAL: u64 = 25; // > 2× the 8-seq fetch chunk; 3 pages of 10
    for index in 1..=TOTAL {
        streams
            .append(
                &stream,
                &schema("paged.v1"),
                &event(&format!("page-{index}")),
                &[index as u8],
            )
            .await
            .unwrap();
    }

    let mut collected: Vec<u64> = Vec::new();
    let mut after = 0u64;
    let mut pages = 0u32;
    loop {
        match streams.read(&stream, after, 10).await {
            Replay::Page { events, complete } => {
                assert!(!events.is_empty(), "a non-terminal page must carry events");
                pages += 1;
                // The resume discipline: after-exclusive read at the
                // LAST FETCHED seq. Any cursor beyond it skips.
                after = events.last().expect("checked nonempty").seq;
                collected.extend(events.iter().map(|event| event.seq));
                if complete {
                    break;
                }
            }
            other => panic!("expected page, got {other:?}"),
        }
    }
    assert!(pages >= 3, "the walk must actually paginate: {pages} pages");
    assert_eq!(collected, (1..=TOTAL).collect::<Vec<_>>());
}

/// `read_config` returns exactly the bytes `create_stream` wrote
/// (verified through the genesis envelope), and `None` for a stream
/// that does not exist.
#[tokio::test]
async fn read_config_returns_genesis_payload_or_absence() {
    let streams = streams_on_in_memory_store();
    let config = b"{\"sid\":\"repo:demo/hello\",\"ref\":\"refs/heads/main\"}".as_slice();
    let stream = streams.create_stream(config).await.unwrap();
    assert_eq!(
        streams.read_config(&stream).await.unwrap().as_deref(),
        Some(config)
    );

    // Empty config round-trips as an empty payload, not absence.
    let bare = streams.create_stream(&[]).await.unwrap();
    assert_eq!(streams.read_config(&bare).await.unwrap(), Some(Vec::new()));

    let ghost = StreamId::new("sghost-config").unwrap();
    assert_eq!(streams.read_config(&ghost).await.unwrap(), None);
}

/// S3 (in-memory leg): a byte-identical re-append converges to the
/// original receipt — no duplicate event. (The lost-response leg is
/// `streams_loopback.rs`.)
#[tokio::test]
async fn s3_idempotent_reappend_converges() {
    let streams = streams_on_in_memory_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    let first = streams
        .append(
            &stream,
            &schema("idem.v1"),
            &event("only-once"),
            b"same bytes",
        )
        .await
        .unwrap();
    let second = streams
        .append(
            &stream,
            &schema("idem.v1"),
            &event("only-once"),
            b"same bytes",
        )
        .await
        .unwrap();
    assert_eq!(
        first.seq, second.seq,
        "idempotent re-append returns the same receipt"
    );
    match streams.read(&stream, 0, 100).await {
        Replay::Page {
            events, complete, ..
        } => {
            assert!(complete);
            assert_eq!(events.len(), 1, "no duplicate event");
            assert_eq!(events[0].stable_event_id.as_str(), "only-once");
        }
        other => panic!("expected page, got {other:?}"),
    }
}

/// Ruled contract (ADR 0017 addendum): inside the idempotency
/// window, the same stable event id with DIFFERENT content is a
/// typed `IdempotencyConflict` — never a silent second landing at
/// another seq.
#[tokio::test]
async fn idempotency_window_conflict_is_typed() {
    let streams = streams_on_in_memory_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    let first = streams
        .append(
            &stream,
            &schema("conflict.v1"),
            &event("same-id"),
            b"original",
        )
        .await
        .unwrap();
    assert_eq!(first.seq, 1);

    // Same id, changed payload, in window.
    let error = streams
        .append(
            &stream,
            &schema("conflict.v1"),
            &event("same-id"),
            b"changed",
        )
        .await
        .unwrap_err();
    match &error {
        StreamsError::IdempotencyConflict {
            stream: conflict_stream,
            stable_event_id,
            conflicting_seq,
        } => {
            assert_eq!(conflict_stream, &stream);
            assert_eq!(stable_event_id.as_str(), "same-id");
            assert_eq!(*conflicting_seq, 1, "names the landed event's seq");
        }
        other => panic!("expected IdempotencyConflict, got {other:?}"),
    }

    // Same id, changed schema: also a conflict.
    let error = streams
        .append(
            &stream,
            &schema("conflict.v2"),
            &event("same-id"),
            b"original",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, StreamsError::IdempotencyConflict { .. }),
        "got {error:?}"
    );

    // Nothing landed from the conflicts: the log holds the original.
    match streams.read(&stream, 0, 100).await {
        Replay::Page {
            events, complete, ..
        } => {
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].payload.as_ref(), b"original");
            assert!(complete);
        }
        other => panic!("expected page, got {other:?}"),
    }
}

/// Ruled contract (ADR 0017 addendum): beyond the pre-scan window,
/// re-appending the same logical event (identical stable id and
/// bytes) lands as a NEW event. Duplicates of a logical event are
/// possible by contract — at-least-once; consumers dedupe by stable
/// event id.
#[tokio::test]
async fn idempotency_beyond_window_reappend_lands_as_new_event() {
    let streams = streams_on_in_memory_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    let first = streams
        .append(
            &stream,
            &schema("dedup.v1"),
            &event("logical-1"),
            b"payload",
        )
        .await
        .unwrap();
    // Push the event beyond the bounded window (16 seqs below the
    // max; the tail-hint floor only ever narrows the window from
    // below).
    for index in 2..=20u64 {
        streams
            .append(
                &stream,
                &schema("dedup.v1"),
                &event(&format!("logical-{index}")),
                b"payload",
            )
            .await
            .unwrap();
    }
    let again = streams
        .append(
            &stream,
            &schema("dedup.v1"),
            &event("logical-1"),
            b"payload",
        )
        .await
        .unwrap();
    assert_eq!(first.seq, 1);
    assert_eq!(
        again.seq, 21,
        "out of window: identical bytes land as a NEW event"
    );
    match streams.read(&stream, 0, 100).await {
        Replay::Page {
            events, complete, ..
        } => {
            assert_eq!(events.len(), 21);
            let occurrences = events
                .iter()
                .filter(|envelope| envelope.stable_event_id.as_str() == "logical-1")
                .count();
            assert_eq!(
                occurrences, 2,
                "duplicate by contract — consumers dedupe by stable event id"
            );
            assert!(complete);
        }
        other => panic!("expected page, got {other:?}"),
    }
}

/// S4: damage is loud and named — a deleted mid-log object yields
/// Corrupt naming the missing seq; deleting every accelerator leaves
/// full function at degraded cost.
#[tokio::test]
async fn s4_damage_loud_and_named() {
    let (streams, kernel) = streams_on_in_memory_store_with_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=5u64 {
        streams
            .append(
                &stream,
                &schema("damage.v1"),
                &event(&format!("d{index}")),
                &[],
            )
            .await
            .unwrap();
    }
    // Delete the mid-log object at seq 3 directly (kernel keyspace).
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    keyspace
        .delete(&format!("{}/log/00000000000000000003", stream.as_str()))
        .await
        .unwrap();
    match streams.read(&stream, 0, 100).await {
        Replay::Corrupt {
            missing_or_mismatched,
        } => {
            assert_eq!(missing_or_mismatched, vec![3], "damage names the seq");
        }
        other => panic!("expected Corrupt, got {other:?}"),
    }
    // A read whose window starts inside the hole is corrupt too.
    match streams.read(&stream, 2, 10).await {
        Replay::Corrupt {
            missing_or_mismatched,
        } => {
            assert_eq!(missing_or_mismatched, vec![3]);
        }
        other => panic!("expected Corrupt from in-window hole, got {other:?}"),
    }

    // Delete ALL accelerators: the tail hint object. Full function at
    // degraded cost — reads still replay and qualify completeness.
    keyspace
        .delete(&format!("{}/tail", stream.as_str()))
        .await
        .unwrap();
    match streams.read(&stream, 0, 2).await {
        Replay::Page { events, complete } => {
            assert_eq!(events.len(), 2);
            assert!(!complete);
        }
        other => panic!("expected partial page, got {other:?}"),
    }
    // Removing the accelerator cannot turn known log damage into a
    // writable stream. Append scans the observed recent window and
    // keeps the missing event loud.
    let error = streams
        .append(&stream, &schema("damage.v1"), &event("d6"), &[])
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        StreamsError::Corrupt {
            missing_or_mismatched,
            ..
        } if missing_or_mismatched == vec![3]
    ));
}

/// S5: v1 is append-only — no operation removes log objects. A battery
/// of every API operation leaves the stored log object set unchanged.
#[tokio::test]
async fn s5_no_delete_path_exists() {
    let (streams, kernel) = streams_on_in_memory_store_with_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=4u64 {
        streams
            .append(
                &stream,
                &schema("trim.v1"),
                &event(&format!("t{index}")),
                &[],
            )
            .await
            .unwrap();
    }
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    let log_objects = || async {
        let mut keys = keyspace
            .list_after(
                Some(&format!("{}/log/00000000000000000000", stream.as_str())),
                1000,
            )
            .await
            .unwrap();
        keys.retain(|key| key.contains("/log/"));
        keys.sort();
        keys
    };
    let before = log_objects().await;
    // Every read-side operation.
    let _ = streams.read(&stream, 0, 100).await;
    let _ = streams.read(&stream, 2, 1).await;
    let _ = streams.read_cursor(&stream, "worker").await.unwrap();
    let _ = streams.advance_cursor(&stream, "worker", 2).await.unwrap();
    // More appends (which also advance the accelerator).
    let _ = streams
        .append(&stream, &schema("trim.v1"), &event("t5"), &[])
        .await
        .unwrap();
    let after = log_objects().await;
    assert!(before.iter().all(|key| after.contains(key)));
    assert_eq!(
        after.len(),
        before.len() + 1,
        "only additions, never deletions"
    );
}

/// S7: schema evolution — unknown schema ids replay opaquely with the
/// id preserved; malformed envelopes are errors, never skips.
#[tokio::test]
async fn s7_schema_evolution_opaque_and_malformed_loud() {
    let streams = streams_on_in_memory_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    streams
        .append(
            &stream,
            &schema("old.shape.v1"),
            &event("old"),
            b"old payload",
        )
        .await
        .unwrap();
    streams
        .append(
            &stream,
            &schema("new.shape.v9"),
            &event("new"),
            b"new payload",
        )
        .await
        .unwrap();
    match streams.read(&stream, 0, 100).await {
        Replay::Page { events, .. } => {
            assert_eq!(events[0].schema_id.as_str(), "old.shape.v1");
            assert_eq!(events[0].payload.as_ref(), b"old payload");
            assert_eq!(events[1].schema_id.as_str(), "new.shape.v9");
            assert_eq!(events[1].payload.as_ref(), b"new payload");
        }
        other => panic!("expected page, got {other:?}"),
    }

    // A malformed envelope in the log is an error naming its seq.
    let (streams, kernel) = streams_on_in_memory_store_with_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    streams
        .append(&stream, &schema("ok.v1"), &event("ok-1"), b"fine")
        .await
        .unwrap();
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    keyspace
        .create(
            &format!("{}/log/00000000000000000002", stream.as_str()),
            bytes::Bytes::from_static(b"{ not an envelope"),
        )
        .await
        .unwrap();
    match streams.read(&stream, 0, 100).await {
        Replay::Corrupt {
            missing_or_mismatched,
        } => {
            assert_eq!(missing_or_mismatched, vec![2]);
        }
        other => panic!("expected Corrupt for malformed envelope, got {other:?}"),
    }
}

/// S8: encoding boundaries — key↔envelope mismatch detection, and the
/// 20-digit seq ceiling (u64::MAX is representable; there is no
/// successor — typed SeqExhausted, never a wrap).
#[tokio::test]
async fn s8_encoding_boundaries() {
    let (streams, kernel) = streams_on_in_memory_store_with_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=6u64 {
        streams
            .append(
                &stream,
                &schema("bound.v1"),
                &event(&format!("b{index}")),
                &[],
            )
            .await
            .unwrap();
    }
    // Envelope claiming seq 5 stored under the key for seq 7.
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    let lying = support::hand_envelope(stream.as_str(), 5, "liar", "lie.v1", b"x");
    keyspace
        .create(
            &format!("{}/log/00000000000000000007", stream.as_str()),
            lying,
        )
        .await
        .unwrap();
    match streams.read(&stream, 0, 100).await {
        Replay::Corrupt {
            missing_or_mismatched,
        } => {
            assert_eq!(
                missing_or_mismatched,
                vec![7],
                "key/envelope mismatch detected"
            );
        }
        other => panic!("expected Corrupt for key/envelope mismatch, got {other:?}"),
    }

    // u64::MAX is representable (20 digits); no successor exists.
    let (streams, kernel) = streams_on_in_memory_store_with_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    let max = u64::MAX;
    let envelope = support::hand_envelope(stream.as_str(), max, "ceiling", "ceil.v1", b"c");
    keyspace
        .create(&format!("{}/log/{max:020}", stream.as_str()), envelope)
        .await
        .unwrap();
    // The ceiling event replays (its key is exactly 20 digits). The
    // hand-seeded log carries no tail hint, so the first read
    // withholds completeness (no verified witness) and recovers one;
    // the second certifies — nothing can exist past u64::MAX.
    match streams.read(&stream, max - 1, 1).await {
        Replay::Page { complete, .. } => {
            assert!(!complete, "no verified witness -> complete withheld");
        }
        other => panic!("expected ceiling page, got {other:?}"),
    }
    match streams.read(&stream, max - 1, 1).await {
        Replay::Page {
            events, complete, ..
        } => {
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].seq, max);
            assert!(complete, "nothing can exist past u64::MAX");
        }
        other => panic!("expected ceiling page, got {other:?}"),
    }
    // Appending past it is typed, never a wrap.
    let error = streams
        .append(&stream, &schema("ceil.v1"), &event("past"), b"p")
        .await
        .unwrap_err();
    assert!(
        matches!(error, StreamsError::SeqExhausted(_)),
        "got {error:?}"
    );
}

/// Cursor contract: monotonic-only, validates the target, missing
/// cursor = replay from start.
#[tokio::test]
async fn cursors_monotonic_and_validated() {
    let streams = streams_on_in_memory_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=3u64 {
        streams
            .append(
                &stream,
                &schema("cursor.v1"),
                &event(&format!("c{index}")),
                &[],
            )
            .await
            .unwrap();
    }
    assert!(
        streams
            .read_cursor(&stream, "worker")
            .await
            .unwrap()
            .is_none()
    );
    let cursor = streams.advance_cursor(&stream, "worker", 2).await.unwrap();
    assert_eq!(cursor.seq, 2);
    assert_eq!(cursor.event_id, "c2");
    // Not monotonic (backwards).
    let error = streams
        .advance_cursor(&stream, "worker", 1)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        StreamsError::CursorNotMonotonic {
            current: 2,
            target: 1,
            ..
        }
    ));
    // Equal target is idempotent (a landed-but-unacknowledged advance
    // converges on retry).
    let again = streams.advance_cursor(&stream, "worker", 2).await.unwrap();
    assert_eq!(again.seq, 2);
    // Target must exist.
    let error = streams
        .advance_cursor(&stream, "worker", 9)
        .await
        .unwrap_err();
    assert!(matches!(error, StreamsError::EventMissing { seq: 9, .. }));
    // Consumers are independent pointers.
    let other = streams.advance_cursor(&stream, "audit", 3).await.unwrap();
    assert_eq!(other.seq, 3);
    assert_eq!(
        streams
            .read_cursor(&stream, "worker")
            .await
            .unwrap()
            .unwrap()
            .seq,
        2
    );
    // Replay from a cursor position continues after it.
    match streams.read(&stream, cursor.seq, 10).await {
        Replay::Page {
            events, complete, ..
        } => {
            assert!(complete);
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].stable_event_id.as_str(), "c3");
        }
        other => panic!("expected page, got {other:?}"),
    }
}

#[tokio::test]
async fn persisted_cursor_integrity_is_verified_on_read_and_advance() {
    let (streams, kernel) = streams_on_in_memory_store_with_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    let receipt = streams
        .append(&stream, &schema("cursor.v1"), &event("c1"), b"one")
        .await
        .unwrap();
    streams
        .advance_cursor(&stream, "worker", receipt.seq)
        .await
        .unwrap();

    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    let cursor_key = format!("{}/cursors/worker", stream.as_str());
    replace_cursor_value(
        &keyspace,
        &cursor_key,
        Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "format_version": 1,
                "stream_id": stream.as_str(),
                "seq": receipt.seq,
                "event_id": "forged-event-id",
            }))
            .unwrap(),
        ),
    )
    .await;

    assert!(matches!(
        streams.read_cursor(&stream, "worker").await,
        Err(StreamsError::CursorCorrupt { .. })
    ));
    assert!(matches!(
        streams.advance_cursor(&stream, "worker", receipt.seq).await,
        Err(StreamsError::CursorCorrupt { .. })
    ));

    replace_cursor_value(
        &keyspace,
        &cursor_key,
        Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "format_version": 99,
                "stream_id": stream.as_str(),
                "seq": receipt.seq,
                "event_id": "c1",
            }))
            .unwrap(),
        ),
    )
    .await;
    assert!(matches!(
        streams.read_cursor(&stream, "worker").await,
        Err(StreamsError::CursorCorrupt { .. })
    ));

    replace_cursor_value(&keyspace, &cursor_key, Bytes::from_static(b"not-a-cursor")).await;
    assert!(matches!(
        streams.read_cursor(&stream, "worker").await,
        Err(StreamsError::CursorCorrupt { .. })
    ));
}

async fn replace_cursor_value(keyspace: &yeetz_s3_kernel::AtomicKeyspace, key: &str, value: Bytes) {
    let (_, etag) = keyspace.get_with_etag(key).await.unwrap().unwrap();
    keyspace.compare_exchange(key, &etag, value).await.unwrap();
}

/// Append to a nonexistent stream is typed StreamNotFound.
#[tokio::test]
async fn append_to_missing_stream_is_typed() {
    let streams = streams_on_in_memory_store();
    let ghost = StreamId::new("snever-created").unwrap();
    let error = streams
        .append(&ghost, &schema("x.v1"), &event("e"), b"p")
        .await
        .unwrap_err();
    assert!(matches!(error, StreamsError::StreamNotFound(_)));
}

#[tokio::test]
async fn append_rejects_corrupt_genesis_and_recent_events() {
    let (streams, kernel) = streams_on_in_memory_store_with_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    replace_keyspace_value(
        &keyspace,
        &format!("{}/log/{:020}", stream.as_str(), 0),
        Bytes::from_static(b"not-a-stream-envelope"),
    )
    .await;
    let error = streams
        .append(
            &stream,
            &schema("x.v1"),
            &event("after-corrupt-genesis"),
            b"p",
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        StreamsError::Corrupt {
            missing_or_mismatched,
            ..
        } if missing_or_mismatched == vec![0]
    ));

    let (streams, kernel) = streams_on_in_memory_store_with_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    streams
        .append(&stream, &schema("x.v1"), &event("first"), b"one")
        .await
        .unwrap();
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    replace_keyspace_value(
        &keyspace,
        &format!("{}/log/{:020}", stream.as_str(), 1),
        Bytes::from_static(b"not-a-stream-envelope"),
    )
    .await;
    let error = streams
        .append(&stream, &schema("x.v1"), &event("second"), b"two")
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        StreamsError::Corrupt {
            missing_or_mismatched,
            ..
        } if missing_or_mismatched == vec![1]
    ));
}

#[tokio::test]
async fn unverified_high_tail_hint_cannot_disable_recent_idempotency() {
    let (streams, kernel) = streams_on_in_memory_store_with_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    streams
        .append(&stream, &schema("x.v1"), &event("same-id"), b"original")
        .await
        .unwrap();
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    replace_keyspace_value(
        &keyspace,
        &format!("{}/tail", stream.as_str()),
        Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "format_version": 1,
                "highest_validated_dense_seq": 100,
                "terminal_record_digest": "not-the-record-digest",
            }))
            .unwrap(),
        ),
    )
    .await;

    let error = streams
        .append(&stream, &schema("x.v1"), &event("same-id"), b"changed")
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        StreamsError::IdempotencyConflict {
            conflicting_seq: 1,
            ..
        }
    ));
}

#[tokio::test]
async fn unsupported_tail_hint_is_not_completeness_evidence_and_self_heals() {
    let (streams, kernel) = streams_on_in_memory_store_with_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    streams
        .append(&stream, &schema("x.v1"), &event("one"), b"one")
        .await
        .unwrap();
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    let event_bytes = keyspace
        .get(&format!("{}/log/{:020}", stream.as_str(), 1))
        .await
        .unwrap()
        .unwrap();
    replace_keyspace_value(
        &keyspace,
        &format!("{}/tail", stream.as_str()),
        Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "format_version": 99,
                "highest_validated_dense_seq": 1,
                "terminal_record_digest": hex::encode(Sha256::digest(&event_bytes)),
            }))
            .unwrap(),
        ),
    )
    .await;

    match streams.read(&stream, 0, 10).await {
        Replay::Page { complete, .. } => assert!(!complete),
        other => panic!("expected uncertified page, got {other:?}"),
    }
    match streams.read(&stream, 0, 10).await {
        Replay::Page { complete, .. } => assert!(complete),
        other => panic!("expected certified page after repair, got {other:?}"),
    }
}

async fn replace_keyspace_value(
    keyspace: &yeetz_s3_kernel::AtomicKeyspace,
    key: &str,
    value: Bytes,
) {
    let (_, etag) = keyspace.get_with_etag(key).await.unwrap().unwrap();
    keyspace.compare_exchange(key, &etag, value).await.unwrap();
}

pub(crate) fn streams_on_in_memory_store_with_store() -> (Streams, KernelHandle) {
    let kernel = KernelHandle::with_in_memory_store("streams-contract");
    (Streams::new(&kernel).unwrap(), kernel)
}

/// R2 (batch 5): a read that would start below the certified trim
/// floor is `OffsetExpired` — a typed boundary, never an empty page,
/// never corruption — both before the sweeper runs (the certificate
/// is the boundary, not object absence) and after it. Reads at or
/// above the floor are unchanged.
#[tokio::test]
async fn r2_read_below_trim_floor_is_offset_expired_not_empty_or_corrupt() {
    let streams = streams_on_in_memory_store();
    let stream = streams.create_stream(b"cfg").await.unwrap();
    for index in 1..=10u64 {
        streams
            .append(&stream, &schema("r.v1"), &event(&format!("r{index}")), &[])
            .await
            .unwrap();
    }

    // No certificate yet: gc is a no-op, reads are whole-log.
    let noop = streams.gc(&stream).await.unwrap();
    assert_eq!(noop.deleted, 0);
    assert!(matches!(
        streams.read(&stream, 0, 100).await,
        Replay::Page { complete: true, .. }
    ));

    streams.trim(&stream, 6).await.unwrap();
    assert_eq!(streams.trim_floor(&stream).await.unwrap(), Some(6));

    // Pre-GC: objects below the floor still exist, but the
    // certificate already rules the read.
    for after in [0u64, 4] {
        match streams.read(&stream, after, 100).await {
            Replay::OffsetExpired { first_retained } => assert_eq!(first_retained, 6),
            other => panic!("expected OffsetExpired, got {other:?}"),
        }
    }
    // The boundary itself is retained: first wanted seq == floor.
    match streams.read(&stream, 5, 100).await {
        Replay::Page { events, complete } => {
            assert_eq!(
                events.iter().map(|event| event.seq).collect::<Vec<_>>(),
                (6..=10).collect::<Vec<_>>()
            );
            assert!(complete);
        }
        other => panic!("expected page above the floor, got {other:?}"),
    }

    // Sweep: exactly the five below-floor events go; replay above the
    // floor is unchanged; the genesis (config) survives.
    let report = streams.gc(&stream).await.unwrap();
    assert_eq!(report.deleted, 5);
    match streams.read(&stream, 5, 100).await {
        Replay::Page { events, complete } => {
            assert_eq!(
                events.iter().map(|event| event.seq).collect::<Vec<_>>(),
                (6..=10).collect::<Vec<_>>()
            );
            assert!(complete);
        }
        other => panic!("expected dense replay above the floor, got {other:?}"),
    }
    assert!(matches!(
        streams.read(&stream, 4, 100).await,
        Replay::OffsetExpired { first_retained: 6 }
    ));
    assert_eq!(
        streams.read_config(&stream).await.unwrap().as_deref(),
        Some(b"cfg".as_slice())
    );
}

/// R6 (batch 5): trim integrates with the write and cursor paths —
/// append allocates above the floor (no resurrection below it), dense
/// replay above the floor continues through the new events, and a
/// cursor below the floor surfaces `OffsetExpired`, never
/// `CursorCorrupt` (trim-induced absence is not damage).
#[tokio::test]
async fn r6_streams_trim_append_and_cursor_respect_the_floor() {
    let streams = streams_on_in_memory_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=10u64 {
        streams
            .append(&stream, &schema("r.v1"), &event(&format!("c{index}")), &[])
            .await
            .unwrap();
    }
    // A cursor below the future floor, acked while its event exists.
    streams.advance_cursor(&stream, "worker", 3).await.unwrap();

    streams.trim(&stream, 6).await.unwrap();
    streams.gc(&stream).await.unwrap();

    // Append lands above the floor — never below the certificate.
    let receipt = streams
        .append(&stream, &schema("r.v1"), &event("c11"), &[])
        .await
        .unwrap();
    assert_eq!(receipt.seq, 11);
    match streams.read(&stream, 5, 100).await {
        Replay::Page { events, complete } => {
            assert_eq!(
                events.iter().map(|event| event.seq).collect::<Vec<_>>(),
                (6..=11).collect::<Vec<_>>()
            );
            assert!(complete);
        }
        other => panic!("expected dense replay through the new event, got {other:?}"),
    }

    // The swept cursor is OffsetExpired, not corrupt; re-advancing to
    // the swept target is rejected the same way; advancing above the
    // floor still works.
    let swept = streams.read_cursor(&stream, "worker").await.unwrap_err();
    assert!(matches!(
        swept,
        StreamsError::OffsetExpired {
            first_retained: 6,
            ..
        }
    ));
    let backwards = streams
        .advance_cursor(&stream, "worker", 3)
        .await
        .unwrap_err();
    assert!(matches!(
        backwards,
        StreamsError::OffsetExpired {
            first_retained: 6,
            ..
        }
    ));
    streams.advance_cursor(&stream, "worker", 8).await.unwrap();
}

/// R7 (streams leg): a stale writer resurrecting a log object below
/// the certified floor (raw keyspace create — a fresh version-0
/// lifetime) changes nothing the API serves: reads below the floor
/// stay `OffsetExpired` (the certificate, not object absence, is the
/// boundary), the sweeper re-collects the zombie, and a lower trim
/// proposal stays rejected.
#[tokio::test]
async fn r7_streams_resurrection_rejected_by_certificate_and_reswept() {
    let (streams, kernel) = streams_on_in_memory_store_with_store();
    let keyspace = support::streams_keyspace(&kernel);
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=10u64 {
        streams
            .append(&stream, &schema("r.v1"), &event(&format!("z{index}")), &[])
            .await
            .unwrap();
    }
    streams.trim(&stream, 6).await.unwrap();
    streams.gc(&stream).await.unwrap();

    // The stale writer lands a byte-valid envelope at swept seq 3.
    keyspace
        .create(
            &format!("{}/log/{:020}", stream.as_str(), 3u64),
            support::hand_envelope(stream.as_str(), 3, "zombie", "z.v1", b"z"),
        )
        .await
        .unwrap();

    // The certificate rules the read: OffsetExpired, never the zombie.
    assert!(matches!(
        streams.read(&stream, 2, 100).await,
        Replay::OffsetExpired { first_retained: 6 }
    ));

    // Idempotent GC re-collects the resurrected object.
    let report = streams.gc(&stream).await.unwrap();
    assert_eq!(report.deleted, 1);

    // A lower trim stays rejected by the certificate.
    let rejected = streams.trim(&stream, 4).await.unwrap_err();
    assert!(matches!(
        rejected,
        StreamsError::InvalidArgument(message) if message.contains("trim not monotone")
    ));
}
