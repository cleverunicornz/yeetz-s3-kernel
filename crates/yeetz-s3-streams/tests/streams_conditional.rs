//! Conditional creation and expected-append contracts (the D7/P7/O7
//! sibling suite, shared contract 2026-09-09): caller-ID stream
//! creation (`c*`) and predecessor-pinned appends (`e*`).
//!
//! Every case observes real kernel behavior — the in-memory kernel
//! for pure logic outcomes, the loopback counterpart for one-shot
//! fault cuts (refused / applied-but-unacknowledged), deterministic
//! request pauses parked immediately before/after the target PUT (so
//! concurrent trim/GC races are witnessed without timing sleeps),
//! stale-LIST snapshots, LIST/GET contradictions, and wire witnesses
//! (no tail writes, no writes at any alternate position). No source
//! assertions, no mock echoes. Pause waits are bounded: an unexpected
//! request shape fails the test, it never hangs.

mod support;

use std::time::Duration;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use support::loopback::{FaultPhase, Loopback, RequestRecord, StorageOp};
use support::{hand_envelope, stream_id, streams_keyspace, streams_on_store};
use yeetz_s3_kernel::AtomicKeyspace;
use yeetz_s3_streams::{
    AppendExpectedEffect, AppendExpectedError, AppendExpectedFailure, AppendReceipt,
    CreateStreamError, CreateStreamOutcome, EventRef, ExpiredSubject, Replay, SchemaId,
    StableEventId, StreamId, Streams, StreamsError,
};

/// Upper bound on deterministic pause waits: an unexpected request
/// shape must fail loudly, not hang the suite.
const PAUSE_TIMEOUT: Duration = Duration::from_secs(30);

fn schema(value: &str) -> SchemaId {
    SchemaId::new(value).unwrap()
}

fn event(value: &str) -> StableEventId {
    StableEventId::new(value).unwrap()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// The genesis/config reference: seq 0, the genesis schema's fixed
/// stable id, digest of the config bytes — the identity
/// `Envelope::genesis` encodes.
fn genesis_ref(stream: &StreamId, config: &[u8]) -> EventRef {
    EventRef {
        stream_id: stream.clone(),
        seq: 0,
        stable_event_id: StableEventId::new("genesis").unwrap(),
        payload_sha256: sha256_hex(config),
    }
}

/// The identifying reference for an event with these bytes.
fn event_ref(stream: &StreamId, seq: u64, stable_id: &str, payload: &[u8]) -> EventRef {
    EventRef {
        stream_id: stream.clone(),
        seq,
        stable_event_id: StableEventId::new(stable_id).unwrap(),
        payload_sha256: sha256_hex(payload),
    }
}

/// Construct an ID that bypasses `new`'s validation — the
/// deserialization path the APIs must still validate themselves.
fn deserialized_stream_id(raw: &str) -> StreamId {
    serde_json::from_str(&serde_json::to_string(raw).unwrap()).unwrap()
}

fn deserialized_schema_id(raw: &str) -> SchemaId {
    serde_json::from_str(&serde_json::to_string(raw).unwrap()).unwrap()
}

fn deserialized_event_id(raw: &str) -> StableEventId {
    serde_json::from_str(&serde_json::to_string(raw).unwrap()).unwrap()
}

/// Streams plus its keyspace (for test-side seeding/damage).
fn in_memory() -> (Streams, AtomicKeyspace) {
    let kernel = yeetz_s3_kernel::KernelHandle::with_in_memory_store("streams-conditional");
    let streams = streams_on_store(&kernel);
    (streams, streams_keyspace(&kernel))
}

async fn counterpart_streams() -> (Loopback, Streams) {
    let loopback = Loopback::start().await;
    let streams = streams_on_store(&loopback.kernel());
    (loopback, streams)
}

/// Keyspace-relative log key of one seq.
fn log_key(stream: &StreamId, seq: u64) -> String {
    format!("{}/log/{seq:020}", stream.as_str())
}

/// The PHYSICAL key the loopback sees (kernel roots included).
fn wire_log_key(stream: &StreamId, seq: u64) -> String {
    format!("keyspace/streams/v1/{}/log/{seq:020}", stream.as_str())
}

/// The PHYSICAL trim-certificate key the loopback sees.
fn wire_cert_key(stream: &StreamId, first_retained: u64) -> String {
    format!(
        "keyspace/streams/v1/{}/trims/{first_retained:020}",
        stream.as_str()
    )
}

/// Keys under this stream's scope, in store order.
async fn stream_objects(keyspace: &AtomicKeyspace, stream: &StreamId) -> Vec<String> {
    let prefix = format!("{}/", stream.as_str());
    let mut objects = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let page = keyspace.list_after(after.as_deref(), 100).await.unwrap();
        let Some(last) = page.last() else {
            break;
        };
        after = Some(last.clone());
        objects.extend(page.into_iter().filter(|key| key.starts_with(&prefix)));
    }
    objects
}

/// Damage an existing object in place (raw CAS — no stream API).
async fn overwrite_key(keyspace: &AtomicKeyspace, key: &str, value: Bytes) {
    let (_, etag) = keyspace.get_with_etag(key).await.unwrap().unwrap();
    keyspace.compare_exchange(key, &etag, value).await.unwrap();
}

/// Unwrap the Storage kind or fail loudly with the whole error.
fn storage(err: &AppendExpectedError) -> &StreamsError {
    match &err.kind {
        AppendExpectedFailure::Storage(inner) => inner,
        other => panic!(
            "expected Storage failure, got {other:?} with effect {:?}",
            err.effect
        ),
    }
}

fn create_storage(err: &CreateStreamError) -> &StreamsError {
    match err {
        CreateStreamError::Storage(inner) => inner,
        other => panic!("expected Storage error, got {other:?}"),
    }
}

/// Every log-key PUT among these wire records, in order — the
/// alternate-position witness.
fn log_put_keys(records: &[RequestRecord], stream: &StreamId) -> Vec<String> {
    let prefix = format!("keyspace/streams/v1/{}/log/", stream.as_str());
    records
        .iter()
        .filter(|record| record.method == "PUT" && record.key.starts_with(&prefix))
        .map(|record| record.key.clone())
        .collect()
}

// --- Creation (c*) ---------------------------------------------------------

/// c1: a fresh caller-ID create lands `Created` and writes exactly one
/// object — the genesis identity at seq 0. No extra identity, no
/// sidecar, no second object.
#[tokio::test]
async fn c1_fresh_create_writes_only_the_genesis_identity() {
    let (streams, keyspace) = in_memory();
    let id = stream_id("cond-c1");
    let outcome = streams.create_stream_with_id(&id, b"cfg-c1").await.unwrap();
    assert_eq!(outcome, CreateStreamOutcome::Created);
    assert_eq!(
        streams.read_config(&id).await.unwrap().as_deref(),
        Some(b"cfg-c1".as_slice()),
        "the config reads back through the verified genesis"
    );
    assert_eq!(
        stream_objects(&keyspace, &id).await,
        vec![log_key(&id, 0)],
        "exactly one object: the genesis identity"
    );
}

/// c2: the identical retry (same ID, same config bytes) returns
/// `Existing` and retains the ID and its config — no second object.
#[tokio::test]
async fn c2_identical_retry_returns_existing_and_retains_config() {
    let (streams, keyspace) = in_memory();
    let id = stream_id("cond-c2");
    streams.create_stream_with_id(&id, b"cfg-c2").await.unwrap();
    let outcome = streams.create_stream_with_id(&id, b"cfg-c2").await.unwrap();
    assert_eq!(outcome, CreateStreamOutcome::Existing);
    assert_eq!(
        streams.read_config(&id).await.unwrap().as_deref(),
        Some(b"cfg-c2".as_slice())
    );
    assert_eq!(
        stream_objects(&keyspace, &id).await,
        vec![log_key(&id, 0)],
        "the retry retains the one genesis identity"
    );
}

/// c3: the same ID with a different config is a typed
/// `ConfigurationConflict`; the incumbent is never overwritten.
#[tokio::test]
async fn c3_different_config_conflicts_and_never_overwrites() {
    let (streams, keyspace) = in_memory();
    let id = stream_id("cond-c3");
    streams
        .create_stream_with_id(&id, b"incumbent")
        .await
        .unwrap();
    let conflict = streams
        .create_stream_with_id(&id, b"pretender")
        .await
        .unwrap_err();
    match &conflict {
        CreateStreamError::ConfigurationConflict { stream } => assert_eq!(stream, &id),
        other => panic!("expected ConfigurationConflict, got {other:?}"),
    }
    assert_eq!(
        streams.read_config(&id).await.unwrap().as_deref(),
        Some(b"incumbent".as_slice()),
        "the conflict did not overwrite the incumbent config"
    );
    assert_eq!(
        stream_objects(&keyspace, &id).await,
        vec![log_key(&id, 0)],
        "no second genesis identity was written"
    );
}

/// c4: an incumbent genesis that fails envelope verification is typed
/// `Storage(Corrupt)` naming seq 0 — never adopted, never overwritten.
#[tokio::test]
async fn c4_corrupt_incumbent_is_typed_storage_corrupt() {
    let (streams, keyspace) = in_memory();
    let id = stream_id("cond-c4");
    streams.create_stream_with_id(&id, b"cfg-c4").await.unwrap();
    overwrite_key(
        &keyspace,
        &log_key(&id, 0),
        Bytes::from_static(b"not-an-envelope"),
    )
    .await;
    let error = streams
        .create_stream_with_id(&id, b"cfg-c4")
        .await
        .unwrap_err();
    assert!(
        matches!(
            create_storage(&error),
            StreamsError::Corrupt {
                missing_or_mismatched,
                ..
            } if missing_or_mismatched.contains(&0)
        ),
        "corrupt incumbent is Storage(Corrupt) at seq 0, got {error:?}"
    );
}

/// c5: two concurrent creates with the same ID and identical bytes:
/// exactly one `Created`, the other converges on `Existing`, and one
/// genesis identity exists — regardless of interleaving order.
#[tokio::test]
async fn c5_concurrent_same_id_yields_one_created_one_existing() {
    let (streams, keyspace) = in_memory();
    let id = stream_id("cond-c5");
    let (first, second) = tokio::join!(
        streams.create_stream_with_id(&id, b"cfg-c5"),
        streams.create_stream_with_id(&id, b"cfg-c5"),
    );
    let outcomes = [first.unwrap(), second.unwrap()];
    assert!(
        outcomes.contains(&CreateStreamOutcome::Created),
        "exactly one winner exists: {outcomes:?}"
    );
    assert!(
        outcomes.contains(&CreateStreamOutcome::Existing),
        "the loser converges on the incumbent: {outcomes:?}"
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == CreateStreamOutcome::Created)
            .count(),
        1,
        "one winner per identity"
    );
    assert_eq!(
        stream_objects(&keyspace, &id).await,
        vec![log_key(&id, 0)],
        "exactly one genesis identity after the race"
    );
}

/// c6: a create whose PUT applied but lost its response (one-shot
/// After fault — consumed by that single request) errors; the same-ID,
/// same-bytes retry then converges on `Existing` without writing a
/// second identity.
#[tokio::test]
async fn c6_lost_create_response_then_same_id_retry_returns_existing() {
    let (loopback, streams) = counterpart_streams().await;
    let keyspace = streams_keyspace(&loopback.kernel());
    let id = stream_id("cond-c6");
    loopback
        .arm_fault(
            StorageOp::Put,
            Some(&wire_log_key(&id, 0)),
            FaultPhase::After,
        )
        .await;
    let lost = streams
        .create_stream_with_id(&id, b"cfg-c6")
        .await
        .unwrap_err();
    assert!(
        matches!(create_storage(&lost), StreamsError::Unavailable { .. }),
        "the lost response surfaces as Storage(Unavailable), got {lost:?}"
    );
    assert!(loopback.fault_fired(), "the cut fired");
    assert!(
        keyspace.get(&log_key(&id, 0)).await.unwrap().is_some(),
        "the cut create applied server-side"
    );
    // The one-shot fault is consumed; the retry runs unencumbered.
    let outcome = streams.create_stream_with_id(&id, b"cfg-c6").await.unwrap();
    assert_eq!(outcome, CreateStreamOutcome::Existing);
    assert_eq!(
        streams.read_config(&id).await.unwrap().as_deref(),
        Some(b"cfg-c6".as_slice())
    );
    let mut objects = stream_objects(&keyspace, &id).await;
    objects.sort();
    assert_eq!(objects, vec![log_key(&id, 0)]);
    loopback.shutdown();
}

/// c7: a deserialized-invalid stream ID is rejected by admission —
/// typed `Storage(InvalidArgument)` with zero storage requests.
#[tokio::test]
async fn c7_invalid_stream_id_rejected_before_any_io() {
    let (loopback, streams) = counterpart_streams().await;
    let base = loopback.request_count();
    let bad = deserialized_stream_id("bad/id");
    let error = streams
        .create_stream_with_id(&bad, b"cfg-c7")
        .await
        .unwrap_err();
    assert!(
        matches!(create_storage(&error), StreamsError::InvalidArgument(_)),
        "invalid ID is admission-typed, got {error:?}"
    );
    assert_eq!(
        loopback.request_count(),
        base,
        "no storage request left the client"
    );
    loopback.shutdown();
}

/// c8: a config whose canonical encoded genesis exceeds the envelope
/// bound is rejected before any effect — typed
/// `Storage(EnvelopeTooLarge)`, zero storage requests.
#[tokio::test]
async fn c8_oversized_genesis_rejected_before_any_io() {
    let (loopback, streams) = counterpart_streams().await;
    let base = loopback.request_count();
    let oversize = vec![b'x'; 16 * 1024 * 1024];
    let error = streams
        .create_stream_with_id(&stream_id("cond-c8"), &oversize)
        .await
        .unwrap_err();
    assert!(
        matches!(
            create_storage(&error),
            StreamsError::EnvelopeTooLarge { .. }
        ),
        "oversized encoded genesis is admission-typed, got {error:?}"
    );
    assert_eq!(
        loopback.request_count(),
        base,
        "no storage request left the client"
    );
    loopback.shutdown();
}

/// c9: an ID that passes stream validation but names the kernel's
/// reserved certificate scope (`trims`) is refused by the kernel's
/// permanent typed guard before I/O — mapped, not name-listed, into
/// `Storage(InvalidArgument)` with zero storage requests.
#[tokio::test]
async fn c9_kernel_reserved_scope_id_rejected_before_any_io() {
    let (loopback, streams) = counterpart_streams().await;
    let base = loopback.request_count();
    let reserved = StreamId::new("trims").unwrap();
    let error = streams
        .create_stream_with_id(&reserved, b"cfg-c9")
        .await
        .unwrap_err();
    assert!(
        matches!(create_storage(&error), StreamsError::InvalidArgument(_)),
        "reserved-scope genesis key is refused before I/O, got {error:?}"
    );
    assert_eq!(
        loopback.request_count(),
        base,
        "the kernel guard fires client-side: no storage request"
    );
    loopback.shutdown();
}

// --- Expected append (e*) --------------------------------------------------

/// e1: a fresh expected append lands at the exact successor of its
/// predecessor with a receipt carrying the envelope's identity.
#[tokio::test]
async fn e1_exact_successor_from_genesis() {
    let (streams, _keyspace) = in_memory();
    let stream = streams.create_stream(b"cfg-e1").await.unwrap();
    let pred = genesis_ref(&stream, b"cfg-e1");
    let receipt = streams
        .append_expected(&pred, &schema("cond.v1"), &event("e1-event"), b"payload-e1")
        .await
        .unwrap();
    assert_eq!(receipt.stream_id, stream);
    assert_eq!(receipt.seq, 1, "the exact successor of seq 0");
    assert_eq!(receipt.stable_event_id, event("e1-event"));
    assert_eq!(receipt.payload_sha256, sha256_hex(b"payload-e1"));
    match streams.read(&stream, 0, 10).await {
        Replay::Page { events, .. } => {
            assert_eq!(
                events
                    .iter()
                    .map(|envelope| (envelope.seq(), envelope.stable_event_id().as_str()))
                    .collect::<Vec<_>>(),
                vec![(1, "e1-event")],
                "replay observes the event at the exact successor"
            );
        }
        other => panic!("expected a page, got {other:?}"),
    }
}

/// e2: an exact retry after the suffix advanced past the target
/// reconciles on the target's bytes — the same receipt — and the
/// reconciliation performs ZERO writes: no retry PUT, no tail hint,
/// no write at any position.
#[tokio::test]
async fn e2_exact_retry_after_suffix_advance_writes_nothing() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(b"cfg-e2").await.unwrap();
    let pred = genesis_ref(&stream, b"cfg-e2");
    let schema = schema("cond.v2");
    let original = streams
        .append_expected(&pred, &schema, &event("e2-event"), b"p2")
        .await
        .unwrap();
    assert_eq!(original.seq, 1);
    // The suffix advances past the target through the ordinary
    // allocation path.
    streams
        .append(&stream, &schema, &event("e2-b"), b"pb")
        .await
        .unwrap();
    streams
        .append(&stream, &schema, &event("e2-c"), b"pc")
        .await
        .unwrap();
    let mark = loopback.request_log().len();
    let retry = streams
        .append_expected(&pred, &schema, &event("e2-event"), b"p2")
        .await
        .unwrap();
    assert_eq!(
        retry, original,
        "the retry converges on the original receipt"
    );
    let since = &loopback.request_log()[mark..];
    assert!(
        since.iter().all(|record| record.method != "PUT"),
        "reconciliation writes nothing, saw {since:?}"
    );
    match streams.read(&stream, 0, 10).await {
        Replay::Page { events, .. } => {
            assert_eq!(
                events
                    .iter()
                    .map(|envelope| envelope.stable_event_id().as_str())
                    .collect::<Vec<_>>(),
                vec!["e2-event", "e2-b", "e2-c"]
            );
        }
        other => panic!("expected a page, got {other:?}"),
    }
    loopback.shutdown();
}

/// e3: an exact retry still converges after the predecessor was
/// collected while the target remains retained (floor == target): the
/// reconciliation path proves the target bytes without reading the
/// predecessor.
#[tokio::test]
async fn e3_exact_retry_after_predecessor_trimmed_target_retained() {
    let (streams, _keyspace) = in_memory();
    let stream = streams.create_stream(b"cfg-e3").await.unwrap();
    let schema = schema("cond.v3");
    streams
        .append(&stream, &schema, &event("e3-one"), b"p1")
        .await
        .unwrap();
    let original = streams
        .append(&stream, &schema, &event("e3-two"), b"p2")
        .await
        .unwrap();
    assert_eq!(original.seq, 2);
    streams.trim(&stream, 2).await.unwrap();
    assert_eq!(streams.trim_floor(&stream).await.unwrap(), Some(2));
    let swept = streams.gc(&stream).await.unwrap();
    assert_eq!(swept.deleted, 1, "the predecessor (seq 1) is collected");
    let retry = streams
        .append_expected(
            &event_ref(&stream, 1, "e3-one", b"p1"),
            &schema,
            &event("e3-two"),
            b"p2",
        )
        .await
        .unwrap();
    assert_eq!(retry, original, "retained target converges on its receipt");
}

/// e4: the same stable id at the target with different payload bytes
/// is a typed IdempotencyConflict — not attempted, nothing written.
#[tokio::test]
async fn e4_same_id_payload_conflict_is_not_attempted() {
    let (streams, keyspace) = in_memory();
    let stream = streams.create_stream(b"cfg-e4").await.unwrap();
    let pred = genesis_ref(&stream, b"cfg-e4");
    let schema = schema("cond.v4");
    streams
        .append_expected(&pred, &schema, &event("shared-id"), b"original")
        .await
        .unwrap();
    let error = streams
        .append_expected(&pred, &schema, &event("shared-id"), b"different")
        .await
        .unwrap_err();
    assert!(
        matches!(storage(&error), StreamsError::IdempotencyConflict { .. }),
        "same id, different payload is typed, got {error:?}"
    );
    assert_eq!(error.effect, AppendExpectedEffect::NotAttempted);
    assert_eq!(
        stream_objects(&keyspace, &stream).await,
        vec![log_key(&stream, 0), log_key(&stream, 1)],
        "the conflict wrote nothing"
    );
}

/// e5: the same stable id with identical payload but a different
/// schema is the same typed conflict — the canonical bytes differ.
#[tokio::test]
async fn e5_same_id_schema_conflict_is_not_attempted() {
    let (streams, _keyspace) = in_memory();
    let stream = streams.create_stream(b"cfg-e5").await.unwrap();
    let pred = genesis_ref(&stream, b"cfg-e5");
    streams
        .append_expected(&pred, &schema("cond.v5a"), &event("shared-id"), b"same")
        .await
        .unwrap();
    let error = streams
        .append_expected(&pred, &schema("cond.v5b"), &event("shared-id"), b"same")
        .await
        .unwrap_err();
    assert!(
        matches!(storage(&error), StreamsError::IdempotencyConflict { .. }),
        "same id, different schema is typed, got {error:?}"
    );
    assert_eq!(error.effect, AppendExpectedEffect::NotAttempted);
}

/// e6: a verified different event occupying the target is a typed
/// PositionConflict naming the occupant — no interleave, no write at
/// the target or any other position.
#[tokio::test]
async fn e6_different_occupant_position_conflict_no_interleave() {
    let (streams, keyspace) = in_memory();
    let stream = streams.create_stream(b"cfg-e6").await.unwrap();
    let pred = genesis_ref(&stream, b"cfg-e6");
    let schema = schema("cond.v6");
    streams
        .append_expected(&pred, &schema, &event("occupant"), b"occupant-payload")
        .await
        .unwrap();
    let error = streams
        .append_expected(&pred, &schema, &event("e6-late"), b"e6-payload")
        .await
        .unwrap_err();
    match &error.kind {
        AppendExpectedFailure::PositionConflict {
            stream: conflicted,
            target_seq,
            occupant,
        } => {
            assert_eq!(conflicted, &stream);
            assert_eq!(*target_seq, 1);
            assert_eq!(
                *occupant,
                event_ref(&stream, 1, "occupant", b"occupant-payload"),
                "the conflict names the verified occupant"
            );
        }
        other => panic!("expected PositionConflict, got {other:?}"),
    }
    assert_eq!(error.effect, AppendExpectedEffect::NotAttempted);
    assert_eq!(
        stream_objects(&keyspace, &stream).await,
        vec![log_key(&stream, 0), log_key(&stream, 1)],
        "no interleave: the log is exactly the incumbent"
    );
}

/// e7: a missing predecessor (object gone, floor unchanged on the
/// re-read) is EventMissing naming the predecessor — not attempted.
#[tokio::test]
async fn e7_missing_predecessor_is_event_missing_not_attempted() {
    let (streams, keyspace) = in_memory();
    let stream = streams.create_stream(b"cfg-e7").await.unwrap();
    let schema = schema("cond.v7");
    streams
        .append(&stream, &schema, &event("e7-one"), b"p1")
        .await
        .unwrap();
    streams
        .append(&stream, &schema, &event("e7-two"), b"p2")
        .await
        .unwrap();
    keyspace.delete(&log_key(&stream, 2)).await.unwrap();
    let error = streams
        .append_expected(
            &event_ref(&stream, 2, "e7-two", b"p2"),
            &schema,
            &event("e7-three"),
            b"p3",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            storage(&error),
            StreamsError::EventMissing { seq, .. } if *seq == 2
        ),
        "missing predecessor names the predecessor seq, got {error:?}"
    );
    assert_eq!(error.effect, AppendExpectedEffect::NotAttempted);
}

/// e8: a predecessor object that fails envelope verification is typed
/// Corrupt naming the predecessor seq — not attempted.
#[tokio::test]
async fn e8_corrupt_predecessor_is_typed_corrupt() {
    let (streams, keyspace) = in_memory();
    let stream = streams.create_stream(b"cfg-e8").await.unwrap();
    let schema = schema("cond.v8");
    streams
        .append(&stream, &schema, &event("e8-one"), b"p1")
        .await
        .unwrap();
    streams
        .append(&stream, &schema, &event("e8-two"), b"p2")
        .await
        .unwrap();
    overwrite_key(
        &keyspace,
        &log_key(&stream, 2),
        Bytes::from_static(b"garbage"),
    )
    .await;
    let error = streams
        .append_expected(
            &event_ref(&stream, 2, "e8-two", b"p2"),
            &schema,
            &event("e8-three"),
            b"p3",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            storage(&error),
            StreamsError::Corrupt {
                missing_or_mismatched,
                ..
            } if missing_or_mismatched.contains(&2)
        ),
        "corrupt predecessor is typed Corrupt at its seq, got {error:?}"
    );
    assert_eq!(error.effect, AppendExpectedEffect::NotAttempted);
}

/// e9: a predecessor reference whose stable id or payload digest
/// disagrees with the verified envelope is PredecessorMismatch
/// carrying both the expected and the observed reference.
#[tokio::test]
async fn e9_mismatched_predecessor_names_expected_and_observed() {
    let (streams, _keyspace) = in_memory();
    let stream = streams.create_stream(b"cfg-e9").await.unwrap();
    let schema = schema("cond.v9");
    streams
        .append(&stream, &schema, &event("e9-real"), b"real-payload")
        .await
        .unwrap();
    let observed = event_ref(&stream, 1, "e9-real", b"real-payload");

    let wrong_id = EventRef {
        stable_event_id: StableEventId::new("e9-wrong").unwrap(),
        ..observed.clone()
    };
    let error = streams
        .append_expected(&wrong_id, &schema, &event("e9-next"), b"p")
        .await
        .unwrap_err();
    match &error.kind {
        AppendExpectedFailure::PredecessorMismatch {
            expected,
            observed: got,
        } => {
            assert_eq!(expected, &wrong_id);
            assert_eq!(got, &observed, "the mismatch names the verified envelope");
        }
        other => panic!("expected PredecessorMismatch, got {other:?}"),
    }
    assert_eq!(error.effect, AppendExpectedEffect::NotAttempted);

    // A well-formed digest of OTHER bytes fails verification the same
    // way — admission passes (canonical hex), identity does not.
    let wrong_digest = EventRef {
        payload_sha256: sha256_hex(b"other-payload"),
        ..observed.clone()
    };
    let error = streams
        .append_expected(&wrong_digest, &schema, &event("e9-next"), b"p")
        .await
        .unwrap_err();
    match &error.kind {
        AppendExpectedFailure::PredecessorMismatch {
            expected,
            observed: got,
        } => {
            assert_eq!(expected, &wrong_digest);
            assert_eq!(got, &observed);
        }
        other => panic!("expected PredecessorMismatch, got {other:?}"),
    }
    assert_eq!(error.effect, AppendExpectedEffect::NotAttempted);
}

/// e10: an absent target with a VERIFIED later event is HoleWitnessed
/// naming the later record — the hole is never filled, nothing is
/// written.
#[tokio::test]
async fn e10_verified_later_event_witnesses_hole_without_filling() {
    let (loopback, streams) = counterpart_streams().await;
    let keyspace = streams_keyspace(&loopback.kernel());
    let stream = streams.create_stream(b"cfg-e10").await.unwrap();
    let seeded = [
        (1u64, "h1", &b"p1"[..]),
        (2, "h2", &b"p2"[..]),
        (4, "h4", &b"p4"[..]),
    ];
    for (seq, stable_id, payload) in seeded {
        keyspace
            .create(
                &log_key(&stream, seq),
                hand_envelope(stream.as_str(), seq, stable_id, "cond.v10", payload),
            )
            .await
            .unwrap();
    }
    let mark = loopback.request_log().len();
    let error = streams
        .append_expected(
            &event_ref(&stream, 2, "h2", b"p2"),
            &schema("cond.v10"),
            &event("h3"),
            b"p3",
        )
        .await
        .unwrap_err();
    match &error.kind {
        AppendExpectedFailure::HoleWitnessed {
            stream: holed,
            target_seq,
            later,
        } => {
            assert_eq!(holed, &stream);
            assert_eq!(*target_seq, 3);
            assert_eq!(
                *later,
                event_ref(&stream, 4, "h4", b"p4"),
                "the later verified record witnesses the hole"
            );
        }
        other => panic!("expected HoleWitnessed, got {other:?}"),
    }
    assert_eq!(error.effect, AppendExpectedEffect::NotAttempted);
    assert!(
        !loopback.request_log()[mark..]
            .iter()
            .any(|record| record.method == "PUT"),
        "hole adjudication writes nothing"
    );
    assert!(
        keyspace.get(&log_key(&stream, 3)).await.unwrap().is_none(),
        "the hole is never filled"
    );
    loopback.shutdown();
}

/// e11: a later record the LIST reports but the GET cannot fetch is a
/// contradiction — BackendUnqualified, fail closed, not attempted.
#[tokio::test]
async fn e11_list_get_contradiction_fails_closed() {
    let (loopback, streams) = counterpart_streams().await;
    let keyspace = streams_keyspace(&loopback.kernel());
    let stream = streams.create_stream(b"cfg-e11").await.unwrap();
    let seeded = [
        (1u64, "c1", &b"p1"[..]),
        (2, "c2", &b"p2"[..]),
        (4, "c4", &b"p4"[..]),
    ];
    for (seq, stable_id, payload) in seeded {
        keyspace
            .create(
                &log_key(&stream, seq),
                hand_envelope(stream.as_str(), seq, stable_id, "cond.v11", payload),
            )
            .await
            .unwrap();
    }
    // LIST will still report seq 4; GET cannot fetch it.
    loopback.hide_key(&wire_log_key(&stream, 4)).await;
    let mark = loopback.request_log().len();
    let error = streams
        .append_expected(
            &event_ref(&stream, 2, "c2", b"p2"),
            &schema("cond.v11"),
            &event("c3"),
            b"p3",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(storage(&error), StreamsError::BackendUnqualified { .. }),
        "LIST/GET contradiction fails closed, got {error:?}"
    );
    assert_eq!(error.effect, AppendExpectedEffect::NotAttempted);
    assert!(
        !loopback.request_log()[mark..]
            .iter()
            .any(|record| record.method == "PUT"),
        "the contradiction path writes nothing"
    );
    loopback.shutdown();
}

/// e12: genesis at seq 0 is immortal — a floor of 1 (== target) does
/// not prevent a fresh expected append from the genesis predecessor.
#[tokio::test]
async fn e12_genesis_predecessor_at_floor_one_still_appends() {
    let (streams, _keyspace) = in_memory();
    let stream = streams.create_stream(b"cfg-e12").await.unwrap();
    streams.trim(&stream, 1).await.unwrap();
    assert_eq!(streams.trim_floor(&stream).await.unwrap(), Some(1));
    let receipt = streams
        .append_expected(
            &genesis_ref(&stream, b"cfg-e12"),
            &schema("cond.v12"),
            &event("e12-first"),
            b"p12",
        )
        .await
        .unwrap();
    assert_eq!(
        receipt.seq, 1,
        "floor == target == 1 still admits the genesis predecessor"
    );
    match streams.read(&stream, 0, 10).await {
        Replay::Page { events, .. } => {
            assert_eq!(
                events
                    .iter()
                    .map(|envelope| envelope.seq())
                    .collect::<Vec<_>>(),
                vec![1]
            );
        }
        other => panic!("expected a page, got {other:?}"),
    }
}

/// e13: a predecessor below the certified floor (target retained) is
/// Expired naming the Predecessor subject — not attempted.
#[tokio::test]
async fn e13_predecessor_expiry_at_floor_is_typed() {
    let (streams, _keyspace) = in_memory();
    let stream = streams.create_stream(b"cfg-e13").await.unwrap();
    let schema = schema("cond.v13");
    streams
        .append(&stream, &schema, &event("e13-one"), b"p1")
        .await
        .unwrap();
    streams
        .append(&stream, &schema, &event("e13-two"), b"p2")
        .await
        .unwrap();
    streams.trim(&stream, 3).await.unwrap();
    streams.gc(&stream).await.unwrap();
    let error = streams
        .append_expected(
            &event_ref(&stream, 2, "e13-two", b"p2"),
            &schema,
            &event("e13-three"),
            b"p3",
        )
        .await
        .unwrap_err();
    match &error.kind {
        AppendExpectedFailure::Expired {
            target_seq,
            first_retained,
            subject,
            ..
        } => {
            assert_eq!(*target_seq, 3);
            assert_eq!(*first_retained, 3);
            assert_eq!(*subject, ExpiredSubject::Predecessor);
        }
        other => panic!("expected Expired(Predecessor), got {other:?}"),
    }
    assert_eq!(error.effect, AppendExpectedEffect::NotAttempted);
}

/// e14: an absent target below the certified floor is Expired naming
/// the Target subject — not attempted.
#[tokio::test]
async fn e14_target_expiry_at_floor_is_typed() {
    let (streams, _keyspace) = in_memory();
    let stream = streams.create_stream(b"cfg-e14").await.unwrap();
    let schema = schema("cond.v14");
    streams
        .append(&stream, &schema, &event("e14-one"), b"p1")
        .await
        .unwrap();
    streams
        .append(&stream, &schema, &event("e14-two"), b"p2")
        .await
        .unwrap();
    streams.trim(&stream, 3).await.unwrap();
    streams.gc(&stream).await.unwrap();
    let error = streams
        .append_expected(
            &event_ref(&stream, 1, "e14-one", b"p1"),
            &schema,
            &event("e14-late"),
            b"late",
        )
        .await
        .unwrap_err();
    match &error.kind {
        AppendExpectedFailure::Expired {
            target_seq,
            first_retained,
            subject,
            ..
        } => {
            assert_eq!(*target_seq, 2);
            assert_eq!(*first_retained, 3);
            assert_eq!(*subject, ExpiredSubject::Target);
        }
        other => panic!("expected Expired(Target), got {other:?}"),
    }
    assert_eq!(error.effect, AppendExpectedEffect::NotAttempted);
}

/// e15: an exact below-floor zombie (retained bytes under a floor
/// past them, pre-GC) carries its receipt — Expired(Target,
/// Committed). After the sweeper collects the target, the same retry
/// is Expired(Target, NotAttempted): a swept target cannot recreate a
/// receipt.
#[tokio::test]
async fn e15_below_floor_zombie_committed_then_swept_target_cannot_recreate() {
    let (streams, _keyspace) = in_memory();
    let stream = streams.create_stream(b"cfg-e15").await.unwrap();
    let schema = schema("cond.v15");
    streams
        .append(&stream, &schema, &event("e15-one"), b"p1")
        .await
        .unwrap();
    let original = streams
        .append(&stream, &schema, &event("e15-two"), b"p2")
        .await
        .unwrap();
    streams.trim(&stream, 3).await.unwrap();

    let zombie = streams
        .append_expected(
            &event_ref(&stream, 1, "e15-one", b"p1"),
            &schema,
            &event("e15-two"),
            b"p2",
        )
        .await
        .unwrap_err();
    match &zombie.kind {
        AppendExpectedFailure::Expired {
            target_seq,
            first_retained,
            subject,
            ..
        } => {
            assert_eq!(*target_seq, 2);
            assert_eq!(*first_retained, 3);
            assert_eq!(*subject, ExpiredSubject::Target);
        }
        other => panic!("expected Expired(Target), got {other:?}"),
    }
    assert_eq!(
        zombie.effect,
        AppendExpectedEffect::Committed(original.clone()),
        "the zombie's bytes prove the commit and carry the receipt"
    );

    streams.gc(&stream).await.unwrap();
    let swept = streams
        .append_expected(
            &event_ref(&stream, 1, "e15-one", b"p1"),
            &schema,
            &event("e15-two"),
            b"p2",
        )
        .await
        .unwrap_err();
    match &swept.kind {
        AppendExpectedFailure::Expired {
            target_seq,
            first_retained,
            subject,
            ..
        } => {
            assert_eq!(*target_seq, 2);
            assert_eq!(*first_retained, 3);
            assert_eq!(*subject, ExpiredSubject::Target);
        }
        other => panic!("expected Expired(Target), got {other:?}"),
    }
    assert_eq!(
        swept.effect,
        AppendExpectedEffect::NotAttempted,
        "a swept target cannot recreate the receipt"
    );
}

/// e16: admission failures perform no I/O — sequence MAX
/// (SeqExhausted), non-canonical/invalid predecessor digests,
/// deserialized-invalid IDs, and an oversized encoded target
/// (EnvelopeTooLarge) are all rejected client-side with NotAttempted
/// and zero storage requests.
#[tokio::test]
async fn e16_seq_max_and_invalid_admission_perform_no_io() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = stream_id("cond-e16");
    let base = loopback.request_count();
    let good_schema = schema("cond.v16");
    let good_stable = event("e16");
    let ok_pred = event_ref(&stream, 1, "e16", b"p");

    // Checked successor: no seq past u64::MAX.
    let error = streams
        .append_expected(
            &event_ref(&stream, u64::MAX, "max", b"p"),
            &good_schema,
            &good_stable,
            b"p",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(storage(&error), StreamsError::SeqExhausted(_)),
        "seq MAX is typed SeqExhausted, got {error:?}"
    );
    assert_eq!(error.effect, AppendExpectedEffect::NotAttempted);

    // Non-canonical / malformed predecessor digests.
    let uppercase_digest = "A".repeat(64);
    for digest in ["not-hex!", "abc", uppercase_digest.as_str()] {
        let bad_digest = EventRef {
            payload_sha256: digest.to_string(),
            ..ok_pred.clone()
        };
        let error = streams
            .append_expected(&bad_digest, &good_schema, &good_stable, b"p")
            .await
            .unwrap_err();
        assert!(
            matches!(storage(&error), StreamsError::InvalidArgument(_)),
            "digest {digest:?} is admission-typed, got {error:?}"
        );
        assert_eq!(error.effect, AppendExpectedEffect::NotAttempted);
    }

    // Deserialized-invalid IDs: predecessor stream, predecessor
    // stable id, target schema, target stable id.
    let bad_stream = EventRef {
        stream_id: deserialized_stream_id("bad/id"),
        ..ok_pred.clone()
    };
    let bad_pred_id = EventRef {
        stable_event_id: deserialized_event_id("bad/id"),
        ..ok_pred.clone()
    };
    let bad_schema = deserialized_schema_id("bad/id");
    let bad_stable = deserialized_event_id("bad/id");
    let cases = [
        (
            "predecessor stream id",
            streams
                .append_expected(&bad_stream, &good_schema, &good_stable, b"p")
                .await
                .unwrap_err(),
        ),
        (
            "predecessor stable id",
            streams
                .append_expected(&bad_pred_id, &good_schema, &good_stable, b"p")
                .await
                .unwrap_err(),
        ),
        (
            "target schema id",
            streams
                .append_expected(&ok_pred, &bad_schema, &good_stable, b"p")
                .await
                .unwrap_err(),
        ),
        (
            "target stable id",
            streams
                .append_expected(&ok_pred, &good_schema, &bad_stable, b"p")
                .await
                .unwrap_err(),
        ),
    ];
    for (label, error) in &cases {
        assert!(
            matches!(storage(error), StreamsError::InvalidArgument(_)),
            "{label} is admission-typed, got {error:?}"
        );
        assert_eq!(error.effect, AppendExpectedEffect::NotAttempted);
    }

    // Oversized encoded target: preflighted before any effect.
    let oversize = vec![b'x'; 16 * 1024 * 1024];
    let error = streams
        .append_expected(&ok_pred, &good_schema, &good_stable, &oversize)
        .await
        .unwrap_err();
    assert!(
        matches!(storage(&error), StreamsError::EnvelopeTooLarge { .. }),
        "oversized encoded target is preflight-typed, got {error:?}"
    );
    assert_eq!(error.effect, AppendExpectedEffect::NotAttempted);

    assert_eq!(
        loopback.request_count(),
        base,
        "every admission failure performed zero storage requests"
    );
    loopback.shutdown();
}

/// e17: a target PUT refused before its effect still reports
/// PossiblyCommitted — a keyspace-create error cannot prove absence of
/// effect, so no Rejected certainty is exposed.
#[tokio::test]
async fn e17_refused_target_put_retains_possibly_committed() {
    let (loopback, streams) = counterpart_streams().await;
    let keyspace = streams_keyspace(&loopback.kernel());
    let stream = streams.create_stream(b"cfg-e17").await.unwrap();
    let target = wire_log_key(&stream, 1);
    loopback
        .arm_fault(StorageOp::Put, Some(&target), FaultPhase::Before)
        .await;
    let error = streams
        .append_expected(
            &genesis_ref(&stream, b"cfg-e17"),
            &schema("cond.v17"),
            &event("e17"),
            b"p17",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(storage(&error), StreamsError::Unavailable { .. }),
        "the refused PUT surfaces as Storage(Unavailable), got {error:?}"
    );
    assert_eq!(
        error.effect,
        AppendExpectedEffect::PossiblyCommitted,
        "a refused keyspace create still carries PossiblyCommitted"
    );
    assert!(
        keyspace.get(&log_key(&stream, 1)).await.unwrap().is_none(),
        "(loopback ground truth: nothing applied — the API still refuses certainty)"
    );
    loopback.shutdown();
}

/// e18: a target PUT that applied but lost its response, followed by
/// an unavailable readback (GET cut), retains PossiblyCommitted —
/// unresolved, never downgraded to NotAttempted. The PUT's one-shot
/// After fault is consumed by the parked request; the readback fault
/// is armed inside the parked window, after that consumption.
#[tokio::test]
async fn e18_lost_target_put_unavailable_readback_retains_possibly_committed() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(b"cfg-e18").await.unwrap();
    let target = wire_log_key(&stream, 1);
    loopback
        .arm_fault(StorageOp::Put, Some(&target), FaultPhase::After)
        .await;
    let pause = loopback.pause_next(StorageOp::Put, Some(&target), FaultPhase::After);
    let driver = streams.clone();
    let pred = genesis_ref(&stream, b"cfg-e18");
    let schema = schema("cond.v18");
    let stable = event("e18");
    let call = tokio::spawn(async move {
        driver
            .append_expected(&pred, &schema, &stable, b"p18")
            .await
    });
    tokio::time::timeout(PAUSE_TIMEOUT, pause.wait_reached())
        .await
        .expect("the target PUT parks after its effect");
    // The After cut was decided at request start; arm the readback cut
    // now, while the writer is parked unacknowledged.
    loopback
        .arm_fault(StorageOp::Get, Some(&target), FaultPhase::Before)
        .await;
    pause.release();
    let error = call.await.expect("append task").unwrap_err();
    assert!(
        matches!(storage(&error), StreamsError::Unavailable { .. }),
        "lost response + unavailable readback is Storage(Unavailable), got {error:?}"
    );
    assert_eq!(
        error.effect,
        AppendExpectedEffect::PossiblyCommitted,
        "an unavailable readback preserves the uncertainty"
    );
    loopback.shutdown();
}

/// e19: when the mandatory post-write floor observation fails, the
/// committed receipt is preserved — Storage(Unavailable) with effect
/// Committed(receipt), never a silent downgrade.
#[tokio::test]
async fn e19_postwrite_floor_failure_preserves_committed_receipt() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(b"cfg-e19").await.unwrap();
    let target = wire_log_key(&stream, 1);
    let pause = loopback.pause_next(StorageOp::Put, Some(&target), FaultPhase::After);
    let driver = streams.clone();
    let pred = genesis_ref(&stream, b"cfg-e19");
    let schema = schema("cond.v19");
    let stable = event("e19");
    let call = tokio::spawn(async move {
        driver
            .append_expected(&pred, &schema, &stable, b"p19")
            .await
    });
    tokio::time::timeout(PAUSE_TIMEOUT, pause.wait_reached())
        .await
        .expect("the target PUT parks after its effect");
    // Cut the next list — the mandatory post-write floor observation,
    // the first request after the acknowledged create.
    loopback
        .arm_fault(StorageOp::List, None, FaultPhase::Before)
        .await;
    pause.release();
    let error = call.await.expect("append task").unwrap_err();
    assert!(
        matches!(storage(&error), StreamsError::Unavailable { .. }),
        "the failed floor observation is Storage(Unavailable), got {error:?}"
    );
    assert_eq!(
        error.effect,
        AppendExpectedEffect::Committed(AppendReceipt {
            stream_id: stream.clone(),
            seq: 1,
            stable_event_id: event("e19"),
            payload_sha256: sha256_hex(b"p19"),
        }),
        "the committed receipt survives the floor failure"
    );
    loopback.shutdown();
}

/// e20: pausing immediately BEFORE the target PUT while a concurrent
/// trim advances the floor exactly TO the target (and GC sweeps below
/// it) leaves the target retained — the released PUT lands and the
/// append succeeds: Ok is allowed when floor == target. The
/// append_expected call performs exactly one log PUT, at the exact
/// target — no tail writes, no alternate positions.
#[tokio::test]
async fn e20_pause_before_target_put_trim_to_target_retained_success() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(b"cfg-e20").await.unwrap();
    let schema = schema("cond.v20");
    streams
        .append(&stream, &schema, &event("e20-one"), b"p1")
        .await
        .unwrap();
    let target = wire_log_key(&stream, 2);
    let pause = loopback.pause_next(StorageOp::Put, Some(&target), FaultPhase::Before);
    let driver = streams.clone();
    let pred = event_ref(&stream, 1, "e20-one", b"p1");
    let stable = event("e20-two");
    let call = tokio::spawn(async move {
        driver
            .append_expected(&pred, &schema, &stable, b"p20")
            .await
    });
    tokio::time::timeout(PAUSE_TIMEOUT, pause.wait_reached())
        .await
        .expect("the target PUT parks before its effect");
    let mark = loopback.request_log().len();
    // Concurrent retention: the floor advances exactly to the target;
    // the sweeper collects only below it.
    streams.trim(&stream, 2).await.unwrap();
    let swept = streams.gc(&stream).await.unwrap();
    assert_eq!(swept.deleted, 1, "only the predecessor went");
    pause.release();
    let receipt = call.await.expect("append task").unwrap();
    assert_eq!(
        receipt.seq, 2,
        "floor == target keeps the target retained: Ok allowed"
    );
    assert_eq!(
        streams.trim_floor(&stream).await.unwrap(),
        Some(2),
        "the certified floor did not retire the target"
    );
    match streams.read(&stream, 1, 10).await {
        Replay::Page { events, .. } => {
            assert_eq!(
                events
                    .iter()
                    .map(|envelope| envelope.seq())
                    .collect::<Vec<_>>(),
                vec![2],
                "replay from the boundary is dense through the target"
            );
        }
        other => panic!("expected a page, got {other:?}"),
    }
    let since = &loopback.request_log()[mark..];
    assert_eq!(
        log_put_keys(since, &stream),
        vec![target],
        "the append performed exactly one log PUT, at the exact target"
    );
    assert!(
        !since
            .iter()
            .any(|record| record.method == "PUT" && record.key.ends_with("/tail")),
        "no tail-hint write by the expected append"
    );
    loopback.shutdown();
}

/// e21: pausing immediately AFTER the target PUT while a concurrent
/// trim advances the floor BEYOND the target and GC collects it: the
/// writer is acknowledged (the commit is known), so the effect is
/// Committed and the observed expiry is Expired(Target, Committed).
#[tokio::test]
async fn e21_pause_after_target_put_trim_beyond_and_gc_expired_committed() {
    let (loopback, streams) = counterpart_streams().await;
    let keyspace = streams_keyspace(&loopback.kernel());
    let stream = streams.create_stream(b"cfg-e21").await.unwrap();
    let schema = schema("cond.v21");
    streams
        .append(&stream, &schema, &event("e21-one"), b"p1")
        .await
        .unwrap();
    let target = wire_log_key(&stream, 2);
    let pause = loopback.pause_next(StorageOp::Put, Some(&target), FaultPhase::After);
    let driver = streams.clone();
    let pred = event_ref(&stream, 1, "e21-one", b"p1");
    let stable = event("e21-two");
    let call = tokio::spawn(async move {
        driver
            .append_expected(&pred, &schema, &stable, b"p21")
            .await
    });
    tokio::time::timeout(PAUSE_TIMEOUT, pause.wait_reached())
        .await
        .expect("the target PUT parks after its effect");
    streams.trim(&stream, 3).await.unwrap();
    streams.gc(&stream).await.unwrap();
    assert!(
        keyspace.get(&log_key(&stream, 2)).await.unwrap().is_none(),
        "the sweeper collected the parked writer's target"
    );
    pause.release();
    let error = call.await.expect("append task").unwrap_err();
    match &error.kind {
        AppendExpectedFailure::Expired {
            target_seq,
            first_retained,
            subject,
            ..
        } => {
            assert_eq!(*target_seq, 2);
            assert_eq!(*first_retained, 3);
            assert_eq!(*subject, ExpiredSubject::Target);
        }
        other => panic!("expected Expired(Target), got {other:?}"),
    }
    assert_eq!(
        error.effect,
        AppendExpectedEffect::Committed(AppendReceipt {
            stream_id: stream.clone(),
            seq: 2,
            stable_event_id: event("e21-two"),
            payload_sha256: sha256_hex(b"p21"),
        }),
        "the acknowledged commit is known and carried despite the sweep"
    );
    loopback.shutdown();
}

/// e22: a target PUT that applied but lost its response, with the
/// floor advancing past the target and GC collecting it BEFORE the
/// readback: the readback sees absence, which proves nothing —
/// Expired(Target, PossiblyCommitted). The unknown commit stays
/// unknown.
#[tokio::test]
async fn e22_lost_target_put_gc_before_readback_expired_possibly_committed() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(b"cfg-e22").await.unwrap();
    let schema = schema("cond.v22");
    streams
        .append(&stream, &schema, &event("e22-one"), b"p1")
        .await
        .unwrap();
    let target = wire_log_key(&stream, 2);
    // One-shot response cut, decided at request start; the pause opens
    // the window inside which retention races the parked writer.
    loopback
        .arm_fault(StorageOp::Put, Some(&target), FaultPhase::After)
        .await;
    let pause = loopback.pause_next(StorageOp::Put, Some(&target), FaultPhase::After);
    let driver = streams.clone();
    let pred = event_ref(&stream, 1, "e22-one", b"p1");
    let stable = event("e22-two");
    let call = tokio::spawn(async move {
        driver
            .append_expected(&pred, &schema, &stable, b"p22")
            .await
    });
    tokio::time::timeout(PAUSE_TIMEOUT, pause.wait_reached())
        .await
        .expect("the target PUT parks after its effect");
    streams.trim(&stream, 3).await.unwrap();
    streams.gc(&stream).await.unwrap();
    pause.release();
    let error = call.await.expect("append task").unwrap_err();
    match &error.kind {
        AppendExpectedFailure::Expired {
            target_seq,
            first_retained,
            subject,
            ..
        } => {
            assert_eq!(*target_seq, 2);
            assert_eq!(*first_retained, 3);
            assert_eq!(*subject, ExpiredSubject::Target);
        }
        other => panic!("expected Expired(Target), got {other:?}"),
    }
    assert_eq!(
        error.effect,
        AppendExpectedEffect::PossiblyCommitted,
        "a swept target cannot prove the lost response's effect either way"
    );
    loopback.shutdown();
}

/// e23: a frozen certificate LIST (stale, under-reporting) hides the
/// concurrent trim certificate from the post-write floor observation:
/// the append is Ok — the explicit qualified-backend residual — and a
/// later exact retry under a fresh LIST honestly reports
/// Expired(Target, Committed) for the same bytes.
#[tokio::test]
async fn e23_frozen_certificate_list_residual_is_honest() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(b"cfg-e23").await.unwrap();
    let schema = schema("cond.v23");
    streams
        .append(&stream, &schema, &event("e23-one"), b"p1")
        .await
        .unwrap();
    let target = wire_log_key(&stream, 2);
    let pause = loopback.pause_next(StorageOp::Put, Some(&target), FaultPhase::After);
    let driver = streams.clone();
    let pred = event_ref(&stream, 1, "e23-one", b"p1");
    let stable = event("e23-two");
    let call = tokio::spawn(async move {
        driver
            .append_expected(&pred, &schema, &stable, b"p23")
            .await
    });
    tokio::time::timeout(PAUSE_TIMEOUT, pause.wait_reached())
        .await
        .expect("the target PUT parks after its effect");
    // Freeze the listing, THEN certify a floor past the target: the
    // certificate is invisible to every frozen-qualified observation.
    loopback.freeze_list().await;
    streams.trim(&stream, 3).await.unwrap();
    pause.release();
    let receipt = call.await.expect("append task").unwrap();
    assert_eq!(
        receipt.seq, 2,
        "Ok under a silently stale certificate LIST — the named residual"
    );
    // The fresh observation retires the target honestly.
    loopback.unfreeze_list().await;
    let error = streams
        .append_expected(
            &event_ref(&stream, 1, "e23-one", b"p1"),
            &schema,
            &event("e23-two"),
            b"p23",
        )
        .await
        .unwrap_err();
    match &error.kind {
        AppendExpectedFailure::Expired {
            target_seq,
            first_retained,
            subject,
            ..
        } => {
            assert_eq!(*target_seq, 2);
            assert_eq!(*first_retained, 3);
            assert_eq!(*subject, ExpiredSubject::Target);
        }
        other => panic!("expected Expired(Target), got {other:?}"),
    }
    assert_eq!(
        error.effect,
        AppendExpectedEffect::Committed(AppendReceipt {
            stream_id: stream.clone(),
            seq: 2,
            stable_event_id: event("e23-two"),
            payload_sha256: sha256_hex(b"p23"),
        }),
        "the retained bytes still prove the commit under the fresh floor"
    );
    loopback.shutdown();
}

/// c10: a losing conditional create (AlreadyExists on the incumbent
/// genesis) whose incumbent readback comes back ABSENT — hidden from
/// GET while the losing PUT parks after its effect — contradicts the
/// conflict: BackendUnqualified, and the incumbent is untouched.
#[tokio::test]
async fn c10_absent_incumbent_after_conflict_is_backend_unqualified() {
    let (loopback, streams) = counterpart_streams().await;
    let id = stream_id("cond-c10");
    streams
        .create_stream_with_id(&id, b"cfg-c10")
        .await
        .unwrap();
    // The losing conditional PUT parks after its (non-)effect; the
    // genesis becomes unfetchable before the incumbent readback runs.
    let pause = loopback.pause_next(
        StorageOp::Put,
        Some(&wire_log_key(&id, 0)),
        FaultPhase::After,
    );
    let driver = streams.clone();
    let retry_id = id.clone();
    let call =
        tokio::spawn(async move { driver.create_stream_with_id(&retry_id, b"cfg-c10").await });
    tokio::time::timeout(PAUSE_TIMEOUT, pause.wait_reached())
        .await
        .expect("the losing conditional PUT parks after its effect");
    loopback.hide_key(&wire_log_key(&id, 0)).await;
    pause.release();
    let error = call.await.expect("create task").unwrap_err();
    assert!(
        matches!(
            create_storage(&error),
            StreamsError::BackendUnqualified { .. }
        ),
        "absent incumbent after conflict fails closed, got {error:?}"
    );
    loopback.unhide_key(&wire_log_key(&id, 0)).await;
    assert_eq!(
        streams.read_config(&id).await.unwrap().as_deref(),
        Some(b"cfg-c10".as_slice()),
        "the contradiction path never touched the incumbent"
    );
    loopback.shutdown();
}

/// e24: a target occupied by bytes that fail envelope verification is
/// typed Corrupt naming the target seq — judged before any attempt,
/// nothing written.
#[tokio::test]
async fn e24_malformed_target_occupant_is_corrupt_before_attempt() {
    let (streams, keyspace) = in_memory();
    let stream = streams.create_stream(b"cfg-e24").await.unwrap();
    let pred = genesis_ref(&stream, b"cfg-e24");
    let schema = schema("cond.v24");
    streams
        .append_expected(&pred, &schema, &event("e24-first"), b"p1")
        .await
        .unwrap();
    overwrite_key(
        &keyspace,
        &log_key(&stream, 1),
        Bytes::from_static(b"not-an-envelope"),
    )
    .await;
    let error = streams
        .append_expected(&pred, &schema, &event("e24-second"), b"p2")
        .await
        .unwrap_err();
    assert!(
        matches!(
            storage(&error),
            StreamsError::Corrupt {
                missing_or_mismatched,
                ..
            } if missing_or_mismatched.contains(&1)
        ),
        "malformed occupant is Storage(Corrupt) naming the target, got {error:?}"
    );
    assert_eq!(
        error.effect,
        AppendExpectedEffect::NotAttempted,
        "the occupant is judged before any attempt"
    );
    assert_eq!(
        stream_objects(&keyspace, &stream).await,
        vec![log_key(&stream, 0), log_key(&stream, 1)],
        "the corrupt occupant was not overwritten"
    );
}

/// e25: a target holding a verified DIFFERENT event below a floor
/// past it (a zombie) is judged by the floor first — Expired(Target),
/// not PositionConflict — and nothing is attempted.
#[tokio::test]
async fn e25_expired_target_overrides_zombie_conflict() {
    let (streams, _keyspace) = in_memory();
    let stream = streams.create_stream(b"cfg-e25").await.unwrap();
    let schema = schema("cond.v25");
    let pred1 = genesis_ref(&stream, b"cfg-e25");
    streams
        .append_expected(&pred1, &schema, &event("e25-one"), b"p1")
        .await
        .unwrap();
    streams
        .append_expected(
            &event_ref(&stream, 1, "e25-one", b"p1"),
            &schema,
            &event("e25-two"),
            b"p2",
        )
        .await
        .unwrap();
    // Floor past the target; the zombie at seq 2 stays physically
    // present with different verified bytes.
    streams.trim(&stream, 3).await.unwrap();
    let error = streams
        .append_expected(
            &event_ref(&stream, 1, "e25-one", b"p1"),
            &schema,
            &event("e25-late"),
            b"late",
        )
        .await
        .unwrap_err();
    match &error.kind {
        AppendExpectedFailure::Expired {
            target_seq,
            first_retained,
            subject,
            ..
        } => {
            assert_eq!(*target_seq, 2);
            assert_eq!(*first_retained, 3);
            assert_eq!(*subject, ExpiredSubject::Target);
        }
        other => panic!(
            "expired target overrides the zombie conflict, got {other:?} (effect {:?})",
            error.effect
        ),
    }
    assert_eq!(
        error.effect,
        AppendExpectedEffect::NotAttempted,
        "the expired zombie is never attempted"
    );
}

/// e26: a later record the ordered probe lists whose bytes fail
/// verification is a Corrupt witness — the hole is not filled,
/// nothing is written.
#[tokio::test]
async fn e26_malformed_later_witness_is_corrupt_without_fill() {
    let (streams, keyspace) = in_memory();
    let stream = streams.create_stream(b"cfg-e26").await.unwrap();
    let seeded = [
        (1u64, "w1", &b"q1"[..]),
        (2, "w2", &b"q2"[..]),
        (4, "w4", &b"q4"[..]),
    ];
    for (seq, stable_id, payload) in seeded {
        keyspace
            .create(
                &log_key(&stream, seq),
                hand_envelope(stream.as_str(), seq, stable_id, "cond.v26", payload),
            )
            .await
            .unwrap();
    }
    overwrite_key(
        &keyspace,
        &log_key(&stream, 4),
        Bytes::from_static(b"garbage-witness"),
    )
    .await;
    let error = streams
        .append_expected(
            &event_ref(&stream, 2, "w2", b"q2"),
            &schema("cond.v26"),
            &event("w3"),
            b"q3",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            storage(&error),
            StreamsError::Corrupt {
                missing_or_mismatched,
                ..
            } if missing_or_mismatched.contains(&4)
        ),
        "the malformed later witness is Storage(Corrupt) naming it, got {error:?}"
    );
    assert_eq!(error.effect, AppendExpectedEffect::NotAttempted);
    assert!(
        keyspace.get(&log_key(&stream, 3)).await.unwrap().is_none(),
        "the hole is not filled behind a corrupt witness"
    );
}

/// e27: a target PUT that applied but lost its response, read back as
/// a VERIFIED DIFFERENT event (fixture CAS inside the parked window):
/// the conflicting witness is preserved — PositionConflict with
/// PossiblyCommitted — and no log write lands at any other position.
#[tokio::test]
async fn e27_lost_put_conflicting_readback_preserves_possibly_committed() {
    let (loopback, streams) = counterpart_streams().await;
    let keyspace = streams_keyspace(&loopback.kernel());
    let stream = streams.create_stream(b"cfg-e27").await.unwrap();
    let schema = schema("cond.v27");
    streams
        .append_expected(
            &genesis_ref(&stream, b"cfg-e27"),
            &schema,
            &event("e27-first"),
            b"p1",
        )
        .await
        .unwrap();
    let target = wire_log_key(&stream, 2);
    loopback
        .arm_fault(StorageOp::Put, Some(&target), FaultPhase::After)
        .await;
    let pause = loopback.pause_next(StorageOp::Put, Some(&target), FaultPhase::After);
    let mark = loopback.request_log().len();
    let driver = streams.clone();
    let pred = event_ref(&stream, 1, "e27-first", b"p1");
    let stable = event("e27-event");
    let call = tokio::spawn(async move {
        driver
            .append_expected(&pred, &schema, &stable, b"p27")
            .await
    });
    tokio::time::timeout(PAUSE_TIMEOUT, pause.wait_reached())
        .await
        .expect("the target PUT parks after its effect");
    // Replace the applied envelope with a verified DIFFERENT event
    // before the response is lost: the readback must witness the
    // conflict, not the desired bytes.
    overwrite_key(
        &keyspace,
        &log_key(&stream, 2),
        hand_envelope(
            stream.as_str(),
            2,
            "e27-replacement",
            "cond.v27",
            b"replacement",
        ),
    )
    .await;
    pause.release();
    let error = call.await.expect("append task").unwrap_err();
    match &error.kind {
        AppendExpectedFailure::PositionConflict {
            stream: conflicted,
            target_seq,
            occupant,
        } => {
            assert_eq!(conflicted, &stream);
            assert_eq!(*target_seq, 2);
            assert_eq!(
                *occupant,
                event_ref(&stream, 2, "e27-replacement", b"replacement"),
                "the readback's conflicting witness keeps its identity"
            );
        }
        other => panic!("expected PositionConflict readback, got {other:?}"),
    }
    assert_eq!(
        error.effect,
        AppendExpectedEffect::PossiblyCommitted,
        "the conflicting readback preserves the uncertainty"
    );
    assert!(
        log_put_keys(&loopback.request_log()[mark..], &stream)
            .iter()
            .all(|key| key == &target),
        "every log write stayed at the exact target"
    );
    loopback.shutdown();
}

/// e28: a target PUT that applied but lost its response, read back as
/// bytes that fail verification (fixture CAS inside the parked
/// window): the corrupt witness is preserved — Storage(Corrupt) with
/// PossiblyCommitted.
#[tokio::test]
async fn e28_lost_put_corrupt_readback_preserves_possibly_committed() {
    let (loopback, streams) = counterpart_streams().await;
    let keyspace = streams_keyspace(&loopback.kernel());
    let stream = streams.create_stream(b"cfg-e28").await.unwrap();
    let schema = schema("cond.v28");
    streams
        .append_expected(
            &genesis_ref(&stream, b"cfg-e28"),
            &schema,
            &event("e28-first"),
            b"p1",
        )
        .await
        .unwrap();
    let target = wire_log_key(&stream, 2);
    loopback
        .arm_fault(StorageOp::Put, Some(&target), FaultPhase::After)
        .await;
    let pause = loopback.pause_next(StorageOp::Put, Some(&target), FaultPhase::After);
    let mark = loopback.request_log().len();
    let driver = streams.clone();
    let pred = event_ref(&stream, 1, "e28-first", b"p1");
    let stable = event("e28-event");
    let call = tokio::spawn(async move {
        driver
            .append_expected(&pred, &schema, &stable, b"p28")
            .await
    });
    tokio::time::timeout(PAUSE_TIMEOUT, pause.wait_reached())
        .await
        .expect("the target PUT parks after its effect");
    overwrite_key(
        &keyspace,
        &log_key(&stream, 2),
        Bytes::from_static(b"corrupt-readback"),
    )
    .await;
    pause.release();
    let error = call.await.expect("append task").unwrap_err();
    assert!(
        matches!(
            storage(&error),
            StreamsError::Corrupt {
                missing_or_mismatched,
                ..
            } if missing_or_mismatched.contains(&2)
        ),
        "the corrupt readback preserves its typed witness, got {error:?}"
    );
    assert_eq!(
        error.effect,
        AppendExpectedEffect::PossiblyCommitted,
        "the corrupt readback preserves the uncertainty"
    );
    assert!(
        log_put_keys(&loopback.request_log()[mark..], &stream)
            .iter()
            .all(|key| key == &target),
        "every log write stayed at the exact target"
    );
    loopback.shutdown();
}

/// e29: a floor observation that REGRESSES below an already observed
/// floor (the higher certificate omitted from LIST inside the parked
/// predecessor-read window) is BackendUnqualified — the maximum is
/// never forgotten — and the append was never attempted.
#[tokio::test]
async fn e29_floor_regression_before_attempt_is_backend_unqualified() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(b"cfg-e29").await.unwrap();
    let schema = schema("cond.v29");
    streams
        .append_expected(
            &genesis_ref(&stream, b"cfg-e29"),
            &schema,
            &event("e29-one"),
            b"p1",
        )
        .await
        .unwrap();
    streams
        .append_expected(
            &event_ref(&stream, 1, "e29-one", b"p1"),
            &schema,
            &event("e29-two"),
            b"p2",
        )
        .await
        .unwrap();
    // Two certificates: floor 1 then floor 2; the first floor
    // observation sees the maximum (2).
    streams.trim(&stream, 1).await.unwrap();
    streams.trim(&stream, 2).await.unwrap();
    // Park the predecessor read (after the first floor observation),
    // omit the higher certificate from LIST, and resume: the next
    // floor observation regresses to 1.
    let pause = loopback.pause_next(
        StorageOp::Get,
        Some(&wire_log_key(&stream, 2)),
        FaultPhase::Before,
    );
    let driver = streams.clone();
    let pred = event_ref(&stream, 2, "e29-two", b"p2");
    let stable = event("e29-three");
    let call =
        tokio::spawn(async move { driver.append_expected(&pred, &schema, &stable, b"p3").await });
    tokio::time::timeout(PAUSE_TIMEOUT, pause.wait_reached())
        .await
        .expect("the predecessor read parks before its effect");
    let higher = wire_cert_key(&stream, 2);
    loopback.omit_from_list(&higher);
    pause.release();
    let error = call.await.expect("append task").unwrap_err();
    assert!(
        matches!(storage(&error), StreamsError::BackendUnqualified { .. }),
        "a regressing floor observation fails closed, got {error:?}"
    );
    assert_eq!(
        error.effect,
        AppendExpectedEffect::NotAttempted,
        "the regression fires before the create"
    );
    // Omission is LIST-only: restoring the certificate brings the
    // true floor straight back.
    loopback.restore_to_list(&higher);
    assert_eq!(
        streams.trim_floor(&stream).await.unwrap(),
        Some(2),
        "the certificate object was never touched"
    );
    loopback.shutdown();
}

/// e30: the same regression observed AFTER an acknowledged create:
/// BackendUnqualified with the CURRENT effect — the committed receipt
/// is preserved, never downgraded.
#[tokio::test]
async fn e30_floor_regression_after_commit_preserves_committed_receipt() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(b"cfg-e30").await.unwrap();
    let schema = schema("cond.v30");
    streams
        .append_expected(
            &genesis_ref(&stream, b"cfg-e30"),
            &schema,
            &event("e30-one"),
            b"p1",
        )
        .await
        .unwrap();
    streams
        .append_expected(
            &event_ref(&stream, 1, "e30-one", b"p1"),
            &schema,
            &event("e30-two"),
            b"p2",
        )
        .await
        .unwrap();
    streams.trim(&stream, 1).await.unwrap();
    streams.trim(&stream, 2).await.unwrap();
    // The create applies and parks unacknowledged; the post-write
    // floor observation then sees the regressed certificate set.
    let pause = loopback.pause_next(
        StorageOp::Put,
        Some(&wire_log_key(&stream, 3)),
        FaultPhase::After,
    );
    let driver = streams.clone();
    let pred = event_ref(&stream, 2, "e30-two", b"p2");
    let stable = event("e30-three");
    let call = tokio::spawn(async move {
        driver
            .append_expected(&pred, &schema, &stable, b"p30")
            .await
    });
    tokio::time::timeout(PAUSE_TIMEOUT, pause.wait_reached())
        .await
        .expect("the target PUT parks after its effect");
    let higher = wire_cert_key(&stream, 2);
    loopback.omit_from_list(&higher);
    pause.release();
    let error = call.await.expect("append task").unwrap_err();
    assert!(
        matches!(storage(&error), StreamsError::BackendUnqualified { .. }),
        "a regressing post-write floor fails closed, got {error:?}"
    );
    assert_eq!(
        error.effect,
        AppendExpectedEffect::Committed(AppendReceipt {
            stream_id: stream.clone(),
            seq: 3,
            stable_event_id: event("e30-three"),
            payload_sha256: sha256_hex(b"p30"),
        }),
        "the acknowledged commit is carried through the contradiction"
    );
    loopback.restore_to_list(&higher);
    assert_eq!(
        streams.trim_floor(&stream).await.unwrap(),
        Some(2),
        "the certificate object was never touched"
    );
    loopback.shutdown();
}
