//! P-000006/O-000006 contract tests — the strict historical reads
//! (`read_event`, `read_range`) and the immutable `Envelope` surface.
//!
//! The r-suite identifiers are predeclared by
//! `situation/oracles/O-000006-strict-stream-reads.md`; each test maps
//! to one oracle leg (P1..P9, judged against F1..F7). Request-shape
//! legs (r2, r8) are decided from the loopback wire witness
//! (`request_log`): strict reads are GET-only, their only permitted
//! LIST is the trim-certificate lookup (its query walks from the
//! `{stream}/trims` sentinel), and they touch no event object outside
//! the demand. Boundary legs (r3, r5) prove the typed outcomes stay
//! distinct; r9 proves the persisted wire format and the
//! payload-digest/encoded-digest distinction against the independent
//! `hand_envelope` encoder.

mod support;

use std::collections::BTreeSet;

use sha2::{Digest, Sha256};
use support::loopback::{FaultPhase, Loopback, RequestRecord, StorageOp};
use support::{streams_on_in_memory_store, streams_on_store};
use yeetz_s3_kernel::KernelHandle;
use yeetz_s3_streams::{
    ENVELOPE_FORMAT_VERSION, EventRef, GENESIS_SCHEMA_ID, RangePage, SchemaId, StableEventId,
    StreamId, Streams, StreamsError,
};

fn schema() -> SchemaId {
    SchemaId::new("strict.v1").unwrap()
}

fn event(value: &str) -> StableEventId {
    StableEventId::new(value).unwrap()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Keyspace-relative log key (in-memory seeding and damage).
fn log_key(stream: &StreamId, seq: u64) -> String {
    format!("{}/log/{seq:020}", stream.as_str())
}

/// The PHYSICAL key the loopback records (kernel keyspace root
/// included).
fn wire_log_key(stream: &StreamId, seq: u64) -> String {
    format!("keyspace/streams/v1/{}/log/{seq:020}", stream.as_str())
}

fn seqs_of(page: &RangePage) -> Vec<u64> {
    page.events.iter().map(|envelope| envelope.seq()).collect()
}

fn ids_of(page: &RangePage) -> Vec<&str> {
    page.events
        .iter()
        .map(|envelope| envelope.stable_event_id().as_str())
        .collect()
}

/// The parsed markers of a ListObjectsV2 request — present only when
/// the query actually carries `list-type=2`.
struct ListQuery {
    prefix: String,
    start_after: Option<String>,
    continuation: Option<String>,
}

fn parse_list_query(record: &RequestRecord) -> Option<ListQuery> {
    let query = record.query.as_deref()?;
    let url = reqwest::Url::parse(&format!("http://counterpart/?{query}")).ok()?;
    let mut prefix = None;
    let mut start_after = None;
    let mut continuation = None;
    let mut list_type = false;
    for (name, value) in url.query_pairs() {
        match &*name {
            "list-type" => list_type = value == "2",
            "prefix" => prefix = Some(value.into_owned()),
            "start-after" => start_after = Some(value.into_owned()),
            "continuation-token" => continuation = Some(value.into_owned()),
            _ => {}
        }
    }
    if !list_type {
        return None;
    }
    Some(ListQuery {
        prefix: prefix.unwrap_or_default(),
        start_after,
        continuation,
    })
}

/// The one LIST a strict read may issue: a GET to the bucket root
/// (`key == ""`) whose ListObjectsV2 query names the streams keyspace
/// prefix exactly and either starts exactly at this stream's
/// trim-certificate sentinel
/// (`start-after == keyspace/streams/v1/<stream>/trims`) or
/// continues a paged certificate walk whose token stays inside that
/// stream's certificate range. A log listing starts after a
/// `log/<seq>` key and never qualifies.
fn is_trim_certificate_list(record: &RequestRecord, stream: &StreamId) -> bool {
    if record.method != "GET" || !record.key.is_empty() {
        return false;
    }
    let Some(list) = parse_list_query(record) else {
        return false;
    };
    if list.prefix != "keyspace/streams/v1/" {
        return false;
    }
    let sentinel = format!("keyspace/streams/v1/{}/trims", stream.as_str());
    match (list.start_after, list.continuation) {
        (Some(after), None) => after == sentinel,
        (start_after, Some(token)) => {
            let certificate_prefix = format!("keyspace/streams/v1/{}/trims/", stream.as_str());
            token.starts_with(&certificate_prefix)
                && start_after.as_deref().is_none_or(|after| after == sentinel)
        }
        (None, None) => false,
    }
}

/// The seq a GET of this stream's log addresses, if it is one.
fn log_get_seq(record: &RequestRecord, stream: &StreamId) -> Option<u64> {
    if parse_list_query(record).is_some() {
        return None;
    }
    let prefix = format!("keyspace/streams/v1/{}/log/", stream.as_str());
    record
        .key
        .strip_prefix(&prefix)
        .and_then(|rest| rest.parse().ok())
}

/// The strict-read request contract, from the wire witness: GET-only;
/// event GETs exactly `{genesis} ∪ demanded`; exactly `trim_lists`
/// trim-certificate LISTs (no other LIST); no other key touched (no
/// tail hint, no cursors, nothing outside the keyspace demand).
fn assert_strict_request_shape(
    records: &[RequestRecord],
    stream: &StreamId,
    demanded: &[u64],
    trim_lists: usize,
) {
    let mut expected: BTreeSet<u64> = demanded.iter().copied().collect();
    expected.insert(0); // the genesis verification GET
    let mut event_gets = BTreeSet::new();
    let mut trim_list_count = 0usize;
    for record in records {
        assert_eq!(
            record.method, "GET",
            "a strict read is GET-only; saw {record:?}"
        );
        if parse_list_query(record).is_some() {
            assert!(
                is_trim_certificate_list(record, stream),
                "a non-trim LIST is forbidden in a strict read: {record:?}"
            );
            trim_list_count += 1;
            continue;
        }
        match log_get_seq(record, stream) {
            Some(seq) => {
                assert!(
                    expected.contains(&seq),
                    "event GET outside the demand: seq {seq}"
                );
                assert!(event_gets.insert(seq), "duplicate event GET: seq {seq}");
            }
            None => panic!("a strict read touched a non-log key: {record:?}"),
        }
    }
    assert_eq!(
        event_gets, expected,
        "the event GET set must be exactly genesis + the demanded seqs"
    );
    assert_eq!(trim_list_count, trim_lists, "trim-certificate LIST count");
}

async fn counterpart_streams() -> (Loopback, Streams) {
    let loopback = Loopback::start().await;
    let streams = streams_on_store(&loopback.kernel());
    (loopback, streams)
}

async fn replace_object(
    keyspace: &yeetz_s3_kernel::AtomicKeyspace,
    key: &str,
    value: bytes::Bytes,
) {
    let (_, etag) = keyspace.get_with_etag(key).await.unwrap().unwrap();
    keyspace.compare_exchange(key, &etag, value).await.unwrap();
}

/// P1: `read_event` returns accessor-complete verified envelopes — for
/// appended seqs and for seq 0 under a certified floor above 1. The
/// genesis is served without a trim-floor boundary (that no lookup
/// runs for seq 0 is witnessed by r2's request shape).
#[tokio::test]
async fn r1_read_event_returns_verified_envelope() {
    let streams = streams_on_in_memory_store();
    let stream = streams.create_stream(b"r1-config").await.unwrap();
    for index in 1..=3u64 {
        streams
            .append(
                &stream,
                &schema(),
                &event(&format!("r1-{index}")),
                format!("payload-{index}").as_bytes(),
            )
            .await
            .unwrap();
    }

    let first = streams.read_event(&stream, 1).await.unwrap();
    assert_eq!(first.format_version(), ENVELOPE_FORMAT_VERSION);
    assert_eq!(first.stream_id(), &stream);
    assert_eq!(first.seq(), 1);
    assert_eq!(first.stable_event_id().as_str(), "r1-1");
    assert_eq!(first.schema_id().as_str(), "strict.v1");
    assert_eq!(first.payload().as_ref(), b"payload-1");
    assert_eq!(first.payload_sha256(), sha256_hex(b"payload-1"));

    let third = streams.read_event(&stream, 3).await.unwrap();
    assert_eq!(third.seq(), 3);
    assert_eq!(third.stable_event_id().as_str(), "r1-3");
    assert_eq!(third.payload().as_ref(), b"payload-3");

    // Seq 0 under a certified floor above 1 — before and after the
    // sweeper: the genesis is immortal and is served as a verified
    // envelope like any other event.
    streams.trim(&stream, 2).await.unwrap();
    let pre_sweep = streams.read_event(&stream, 0).await.unwrap();
    assert_eq!(pre_sweep.seq(), 0);
    assert_eq!(pre_sweep.schema_id().as_str(), GENESIS_SCHEMA_ID);
    assert_eq!(pre_sweep.payload().as_ref(), b"r1-config");
    streams.gc(&stream).await.unwrap();
    let genesis = streams.read_event(&stream, 0).await.unwrap();
    assert_eq!(genesis.seq(), 0);
    assert_eq!(genesis.payload().as_ref(), b"r1-config");
    assert_eq!(genesis.event_ref().seq, 0);
}

/// P2/F2: `read_event`'s request shape is exact — for seq 0 nothing
/// but the genesis GET; for a nonzero target the genesis GET, one
/// trim-certificate LIST, and the target GET. No other event GET, no
/// tail-hint read, no log LIST, no write.
#[tokio::test]
async fn r2_read_event_request_shape_is_exact() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=3u64 {
        streams
            .append(&stream, &schema(), &event(&format!("shape-{index}")), &[])
            .await
            .unwrap();
    }

    // Seq 0: the genesis alone — no retention lookup, no LIST at all.
    let baseline = loopback.request_log().len();
    let genesis = streams.read_event(&stream, 0).await.unwrap();
    assert_eq!(genesis.seq(), 0);
    assert_strict_request_shape(&loopback.request_log()[baseline..], &stream, &[], 0);

    // A nonzero target: exactly the genesis GET, one trim-certificate
    // LIST, and the target GET (never the neighbor at seq 1 or 3).
    let baseline = loopback.request_log().len();
    let target = streams.read_event(&stream, 2).await.unwrap();
    assert_eq!(target.seq(), 2);
    assert_strict_request_shape(&loopback.request_log()[baseline..], &stream, &[2], 1);

    loopback.shutdown();
}

/// P3/F1/F3: every `read_event` boundary is typed and distinct —
/// invalid id (refused before any storage request), absent genesis,
/// below-floor target, absent target at or above the floor, malformed
/// and key-mismatched objects, cut target GET, cut retention lookup —
/// and a broken retention lookup never serves the genesis wrong.
#[tokio::test]
async fn r3_read_event_typed_boundaries() {
    // --- invalid id, including one that bypassed the constructor by
    // deserialization: InvalidArgument before any storage request.
    let (loopback, streams) = counterpart_streams().await;
    let invalid: StreamId = serde_json::from_str("\"\"").unwrap();
    let baseline = loopback.request_log().len();
    assert!(matches!(
        streams.read_event(&invalid, 1).await,
        Err(StreamsError::InvalidArgument(_))
    ));
    assert!(matches!(
        streams.read_range(&invalid, 0, 3, 3).await,
        Err(StreamsError::InvalidArgument(_))
    ));
    assert_eq!(
        loopback.request_log().len(),
        baseline,
        "argument-invalid inputs must not touch storage"
    );

    // --- cut target GET: the store-failure error, never a boundary.
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=3u64 {
        streams
            .append(&stream, &schema(), &event(&format!("cut-{index}")), &[])
            .await
            .unwrap();
    }
    loopback
        .arm_fault(
            StorageOp::Get,
            Some(&wire_log_key(&stream, 2)),
            FaultPhase::Before,
        )
        .await;
    let cut = streams.read_event(&stream, 2).await.unwrap_err();
    assert!(
        matches!(cut, StreamsError::Unavailable { .. }),
        "a cut target GET is the store-failure error, got {cut:?}"
    );

    // --- cut retention lookup: the bounded API's deterministic store
    // fault — `Unavailable` — never a silent success, never a
    // boundary variant, and never live replay's `BackendUnqualified`
    // fail-closed (which would mean the strict path fell back to the
    // live read machinery).
    loopback
        .arm_fault(StorageOp::List, None, FaultPhase::Before)
        .await;
    let retention = streams.read_event(&stream, 2).await.unwrap_err();
    assert!(
        matches!(retention, StreamsError::Unavailable { .. }),
        "a cut retention lookup is the store-failure error, got {retention:?}"
    );

    // The genesis read succeeding, with no new LIST recorded on the
    // wire, proves seq 0 consults no retention lookup at all — the
    // genesis stays readable even with a broken retention path.
    let lists_before = loopback
        .request_log()
        .iter()
        .filter(|record| parse_list_query(record).is_some())
        .count();
    let genesis = streams.read_event(&stream, 0).await.unwrap();
    assert_eq!(genesis.seq(), 0);
    let lists_after = loopback
        .request_log()
        .iter()
        .filter(|record| parse_list_query(record).is_some())
        .count();
    assert_eq!(
        lists_before, lists_after,
        "seq 0 must be served without a retention lookup"
    );
    loopback.shutdown();

    // --- absent genesis: StreamNotFound (a valid id, no stream).
    let streams = streams_on_in_memory_store();
    let ghost = StreamId::new("r3-ghost").unwrap();
    assert!(matches!(
        streams.read_event(&ghost, 1).await,
        Err(StreamsError::StreamNotFound(_))
    ));

    // --- a damaged genesis is Corrupt naming seq 0 on both strict
    // surfaces: malformed bytes, and an envelope claiming a different
    // stream under the genesis key.
    let kernel = KernelHandle::with_in_memory_store("r3-genesis");
    let streams = streams_on_store(&kernel);
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    let stream = streams.create_stream(b"cfg").await.unwrap();
    replace_object(
        &keyspace,
        &log_key(&stream, 0),
        bytes::Bytes::from_static(b"not-a-stream-envelope"),
    )
    .await;
    assert!(matches!(
        streams.read_event(&stream, 1).await.unwrap_err(),
        StreamsError::Corrupt {
            missing_or_mismatched,
            ..
        } if missing_or_mismatched == vec![0]
    ));
    assert!(matches!(
        streams.read_range(&stream, 0, 3, 10).await.unwrap_err(),
        StreamsError::Corrupt {
            missing_or_mismatched,
            ..
        } if missing_or_mismatched == vec![0]
    ));
    let alien = support::hand_envelope("r3-alien-stream", 0, "genesis", GENESIS_SCHEMA_ID, b"cfg");
    replace_object(&keyspace, &log_key(&stream, 0), alien).await;
    assert!(matches!(
        streams.read_event(&stream, 1).await.unwrap_err(),
        StreamsError::Corrupt {
            missing_or_mismatched,
            ..
        } if missing_or_mismatched == vec![0]
    ));
    assert!(matches!(
        streams.read_range(&stream, 0, 3, 10).await.unwrap_err(),
        StreamsError::Corrupt {
            missing_or_mismatched,
            ..
        } if missing_or_mismatched == vec![0]
    ));

    // --- floor, absence, damage: typed and distinct from each other.
    let kernel = KernelHandle::with_in_memory_store("r3-boundaries");
    let streams = streams_on_store(&kernel);
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=5u64 {
        streams
            .append(&stream, &schema(), &event(&format!("edge-{index}")), &[])
            .await
            .unwrap();
    }
    streams.trim(&stream, 3).await.unwrap();
    // Pre-GC: the object at seq 2 still exists — the certificate, not
    // object absence, is the boundary.
    assert!(matches!(
        streams.read_event(&stream, 2).await,
        Err(StreamsError::OffsetExpired {
            first_retained: 3,
            ..
        })
    ));
    streams.gc(&stream).await.unwrap();
    assert!(matches!(
        streams.read_event(&stream, 2).await,
        Err(StreamsError::OffsetExpired {
            first_retained: 3,
            ..
        })
    ));
    // Absence at or above the floor is a missing event, not expiry
    // and not damage.
    assert!(matches!(
        streams.read_event(&stream, 9).await,
        Err(StreamsError::EventMissing { seq: 9, .. })
    ));
    // A malformed object at the demanded seq is Corrupt naming it.
    replace_object(
        &keyspace,
        &log_key(&stream, 4),
        bytes::Bytes::from_static(b"not-an-envelope"),
    )
    .await;
    assert!(matches!(
        streams.read_event(&stream, 4).await.unwrap_err(),
        StreamsError::Corrupt {
            missing_or_mismatched,
            ..
        } if missing_or_mismatched == vec![4]
    ));
    // A key-mismatched envelope (claims seq 5 under seq 4's key) is
    // the same loud corruption, never a served lie.
    let lying = support::hand_envelope(stream.as_str(), 5, "liar", "lie.v1", b"x");
    replace_object(&keyspace, &log_key(&stream, 4), lying).await;
    assert!(matches!(
        streams.read_event(&stream, 4).await.unwrap_err(),
        StreamsError::Corrupt {
            missing_or_mismatched,
            ..
        } if missing_or_mismatched == vec![4]
    ));
}

/// P4/F1: `read_range` refuses `limit == 0` and
/// `after_seq >= through_seq` before any storage request.
#[tokio::test]
async fn r4_read_range_argument_preflight_is_side_effect_free() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(&[]).await.unwrap();
    streams
        .append(&stream, &schema(), &event("r4-1"), b"one")
        .await
        .unwrap();
    let baseline = loopback.request_log().len();

    for (after, through, limit) in [
        (0u64, 5u64, 0usize),    // limit == 0
        (5, 5, 3),               // after == through
        (7, 3, 3),               // after > through
        (u64::MAX, u64::MAX, 1), // degenerate at the ceiling
    ] {
        let error = streams
            .read_range(&stream, after, through, limit)
            .await
            .unwrap_err();
        assert!(
            matches!(error, StreamsError::InvalidArgument(_)),
            "({after}, {through}, {limit}) must be InvalidArgument, got {error:?}"
        );
    }
    assert_eq!(
        loopback.request_log().len(),
        baseline,
        "argument preflight must be side-effect free"
    );
    loopback.shutdown();
}

/// P5/F3/F4: a dense window is served complete, contiguous, and in
/// order, or the call is a typed error — an interior hole is
/// `EventMissing` naming the lowest absent seq, damage names the
/// damaged seq, a below-floor start is
/// `OffsetExpired`, and a store failure is the store-failure error.
/// Never a partial successful page with missing history.
#[tokio::test]
async fn r5_read_range_complete_window_or_typed_error() {
    // --- dense windows and the limit cut.
    let streams = streams_on_in_memory_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=6u64 {
        streams
            .append(&stream, &schema(), &event(&format!("r5-{index}")), &[])
            .await
            .unwrap();
    }
    let whole = streams.read_range(&stream, 0, 6, 100).await.unwrap();
    assert_eq!(seqs_of(&whole), (1..=6).collect::<Vec<_>>());
    assert_eq!(
        ids_of(&whole),
        vec!["r5-1", "r5-2", "r5-3", "r5-4", "r5-5", "r5-6"]
    );
    assert!(whole.reached_end);
    let interior = streams.read_range(&stream, 2, 5, 100).await.unwrap();
    assert_eq!(seqs_of(&interior), vec![3, 4, 5]);
    assert!(interior.reached_end);
    let cut = streams.read_range(&stream, 0, 6, 2).await.unwrap();
    assert_eq!(seqs_of(&cut), vec![1, 2]);
    assert!(!cut.reached_end);
    // A limit exactly the window still serves it complete.
    let exact = streams.read_range(&stream, 0, 6, 6).await.unwrap();
    assert_eq!(seqs_of(&exact), (1..=6).collect::<Vec<_>>());
    assert!(exact.reached_end);

    // --- interior hole: typed error naming the missing seq.
    let kernel = KernelHandle::with_in_memory_store("r5-hole");
    let streams = streams_on_store(&kernel);
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=6u64 {
        streams
            .append(&stream, &schema(), &event(&format!("hole-{index}")), &[])
            .await
            .unwrap();
    }
    keyspace.delete(&log_key(&stream, 4)).await.unwrap();
    let error = streams.read_range(&stream, 2, 6, 100).await.unwrap_err();
    assert!(
        matches!(error, StreamsError::EventMissing { seq: 4, .. }),
        "a missing demanded slot is EventMissing naming the lowest absent seq, got {error:?}"
    );

    // --- damage inside a demanded window: Corrupt naming the seq.
    let kernel = KernelHandle::with_in_memory_store("r5-damage");
    let streams = streams_on_store(&kernel);
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=6u64 {
        streams
            .append(&stream, &schema(), &event(&format!("damaged-{index}")), &[])
            .await
            .unwrap();
    }
    replace_object(
        &keyspace,
        &log_key(&stream, 5),
        bytes::Bytes::from_static(b"not-an-envelope"),
    )
    .await;
    let error = streams.read_range(&stream, 0, 6, 100).await.unwrap_err();
    assert!(
        matches!(
            error,
            StreamsError::Corrupt {
                missing_or_mismatched,
                ..
            } if missing_or_mismatched.contains(&5)
        ),
        "damage must be Corrupt naming seq 5, got {error:?}"
    );

    // --- retention: a window starting below the certified floor is
    // the typed boundary (before and after the sweeper); the boundary
    // window itself still serves densely.
    let kernel = KernelHandle::with_in_memory_store("r5-retention");
    let streams = streams_on_store(&kernel);
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=6u64 {
        streams
            .append(&stream, &schema(), &event(&format!("kept-{index}")), &[])
            .await
            .unwrap();
    }
    streams.trim(&stream, 4).await.unwrap();
    assert!(matches!(
        streams.read_range(&stream, 1, 6, 100).await,
        Err(StreamsError::OffsetExpired {
            first_retained: 4,
            ..
        })
    ));
    streams.gc(&stream).await.unwrap();
    assert!(matches!(
        streams.read_range(&stream, 1, 6, 100).await,
        Err(StreamsError::OffsetExpired {
            first_retained: 4,
            ..
        })
    ));
    let boundary = streams.read_range(&stream, 3, 6, 100).await.unwrap();
    assert_eq!(seqs_of(&boundary), vec![4, 5, 6]);
    assert!(boundary.reached_end);

    // --- store failure inside the window: the store-failure error,
    // never a boundary outcome, never a partial success.
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=3u64 {
        streams
            .append(&stream, &schema(), &event(&format!("fetch-{index}")), &[])
            .await
            .unwrap();
    }
    loopback
        .arm_fault(
            StorageOp::Get,
            Some(&wire_log_key(&stream, 2)),
            FaultPhase::Before,
        )
        .await;
    let cut = streams.read_range(&stream, 0, 3, 10).await.unwrap_err();
    assert!(
        matches!(cut, StreamsError::Unavailable { .. }),
        "a cut window GET is the store-failure error, got {cut:?}"
    );

    // The retention lookup the range path requires: cutting it is the
    // bounded API's deterministic store fault — `Unavailable` — never
    // a success, never a boundary variant, and never live replay's
    // `BackendUnqualified` fail-closed.
    loopback
        .arm_fault(StorageOp::List, None, FaultPhase::Before)
        .await;
    let retention = streams.read_range(&stream, 0, 3, 10).await.unwrap_err();
    assert!(
        matches!(retention, StreamsError::Unavailable { .. }),
        "a cut trim lookup is the store-failure error, got {retention:?}"
    );
    loopback.shutdown();
}

/// P6/F5: `reached_end` tracks the window boundary, not the live
/// tail — true at `through_seq` with events existing beyond it, false
/// on a limit cut before it.
#[tokio::test]
async fn r6_read_range_reached_end_is_window_not_eof() {
    let streams = streams_on_in_memory_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=8u64 {
        streams
            .append(&stream, &schema(), &event(&format!("eof-{index}")), &[])
            .await
            .unwrap();
    }

    // through_seq == 5 while events 6..=8 live beyond it: the page
    // claims its window boundary, not live EOF.
    let window = streams.read_range(&stream, 2, 5, 100).await.unwrap();
    assert_eq!(seqs_of(&window), vec![3, 4, 5]);
    assert_eq!(window.events.last().expect("nonempty").seq(), 5);
    assert!(
        window.reached_end,
        "reached_end is the demanded window boundary"
    );

    // A limit cut before through_seq: false, even though the log
    // continues.
    let cut = streams.read_range(&stream, 0, 8, 3).await.unwrap();
    assert_eq!(seqs_of(&cut), vec![1, 2, 3]);
    assert_eq!(cut.events.last().expect("nonempty").seq(), 3);
    assert!(
        !cut.reached_end,
        "a limit cut has not reached the window boundary"
    );
}

/// P7/F6: resuming with `after_seq` set to the last returned seq
/// walks the whole window without gap or duplication, and a window
/// ending at `u64::MAX` is served without overflow.
#[tokio::test]
async fn r7_read_range_resume_and_seq_max() {
    let streams = streams_on_in_memory_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=7u64 {
        streams
            .append(&stream, &schema(), &event(&format!("walk-{index}")), &[])
            .await
            .unwrap();
    }

    // 3+3+1: every page resumes at its last returned seq.
    let mut collected: Vec<u64> = Vec::new();
    let mut after = 0u64;
    let mut pages = 0u32;
    let mut end_flags = Vec::new();
    loop {
        let page = streams.read_range(&stream, after, 7, 3).await.unwrap();
        assert!(!page.events.is_empty(), "a strict page is never empty");
        collected.extend(seqs_of(&page));
        after = page.events.last().expect("nonempty").seq();
        pages += 1;
        end_flags.push(page.reached_end);
        if page.reached_end {
            break;
        }
    }
    assert_eq!(pages, 3, "the walk paginates 3+3+1");
    assert_eq!(end_flags, vec![false, false, true]);
    assert_eq!(
        collected,
        (1..=7).collect::<Vec<_>>(),
        "resume walks exactly: no gap, no duplication"
    );

    // u64::MAX: representable, demandable, no successor — the window
    // (MAX-1, MAX] serves without overflow.
    let kernel = KernelHandle::with_in_memory_store("r7-seq-max");
    let streams = streams_on_store(&kernel);
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    let stream = streams.create_stream(&[]).await.unwrap();
    keyspace
        .create(
            &log_key(&stream, u64::MAX),
            support::hand_envelope(stream.as_str(), u64::MAX, "ceiling", "ceil.v1", b"c"),
        )
        .await
        .unwrap();
    let ceiling = streams
        .read_range(&stream, u64::MAX - 1, u64::MAX, 10)
        .await
        .unwrap();
    assert_eq!(seqs_of(&ceiling), vec![u64::MAX]);
    assert_eq!(ceiling.events.last().expect("nonempty").seq(), u64::MAX);
    assert!(ceiling.reached_end, "the window ends at u64::MAX");
}

/// P8/F2: across a multi-page walk, `read_range` issues no log LIST,
/// no tail-hint read, and no write of any kind; its only LIST is the
/// trim-certificate lookup, and the per-page event GET set is exactly
/// the genesis plus the demanded subwindow — never an event beyond
/// the current page. The growth-then-damage leg holds the boundary
/// twice: answers captured with the head at the cutoff are re-served
/// identically — with the same shape — after events append beyond
/// it, and again after the suffix is hidden and the tail-hint
/// accelerator corrupted.
#[tokio::test]
async fn r8_read_range_request_shape_no_log_list_no_tail_no_writes() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=9u64 {
        streams
            .append(&stream, &schema(), &event(&format!("page-{index}")), &[])
            .await
            .unwrap();
    }

    let mut collected: Vec<u64> = Vec::new();
    let mut after = 0u64;
    for (demanded, reached_end) in [
        (vec![1, 2, 3, 4], false),
        (vec![5, 6, 7, 8], false),
        (vec![9], true),
    ] {
        let baseline = loopback.request_log().len();
        let page = streams.read_range(&stream, after, 9, 4).await.unwrap();
        assert_eq!(seqs_of(&page), demanded);
        assert_eq!(page.reached_end, reached_end);
        after = page.events.last().expect("nonempty").seq();
        collected.extend(seqs_of(&page));
        assert_strict_request_shape(&loopback.request_log()[baseline..], &stream, &demanded, 1);
    }
    assert_eq!(collected, (1..=9).collect::<Vec<_>>());
    assert_eq!(after, 9);

    // --- growth-then-damage boundary: a fixed endpoint answers
    // nothing about the live tail. Capture the strict answers with
    // the head at 9, append beyond the cutoff and assert the answers
    // and the request shape are unchanged, then damage the suffix (a
    // hidden log object — GET-absent while LIST still reports it) and
    // the tail-hint accelerator the live read path treats as its
    // completeness witness, and assert again.
    let reference_point = streams.read_event(&stream, 7).await.unwrap();
    let reference_page = streams.read_range(&stream, 4, 9, 100).await.unwrap();
    assert_eq!(seqs_of(&reference_page), vec![5, 6, 7, 8, 9]);
    assert!(reference_page.reached_end);

    // Growth: three more events land beyond the cutoff; the captured
    // answers are re-served identically and the wire never leaves the
    // demanded window.
    for index in 10..=12u64 {
        streams
            .append(&stream, &schema(), &event(&format!("suffix-{index}")), &[])
            .await
            .unwrap();
    }
    let baseline = loopback.request_log().len();
    let grown_point = streams.read_event(&stream, 7).await.unwrap();
    assert_eq!(grown_point.seq(), reference_point.seq());
    assert_eq!(
        grown_point.stable_event_id(),
        reference_point.stable_event_id()
    );
    assert_eq!(grown_point.payload(), reference_point.payload());
    assert_eq!(
        grown_point.payload_sha256(),
        reference_point.payload_sha256()
    );
    assert_strict_request_shape(&loopback.request_log()[baseline..], &stream, &[7], 1);

    let baseline = loopback.request_log().len();
    let grown_page = streams.read_range(&stream, 4, 9, 100).await.unwrap();
    assert_eq!(seqs_of(&grown_page), seqs_of(&reference_page));
    assert_eq!(ids_of(&grown_page), ids_of(&reference_page));
    assert_eq!(grown_page.reached_end, reference_page.reached_end);
    assert_strict_request_shape(
        &loopback.request_log()[baseline..],
        &stream,
        &[5, 6, 7, 8, 9],
        1,
    );

    // Damage: hide the suffix event at seq 11 and corrupt the tail
    // hint.
    loopback.hide_key(&wire_log_key(&stream, 11)).await;
    let keyspace = loopback.kernel().atomic_keyspace("streams/v1").unwrap();
    replace_object(
        &keyspace,
        &format!("{}/tail", stream.as_str()),
        bytes::Bytes::from_static(b"not-a-tail-hint"),
    )
    .await;

    // The damage is in effect, and a strict answer does change when
    // the damaged seq is the demand: the hidden key reads as a
    // confirmed absence.
    assert!(matches!(
        streams.read_event(&stream, 11).await,
        Err(StreamsError::EventMissing { seq: 11, .. })
    ));

    // The point read within the cutoff: identical answer, and the
    // wire shows exactly genesis + target + one trims LIST — no
    // suffix GET, no tail access.
    let baseline = loopback.request_log().len();
    let point = streams.read_event(&stream, 7).await.unwrap();
    assert_eq!(point.seq(), reference_point.seq());
    assert_eq!(point.stable_event_id(), reference_point.stable_event_id());
    assert_eq!(point.payload(), reference_point.payload());
    assert_eq!(point.payload_sha256(), reference_point.payload_sha256());
    assert_strict_request_shape(&loopback.request_log()[baseline..], &stream, &[7], 1);

    // The range read within the cutoff: the identical page, again
    // with an unchanged request shape.
    let baseline = loopback.request_log().len();
    let page = streams.read_range(&stream, 4, 9, 100).await.unwrap();
    assert_eq!(seqs_of(&page), seqs_of(&reference_page));
    assert_eq!(ids_of(&page), ids_of(&reference_page));
    assert_eq!(page.reached_end, reference_page.reached_end);
    assert_strict_request_shape(
        &loopback.request_log()[baseline..],
        &stream,
        &[5, 6, 7, 8, 9],
        1,
    );
    loopback.shutdown();
}

/// P9/F7: the immutable envelope surface, the digest contract, and
/// `EventRef` — proven against the independent `hand_envelope` wire
/// encoder, so the persisted format itself is the witness: the object
/// still decodes and verifies through the strict read path;
/// `payload_sha256()` is the payload's digest and the digest persisted
/// in the object; the tail-witness digest over the encoded envelope
/// stays a different value; `EventRef` round-trips with exactly its
/// four fields; and `AppendReceipt::event_ref()` agrees with the
/// landed envelope's. (Field privacy itself is compiler-enforced —
/// decided by source inspection, not a runtime leg.)
#[tokio::test]
async fn r9_envelope_immutable_surface_and_event_ref() {
    let kernel = KernelHandle::with_in_memory_store("r9-surface");
    let streams = streams_on_store(&kernel);
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    let stream = streams.create_stream(&[]).await.unwrap();
    let payload = b"hand payload".as_slice();
    keyspace
        .create(
            &log_key(&stream, 1),
            support::hand_envelope(stream.as_str(), 1, "hand-1", "hand.v1", payload),
        )
        .await
        .unwrap();

    // The independently encoded object decodes and verifies through
    // the strict read path, accessor-complete.
    let envelope = streams.read_event(&stream, 1).await.unwrap();
    assert_eq!(envelope.format_version(), ENVELOPE_FORMAT_VERSION);
    assert_eq!(envelope.stream_id(), &stream);
    assert_eq!(envelope.seq(), 1);
    assert_eq!(envelope.stable_event_id().as_str(), "hand-1");
    assert_eq!(envelope.schema_id().as_str(), "hand.v1");
    assert_eq!(envelope.payload().as_ref(), payload);

    // payload_sha256: the SHA-256 of the payload, the digest persisted
    // in the object, and stable across reads.
    let digest = sha256_hex(payload);
    assert_eq!(envelope.payload_sha256(), digest);
    let raw = keyspace.get(&log_key(&stream, 1)).await.unwrap().unwrap();
    let persisted: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(
        persisted["payload_sha256"].as_str(),
        Some(digest.as_str()),
        "the getter returns the digest persisted in the object"
    );
    let reread = streams.read_event(&stream, 1).await.unwrap();
    assert_eq!(reread.payload_sha256(), digest);

    // The tail-witness digest — SHA-256 over the ENCODED envelope
    // bytes — is a different value `payload_sha256` neither equals
    // nor stands in for.
    let encoded_digest = sha256_hex(&raw);
    assert_ne!(encoded_digest, digest);
    assert_ne!(envelope.payload_sha256(), encoded_digest);

    // EventRef: exactly the four fields, serde-round-trippable.
    let reference = envelope.event_ref().clone();
    let json = serde_json::to_value(&reference).unwrap();
    let mut keys: Vec<&str> = json
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        ["payload_sha256", "seq", "stable_event_id", "stream_id"],
        "EventRef serializes exactly its four fields"
    );
    let round_tripped: EventRef = serde_json::from_value(json).unwrap();
    assert_eq!(round_tripped, reference);
    assert_eq!(reference.stream_id, stream);
    assert_eq!(reference.seq, 1);
    assert_eq!(reference.stable_event_id.as_str(), "hand-1");
    assert_eq!(reference.payload_sha256, digest);

    // AppendReceipt::event_ref() equals the landed envelope's.
    let receipt = streams
        .append(&stream, &schema(), &event("r9-appended"), b"appended")
        .await
        .unwrap();
    let landed = streams.read_event(&stream, receipt.seq).await.unwrap();
    assert_eq!(receipt.event_ref(), landed.event_ref().clone());
    assert_eq!(receipt.event_ref().payload_sha256, sha256_hex(b"appended"));
}
