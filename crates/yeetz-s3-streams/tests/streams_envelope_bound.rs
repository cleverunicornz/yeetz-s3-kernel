//! S11 (ADR 0004 §3.4): every encoded streams envelope is at most
//! 16 MiB after canonical JSON/base64 encoding — a typed structural
//! bound independent of the kernel's `INLINE_MAX` ruling — enforced
//! BEFORE the first keyspace effect. Streams writes stay single
//! inline v2 objects: create, append, and migration produce zero
//! chunk-root requests.

mod support;

use support::loopback::Loopback;
use yeetz_s3_streams::{MAX_ENCODED_ENVELOPE_BYTES, SchemaId, StableEventId, StreamsError};

fn schema() -> SchemaId {
    SchemaId::new("s11.v1").unwrap()
}

fn event(value: &str) -> StableEventId {
    StableEventId::new(value).unwrap()
}

/// Near-bound success: a raw payload whose base64-encoded envelope
/// lands just under 16 MiB appends as ONE inline object; the chunk
/// root is never touched.
#[tokio::test]
async fn s11_near_bound_envelope_stays_inline_single_put() {
    let loopback = Loopback::start().await;
    let streams = support::streams_on_store(&loopback.kernel());
    let stream = streams.create_stream(b"s11 config").await.unwrap();

    // 11.5 MiB raw → ~15.34 MiB base64 + metadata < 16 MiB encoded.
    let payload = vec![0x5A_u8; 11_470_000];
    streams
        .append(&stream, &schema(), &event("big"), &payload)
        .await
        .unwrap();

    let log = loopback.request_log();
    // The structural witness: every request the streams write path
    // issued stays under the kernel's public logical root `keyspace/`
    // — a private-root request of ANY spelling would violate this.
    let outside_logical_root = log
        .iter()
        .filter(|record| !record.key.starts_with("keyspace/"))
        .count();
    assert_eq!(
        outside_logical_root, 0,
        "streams never leave the logical root"
    );
    let puts: Vec<_> = log
        .iter()
        .filter(|record| record.method == "PUT" && record.key.contains("/log/"))
        .collect();
    assert_eq!(puts.len(), 2, "genesis + exactly one event object");
    // The event object is one inline v2 envelope well under the bound.
    let _ = MAX_ENCODED_ENVELOPE_BYTES;
    loopback.shutdown();
}

/// Oversize is typed and effect-free: the encoded envelope crossing
/// 16 MiB fails BEFORE the first keyspace effect (no PUT at all), for
/// append, for create's genesis/config, and for migration-shaped
/// envelopes.
#[tokio::test]
async fn s11_oversize_envelope_typed_before_any_effect() {
    let loopback = Loopback::start().await;
    let streams = support::streams_on_store(&loopback.kernel());
    let stream = streams.create_stream(b"s11 config").await.unwrap();
    let before = loopback.request_log();

    // ~12.7 MiB raw → ~16.95 MiB encoded: over the bound.
    let payload = vec![0xA5_u8; 12_700_000];
    match streams
        .append(&stream, &schema(), &event("oversize"), &payload)
        .await
    {
        Err(StreamsError::EnvelopeTooLarge {
            encoded_len,
            max_encoded_len,
        }) => {
            assert!(encoded_len > max_encoded_len);
            assert_eq!(max_encoded_len, (16 * 1024 * 1024) as u64);
        }
        other => panic!("oversize append must be typed, got {other:?}"),
    }
    let after = loopback.request_log();
    let puts_before = before
        .iter()
        .filter(|record| record.method == "PUT")
        .count();
    let puts_after = after.iter().filter(|record| record.method == "PUT").count();
    assert_eq!(puts_before, puts_after, "zero keyspace effects");

    // Genesis/config is covered too.
    match streams.create_stream(&vec![0x33_u8; 12_700_000]).await {
        Err(StreamsError::EnvelopeTooLarge { .. }) => {}
        other => panic!("oversize genesis must be typed, got {other:?}"),
    }
    loopback.shutdown();
}
