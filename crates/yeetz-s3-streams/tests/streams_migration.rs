//! Migration-contract tests (ADR 0017 addendum): explicit-seq copy,
//! density verification, idempotent re-run, disagreement errors, and
//! the immutable seal.

mod support;

use bytes::Bytes;
use support::{streams_on_in_memory_store, streams_on_store};
use yeetz_s3_kernel::KernelHandle;
use yeetz_s3_streams::migration::{MigrationEntry, MigrationSeal};
use yeetz_s3_streams::{Replay, SchemaId, StableEventId, StreamsError};

fn schema() -> SchemaId {
    SchemaId::new("migrated.event.v1").unwrap()
}

fn stable(id: &str) -> StableEventId {
    StableEventId::new(id).unwrap()
}

/// Owned entries avoid lifetime gymnastics in the literals.
fn owned(seq: u64, id: &str, payload: &[u8]) -> OwnedEntry {
    OwnedEntry {
        seq,
        schema_id: schema(),
        stable_event_id: stable(id),
        payload: payload.to_vec(),
    }
}

struct OwnedEntry {
    seq: u64,
    schema_id: SchemaId,
    stable_event_id: StableEventId,
    payload: Vec<u8>,
}

impl OwnedEntry {
    fn borrowed(&self) -> MigrationEntry<'_> {
        MigrationEntry {
            seq: self.seq,
            schema_id: &self.schema_id,
            stable_event_id: &self.stable_event_id,
            payload: &self.payload,
        }
    }
}

#[tokio::test]
async fn migrate_log_copies_at_explicit_seqs_and_verifies_density() {
    let streams = streams_on_in_memory_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    let receipt = streams
        .migrate_log(
            &stream,
            &[
                owned(1, "old-1", b"one").borrowed(),
                owned(2, "old-2", b"two").borrowed(),
                owned(3, "old-3", b"three").borrowed(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(receipt.count, 3);
    // Seqs preserved exactly.
    match streams.read(&stream, 0, 10).await {
        Replay::Page {
            events, complete, ..
        } => {
            assert!(complete);
            let seqs: Vec<u64> = events.iter().map(|e| e.seq).collect();
            assert_eq!(seqs, vec![1, 2, 3]);
            assert_eq!(events[0].payload.as_ref(), b"one");
            assert_eq!(events[2].stable_event_id.as_str(), "old-3");
        }
        other => panic!("expected page, got {other:?}"),
    }
    // Re-run over identical bytes is idempotent with the same root.
    let again = streams
        .migrate_log(
            &stream,
            &[
                owned(1, "old-1", b"one").borrowed(),
                owned(2, "old-2", b"two").borrowed(),
                owned(3, "old-3", b"three").borrowed(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(again.event_root_digest, receipt.event_root_digest);
}

#[tokio::test]
async fn migrate_log_rejects_sparse_and_conflicting_entries() {
    let streams = streams_on_in_memory_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    // Sparse (gap at 2) is rejected up front.
    let sparse = streams
        .migrate_log(
            &stream,
            &[
                owned(1, "a", b"a").borrowed(),
                owned(3, "c", b"c").borrowed(),
            ],
        )
        .await;
    assert!(sparse.is_err(), "gap rejected before any write");
    // Nothing was written by the rejected run.
    assert!(matches!(streams.read(&stream, 0, 10).await, Replay::Empty));

    // Disagreement with landed bytes is a typed error, never an
    // overwrite.
    streams
        .migrate_log(&stream, &[owned(1, "a", b"a").borrowed()])
        .await
        .unwrap();
    let conflict = streams
        .migrate_log(&stream, &[owned(1, "different", b"other").borrowed()])
        .await
        .unwrap_err();
    assert!(conflict.to_string().contains("different bytes"));
    match streams.read(&stream, 0, 10).await {
        Replay::Page { events, .. } => {
            assert_eq!(events[0].stable_event_id.as_str(), "a");
            assert_eq!(events[0].payload.as_ref(), b"a");
        }
        other => panic!("expected page, got {other:?}"),
    }
    let _ = conflict;
}

#[tokio::test]
async fn seal_is_immutable_and_readable() {
    let streams = streams_on_in_memory_store();
    let stream = streams.create_stream(&[]).await.unwrap();
    let receipt = streams
        .migrate_log(&stream, &[owned(1, "a", b"a").borrowed()])
        .await
        .unwrap();
    let seal = MigrationSeal {
        format_version: 1,
        source_lineage: "events/demo/hello".into(),
        source_head_digest: "cafebabe".into(),
        event_count: receipt.count,
        event_root_digest: receipt.event_root_digest.clone(),
    };
    streams.write_migration_seal(&stream, &seal).await.unwrap();
    // Identical re-write is idempotent.
    streams.write_migration_seal(&stream, &seal).await.unwrap();
    // Different content is refused.
    let mut tampered = seal.clone();
    tampered.event_count = 99;
    let err = streams
        .write_migration_seal(&stream, &tampered)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("different content"));
    // Readback round-trips.
    let landed = streams.read_migration_seal(&stream).await.unwrap().unwrap();
    assert_eq!(landed, seal);
    // Unmigrated streams read None.
    let other = streams.create_stream(&[]).await.unwrap();
    assert!(streams.read_migration_seal(&other).await.unwrap().is_none());
}

#[tokio::test]
async fn migrate_requires_existing_stream_genesis() {
    let streams = streams_on_in_memory_store();
    let ghost = yeetz_s3_streams::StreamId::new("sghost").unwrap();
    let err = streams
        .migrate_log(&ghost, &[owned(1, "a", b"a").borrowed()])
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        yeetz_s3_streams::StreamsError::StreamNotFound(_)
    ));
}

#[tokio::test]
async fn migration_validates_genesis_before_writing_events() {
    let kernel = KernelHandle::with_in_memory_store("migration-genesis-integrity");
    let streams = streams_on_store(&kernel);
    let stream = streams.create_stream(&[]).await.unwrap();
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    let genesis_key = format!("{}/log/{:020}", stream.as_str(), 0);
    replace_value(
        &keyspace,
        &genesis_key,
        Bytes::from_static(b"not-a-stream-envelope"),
    )
    .await;

    let result = streams
        .migrate_log(&stream, &[owned(1, "old-1", b"one").borrowed()])
        .await;
    assert!(matches!(
        result,
        Err(StreamsError::Corrupt {
            missing_or_mismatched,
            ..
        }) if missing_or_mismatched == vec![0]
    ));
    assert!(
        keyspace
            .get(&format!("{}/log/{:020}", stream.as_str(), 1))
            .await
            .unwrap()
            .is_none(),
        "no event may land after genesis validation fails"
    );
}

#[tokio::test]
async fn migration_distinguishes_corrupt_event_from_valid_conflict() {
    let kernel = KernelHandle::with_in_memory_store("migration-event-integrity");
    let streams = streams_on_store(&kernel);
    let stream = streams.create_stream(&[]).await.unwrap();
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    keyspace
        .create(
            &format!("{}/log/{:020}", stream.as_str(), 1),
            Bytes::from_static(b"not-a-stream-envelope"),
        )
        .await
        .unwrap();

    let result = streams
        .migrate_log(&stream, &[owned(1, "old-1", b"one").borrowed()])
        .await;
    assert!(matches!(
        result,
        Err(StreamsError::Corrupt {
            missing_or_mismatched,
            ..
        }) if missing_or_mismatched == vec![1]
    ));
}

#[tokio::test]
async fn migration_seal_format_and_stream_existence_are_validated() {
    let kernel = KernelHandle::with_in_memory_store("migration-seal-integrity");
    let streams = streams_on_store(&kernel);
    let stream = streams.create_stream(&[]).await.unwrap();
    let unsupported = MigrationSeal {
        format_version: 99,
        source_lineage: "events/demo/repo".into(),
        source_head_digest: "source-head".into(),
        event_count: 0,
        event_root_digest: "event-root".into(),
    };
    assert!(matches!(
        streams.write_migration_seal(&stream, &unsupported).await,
        Err(StreamsError::InvalidArgument(_))
    ));
    assert!(
        streams
            .read_migration_seal(&stream)
            .await
            .unwrap()
            .is_none()
    );

    let ghost = yeetz_s3_streams::StreamId::new("sghost-seal").unwrap();
    let mut current = unsupported.clone();
    current.format_version = 1;
    assert!(matches!(
        streams.write_migration_seal(&ghost, &current).await,
        Err(StreamsError::StreamNotFound(_))
    ));

    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    keyspace
        .create(
            &format!("{}/migration-seal", stream.as_str()),
            Bytes::from(serde_json::to_vec(&unsupported).unwrap()),
        )
        .await
        .unwrap();
    assert!(matches!(
        streams.read_migration_seal(&stream).await,
        Err(StreamsError::MigrationSealCorrupt { .. })
    ));
}

async fn replace_value(keyspace: &yeetz_s3_kernel::AtomicKeyspace, key: &str, value: Bytes) {
    let (_, etag) = keyspace.get_with_etag(key).await.unwrap().unwrap();
    keyspace.compare_exchange(key, &etag, value).await.unwrap();
}
