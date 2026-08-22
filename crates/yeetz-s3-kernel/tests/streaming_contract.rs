//! The ADR 0004 public-API contract legs on the shared in-memory
//! store: the A28 logical-range boundary table, the A32 public bound
//! surface (seal's canonicality floor, whole-value size bounds), the
//! A30 state algebra parity, and the A35 reserved-root guard. The
//! wire legs (request shapes, fault cuts, conditional races, GC) run
//! in the kernel's loopback contract module; in-memory conditional
//! deletes fail closed by design (batch 8), so nothing here exercises
//! them.

use bytes::Bytes;
use yeetz_s3_kernel::state_kernel::{KernelLineage, SuccessorPolicy};
use yeetz_s3_kernel::{
    AtomicKeyspace, ChunkInventory, KernelHandle, KeyspaceError, StreamKeyState,
    ValueRepresentation,
};

const CHUNK_BYTES: usize = 16 * 1024 * 1024;
/// Two full chunks plus a tail: the smallest canonical v3.
const STREAMED_LEN: usize = 2 * CHUNK_BYTES + 4096;

fn keyspace(bucket: &str, namespace: &str) -> AtomicKeyspace {
    KernelHandle::with_in_memory_store(bucket)
        .atomic_keyspace(namespace)
        .unwrap()
}

fn pattern(len: usize, seed: u8) -> Bytes {
    let mut bytes = Vec::with_capacity(len);
    let mut state = u32::from(seed) | 0x9E37_79B9;
    while bytes.len() < len {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        bytes.push((state >> 24) as u8);
    }
    Bytes::from(bytes)
}

async fn stream_create(keyspace: &AtomicKeyspace, key: &str, data: &Bytes) {
    use tokio::io::AsyncWriteExt;
    let mut writer = keyspace.begin_stream_create(key).await.unwrap();
    writer.write_all(data).await.unwrap();
    writer.seal().await.unwrap().commit().await.unwrap();
}

async fn read_range(keyspace: &AtomicKeyspace, key: &str, start: u64, end: u64) -> Bytes {
    use tokio::io::AsyncReadExt;
    let mut reader = keyspace
        .open_stream_range(key, start..end)
        .await
        .unwrap()
        .expect("present");
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    Bytes::from(out)
}

// --- A28: the range boundary table -------------------------------------------

#[tokio::test]
async fn a28_range_boundary_table() {
    let keyspace = keyspace("a28", "ns");
    let data = pattern(STREAMED_LEN, 0x28);
    stream_create(&keyspace, "cell", &data).await;
    let len = STREAMED_LEN as u64;
    let slice = |start: usize, end: usize| data.slice(start..end);

    // Every boundary shape: empty, first byte, mid-chunk, the exact
    // 16 MiB boundary, a window crossing it, the final byte, the
    // whole value, and an EOF-length empty window.
    let cases: Vec<(u64, u64, Bytes)> = vec![
        (0, 0, Bytes::new()),
        (0, 1, slice(0, 1)),
        (1, CHUNK_BYTES as u64, slice(1, CHUNK_BYTES)),
        (0, CHUNK_BYTES as u64, slice(0, CHUNK_BYTES)),
        (
            CHUNK_BYTES as u64 - 1,
            CHUNK_BYTES as u64 + 1,
            slice(CHUNK_BYTES - 1, CHUNK_BYTES + 1),
        ),
        (
            CHUNK_BYTES as u64,
            2 * CHUNK_BYTES as u64,
            slice(CHUNK_BYTES, 2 * CHUNK_BYTES),
        ),
        (len - 1, len, slice(STREAMED_LEN - 1, STREAMED_LEN)),
        (0, len, data.clone()),
        (len, len, Bytes::new()),
        (
            2 * CHUNK_BYTES as u64,
            len,
            slice(2 * CHUNK_BYTES, STREAMED_LEN),
        ),
    ];
    for (start, end, expected) in cases {
        let observed = read_range(&keyspace, "cell", start, end).await;
        assert_eq!(
            (start, end, observed.len()),
            (start, end, expected.len()),
            "range [{start},{end}) length"
        );
        assert_eq!(observed, expected, "range [{start},{end}) bytes");
    }

    // Out of bounds is typed, never clamped.
    match keyspace.open_stream_range("cell", 0..len + 1).await {
        Err(KeyspaceError::InvalidRange { logical_len, .. }) => assert_eq!(logical_len, len),
        other => panic!("end beyond length must be typed InvalidRange, got {other:?}"),
    }
    match keyspace.open_stream_range("cell", len + 1..len + 2).await {
        Err(KeyspaceError::InvalidRange { .. }) => {}
        other => panic!("start beyond length must be typed, got {other:?}"),
    }
    match keyspace.open_stream_range("cell", 5..4).await {
        Err(KeyspaceError::InvalidRange {
            start: 5, end: 4, ..
        }) => {}
        other => panic!("reversed range must be typed InvalidRange, got {other:?}"),
    }
    match keyspace.open_stream_range("cell", len + 1..len).await {
        Err(KeyspaceError::InvalidRange { .. }) => {}
        other => panic!("reversed range above EOF must be typed, got {other:?}"),
    }

    // The full-stream read equals the collected whole value.
    let whole = keyspace.get("cell").await.unwrap().unwrap();
    assert_eq!(whole, data);
    let full = read_range(&keyspace, "cell", 0, len).await;
    assert_eq!(full, whole);
}

// --- A30 (public-API parity) ---------------------------------------------------

#[tokio::test]
async fn a30_stream_state_parity_on_the_public_surface() {
    let keyspace = keyspace("a30p", "ns");
    let data = pattern(STREAMED_LEN, 0x30);
    stream_create(&keyspace, "cell", &data).await;

    match keyspace.read_state_stream("cell").await.unwrap() {
        StreamKeyState::Present { reader, metadata } => {
            assert_eq!(metadata.representation, ValueRepresentation::Chunked);
            assert_eq!(metadata.logical_len as usize, STREAMED_LEN);
            // The digest table is the cache identity — opaque, ordered.
            assert_eq!(reader.chunk_digests().len(), 3);
        }
        other => panic!("Present expected, got {other:?}"),
    }
    assert!(matches!(
        keyspace.read_state_stream("ghost").await.unwrap(),
        StreamKeyState::Absent
    ));
    keyspace.destroy("cell", "a30", "test").await.unwrap();
    assert!(matches!(
        keyspace.read_state_stream("cell").await.unwrap(),
        StreamKeyState::Destroyed { .. }
    ));
}

// --- A32 (public bound surface) --------------------------------------------------

#[tokio::test]
async fn a32_public_bounds_seal_floor_and_inline_band() {
    let keyspace = keyspace("a32p", "ns");

    // The streamed writer refuses a sub-canonical value: one chunk
    // would make v3 non-canonical (inline is canonical).
    use tokio::io::AsyncWriteExt;
    let small = pattern(1024, 0x32);
    let mut writer = keyspace.begin_stream_create("small").await.unwrap();
    writer.write_all(&small).await.unwrap();
    match writer.seal().await {
        Err(KeyspaceError::ChunkCountInvalid { count, .. }) => assert_eq!(count, 1),
        other => panic!("one-chunk stream must refuse (inline is canonical), got {other:?}"),
    }
    assert_eq!(keyspace.get("small").await.unwrap(), None);

    // The 16–64 MiB band stays inline through the whole-value API.
    let band = pattern(32 * 1024 * 1024, 0x33);
    keyspace.create("band", band.clone()).await.unwrap();
    let reader = keyspace.open_stream("band").await.unwrap().unwrap();
    assert_eq!(
        reader.metadata().representation,
        ValueRepresentation::Inline
    );
    assert!(reader.chunk_digests().is_empty());
    drop(reader);
    assert_eq!(keyspace.get("band").await.unwrap(), Some(band));
}

// --- A35: the reserved-root guard --------------------------------------------------

#[tokio::test]
async fn a35_lineage_reserved_roots_rejected_and_near_misses_accepted() {
    for rejected in [
        "keyspace",
        "keyspace/a",
        "keyspace-chunks",
        "keyspace-chunks/x/y",
    ] {
        assert!(
            KernelLineage::new(rejected, SuccessorPolicy::GenesisOnly).is_err(),
            "{rejected} must not be an occupiable lineage"
        );
    }
    // Segment equality, not substring matching.
    for accepted in [
        "keyspace-x",
        "keyspaces",
        "x/keyspace",
        "keyspac",
        "keyspace-chunks-x",
        "ordinary-lineage",
    ] {
        assert!(
            KernelLineage::new(accepted, SuccessorPolicy::GenesisOnly).is_ok(),
            "{accepted} remains valid"
        );
    }
}

// --- Metering classification on the public surface -----------------------------------

#[tokio::test]
async fn inventory_classifies_without_deleting() {
    let keyspace = keyspace("inv", "ns");
    let data = pattern(STREAMED_LEN, 0x40);
    stream_create(&keyspace, "cell", &data).await;
    let inventory: ChunkInventory = keyspace.chunk_inventory().await.unwrap();
    assert_eq!(inventory.listed_chunks, 3);
    assert_eq!(inventory.referenced_chunks, 3);
    assert_eq!(inventory.candidate_orphan_chunks, 0);
    // The whole value survives metering untouched.
    assert_eq!(keyspace.get("cell").await.unwrap(), Some(data));
}
