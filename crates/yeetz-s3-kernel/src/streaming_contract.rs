//! The ADR 0004 streaming wire contract suite (A24–A34): the
//! loopback-S3 legs proving manifest-only visibility, the commit
//! oracle, conditional stale-era eviction, the corruption taxonomy,
//! state/deletion composition, the inline request profile, the
//! lost-response oracle and crash matrix, and the GC precondition
//! boundary including the broken-quiescence demonstration cut.
//!
//! These run against the kernel's fault-injecting loopback counterpart
//! (the same wire-fidelity surface the batch-8 conditional-delete
//! contracts use): conditional PUTs with etags, conditional deletes,
//! LIST pagination, one-shot fault cuts by request shape, LIST
//! freezing, and deterministic object corruption.

use std::sync::Arc;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use yeetz_sdk_s3::ObjectStoreClient;

use crate::atomic_keyspace::{AtomicKeyspace, KEYSPACE_ROOT, KeyState, KeyspaceError};
use crate::state_kernel::gateway_state_contract::{
    CounterpartSnapshot, LoopbackCounterpart, LoopbackRequestObservation, StorageFaultCut,
    StorageFaultPhase,
};
use crate::value_manifest::{CHUNK_BYTES, CHUNK_ROOT, ManifestEntry, ValueManifest};
use crate::value_stream::{
    Ambiguity, CommitReceipt, StreamKeyState, ValueReader, ValueRepresentation,
};

/// Deterministic test bytes: cheap to generate, distinct per seed, so
/// every chunk of a value hashes differently.
fn pattern(len: usize, seed: u8) -> Bytes {
    let mut bytes = Vec::with_capacity(len);
    let mut state = u32::from(seed) | 0x9E37_79B9;
    while bytes.len() < len {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        bytes.push((state >> 24) as u8);
    }
    Bytes::from(bytes)
}

/// Two full chunks plus a short tail: the smallest canonical v3.
const STREAMED_LEN: usize = 2 * CHUNK_BYTES + 4096;

/// Above `INLINE_MAX` (64 MiB) — six chunks — the whole-value
/// threshold witness.
const WHOLE_CHUNKED_LEN: usize = 5 * CHUNK_BYTES + 99;

/// Inside the preserved 16–64 MiB encoded band: one object.
const INLINE_BAND_LEN: usize = 32 * 1024 * 1024;

async fn keyspace_fixture(
    namespace: &str,
) -> (
    Arc<ObjectStoreClient>,
    Arc<AtomicKeyspace>,
    LoopbackCounterpart,
) {
    let (counterpart, store) = LoopbackCounterpart::start().await;
    let keyspace =
        AtomicKeyspace::new(Arc::clone(&store), namespace).expect("valid keyspace namespace");
    (store, Arc::new(keyspace), counterpart)
}

async fn stream_create(
    keyspace: &AtomicKeyspace,
    key: &str,
    data: &Bytes,
) -> Result<CommitReceipt, KeyspaceError> {
    let mut writer = keyspace.begin_stream_create(key).await?;
    writer.write_all(data).await.expect("streamed write");
    writer.seal().await?.commit().await
}

async fn stream_cas(
    keyspace: &AtomicKeyspace,
    key: &str,
    expected_etag: &str,
    data: &Bytes,
) -> Result<CommitReceipt, KeyspaceError> {
    let mut writer = keyspace
        .begin_stream_compare_exchange(key, expected_etag)
        .await?;
    writer.write_all(data).await.expect("streamed CAS write");
    writer.seal().await?.commit().await
}

async fn read_reader(reader: &mut ValueReader) -> Bytes {
    let mut out = Vec::new();
    reader
        .read_to_end(&mut out)
        .await
        .expect("verified reader reaches EOF");
    Bytes::from(out)
}

async fn wait_for_first_barrier_arrival(counterpart: &LoopbackCounterpart) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if counterpart
                .snapshot()
                .await
                .barrier
                .is_some_and(|barrier| barrier.arrivals >= 1)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("conditional control PUT reaches the barrier");
}

fn control_key(namespace: &str, key: &str) -> String {
    format!("{KEYSPACE_ROOT}/{namespace}/{key}")
}

/// The physical chunk path of `ordinal` for a generation-(0,0) write
/// of `data`: generation-scoped content address, exactly the layout
/// of ADR 0004 §1.3.
fn chunk_key_of(namespace: &str, key: &str, ordinal: usize, data: &Bytes) -> String {
    let start = ordinal * CHUNK_BYTES;
    let end = start + CHUNK_BYTES.min(data.len() - start);
    let digest = hex::encode(Sha256::digest(data.slice(start..end)));
    format!(
        "{CHUNK_ROOT}/v1/{namespace}/{}/{:020}/{:020}/{digest}",
        hex::encode(key),
        0,
        0
    )
}

fn chunk_requests(snapshot: &CounterpartSnapshot) -> Vec<&LoopbackRequestObservation> {
    snapshot
        .requests
        .iter()
        .filter(|request| {
            request
                .key
                .as_deref()
                .is_some_and(|key| key.starts_with(&format!("{CHUNK_ROOT}/")))
        })
        .collect()
}

fn requests_for<'a>(
    snapshot: &'a CounterpartSnapshot,
    key: &str,
) -> Vec<&'a LoopbackRequestObservation> {
    snapshot
        .requests
        .iter()
        .filter(|request| request.key.as_deref() == Some(key))
        .collect()
}

/// Craft a canonical manifest for oracle-driven legs (in-crate
/// construction; the decode rules are proven by the value_manifest
/// unit tests).
fn craft_manifest(incarnation: u64, version: u64, commit_id: [u8; 16]) -> ValueManifest {
    let entries = vec![
        ManifestEntry {
            encoded_len: CHUNK_BYTES as u32,
            sha256: [1; 32],
        },
        ManifestEntry {
            encoded_len: 128,
            sha256: [2; 32],
        },
    ];
    let logical_len = CHUNK_BYTES as u64 + 128;
    ValueManifest {
        incarnation,
        version,
        commit_id,
        value_root_sha256: ValueManifest::compute_value_root(
            logical_len,
            CHUNK_BYTES as u32,
            &entries,
        ),
        logical_len,
        chunk_bytes: CHUNK_BYTES as u32,
        entries,
    }
}

// --- A24: candidate chunks are invisible; the manifest is the commit --------

#[tokio::test]
async fn a24_manifest_only_visibility_and_control_cuts() {
    let (_store, keyspace, counterpart) = keyspace_fixture("a24").await;
    let data = pattern(STREAMED_LEN, 0xA0);
    let object_key = control_key("a24", "cell");

    // Between chunks: candidate chunks exist, the logical map does not
    // move. Seal WITHOUT committing — a complete unreachable candidate.
    let mut writer = keyspace.begin_stream_create("cell").await.unwrap();
    writer.write_all(&data).await.unwrap();
    let pending = writer.seal().await.unwrap();
    assert_eq!(pending.logical_len() as usize, STREAMED_LEN);
    assert_eq!(pending.chunk_count(), 3);
    assert_eq!(keyspace.get("cell").await.unwrap(), None, "invisible");
    assert!(matches!(
        keyspace.read_state_stream("cell").await.unwrap(),
        StreamKeyState::Absent
    ));
    // LIST exposes only `keyspace/...` logical keys — never the
    // private chunk root (ADR 0004 §1.3).
    assert!(keyspace.list_after(None, 1000).await.unwrap().is_empty());

    // Cut the manifest PUT (BeforeEffect): publication refused, the
    // logical state still shows nothing, and the outcome is ambiguous
    // with an absent control — `Unavailable`, never success or
    // conflict.
    counterpart
        .arm_storage_fault(
            StorageFaultCut::KeyspaceConditionalPut,
            StorageFaultPhase::BeforeEffect,
            &object_key,
        )
        .await;
    match pending.commit().await {
        Err(KeyspaceError::Unavailable { operation }) => {
            assert!(operation.contains("ambiguous"), "{operation}");
        }
        other => panic!("refused manifest PUT must be ambiguous-unavailable, got {other:?}"),
    }
    assert_eq!(keyspace.get("cell").await.unwrap(), None);
    assert!(keyspace.list_after(None, 1000).await.unwrap().is_empty());

    // A retrying writer converges: the verified candidate chunks are
    // reused (put-if-absent conflicts verify-accept), the manifest
    // publishes, and the value lands exactly once.
    let receipt = stream_create(&keyspace, "cell", &data).await.unwrap();
    assert_eq!(receipt.chunk_count, 3);
    assert_eq!(receipt.representation, ValueRepresentation::Chunked);
    assert_eq!(keyspace.get("cell").await.unwrap(), Some(data));

    let snapshot = counterpart.snapshot().await;
    let chunk_puts = chunk_requests(&snapshot)
        .into_iter()
        .filter(|request| request.method == "PUT")
        .count();
    assert_eq!(
        chunk_puts, 6,
        "3 candidate chunk PUTs + 3 verify-accepted re-PUTs by the retry"
    );
    let manifest_puts = requests_for(&snapshot, &object_key)
        .into_iter()
        .filter(|request| request.method == "PUT" && request.if_none_match.as_deref() == Some("*"))
        .count();
    assert_eq!(manifest_puts, 2, "the refused cut + the retry's commit");
    counterpart.shutdown().await;
}

// --- A25: whole/stream byte equivalence across inline↔chunked ---------------

#[tokio::test]
async fn a25_whole_stream_equivalence_across_transitions() {
    let (_store, keyspace, counterpart) = keyspace_fixture("a25").await;
    let small = pattern(1024, 0x01);
    let big = pattern(WHOLE_CHUNKED_LEN, 0x02);
    let small_again = pattern(2048, 0x03);

    // Inline start.
    keyspace.create("cell", small.clone()).await.unwrap();
    let (value, etag) = keyspace.get_with_etag("cell").await.unwrap().unwrap();
    assert_eq!(value, small);
    let mut reader = keyspace.open_stream("cell").await.unwrap().unwrap();
    assert_eq!(
        reader.metadata().representation,
        ValueRepresentation::Inline
    );
    assert_eq!(read_reader(&mut reader).await, small);

    // Inline → chunked: whole-value CAS above INLINE_MAX routes
    // through the chunked writer.
    let chunked_etag = keyspace
        .compare_exchange("cell", &etag, big.clone())
        .await
        .unwrap();
    assert_eq!(keyspace.get("cell").await.unwrap(), Some(big.clone()));
    let mut reader = keyspace.open_stream("cell").await.unwrap().unwrap();
    assert_eq!(
        reader.metadata().representation,
        ValueRepresentation::Chunked
    );
    assert_eq!(reader.metadata().logical_len as usize, WHOLE_CHUNKED_LEN);
    assert!(reader.metadata().value_root_sha256.is_some());
    assert_eq!(reader.chunk_digests().len(), 6);
    assert_eq!(read_reader(&mut reader).await, big, "whole == streamed");

    // Chunked → inline.
    keyspace
        .compare_exchange("cell", &chunked_etag, small_again.clone())
        .await
        .unwrap();
    assert_eq!(
        keyspace.get("cell").await.unwrap(),
        Some(small_again.clone())
    );
    let mut reader = keyspace.open_stream("cell").await.unwrap().unwrap();
    assert_eq!(
        reader.metadata().representation,
        ValueRepresentation::Inline
    );
    assert_eq!(read_reader(&mut reader).await, small_again);
    counterpart.shutdown().await;
}

// --- A26: one manifest winner ------------------------------------------------

#[tokio::test]
async fn a26_concurrent_writers_distinct_and_identical_matrix() {
    let (_store, keyspace, counterpart) = keyspace_fixture("a26").await;
    let object_key = control_key("a26", "cell");

    // Distinct content → distinct commit IDs: both bind (0, 0); the
    // conditional-head barrier forces a true simultaneous manifest
    // race. Exactly one writer reports target publication; the loser
    // gets the typed conflict. Chunk presence decides nothing.
    let left = pattern(STREAMED_LEN, 0x11);
    let right = pattern(STREAMED_LEN, 0x22);
    counterpart.arm_conditional_head_barrier(&object_key).await;
    let a = {
        let keyspace = Arc::clone(&keyspace);
        let data = left.clone();
        tokio::spawn(async move { stream_create(&keyspace, "cell", &data).await })
    };
    let b = {
        let keyspace = Arc::clone(&keyspace);
        let data = right.clone();
        tokio::spawn(async move { stream_create(&keyspace, "cell", &data).await })
    };
    let a = a.await.unwrap();
    let b = b.await.unwrap();
    counterpart
        .assert_conditional_head_race(&object_key, true)
        .await;
    match (&a, &b) {
        (Ok(_), Err(KeyspaceError::AlreadyExists(key)))
        | (Err(KeyspaceError::AlreadyExists(key)), Ok(_)) => assert_eq!(key, "cell"),
        other => panic!("exactly one receipt + one typed conflict, got {other:?}"),
    }
    let winner = if a.is_ok() { left } else { right };
    assert_eq!(keyspace.get("cell").await.unwrap(), Some(winner));

    // Identical content: contenders share verified chunks (accepted
    // only after full digest verify), and the control still has
    // exactly one winner.
    let data = pattern(STREAMED_LEN, 0x33);
    let object_key2 = control_key("a26-identical", "cell");
    let keyspace_ident =
        Arc::new(AtomicKeyspace::new(Arc::clone(&_store), "a26-identical").expect("namespace"));
    counterpart.arm_conditional_head_barrier(&object_key2).await;
    let a = {
        let keyspace = Arc::clone(&keyspace_ident);
        let data = data.clone();
        tokio::spawn(async move { stream_create(&keyspace, "cell", &data).await })
    };
    let b = {
        let keyspace = Arc::clone(&keyspace_ident);
        let data = data.clone();
        tokio::spawn(async move { stream_create(&keyspace, "cell", &data).await })
    };
    let a = a.await.unwrap();
    let b = b.await.unwrap();
    assert!(a.is_ok() ^ b.is_ok(), "one winner: {a:?} vs {b:?}");
    assert_eq!(keyspace_ident.get("cell").await.unwrap(), Some(data));
    let inventory = keyspace_ident.chunk_inventory().await.unwrap();
    assert_eq!(
        inventory.listed_chunks, 3,
        "identical contenders share chunks"
    );
    assert_eq!(inventory.referenced_chunks, 3);
    assert_eq!(inventory.candidate_orphan_chunks, 0);
    counterpart.shutdown().await;
}

// --- A27: stale-era eviction must be conditional -------------------------------

#[tokio::test]
async fn a27_chunked_incarnation_race_and_conditional_eviction() {
    let (store, keyspace, counterpart) = keyspace_fixture("a27").await;
    let object_key = control_key("a27", "cell");
    let data = pattern(STREAMED_LEN, 0xA7);

    // Leg 1 — a published stale-era manifest is evicted ONLY with the
    // observed etag. The counter advanced out-of-band (a destroy raced
    // the bind), the manifest PUT's response is lost (AfterEffect),
    // the oracle rules "landed", the post-check sees the moved
    // counter, rereads WITH etag, confirms our exact commit, and
    // evicts conditionally. The streamed writer reports the typed
    // stale-era failure; the key ends empty.
    let mut writer = keyspace.begin_stream_create("cell").await.unwrap();
    writer.write_all(&data).await.unwrap();
    let pending = writer.seal().await.unwrap();
    let incarnation_object_key = format!("{KEYSPACE_ROOT}/a27/incarnations/cell");
    store
        .upload_conditional(
            &incarnation_object_key,
            Bytes::from(1u64.to_be_bytes().to_vec()),
            None,
        )
        .await
        .unwrap();
    counterpart
        .arm_storage_fault(
            StorageFaultCut::KeyspaceConditionalPut,
            StorageFaultPhase::AfterEffect,
            &object_key,
        )
        .await;
    match pending.commit().await {
        Err(KeyspaceError::StaleIncarnation(key)) => assert_eq!(key, "cell"),
        other => panic!("stale-era streamed create must fail typed, got {other:?}"),
    }
    assert_eq!(
        keyspace.get("cell").await.unwrap(),
        None,
        "stale bytes evicted"
    );
    let snapshot = counterpart.snapshot().await;
    let eviction = requests_for(&snapshot, &object_key)
        .into_iter()
        .filter(|request| request.method == "DELETE" && request.if_match.is_some())
        .count();
    assert_eq!(eviction, 1, "the eviction is a conditional delete");
    // A fresh create at the fresh era succeeds.
    keyspace.create("cell", pattern(64, 0xE1)).await.unwrap();

    // Leg 2 — the §2.4 counterexample: a stale token cannot delete a
    // fresh confirmed value B.
    let b_key = "replacement";
    let b_object_key = control_key("a27", b_key);
    let receipt = stream_create(&keyspace, b_key, &data).await.unwrap();
    keyspace.destroy(b_key, "a27", "test").await.unwrap();
    let b_value = pattern(64, 0xE2);
    keyspace.create(b_key, b_value.clone()).await.unwrap();
    match keyspace.delete_if_match(b_key, &receipt.etag).await {
        Err(KeyspaceError::PreconditionFailed {
            observed_incarnation: Some(1),
            observed_version: Some(0),
            ..
        }) => {}
        other => panic!("stale v3 token must refuse naming era (1,0), got {other:?}"),
    }
    assert_eq!(
        keyspace.get(b_key).await.unwrap(),
        Some(b_value),
        "B survives the refused stale-token delete"
    );

    // Leg 3 — the raw-unconditional defect signature: the same
    // interleaving with an unconditional delete destroys B. This is
    // the demonstration of WHY §2.4 is law.
    store.delete(&b_object_key).await.unwrap();
    assert_eq!(
        keyspace.get(b_key).await.unwrap(),
        None,
        "raw delete destroys B — the defect signature"
    );
    counterpart.shutdown().await;
}

/// Batch-10 teardown: the VALUE control needs the same deletion-era
/// closure as the lineage HEAD. A matching etag on a control from a
/// closed incarnation is refused before CAS; a destroy bump between
/// that gate and the manifest PUT makes the landed publication
/// self-evict; and destroy's tail is bound to the exact control etag it
/// loaded.
#[tokio::test]
async fn teardown_value_control_has_era_gates_and_conditional_destroy_tail() {
    let (store, keyspace, counterpart) = keyspace_fixture("k10-value-life").await;
    let data = pattern(STREAMED_LEN, 0xC1);

    // Pre-CAS gate: model destroy's post-bump/pre-delete crash window.
    stream_create(&keyspace, "pre-gate", &data).await.unwrap();
    let (_, stale_etag) = keyspace.get_with_etag("pre-gate").await.unwrap().unwrap();
    store
        .upload_conditional(
            &format!("{KEYSPACE_ROOT}/k10-value-life/incarnations/pre-gate"),
            Bytes::from(1_u64.to_be_bytes().to_vec()),
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        keyspace
            .begin_stream_compare_exchange("pre-gate", &stale_etag)
            .await,
        Err(KeyspaceError::StaleIncarnation(key)) if key == "pre-gate"
    ));
    assert!(matches!(
        keyspace
            .compare_exchange("pre-gate", &stale_etag, Bytes::from_static(b"stale"))
            .await,
        Err(KeyspaceError::StaleIncarnation(key)) if key == "pre-gate"
    ));

    // Post-CAS gate: park the manifest PUT after its era check, advance
    // the counter, then release it with an always-failing second CAS.
    stream_create(&keyspace, "post-gate", &data).await.unwrap();
    let (_, etag) = keyspace.get_with_etag("post-gate").await.unwrap().unwrap();
    let mut writer = keyspace
        .begin_stream_compare_exchange("post-gate", &etag)
        .await
        .unwrap();
    writer
        .write_all(&pattern(STREAMED_LEN, 0xC2))
        .await
        .unwrap();
    let pending = writer.seal().await.unwrap();
    let post_gate_control_key = control_key("k10-value-life", "post-gate");
    counterpart
        .arm_conditional_head_barrier(&post_gate_control_key)
        .await;
    let commit = tokio::spawn(async move { pending.commit().await });
    wait_for_first_barrier_arrival(&counterpart).await;
    store
        .upload_conditional(
            &format!("{KEYSPACE_ROOT}/k10-value-life/incarnations/post-gate"),
            Bytes::from(1_u64.to_be_bytes().to_vec()),
            None,
        )
        .await
        .unwrap();
    let opener = store
        .upload_conditional(
            &post_gate_control_key,
            Bytes::from_static(b"barrier-opener"),
            Some("never-matches"),
        )
        .await;
    assert!(matches!(
        opener,
        Err(yeetz_sdk_s3::ObjectStoreError::PreconditionFailed(_))
    ));
    assert!(matches!(
        commit.await.unwrap(),
        Err(KeyspaceError::StaleIncarnation(key)) if key == "post-gate"
    ));
    assert_eq!(
        keyspace.get("post-gate").await.unwrap(),
        None,
        "the closed-era publication self-evicts"
    );
    counterpart
        .assert_conditional_head_race(&post_gate_control_key, false)
        .await;

    // Destroy's tail owns only the control etag it observed.
    stream_create(&keyspace, "destroy-tail", &data)
        .await
        .unwrap();
    keyspace
        .destroy("destroy-tail", "teardown", "validator")
        .await
        .unwrap();
    let destroy_key = control_key("k10-value-life", "destroy-tail");
    let snapshot = counterpart.snapshot().await;
    let deletes: Vec<_> = requests_for(&snapshot, &destroy_key)
        .into_iter()
        .filter(|request| request.method == "DELETE")
        .collect();
    assert_eq!(deletes.len(), 1, "destroy emits one control delete");
    assert!(
        deletes[0].if_match.is_some(),
        "destroy's control delete must carry If-Match"
    );
    counterpart.shutdown().await;
}

// --- A29: damage taxonomy (integrity, never absence) ---------------------------

#[tokio::test]
async fn a29_missing_truncated_swapped_and_bad_root_taxonomy() {
    let (store, keyspace, counterpart) = keyspace_fixture("a29").await;
    let data = pattern(STREAMED_LEN, 0xB0);
    stream_create(&keyspace, "cell", &data).await.unwrap();
    let chunk0 = chunk_key_of("a29", "cell", 0, &data);
    let chunk1 = chunk_key_of("a29", "cell", 1, &data);
    let object_key = control_key("a29", "cell");

    // Missing referenced chunk: Present-but-damaged, never absence.
    store.delete(&chunk0).await.unwrap();
    match keyspace.get("cell").await {
        Err(KeyspaceError::ChunkMissing { key, chunk }) => {
            assert_eq!((key.as_str(), chunk), ("cell", 0));
        }
        other => panic!("missing chunk must be typed ChunkMissing, got {other:?}"),
    }
    // The state read never converts damage to absence: the control
    // stands, so the state is Present — the damage surfaces as a typed
    // integrity error when the reader fetches the chunk.
    match keyspace.read_state_stream("cell").await.unwrap() {
        StreamKeyState::Present { mut reader, .. } => {
            assert!(
                reader.read_to_end(&mut Vec::new()).await.is_err(),
                "the reader surfaces the missing chunk"
            );
        }
        other => panic!("Present despite chunk damage, got {other:?}"),
    }
    store
        .upload(&chunk0, data.slice(0..CHUNK_BYTES))
        .await
        .unwrap();

    // Truncated/corrupted stored chunk.
    counterpart.corrupt_object(&chunk1).await;
    match keyspace.get("cell").await {
        Err(KeyspaceError::ChunkIntegrity { key, chunk: 1, .. }) => assert_eq!(key, "cell"),
        other => panic!("corrupt chunk must be typed ChunkIntegrity, got {other:?}"),
    }
    store
        .upload(&chunk1, data.slice(CHUNK_BYTES..2 * CHUNK_BYTES))
        .await
        .unwrap();

    // Swapped: chunk 2's bytes under chunk 0's content address.
    store
        .upload(&chunk0, data.slice(2 * CHUNK_BYTES..))
        .await
        .unwrap();
    match keyspace.get("cell").await {
        Err(KeyspaceError::ChunkIntegrity { chunk: 0, .. }) => {}
        other => panic!("swapped chunk must be typed ChunkIntegrity, got {other:?}"),
    }
    store
        .upload(&chunk0, data.slice(0..CHUNK_BYTES))
        .await
        .unwrap();
    assert_eq!(keyspace.get("cell").await.unwrap(), Some(data), "restored");

    // Bad root: a manifest whose value_root disagrees with its table.
    let mut bad_root = craft_manifest(0, 0, [9; 16]);
    bad_root.value_root_sha256 = [0xFF; 32];
    store.upload(&object_key, bad_root.encode()).await.unwrap();
    match keyspace.get("cell").await {
        Err(KeyspaceError::ManifestRootMismatch(key)) => assert_eq!(key, "cell"),
        other => panic!("bad root must be typed ManifestRootMismatch, got {other:?}"),
    }
    // Malformed control: integrity failure, still never absence.
    store
        .upload(
            &object_key,
            Bytes::from_static(b"yeetz-keyspace-value/v3\0truncated"),
        )
        .await
        .unwrap();
    match keyspace.get("cell").await {
        Err(KeyspaceError::ManifestMalformed(_)) => {}
        other => panic!("malformed control must be typed, got {other:?}"),
    }
    counterpart.shutdown().await;
}

// --- A30: state algebra and batch-8 deletion compose with v3 -------------------

#[tokio::test]
async fn a30_state_parity_v3_delete_if_match_and_destroy_metadata() {
    let (_store, keyspace, counterpart) = keyspace_fixture("a30").await;
    let data = pattern(STREAMED_LEN, 0xC0);
    let successor = pattern(STREAMED_LEN, 0xC1);

    // Present (chunked) via the streamed state read.
    stream_create(&keyspace, "cell", &data).await.unwrap();
    match keyspace.read_state_stream("cell").await.unwrap() {
        StreamKeyState::Present { reader, metadata } => {
            assert_eq!(metadata.representation, ValueRepresentation::Chunked);
            assert_eq!(metadata.logical_len as usize, STREAMED_LEN);
            let _ = reader;
        }
        other => panic!("Present expected, got {other:?}"),
    }

    // v3 conflict enrichment: a stale-token conditional delete names
    // the v3 era (the §2.3 decoder composition), and a v3→v3 CAS
    // advances the era.
    let (value, era1_etag) = keyspace.get_with_etag("cell").await.unwrap().unwrap();
    assert_eq!(value, data);
    stream_cas(&keyspace, "cell", &era1_etag, &successor)
        .await
        .unwrap();
    match keyspace.delete_if_match("cell", &era1_etag).await {
        Err(KeyspaceError::PreconditionFailed {
            observed_incarnation: Some(0),
            observed_version: Some(1),
            ..
        }) => {}
        other => panic!("stale v3 token names the manifest era (0,1), got {other:?}"),
    }
    assert_eq!(keyspace.get("cell").await.unwrap(), Some(successor));

    // v3 destroy: the tombstone carries the v3 era; the
    // control-metadata read fetches NO chunks (request-log witness).
    let before = counterpart.snapshot().await;
    let chunk_gets_before = chunk_requests(&before)
        .into_iter()
        .filter(|request| request.method == "GET")
        .count();
    keyspace.destroy("cell", "a30", "test").await.unwrap();
    let after = counterpart.snapshot().await;
    let chunk_gets_after = chunk_requests(&after)
        .into_iter()
        .filter(|request| request.method == "GET")
        .count();
    assert_eq!(
        chunk_gets_after, chunk_gets_before,
        "destroy reads control metadata, never chunks"
    );
    match keyspace.read_state("cell").await.unwrap() {
        KeyState::Destroyed { tombstone } => {
            assert_eq!(tombstone.deleted_at_gen, 1, "the v3 era's version");
        }
        other => panic!("Destroyed expected, got {other:?}"),
    }
    match keyspace.read_state_stream("cell").await.unwrap() {
        StreamKeyState::Destroyed { .. } => {}
        other => panic!("streamed Destroyed expected, got {other:?}"),
    }

    // Successful conditional delete of a v3 manifest removes the
    // control ONLY: chunks become garbage, not logical state, and the
    // incarnation is untouched (batch-8 layering).
    let again = pattern(STREAMED_LEN, 0xC2);
    let receipt = stream_create(&keyspace, "cell2", &again).await.unwrap();
    keyspace
        .delete_if_match("cell2", &receipt.etag)
        .await
        .unwrap();
    assert_eq!(keyspace.get("cell2").await.unwrap(), None);
    assert!(matches!(
        keyspace.read_state("cell2").await.unwrap(),
        KeyState::Absent
    ));
    let inventory = keyspace.chunk_inventory().await.unwrap();
    assert_eq!(
        inventory.listed_chunks, 9,
        "cell's two generations (6) + cell2's (3)"
    );
    assert_eq!(
        inventory.candidate_orphan_chunks, 9,
        "all garbage until sweep"
    );
    assert_eq!(inventory.referenced_chunks, 0);

    // OffsetExpired parity: a seq-shaped chunked key below a certified
    // root floor reads expired and fetches no chunks.
    let seq_key = "log/00000000000000000003";
    stream_create(&keyspace, seq_key, &data).await.unwrap();
    keyspace.propose_trim("", 5).await.unwrap();
    let before = counterpart.snapshot().await;
    let gets_before = chunk_requests(&before)
        .into_iter()
        .filter(|request| request.method == "GET")
        .count();
    match keyspace.read_state_stream(seq_key).await.unwrap() {
        StreamKeyState::OffsetExpired { first_retained: 5 } => {}
        other => panic!("OffsetExpired expected, got {other:?}"),
    }
    let after = counterpart.snapshot().await;
    let gets_after = chunk_requests(&after)
        .into_iter()
        .filter(|request| request.method == "GET")
        .count();
    assert_eq!(gets_after, gets_before, "expired reads fetch no chunks");
    counterpart.shutdown().await;
}

// --- A31: small paths stay one-object structurally ------------------------------

#[tokio::test]
async fn a31_inline_request_profile_at_the_selected_threshold() {
    let (_store, keyspace, counterpart) = keyspace_fixture("a31").await;
    // Inside the 16–64 MiB preserved band: the human threshold ruling
    // keeps the one-object request profile — no chunk-root request, no
    // chunk hash, one PUT.
    let band_value = pattern(INLINE_BAND_LEN, 0xD0);
    keyspace.create("band", band_value.clone()).await.unwrap();
    let snapshot = counterpart.snapshot().await;
    assert!(
        chunk_requests(&snapshot).is_empty(),
        "inline band never touches the chunk root"
    );
    let puts = requests_for(&snapshot, &control_key("a31", "band"))
        .into_iter()
        .filter(|request| request.method == "PUT")
        .count();
    assert_eq!(puts, 1, "one PUT for the whole value");
    assert_eq!(keyspace.get("band").await.unwrap(), Some(band_value));
    let reader = keyspace.open_stream("band").await.unwrap().unwrap();
    assert_eq!(
        reader.metadata().representation,
        ValueRepresentation::Inline
    );

    // Above the threshold: the whole-value create becomes N chunk PUTs
    // + one manifest PUT — the representation switch is visible on the
    // wire.
    let over = pattern(WHOLE_CHUNKED_LEN, 0xD1);
    keyspace.create("over", over.clone()).await.unwrap();
    let snapshot = counterpart.snapshot().await;
    let chunk_puts = chunk_requests(&snapshot)
        .into_iter()
        .filter(|request| request.method == "PUT")
        .count();
    assert_eq!(chunk_puts, 6, "six chunks");
    let manifest_puts = requests_for(&snapshot, &control_key("a31", "over"))
        .into_iter()
        .filter(|request| request.method == "PUT" && request.if_none_match.as_deref() == Some("*"))
        .count();
    assert_eq!(manifest_puts, 1, "one manifest commit");
    assert_eq!(keyspace.get("over").await.unwrap(), Some(over));
    counterpart.shutdown().await;
}

// --- A32: the 892-byte worst-case physical chunk key ----------------------------

#[tokio::test]
async fn a32_worst_case_key_encoding_892_bytes_on_the_wire() {
    // Property-generated extremes: 255-byte namespace, 255-byte key
    // (the identifier-rule maximum). The physical chunk key is exactly
    // 892 bytes — below S3's 1,024 — and the store accepts it; the
    // decoder classifies every listed path (no unresolved).
    let namespace = "n".repeat(255);
    let key = "k".repeat(255);
    let (store, keyspace, counterpart) = keyspace_fixture(&namespace).await;
    let data = pattern(STREAMED_LEN, 0xD2);
    stream_create(&keyspace, &key, &data).await.unwrap();
    assert_eq!(keyspace.get(&key).await.unwrap(), Some(data.clone()));
    let listed = store
        .list_prefix_after(&format!("{CHUNK_ROOT}/v1/{namespace}/"), None, 1000)
        .await
        .unwrap();
    assert_eq!(listed.len(), 3);
    for chunk_key in &listed {
        assert_eq!(chunk_key.len(), 892, "the proven worst case");
        assert!(chunk_key.len() <= 1024, "S3 key limit");
    }
    let inventory = keyspace.chunk_inventory().await.unwrap();
    assert_eq!(inventory.listed_chunks, 3);
    assert_eq!(inventory.unresolved_chunks, 0);
    counterpart.shutdown().await;
}

// --- A33: the complete lost-response oracle --------------------------------------

#[tokio::test]
async fn a33_oracle_rows_and_raw_delete_window() {
    let (store, keyspace, counterpart) = keyspace_fixture("a33").await;
    let object_key = control_key("a33", "cell");
    let ours = craft_manifest(0, 0, [0x42; 16]);
    let foreign = craft_manifest(0, 0, [0x77; 16]);

    // Row 1 — exactly the bound target and our commit: landed.
    store
        .upload_conditional(&object_key, ours.encode(), None)
        .await
        .unwrap();
    match keyspace
        .adjudicate_manifest_put("cell", &object_key, &ours)
        .await
        .unwrap()
    {
        Ambiguity::Landed { etag } => assert!(etag.is_some()),
        other => panic!("row 1 must be Landed, got {other:?}"),
    }

    // Row 2 — exactly the bound target, foreign commit: typed conflict.
    store.upload(&object_key, foreign.encode()).await.unwrap();
    match keyspace
        .adjudicate_manifest_put("cell", &object_key, &ours)
        .await
        .unwrap()
    {
        Ambiguity::LostConflict => {}
        other => panic!("row 2 must be LostConflict, got {other:?}"),
    }

    // Row 3 — beyond the target (successor CAS), a higher incarnation,
    // Absent (destroyed or raw-deleted — the N1 attribution window),
    // and logically retired OffsetExpired: ambiguous, never a
    // fabricated success or conflict.
    let successor = craft_manifest(0, 1, [0x88; 16]);
    store.upload(&object_key, successor.encode()).await.unwrap();
    assert!(matches!(
        keyspace
            .adjudicate_manifest_put("cell", &object_key, &ours)
            .await
            .unwrap(),
        Ambiguity::Ambiguous
    ));
    let recreated = craft_manifest(1, 0, [0x88; 16]);
    store.upload(&object_key, recreated.encode()).await.unwrap();
    assert!(matches!(
        keyspace
            .adjudicate_manifest_put("cell", &object_key, &ours)
            .await
            .unwrap(),
        Ambiguity::Ambiguous
    ));
    store.delete(&object_key).await.unwrap();
    assert!(matches!(
        keyspace
            .adjudicate_manifest_put("cell", &object_key, &ours)
            .await
            .unwrap(),
        Ambiguity::Ambiguous
    ));

    // OffsetExpired with a zombie control at exactly the bound target:
    // logically retired is row 3 regardless.
    let seq_key = "log/00000000000000000002";
    let seq_object_key = control_key("a33", seq_key);
    store
        .upload_conditional(&seq_object_key, ours.encode(), None)
        .await
        .unwrap();
    keyspace.propose_trim("", 5).await.unwrap();
    assert!(matches!(
        keyspace
            .adjudicate_manifest_put(seq_key, &seq_object_key, &ours)
            .await
            .unwrap(),
        Ambiguity::Ambiguous
    ));

    // Malformed current control: integrity failure, not a row.
    store
        .upload(&object_key, Bytes::from_static(b"nonsense"))
        .await
        .unwrap();
    match keyspace
        .adjudicate_manifest_put("cell", &object_key, &ours)
        .await
    {
        Err(
            error
            @ (KeyspaceError::ManifestMalformed(_) | KeyspaceError::ValueEnvelopeMalformed(_)),
        ) => {
            let _ = error;
        }
        other => panic!("malformed control stays an integrity failure, got {other:?}"),
    }
    counterpart.shutdown().await;
}

/// A33's crash matrix: cut every storage request of a two-chunk-plus
/// streamed create in turn. The sequential oracle permits
/// old/new/superseded — never a fabricated success or conflict, never
/// a manifest naming missing chunks.
#[tokio::test]
async fn a33_crash_after_every_storage_request() {
    // (a) The begin's incarnation read refused: no effects.
    {
        let (store, keyspace, counterpart) = keyspace_fixture("a33a").await;
        let incarnation_key = format!("{KEYSPACE_ROOT}/a33a/incarnations/cell");
        counterpart
            .arm_storage_fault(
                StorageFaultCut::KeyspaceControlRead,
                StorageFaultPhase::BeforeEffect,
                &incarnation_key,
            )
            .await;
        assert!(keyspace.begin_stream_create("cell").await.is_err());
        assert_eq!(keyspace.get("cell").await.unwrap(), None);
        assert!(
            store
                .list_prefix_after(&format!("{CHUNK_ROOT}/v1/a33a/"), None, 100)
                .await
                .unwrap()
                .is_empty()
        );
        counterpart.shutdown().await;
    }
    // (b) A wrong object pre-placed under a chunk content address: the
    // put-if-absent conflict verifies, the digest disagrees, and the
    // write fails typed integrity — never accepted, never silent.
    {
        let (store, keyspace, counterpart) = keyspace_fixture("a33b").await;
        let data = pattern(STREAMED_LEN, 0x5B);
        let first_chunk = chunk_key_of("a33b", "cell", 0, &data);
        store
            .upload(&first_chunk, Bytes::from_static(b"wrong-object"))
            .await
            .unwrap();
        let mut writer = keyspace.begin_stream_create("cell").await.unwrap();
        writer.write_all(&data).await.unwrap();
        match writer.seal().await {
            Err(KeyspaceError::ChunkIntegrity { key, chunk: 0, .. }) => {
                assert_eq!(key, "cell");
            }
            other => panic!("wrong-object collision must fail typed, got {other:?}"),
        }
        assert_eq!(keyspace.get("cell").await.unwrap(), None);
        counterpart.shutdown().await;
    }
    // (c) A chunk PUT applied with the response lost: reconciled by
    // exact GET + full digest; the write continues and lands.
    {
        let (_store, keyspace, counterpart) = keyspace_fixture("a33c").await;
        let data = pattern(STREAMED_LEN, 0x5C);
        let first_chunk = chunk_key_of("a33c", "cell", 0, &data);
        counterpart
            .arm_storage_fault(
                StorageFaultCut::ChunkPut,
                StorageFaultPhase::AfterEffect,
                &first_chunk,
            )
            .await;
        stream_create(&keyspace, "cell", &data).await.unwrap();
        assert_eq!(keyspace.get("cell").await.unwrap(), Some(data));
        counterpart.shutdown().await;
    }
    // (d) The manifest PUT applied with the response lost: the oracle
    // rules row 1 and the caller gets the receipt.
    {
        let (_store, keyspace, counterpart) = keyspace_fixture("a33d").await;
        let data = pattern(STREAMED_LEN, 0x5D);
        let object_key = control_key("a33d", "cell");
        let mut writer = keyspace.begin_stream_create("cell").await.unwrap();
        writer.write_all(&data).await.unwrap();
        let pending = writer.seal().await.unwrap();
        counterpart
            .arm_storage_fault(
                StorageFaultCut::KeyspaceConditionalPut,
                StorageFaultPhase::AfterEffect,
                &object_key,
            )
            .await;
        let receipt = pending.commit().await.unwrap();
        assert_eq!(keyspace.get("cell").await.unwrap(), Some(data));
        assert_eq!(
            receipt.etag,
            keyspace.get_with_etag("cell").await.unwrap().unwrap().1
        );
        counterpart.shutdown().await;
    }
}

// --- A34: GC is safe only under its explicit precondition ------------------------

#[tokio::test]
async fn a34_quiesced_sweep_inventory_and_fences() {
    let (_store, keyspace, counterpart) = keyspace_fixture("a34").await;
    let data = pattern(STREAMED_LEN, 0xE0);
    let successor = pattern(STREAMED_LEN, 0xE1);

    // Sweep requires the fence.
    match keyspace.sweep_chunks().await {
        Err(KeyspaceError::MaintenanceFenceRequired(_)) => {}
        other => panic!("sweep without fence must refuse, got {other:?}"),
    }

    stream_create(&keyspace, "cell", &data).await.unwrap();
    let (_, etag) = keyspace.get_with_etag("cell").await.unwrap().unwrap();
    stream_cas(&keyspace, "cell", &etag, &successor)
        .await
        .unwrap();

    // Online metering is delete-free.
    let before = counterpart.snapshot().await;
    let deletes_before = before
        .requests
        .iter()
        .filter(|request| request.method == "DELETE")
        .count();
    let inventory = keyspace.chunk_inventory().await.unwrap();
    let after = counterpart.snapshot().await;
    let deletes_after = after
        .requests
        .iter()
        .filter(|request| request.method == "DELETE")
        .count();
    assert_eq!(deletes_after, deletes_before, "inventory deletes nothing");
    assert_eq!(inventory.listed_chunks, 6);
    assert_eq!(inventory.referenced_chunks, 3);
    assert_eq!(inventory.candidate_orphan_chunks, 3);
    assert_eq!(
        inventory.candidate_orphan_bytes,
        2 * CHUNK_BYTES as u64 + 4096
    );
    assert_eq!(inventory.unresolved_chunks, 0);

    // Fence semantics: streamed begins refuse while fenced.
    keyspace.set_maintenance_fence().await.unwrap();
    keyspace.set_maintenance_fence().await.unwrap(); // idempotent
    match keyspace.begin_stream_create("blocked").await {
        Err(KeyspaceError::MaintenanceFenced(key)) => assert_eq!(key, "blocked"),
        other => panic!("fenced begin must refuse, got {other:?}"),
    }
    // The quiesced sweep (the external assertion — no writers — holds
    // in this test).
    let report = keyspace.sweep_chunks().await.unwrap();
    assert_eq!(report.examined, 6);
    assert_eq!(report.deleted, 3);
    assert_eq!(report.retained, 3);
    assert_eq!(report.remaining, 0);
    // live == retained after the sweep.
    let inventory = keyspace.chunk_inventory().await.unwrap();
    assert_eq!(inventory.listed_chunks, 3);
    assert_eq!(inventory.referenced_chunks, 3);
    assert_eq!(inventory.candidate_orphan_chunks, 0);
    // Idempotent re-run converges with nothing to do.
    let report = keyspace.sweep_chunks().await.unwrap();
    assert_eq!(report.examined, 3);
    assert_eq!(report.deleted, 0);
    assert_eq!(report.retained, 3);
    // The value survives the sweep intact.
    assert_eq!(keyspace.get("cell").await.unwrap(), Some(successor));
    keyspace.release_maintenance_fence().await.unwrap();
    keyspace.release_maintenance_fence().await.unwrap(); // idempotent
    // Released: begins work again (fence GET absent).
    keyspace.begin_stream_create("unblocked").await.unwrap();
    counterpart.shutdown().await;
}

#[tokio::test]
async fn teardown_reerected_fence_rejects_a_stale_release_etag() {
    let (store, keyspace, counterpart) = keyspace_fixture("fence-aba").await;
    let fence_key = control_key("fence-aba", "fences/gc");

    keyspace.set_maintenance_fence().await.unwrap();
    let first_etag = store
        .download_with_etag(&fence_key)
        .await
        .unwrap()
        .etag
        .unwrap();
    keyspace.release_maintenance_fence().await.unwrap();

    keyspace.set_maintenance_fence().await.unwrap();
    let second_etag = store
        .download_with_etag(&fence_key)
        .await
        .unwrap()
        .etag
        .unwrap();
    assert_ne!(first_etag, second_etag, "fence epochs must not recur");
    assert!(matches!(
        keyspace
            .release_observed_maintenance_fence(&first_etag)
            .await,
        Err(KeyspaceError::MaintenanceFenceConflict(namespace)) if namespace == "fence-aba"
    ));
    assert!(keyspace.maintenance_fence_present_for_test().await.unwrap());

    // Repeating set while the second fence stands is still idempotent.
    keyspace.set_maintenance_fence().await.unwrap();
    assert_eq!(
        store
            .download_with_etag(&fence_key)
            .await
            .unwrap()
            .etag
            .unwrap(),
        second_etag
    );
    keyspace.release_maintenance_fence().await.unwrap();
    counterpart.shutdown().await;
}

#[tokio::test]
async fn teardown_sweep_reclaims_chunks_of_a_trimmed_zombie_control() {
    let (_store, keyspace, counterpart) = keyspace_fixture("trimmed-chunks").await;
    let key = "log/00000000000000000003";
    let data = pattern(STREAMED_LEN, 0xEC);
    stream_create(&keyspace, key, &data).await.unwrap();

    // The certificate is the logical commit. Deliberately leave the
    // old v3 control in place to model the window before delete_below.
    keyspace.propose_trim("", 5).await.unwrap();
    assert!(matches!(
        keyspace.read_state_stream(key).await.unwrap(),
        StreamKeyState::OffsetExpired { first_retained: 5 }
    ));
    let inventory = keyspace.chunk_inventory().await.unwrap();
    assert_eq!(inventory.referenced_chunks, 0);
    assert_eq!(inventory.candidate_orphan_chunks, 3);

    keyspace.set_maintenance_fence().await.unwrap();
    let report = keyspace.sweep_chunks().await.unwrap();
    assert_eq!(report.examined, 3);
    assert_eq!(report.deleted, 3);
    assert_eq!(report.retained, 0);
    assert_eq!(report.remaining, 0);
    assert!(matches!(
        keyspace.read_state_stream(key).await.unwrap(),
        StreamKeyState::OffsetExpired { first_retained: 5 }
    ));
    keyspace.release_maintenance_fence().await.unwrap();
    counterpart.shutdown().await;
}

/// Unavailable or corrupt control fails closed for that key: the sweep
/// refuses to delete what it cannot classify.
#[tokio::test]
async fn a34_unavailable_and_corrupt_control_fail_closed() {
    let (_store, keyspace, counterpart) = keyspace_fixture("a34c").await;
    let data = pattern(STREAMED_LEN, 0xE2);
    stream_create(&keyspace, "cell", &data).await.unwrap();
    let object_key = control_key("a34c", "cell");

    keyspace.set_maintenance_fence().await.unwrap();
    counterpart
        .arm_storage_fault(
            StorageFaultCut::KeyspaceControlRead,
            StorageFaultPhase::BeforeEffect,
            &object_key,
        )
        .await;
    let report = keyspace.sweep_chunks().await.unwrap();
    assert_eq!(report.examined, 3);
    assert_eq!(report.deleted, 0, "fail closed");
    assert_eq!(report.retained, 0);
    assert_eq!(report.remaining, 3, "the unresolved remainder");
    assert_eq!(
        keyspace.get("cell").await.unwrap(),
        Some(data),
        "value intact"
    );

    // Corrupt control: decode failure is fail-closed too.
    counterpart.corrupt_object(&object_key).await;
    let report = keyspace.sweep_chunks().await.unwrap();
    assert_eq!(report.deleted, 0);
    assert_eq!(report.remaining, 3);
    keyspace.release_maintenance_fence().await.unwrap();
    counterpart.shutdown().await;
}

#[tokio::test]
async fn teardown_malformed_chunk_path_is_unresolved_and_never_deleted() {
    let (store, keyspace, counterpart) = keyspace_fixture("malformed-path").await;
    let malformed = format!(
        "{CHUNK_ROOT}/v1/malformed-path/2f/{:020}/{:020}/{}",
        0,
        0,
        "ab".repeat(32)
    );
    store
        .upload(&malformed, Bytes::from_static(b"unowned"))
        .await
        .unwrap();

    let inventory = keyspace.chunk_inventory().await.unwrap();
    assert_eq!(inventory.listed_chunks, 1);
    assert_eq!(inventory.unresolved_chunks, 1);
    assert_eq!(inventory.candidate_orphan_chunks, 0);

    keyspace.set_maintenance_fence().await.unwrap();
    let report = keyspace.sweep_chunks().await.unwrap();
    assert_eq!(report.examined, 1);
    assert_eq!(report.deleted, 0);
    assert_eq!(report.remaining, 1);
    assert!(store.exists(&malformed).await.unwrap());
    keyspace.release_maintenance_fence().await.unwrap();
    counterpart.shutdown().await;
}

/// A frozen (stale) chunk LIST hides garbage and causes a leak only —
/// eligibility always comes from the exact control read, so no live
/// chunk is deleted on the stale view.
#[tokio::test]
async fn a34_frozen_list_leaks_only() {
    let (_store, keyspace, counterpart) = keyspace_fixture("a34f").await;
    let data = pattern(STREAMED_LEN, 0xE3);
    let successor = pattern(STREAMED_LEN, 0xE4);
    stream_create(&keyspace, "cell", &data).await.unwrap();
    let (_, etag) = keyspace.get_with_etag("cell").await.unwrap().unwrap();

    // Freeze the chunk listing with only the first generation
    // visible, then CAS the second generation (invisible to LIST).
    counterpart
        .arm_frozen_list(&format!("{CHUNK_ROOT}/v1/a34f/"))
        .await;
    stream_cas(&keyspace, "cell", &etag, &successor)
        .await
        .unwrap();

    keyspace.set_maintenance_fence().await.unwrap();
    let report = keyspace.sweep_chunks().await.unwrap();
    assert_eq!(report.deleted, 3, "stale-visible orphans reclaimed");
    assert_eq!(report.retained, 0, "the current refs were LIST-hidden");
    // The live value is intact — its chunks were never touched.
    assert_eq!(keyspace.get("cell").await.unwrap(), Some(successor));
    counterpart.unfreeze_list().await;
    let inventory = keyspace.chunk_inventory().await.unwrap();
    assert_eq!(inventory.listed_chunks, 3, "the leaked new generation");
    assert_eq!(inventory.referenced_chunks, 3);
    assert_eq!(inventory.candidate_orphan_chunks, 0);
    keyspace.release_maintenance_fence().await.unwrap();
    counterpart.shutdown().await;
}

/// The A34 broken-quiescence demonstration cut (ADR 0004 §5.2): a
/// writer that began BEFORE the fence is drained by no one; the
/// sweep, falsely believing quiescence, deletes its candidate chunks;
/// the writer's conditional manifest PUT then succeeds — and the
/// committed manifest names absent chunks. The rig REQUIRES detecting
/// the forbidden `ManifestIncomplete` signature. This leg documents
/// the blast radius of violating the precondition; it is not a claim
/// that the kernel can prevent deployment misconduct.
#[tokio::test]
async fn a34_broken_quiescence_demonstration_cut() {
    let (_store, keyspace, counterpart) = keyspace_fixture("a34x").await;
    let data = pattern(STREAMED_LEN, 0xE5);

    // 1. The writer begins and lands its candidate chunks (pre-fence).
    let mut writer = keyspace.begin_stream_create("cell").await.unwrap();
    writer.write_all(&data).await.unwrap();
    let pending = writer.seal().await.unwrap();

    // 2. Operators fence and drain — falsely: the pre-fence writer is
    //    still pending. (The fence is NOT drain proof — ADR 0004 §5.1.)
    keyspace.set_maintenance_fence().await.unwrap();

    // 3. The sweep, believing quiescence, sees no current manifest
    //    reference and deletes the candidate chunks.
    let report = keyspace.sweep_chunks().await.unwrap();
    assert_eq!(report.deleted, 3, "the violated-precondition sweep");

    // 4. The writer's conditional manifest PUT succeeds anyway.
    pending.commit().await.expect("the manifest publishes");

    // 5. The committed manifest names absent chunks: Present but
    //    damaged — the forbidden state P8/A33 exist to exclude — and
    //    the rig detects it.
    match keyspace.get("cell").await {
        Err(KeyspaceError::ChunkMissing { key, chunk }) => {
            assert_eq!((key.as_str(), chunk), ("cell", 0));
        }
        other => panic!(
            "broken quiescence must produce the detectable ManifestIncomplete state, got {other:?}"
        ),
    }
    match keyspace.open_stream("cell").await {
        Ok(Some(mut reader)) => {
            assert!(
                reader.read_to_end(&mut Vec::new()).await.is_err(),
                "the reader detects the missing chunk"
            );
        }
        other => panic!("control is present, got {other:?}"),
    }
    keyspace.release_maintenance_fence().await.unwrap();
    counterpart.shutdown().await;
}
