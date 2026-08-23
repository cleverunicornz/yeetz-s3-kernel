//! The ADR 0005 deletion contract suite (A36–A45): the loopback-S3
//! legs proving bounded side-effect-free admission, exact 1,000-key
//! sequential chunking, typed per-key partial outcomes, cross-chunk
//! remainder classification, whole-request ambiguity fail-closed
//! behavior, response-bijection fail-closed behavior, wire-incapable
//! backend fail-closed behavior, the below-lifecycle side-effect
//! profile with the ADR 0004 quiescence coupling, the unconditional
//! non-transactional boundary, and the unchanged published deletion
//! surface.
//!
//! These run against the kernel's fault-injecting loopback counterpart
//! extended by the ADR 0005 rig deltas: the bulk-delete key-vector
//! observation, key-less occurrence-scoped fault arming, configurable
//! per-entry Code/Message, whole-request cuts, the configurable
//! 405/501 refusal mode, and the response-position mutations.

use std::sync::Arc;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use yeetz_sdk_s3::ObjectStoreClient;

use crate::atomic_keyspace::{
    AtomicKeyspace, DELETE_OBJECTS_MAX_DIAGNOSTIC_BYTES, DELETE_OBJECTS_MAX_INPUT,
    DELETE_OBJECTS_MAX_KEYS, DeleteObjectsFailure, DeleteObjectsInputError,
    DeleteObjectsOutcome, DeleteObjectsUnconfirmedReason, KEYSPACE_ROOT, KeyState, KeyspaceError,
};
use crate::state_kernel::gateway_state_contract::{
    CounterpartSnapshot, LoopbackCounterpart, LoopbackRequestObservation, MultiDeleteResponseMutation,
    StorageFaultCut, StorageFaultPhase,
};
use crate::value_manifest::{CHUNK_BYTES, chunk_object_key};

/// Deterministic test bytes (same generator as the streaming suite).
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

fn control_path(namespace: &str, key: &str) -> String {
    format!("{KEYSPACE_ROOT}/{namespace}/{key}")
}

/// The physical chunk path of `ordinal` for an incarnation-0,
/// version-0 write of `data` (ADR 0004 §1.3 layout).
fn chunk_key_of(namespace: &str, key: &str, ordinal: usize, data: &Bytes) -> String {
    let start = ordinal * CHUNK_BYTES;
    let end = start + CHUNK_BYTES.min(data.len() - start);
    let digest = hex::encode(Sha256::digest(data.slice(start..end)));
    chunk_object_key(namespace, key, 0, 0, &digest)
}

/// The bucket-level bulk-delete POST observations (the A37 oracle's
/// partition input).
fn bulk_posts(snapshot: &CounterpartSnapshot) -> Vec<&LoopbackRequestObservation> {
    snapshot
        .requests
        .iter()
        .filter(|request| request.method == "POST" && request.delete_keys.is_some())
        .collect()
}

/// Exact object-state read: `present` names the expected outcome; any
/// other store failure fails the leg.
async fn assert_object_state(store: &ObjectStoreClient, path: &str, present: bool) {
    match (store.download(path).await, present) {
        (Ok(_), true) | (Err(yeetz_sdk_s3::ObjectStoreError::NotFound(_)), false) => {}
        (outcome, expected) => panic!("object {path} present={expected}, got {outcome:?}"),
    }
}

fn rejected_diagnostic(
    outcome: &DeleteObjectsOutcome,
) -> &Arc<crate::atomic_keyspace::DeleteObjectsDiagnostic> {
    match outcome.result.as_ref() {
        Err(DeleteObjectsFailure::Rejected { diagnostic }) => diagnostic,
        other => panic!("expected Rejected for {}, got {other:?}", outcome.key),
    }
}

fn unconfirmed_parts(
    outcome: &DeleteObjectsOutcome,
) -> (
    DeleteObjectsUnconfirmedReason,
    &Arc<crate::atomic_keyspace::DeleteObjectsDiagnostic>,
) {
    match outcome.result.as_ref() {
        Err(DeleteObjectsFailure::Unconfirmed { reason, diagnostic }) => (*reason, diagnostic),
        other => panic!("expected Unconfirmed for {}, got {other:?}", outcome.key),
    }
}

fn unsupported_diagnostic(
    outcome: &DeleteObjectsOutcome,
) -> &Arc<crate::atomic_keyspace::DeleteObjectsDiagnostic> {
    match outcome.result.as_ref() {
        Err(DeleteObjectsFailure::Unsupported { diagnostic }) => diagnostic,
        other => panic!("expected Unsupported for {}, got {other:?}", outcome.key),
    }
}

fn assert_not_attempted(outcome: &DeleteObjectsOutcome) {
    assert_eq!(
        outcome.result.as_ref(),
        Err(&DeleteObjectsFailure::NotAttempted),
        "expected NotAttempted for {}",
        outcome.key
    );
}

/// A caller's bounded classification over the outcome vector (A39):
/// transient provider codes replay under a ceiling; permanent
/// rejections surface; Unsupported is terminal; NotAttempted keys
/// replay only under the attempt ceiling.
enum PolicyAction {
    Replay,
    Surface,
    Terminal,
}

fn classify_outcome(outcome: &DeleteObjectsOutcome) -> PolicyAction {
    match outcome.result.as_ref() {
        Ok(()) => panic!("policy iterates failures, not confirmations"),
        Err(DeleteObjectsFailure::Rejected { diagnostic }) => match diagnostic.code.as_deref() {
            Some("TransientHiccup") => PolicyAction::Replay,
            _ => PolicyAction::Surface,
        },
        Err(DeleteObjectsFailure::Unconfirmed { .. })
        | Err(DeleteObjectsFailure::NotAttempted) => PolicyAction::Replay,
        Err(DeleteObjectsFailure::Unsupported { .. }) => PolicyAction::Terminal,
    }
}

// --- A36: admission is bounded, complete, and effect-free ----------

#[tokio::test]
async fn a36_delete_objects_input_preflight_is_side_effect_free() {
    let (store, keyspace, counterpart) = keyspace_fixture("a36").await;

    // Empty input: Ok with no vector, zero requests.
    let empty = keyspace.delete_objects(&[]).await.unwrap();
    assert!(empty.is_empty());
    assert_eq!(bulk_posts(&counterpart.snapshot().await).len(), 0);

    // Size error precedes every member error — without scanning.
    let oversized: Vec<String> = (0..=DELETE_OBJECTS_MAX_INPUT)
        .map(|index| format!("k{index}"))
        .collect();
    let oversized_refs: Vec<&str> = oversized.iter().map(String::as_str).collect();
    match keyspace.delete_objects(&oversized_refs).await {
        Err(DeleteObjectsInputError::TooManyKeys { provided, max }) => {
            assert_eq!(provided, 100_001);
            assert_eq!(max, DELETE_OBJECTS_MAX_INPUT);
        }
        other => panic!("expected TooManyKeys, got {other:?}"),
    }

    // Reserved families, reserved-state precedence before identifier
    // validation, composed fence-path refusal, duplicates at boundary
    // positions, and deterministic first-error-by-index.
    let cases: [(&str, fn(&KeyspaceError) -> bool); 6] = [
        ("tombstones/ref", |error| {
            matches!(error, KeyspaceError::TombstoneImmutable(_))
        }),
        ("incarnations/ref", |error| {
            matches!(error, KeyspaceError::IncarnationCounterImmutable(_))
        }),
        ("fences/gc", |error| {
            matches!(error, KeyspaceError::MaintenanceFenceImmutable(_))
        }),
        // Composed physical path: neither segment is reserved alone.
        ("deep/fences/gc", |error| {
            matches!(error, KeyspaceError::MaintenanceFenceImmutable(_))
        }),
        ("scope/trims/00000000000000000001", |error| {
            matches!(error, KeyspaceError::TrimCertificateImmutable(_))
        }),
        // Reserved precedes malformed (tombstones/a!b reports the
        // reserved family, never InvalidIdentifier).
        ("tombstones/a!b", |error| {
            matches!(error, KeyspaceError::TombstoneImmutable(_))
        }),
    ];
    for (key, is_family) in cases {
        match keyspace.delete_objects(&["sibling", key]).await {
            Err(DeleteObjectsInputError::Key {
                index,
                key: reported,
                source,
            }) => {
                assert_eq!((index, reported.as_str()), (1, key));
                assert!(is_family(&source), "reserved family for {key}: {source:?}");
            }
            other => panic!("expected Key rejection for {key}, got {other:?}"),
        }
    }

    // Invalid identifier, by exact index.
    match keyspace.delete_objects(&["fine", "bad key!", "tombstones/later"]).await {
        Err(DeleteObjectsInputError::Key { index, key, source }) => {
            assert_eq!((index, key.as_str()), (1, "bad key!"));
            assert!(matches!(source, KeyspaceError::InvalidIdentifier(_)));
        }
        other => panic!("expected the index-1 identifier error, got {other:?}"),
    }

    // Duplicates reject at the second index, at both boundary
    // positions.
    match keyspace.delete_objects(&["dup", "dup"]).await {
        Err(DeleteObjectsInputError::Duplicate {
            key,
            first_index,
            duplicate_index,
        }) => assert_eq!((key.as_str(), first_index, duplicate_index), ("dup", 0, 1)),
        other => panic!("expected Duplicate at head boundary, got {other:?}"),
    }
    match keyspace.delete_objects(&["x", "y", "x"]).await {
        Err(DeleteObjectsInputError::Duplicate {
            first_index,
            duplicate_index,
            ..
        }) => assert_eq!((first_index, duplicate_index), (0, 2)),
        other => panic!("expected Duplicate at tail boundary, got {other:?}"),
    }

    // Admission parity with delete_many: the same reserved key errors
    // identically before any effect.
    match keyspace.delete_many(&["tombstones/ref"]).await {
        Err(KeyspaceError::TombstoneImmutable(_)) => {}
        other => panic!("delete_many reserved parity, got {other:?}"),
    }

    // Valid siblings before a bad member stay intact and the wire
    // stays silent: any admission error means zero delete requests.
    keyspace
        .create("keep", Bytes::from_static(b"value"))
        .await
        .unwrap();
    match keyspace.delete_objects(&["keep", "bad key!"]).await {
        Err(DeleteObjectsInputError::Key { index: 1, .. }) => {}
        other => panic!("expected the member error, got {other:?}"),
    }
    assert_object_state(&store, &control_path("a36", "keep"), true).await;
    assert_eq!(bulk_posts(&counterpart.snapshot().await).len(), 0);
    counterpart.shutdown().await;
}

// --- A37: wire chunks are bounded and ordered ----------------------

#[tokio::test]
async fn a37_delete_objects_chunks_exactly_at_1000() {
    let (_store, keyspace, counterpart) = keyspace_fixture("a37").await;

    // Public sizing constants are the contract.
    assert_eq!(DELETE_OBJECTS_MAX_KEYS, 1_000);
    assert_eq!(DELETE_OBJECTS_MAX_INPUT, 100_000);
    assert_eq!(DELETE_OBJECTS_MAX_DIAGNOSTIC_BYTES, 512);

    for size in [0usize, 1, 999, 1_000, 1_001, 2_000, 2_001] {
        let keys: Vec<String> = (0..size).map(|index| format!("s{size}-k{index}")).collect();
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let before = bulk_posts(&counterpart.snapshot().await).len();
        let outcomes = keyspace.delete_objects(&refs).await.unwrap();
        let snapshot = counterpart.snapshot().await;
        let posts = bulk_posts(&snapshot);
        let call_posts = &posts[before..];

        // Exactly ceil(n / 1000) logical operations.
        assert_eq!(
            call_posts.len(),
            size.div_ceil(DELETE_OBJECTS_MAX_KEYS),
            "size {size}"
        );
        // Each POST carries at most 1,000 exact physical keys, unique,
        // in input order across the whole call (sequential chunks).
        let mut flattened: Vec<String> = Vec::with_capacity(size);
        for post in call_posts {
            let delete_keys = post.delete_keys.as_deref().unwrap_or_default();
            assert!(delete_keys.len() <= DELETE_OBJECTS_MAX_KEYS, "size {size}");
            flattened.extend(delete_keys.iter().cloned());
            // Verbose mode only — quiet is forbidden.
            assert_eq!(post.quiet, Some(false), "size {size}");
            // Unconditional transport: no conditional header ever.
            assert!(post.if_match.is_none() && post.if_none_match.is_none());
        }
        let expected: Vec<String> = keys
            .iter()
            .map(|key| control_path("a37", key))
            .collect();
        assert_eq!(flattened, expected, "size {size}");
        let mut unique = flattened.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), size, "size {size}");

        // No hidden per-key fallback: the call issues no single-key
        // DELETE requests.
        assert!(
            !snapshot.requests.iter().any(|request| request.method == "DELETE"),
            "size {size}"
        );

        // Output length and order equal input.
        assert_eq!(outcomes.len(), size);
        for (outcome, key) in outcomes.iter().zip(&keys) {
            assert_eq!(outcome.key, *key);
            assert!(outcome.result.is_ok(), "size {size}, key {key}");
        }
    }
    counterpart.shutdown().await;
}

// --- A38: a valid partial response is reported per key -------------

#[tokio::test]
async fn a38_delete_objects_partial_batch_is_typed_per_key() {
    let (store, keyspace, counterpart) = keyspace_fixture("a38").await;

    // Seed five controls; arm distinct bounded per-entry errors on
    // k1/k3 (BeforeEffect leaves them present while siblings are
    // deleted). The rig's emission order (all Deleted first, then all
    // Error entries) differs from input interleaving, so exact-key
    // reconciliation is exercised.
    let keys: Vec<String> = (0..5).map(|index| format!("k{index}")).collect();
    for key in &keys {
        keyspace
            .create(key, Bytes::from_static(b"v"))
            .await
            .unwrap();
    }
    counterpart
        .arm_multi_delete_entry_fault(
            &control_path("a38", "k1"),
            StorageFaultPhase::BeforeEffect,
            "TransientHiccup",
            "retryable under policy",
        )
        .await;
    counterpart
        .arm_multi_delete_entry_fault(
            &control_path("a38", "k3"),
            StorageFaultPhase::BeforeEffect,
            "AccessDenied",
            "permanent rejection",
        )
        .await;
    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let outcomes = keyspace.delete_objects(&refs).await.unwrap();
    assert_eq!(outcomes.len(), 5);
    for (index, outcome) in outcomes.iter().enumerate() {
        assert_eq!(outcome.key, keys[index]);
        match index {
            1 | 3 => {
                let diagnostic = rejected_diagnostic(outcome);
                let expected_code = if index == 1 {
                    "TransientHiccup"
                } else {
                    "AccessDenied"
                };
                assert_eq!(diagnostic.code.as_deref(), Some(expected_code));
                assert!(!diagnostic.truncated);
            }
            _ => assert!(outcome.result.is_ok()),
        }
    }
    // Exact object map: confirmed keys gone; rejected keys present.
    for (index, key) in keys.iter().enumerate() {
        let present = index == 1 || index == 3;
        assert_object_state(&store, &control_path("a38", key), present).await;
    }
    // remaining() serializes exactly the unresolved keys, in order.
    assert_eq!(
        DeleteObjectsOutcome::remaining(&outcomes),
        vec!["k1".to_string(), "k3".to_string()]
    );
    // The legacy cut was never armed.
    assert!(
        !counterpart
            .snapshot()
            .await
            .faults
            .iter()
            .any(|fault| fault.cut == StorageFaultCut::KeyspaceDelete)
    );

    // A bounded sample policy iterates the first call's outcomes and
    // replays only the transient classification; the permanent
    // rejection surfaces and is never replayed.
    let replay: Vec<&str> = outcomes
        .iter()
        .filter(|outcome| outcome.result.is_err())
        .filter(|outcome| matches!(classify_outcome(outcome), PolicyAction::Replay))
        .map(|outcome| outcome.key.as_str())
        .collect();
    assert_eq!(replay, ["k1"]);
    let replayed = keyspace
        .delete_objects(&["k1"])
        .await
        .unwrap();
    assert!(replayed[0].result.is_ok());
    assert_object_state(&store, &control_path("a38", "k3"), true).await;

    // Diagnostics are bounded: over-budget code+message truncates to
    // the combined 512-byte budget.
    counterpart
        .arm_multi_delete_entry_fault(
            &control_path("a38", "k3"),
            StorageFaultPhase::BeforeEffect,
            &"X".repeat(600),
            &"Y".repeat(600),
        )
        .await;
    let bounded = keyspace.delete_objects(&["k3"]).await.unwrap();
    let diagnostic = rejected_diagnostic(&bounded[0]);
    assert!(diagnostic.truncated);
    let combined = diagnostic.code.as_deref().map_or(0, str::len) + diagnostic.message.len();
    assert!(combined <= DELETE_OBJECTS_MAX_DIAGNOSTIC_BYTES);

    // AfterEffect entry cut: applied but reported as an error — a
    // failure outcome is never a presence proof.
    keyspace
        .create("k9", Bytes::from_static(b"v"))
        .await
        .unwrap();
    counterpart
        .arm_multi_delete_entry_fault(
            &control_path("a38", "k9"),
            StorageFaultPhase::AfterEffect,
            "InternalError",
            "applied but lost",
        )
        .await;
    let applied = keyspace.delete_objects(&["k9"]).await.unwrap();
    assert!(applied[0].result.is_err());
    assert_object_state(&store, &control_path("a38", "k9"), false).await;
    counterpart.shutdown().await;
}

// --- A39: cross-chunk classification is complete -------------------

#[tokio::test]
async fn a39_delete_objects_remainder_crosses_chunks() {
    let (store, keyspace, counterpart) = keyspace_fixture("a39").await;

    // 3,001 keys → chunks [0,1000) [1000,2000) [2000,3000) and a
    // one-key tail chunk [3000]. Per-entry failures on both sides of
    // the 1,000 boundary; the third chunk loses its whole response;
    // the tail is NotAttempted.
    const SIZE: usize = 3_001;
    let keys: Vec<String> = (0..SIZE).map(|index| format!("c{index}")).collect();
    keyspace
        .create(&keys[999], Bytes::from_static(b"a"))
        .await
        .unwrap();
    keyspace
        .create(&keys[1000], Bytes::from_static(b"b"))
        .await
        .unwrap();
    counterpart
        .arm_multi_delete_entry_fault(
            &control_path("a39", &keys[999]),
            StorageFaultPhase::BeforeEffect,
            "TransientHiccup",
            "retryable",
        )
        .await;
    counterpart
        .arm_multi_delete_entry_fault(
            &control_path("a39", &keys[1000]),
            StorageFaultPhase::BeforeEffect,
            "AccessDenied",
            "permanent",
        )
        .await;
    // Chunk 3 (occurrence 3) loses its response after effects.
    counterpart
        .arm_multi_delete_request_fault(StorageFaultPhase::AfterEffect, 3)
        .await;

    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let outcomes = keyspace.delete_objects(&refs).await.unwrap();
    assert_eq!(outcomes.len(), SIZE);

    // Valid per-key errors did not prevent later chunks: chunks 1–3
    // were all sent; the refused chunk 3's lost response then stops
    // the call before the one-key tail chunk.
    assert_eq!(bulk_posts(&counterpart.snapshot().await).len(), 3);

    for (index, outcome) in outcomes.iter().enumerate() {
        assert_eq!(outcome.key, keys[index]);
        match index {
            999 => {
                let diagnostic = rejected_diagnostic(outcome);
                assert_eq!(diagnostic.code.as_deref(), Some("TransientHiccup"));
            }
            1000 => {
                let diagnostic = rejected_diagnostic(outcome);
                assert_eq!(diagnostic.code.as_deref(), Some("AccessDenied"));
            }
            2000..=2999 => {
                let (reason, _) = unconfirmed_parts(outcome);
                assert_eq!(reason, DeleteObjectsUnconfirmedReason::RequestFailed);
            }
            3000 => assert_not_attempted(outcome),
            _ => assert!(outcome.result.is_ok(), "index {index}"),
        }
    }

    // The classification coverage: policy iterates the vector and
    // terminates — replay transient + unconfirmed under the ceiling,
    // surface the permanent rejection, never touch confirmations.
    let mut replay_set: Vec<&str> = Vec::new();
    let mut surfaced = 0usize;
    for outcome in &outcomes {
        if outcome.result.is_ok() {
            continue;
        }
        match classify_outcome(outcome) {
            PolicyAction::Replay => replay_set.push(outcome.key.as_str()),
            PolicyAction::Surface => surfaced += 1,
            PolicyAction::Terminal => {}
        }
    }
    assert_eq!(surfaced, 1, "exactly the AccessDenied rejection surfaces");
    let expected_replay: Vec<&str> = std::iter::once(keys[999].as_str())
        .chain((2000..=3000).map(|index| keys[index].as_str()))
        .collect();
    assert_eq!(replay_set, expected_replay);

    // remaining() serializes exactly the unresolved keys in order.
    let expected_remaining: Vec<String> = [999usize, 1000]
        .into_iter()
        .chain(2000..SIZE)
        .map(|index| keys[index].clone())
        .collect();
    debug_assert_eq!(expected_remaining.len(), 1_003);
    assert_eq!(DeleteObjectsOutcome::remaining(&outcomes), expected_remaining);

    // The bounded replay converges with no side effects on the
    // confirmed set; the surfaced key stays.
    let replayed = keyspace.delete_objects(&replay_set).await.unwrap();
    assert!(replayed.iter().all(|outcome| outcome.result.is_ok()));
    assert_object_state(&store, &control_path("a39", &keys[999]), false).await;
    assert_object_state(&store, &control_path("a39", &keys[1000]), true).await;
    counterpart.shutdown().await;
}

// --- A40: whole-request ambiguity never fabricates success ----------

#[tokio::test]
async fn a40_delete_objects_lost_response_marks_chunk_unconfirmed() {
    let (store, keyspace, counterpart) = keyspace_fixture("a40").await;

    // BeforeEffect on the first chunk: nothing applied, the chunk is
    // Unconfirmed, the tail NotAttempted.
    const SIZE: usize = 1_500;
    let keys: Vec<String> = (0..SIZE).map(|index| format!("b{index}")).collect();
    for key in [&keys[0], &keys[1_200]] {
        keyspace
            .create(key, Bytes::from_static(b"v"))
            .await
            .unwrap();
    }
    counterpart
        .arm_multi_delete_request_fault(StorageFaultPhase::BeforeEffect, 1)
        .await;
    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let refused = keyspace.delete_objects(&refs).await.unwrap();
    assert_eq!(refused.len(), SIZE);
    for (index, outcome) in refused.iter().enumerate() {
        if index < 1_000 {
            let (reason, _) = unconfirmed_parts(outcome);
            assert_eq!(reason, DeleteObjectsUnconfirmedReason::RequestFailed);
        } else {
            assert_not_attempted(outcome);
        }
    }
    assert_object_state(&store, &control_path("a40", &keys[0]), true).await;
    assert_object_state(&store, &control_path("a40", &keys[1_200]), true).await;

    counterpart.shutdown().await;

    // AfterEffect on the second chunk of three (fresh fixture so the
    // occurrence counter starts clean): the first chunk stays
    // confirmed, the second is Unconfirmed (applied — some or all of
    // its keys may already be gone), the third is NotAttempted.
    const THREE: usize = 2_500;
    let (store, keyspace, counterpart) = keyspace_fixture("a40a").await;
    let keys3: Vec<String> = (0..THREE).map(|index| format!("t{index}")).collect();
    for index in [3usize, 1_003, 2_003] {
        keyspace
            .create(&keys3[index], Bytes::from_static(b"v"))
            .await
            .unwrap();
    }
    counterpart
        .arm_multi_delete_request_fault(StorageFaultPhase::AfterEffect, 2)
        .await;
    let refs3: Vec<&str> = keys3.iter().map(String::as_str).collect();
    let lost = keyspace.delete_objects(&refs3).await.unwrap();
    assert_eq!(lost.len(), THREE);
    assert!(lost[0..1_000].iter().all(|outcome| outcome.result.is_ok()));
    for outcome in &lost[1_000..2_000] {
        let (reason, _) = unconfirmed_parts(outcome);
        assert_eq!(reason, DeleteObjectsUnconfirmedReason::RequestFailed);
    }
    for outcome in &lost[2_000..] {
        assert_not_attempted(outcome);
    }
    // Confirmed prefix applied; the ambiguous chunk also applied (its
    // seeded key is gone — Unconfirmed is not a presence proof); the
    // untouched tail stayed.
    assert_object_state(&store, &control_path("a40a", &keys3[3]), false).await;
    assert_object_state(&store, &control_path("a40a", &keys3[1_003]), false).await;
    assert_object_state(&store, &control_path("a40a", &keys3[2_003]), true).await;

    // One request-level diagnostic allocation is shared across the
    // ambiguous chunk's outcomes.
    let shared = unconfirmed_parts(&lost[1_000]).1;
    for outcome in &lost[1_000..2_000] {
        let (_, diagnostic) = unconfirmed_parts(outcome);
        assert!(Arc::ptr_eq(shared, diagnostic));
    }

    // A service 403 stays distinguishable from a transport reset:
    // both are Unconfirmed/RequestFailed, but the diagnostics differ
    // (provider code present vs absent).
    let fresh: Vec<String> = (0..3).map(|index| format!("d{index}")).collect();
    let fresh_refs: Vec<&str> = fresh.iter().map(String::as_str).collect();

    // 403: definitive service error, not a 405/501 refusal.
    let (_, keyspace_403, counterpart_403) = keyspace_fixture("a40x").await;
    counterpart_403
        .arm_multi_delete_refusal(1, 403, "AccessDenied", "denied by policy")
        .await;
    let denied = keyspace_403.delete_objects(&fresh_refs).await.unwrap();
    let (reason_403, diagnostic_403) = unconfirmed_parts(&denied[0]);
    assert_eq!(reason_403, DeleteObjectsUnconfirmedReason::RequestFailed);
    assert_eq!(diagnostic_403.code.as_deref(), Some("AccessDenied"));

    // Transport-class failure (fault status, no S3 body): same class,
    // no provider code.
    let (_, keyspace_400, counterpart_400) = keyspace_fixture("a40y").await;
    counterpart_400
        .arm_multi_delete_request_fault(StorageFaultPhase::BeforeEffect, 1)
        .await;
    let reset = keyspace_400.delete_objects(&fresh_refs).await.unwrap();
    let (reason_400, diagnostic_400) = unconfirmed_parts(&reset[0]);
    assert_eq!(reason_400, DeleteObjectsUnconfirmedReason::RequestFailed);
    assert_eq!(diagnostic_400.code.as_deref(), None);
    assert_ne!(diagnostic_403.code, diagnostic_400.code);

    counterpart.shutdown().await;
    counterpart_403.shutdown().await;
    counterpart_400.shutdown().await;
}

// --- A41: invalid provider responses fail closed --------------------

#[tokio::test]
async fn a41_delete_objects_invalid_response_is_never_success() {
    let mutations = [
        MultiDeleteResponseMutation::MissingMember,
        MultiDeleteResponseMutation::DuplicateMember,
        MultiDeleteResponseMutation::DeletedErrorConflict,
        MultiDeleteResponseMutation::UnknownKey,
        MultiDeleteResponseMutation::MalformedXml,
    ];
    for mutation in mutations {
        let (store, keyspace, counterpart) = keyspace_fixture("a41").await;
        let keys: Vec<String> = (0..3).map(|index| format!("m{index}")).collect();
        for key in &keys {
            keyspace
                .create(key, Bytes::from_static(b"v"))
                .await
                .unwrap();
        }
        counterpart.arm_multi_delete_response_mutation(1, mutation).await;
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let outcomes = keyspace.delete_objects(&refs).await.unwrap();

        // Every position of the corrupted response fails closed: the
        // whole chunk is Unconfirmed/InvalidResponse; no omitted key
        // defaults to success.
        assert_eq!(outcomes.len(), 3, "{mutation:?}");
        for outcome in &outcomes {
            let (reason, _) = unconfirmed_parts(outcome);
            assert_eq!(
                reason,
                DeleteObjectsUnconfirmedReason::InvalidResponse,
                "{mutation:?}"
            );
        }
        // Effects WERE applied (mutations corrupt the response, not
        // the deletes): any subset may be gone — and no success was
        // inferred from the applied state.
        for key in &keys {
            assert_object_state(&store, &control_path("a41", key), false).await;
        }
        // One logical operation only: no blind retry was issued inside
        // the call.
        assert_eq!(bulk_posts(&counterpart.snapshot().await).len(), 1, "{mutation:?}");
        counterpart.shutdown().await;
    }
}

// --- A42: incapable or refusing backends never emulate the wire -----

#[tokio::test]
async fn a42_delete_objects_fails_closed_without_wire_support() {
    // In-memory stores have no DeleteObjects wire: Unsupported with
    // values intact, no sequential emulation.
    {
        let store = Arc::new(ObjectStoreClient::in_memory("a42"));
        let keyspace =
            AtomicKeyspace::new(Arc::clone(&store), "ns").expect("valid keyspace namespace");
        keyspace
            .create("a", Bytes::from_static(b"va"))
            .await
            .unwrap();
        keyspace
            .create("b", Bytes::from_static(b"vb"))
            .await
            .unwrap();
        let outcomes = keyspace.delete_objects(&["a", "b"]).await.unwrap();
        assert_eq!(outcomes.len(), 2);
        for outcome in &outcomes {
            assert!(
                matches!(
                    outcome.result.as_ref(),
                    Err(DeleteObjectsFailure::Unsupported { .. })
                ),
                "expected Unsupported, got {:?}",
                outcome.result
            );
        }
        // Values intact.
        assert_eq!(
            keyspace.get("a").await.unwrap().as_deref(),
            Some(b"va".as_slice())
        );
        assert_eq!(
            keyspace.get("b").await.unwrap().as_deref(),
            Some(b"vb".as_slice())
        );
    }

    // 405 refusal after one confirmed chunk of a 2,001-key call: the
    // confirmed prefix stays Ok; the refusing chunk AND the untouched
    // tail are Unsupported — never NotAttempted — sharing one
    // diagnostic; no per-key fallback reaches a single-key delete.
    {
        let (store, keyspace, counterpart) = keyspace_fixture("a42").await;
        const SIZE: usize = 2_001;
        let keys: Vec<String> = (0..SIZE).map(|index| format!("r{index}")).collect();
        for index in [5usize, 1_500] {
            keyspace
                .create(&keys[index], Bytes::from_static(b"v"))
                .await
                .unwrap();
        }
        counterpart
            .arm_multi_delete_refusal(2, 405, "MethodNotAllowed", "DeleteObjects not supported")
            .await;
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let outcomes = keyspace.delete_objects(&refs).await.unwrap();
        assert_eq!(outcomes.len(), SIZE);
        assert!(outcomes[..1_000].iter().all(|outcome| outcome.result.is_ok()));
        for outcome in &outcomes[1_000..] {
            unsupported_diagnostic(outcome);
        }
        let shared = unsupported_diagnostic(&outcomes[1_000]);
        assert!(Arc::ptr_eq(shared, unsupported_diagnostic(&outcomes[2_000])));
        // Confirmed prefix applied; refused keys were not sent and
        // remain present.
        assert_object_state(&store, &control_path("a42", &keys[5]), false).await;
        assert_object_state(&store, &control_path("a42", &keys[1_500]), true).await;
        // No fallback: zero single-object DELETE requests on the wire.
        let snapshot = counterpart.snapshot().await;
        assert!(
            !snapshot
                .requests
                .iter()
                .any(|request| request.method == "DELETE")
        );
        assert_eq!(bulk_posts(&snapshot).len(), 2);
        counterpart.shutdown().await;
    }

    // 501/NotImplemented at the first chunk: everything Unsupported.
    {
        let (_, keyspace, counterpart) = keyspace_fixture("a42n").await;
        counterpart
            .arm_multi_delete_refusal(1, 501, "NotImplemented", "not implemented")
            .await;
        let outcomes = keyspace.delete_objects(&["x", "y"]).await.unwrap();
        assert!(outcomes.iter().all(|outcome| {
            matches!(
                outcome.result.as_ref(),
                Err(DeleteObjectsFailure::Unsupported { .. })
            )
        }));
        counterpart.shutdown().await;
    }

    // Other request failures remain Unconfirmed, not Unsupported.
    {
        let (_, keyspace, counterpart) = keyspace_fixture("a42u").await;
        counterpart
            .arm_multi_delete_request_fault(StorageFaultPhase::BeforeEffect, 1)
            .await;
        let outcomes = keyspace.delete_objects(&["x"]).await.unwrap();
        let (reason, _) = unconfirmed_parts(&outcomes[0]);
        assert_eq!(reason, DeleteObjectsUnconfirmedReason::RequestFailed);
        counterpart.shutdown().await;
    }
}

// --- A43: lifecycle machinery untouched, quiescence consequence -----

#[tokio::test]
async fn a43_delete_objects_stays_below_lifecycle_state() {
    let (store, keyspace, counterpart) = keyspace_fixture("a43").await;
    let fence_path = control_path("a43", "fences/gc");

    // Inline control: gone, Absent, no tombstone written, no counter.
    keyspace
        .create("inline", Bytes::from_static(b"v"))
        .await
        .unwrap();
    let inline = keyspace.delete_objects(&["inline"]).await.unwrap();
    assert!(inline[0].result.is_ok());
    assert_eq!(keyspace.read_state("inline").await.unwrap(), KeyState::Absent);
    assert_object_state(&store, &control_path("a43", "tombstones/inline"), false).await;
    assert_eq!(keyspace.incarnation_for_test("inline").await.unwrap(), 0);

    // Absent key: idempotent Ok (S3's unconditional-delete contract).
    let absent = keyspace.delete_objects(&["ghost"]).await.unwrap();
    assert!(absent[0].result.is_ok());

    // Tombstone case: a standing tombstone surfaces once the masking
    // value is batch-deleted; the counter stays at destroy's bump.
    keyspace
        .create("tomb", Bytes::from_static(b"v1"))
        .await
        .unwrap();
    keyspace.destroy("tomb", "cause", "a43").await.unwrap();
    assert_eq!(
        keyspace.incarnation_for_test("tomb").await.unwrap(),
        1,
        "destroy bumps once"
    );
    keyspace
        .create("tomb", Bytes::from_static(b"v2"))
        .await
        .unwrap();
    let tomb = keyspace.delete_objects(&["tomb"]).await.unwrap();
    assert!(tomb[0].result.is_ok());
    assert!(matches!(
        keyspace.read_state("tomb").await.unwrap(),
        KeyState::Destroyed { .. }
    ));
    assert_eq!(
        keyspace.incarnation_for_test("tomb").await.unwrap(),
        1,
        "the raw batch delete never bumps the counter"
    );

    // Trim case: the certified floor's authority survives unrelated
    // batch deletion, and the certificate object is untouched.
    keyspace.propose_trim("", 10).await.unwrap();
    assert!(matches!(
        keyspace.read_state("00000000000000000003").await.unwrap(),
        KeyState::OffsetExpired { first_retained: 10 }
    ));
    let unrelated = keyspace.delete_objects(&["plain"]).await.unwrap();
    assert!(unrelated[0].result.is_ok());
    assert!(matches!(
        keyspace.read_state("00000000000000000003").await.unwrap(),
        KeyState::OffsetExpired { first_retained: 10 }
    ));
    assert_object_state(
        &store,
        &control_path("a43", "trims/00000000000000000010"),
        true,
    )
    .await;

    // v3 control: batch delete leaves the counter unchanged, keeps
    // the chunks, and a byte-identical recreate re-materializes the
    // SAME incarnation/version-0 chunk paths.
    let data = pattern(STREAMED_LEN, 0xA5);
    {
        let mut writer = keyspace.begin_stream_create("cell").await.unwrap();
        writer.write_all(&data).await.unwrap();
        writer.seal().await.unwrap().commit().await.unwrap();
    }
    let expected_chunk_paths: Vec<String> = (0..3)
        .map(|ordinal| chunk_key_of("a43", "cell", ordinal, &data))
        .collect();
    for path in &expected_chunk_paths {
        assert_object_state(&store, path, true).await;
    }
    let v3 = keyspace.delete_objects(&["cell"]).await.unwrap();
    assert!(v3[0].result.is_ok());
    assert_eq!(
        keyspace.incarnation_for_test("cell").await.unwrap(),
        0,
        "no incarnation bump"
    );
    // Chunks remain: control deletion reclaims nothing.
    for path in &expected_chunk_paths {
        assert_object_state(&store, path, true).await;
    }
    // Byte-identical recreate: same incarnation (0), version 0 — the
    // identical physical chunk paths.
    {
        let mut writer = keyspace.begin_stream_create("cell").await.unwrap();
        writer.write_all(&data).await.unwrap();
        writer.seal().await.unwrap().commit().await.unwrap();
    }
    let snapshot = counterpart.snapshot().await;
    let recreated_chunk_puts: Vec<&str> = snapshot
        .requests
        .iter()
        .filter(|request| {
            request.method == "PUT"
                && request
                    .key
                    .as_deref()
                    .is_some_and(|key| key.starts_with("keyspace-chunks/"))
        })
        .filter_map(|request| request.key.as_deref())
        .collect();
    for path in &expected_chunk_paths {
        assert!(
            recreated_chunk_puts.contains(&path.as_str()),
            "recreate re-materializes {path}"
        );
    }

    // Fence blindness, scoped to the primitive: a delete_objects call
    // issues no request at all against the fence control (streamed
    // writers legitimately read the fence to respect it; this
    // primitive does not).
    {
        keyspace
            .create("fence-probe", Bytes::from_static(b"v"))
            .await
            .unwrap();
        let before = counterpart.snapshot().await.requests.len();
        let probe = keyspace.delete_objects(&["fence-probe"]).await.unwrap();
        assert!(probe[0].result.is_ok());
        let delta = &counterpart.snapshot().await.requests[before..];
        assert!(
            !delta
                .iter()
                .any(|request| request.key.as_deref() == Some(fence_path.as_str())),
            "delete_objects is fence-blind"
        );
    }
    {
        let victim_data = pattern(STREAMED_LEN, 0xE6);
        {
            let mut writer = keyspace.begin_stream_create("victim").await.unwrap();
            writer.write_all(&victim_data).await.unwrap();
            writer.seal().await.unwrap().commit().await.unwrap();
        }
        // The raw batch delete removes the control without a bump.
        let removed = keyspace.delete_objects(&["victim"]).await.unwrap();
        assert!(removed[0].result.is_ok());
        assert_eq!(keyspace.incarnation_for_test("victim").await.unwrap(), 0);
        // The pre-fence writer begins the byte-identical recreate.
        let mut writer = keyspace.begin_stream_create("victim").await.unwrap();
        writer.write_all(&victim_data).await.unwrap();
        let pending = writer.seal().await.unwrap();
        // Operators fence and drain — falsely.
        keyspace.set_maintenance_fence().await.unwrap();
        // The sweep exact-reads the control absent (the batch delete
        // removed it) and deletes the candidate chunks.
        let report = keyspace.sweep_chunks().await.unwrap();
        assert!(report.deleted >= 3, "the violated-precondition sweep");
        // The writer's conditional manifest publication succeeds and
        // names absent chunks: Present but damaged.
        pending.commit().await.expect("the manifest publishes");
        match keyspace.get("victim").await {
            Err(KeyspaceError::ChunkMissing { key, chunk }) => {
                assert_eq!((key.as_str(), chunk), ("victim", 0));
            }
            other => panic!("broken quiescence must surface ChunkMissing, got {other:?}"),
        }
        keyspace.release_maintenance_fence().await.unwrap();
    }

    counterpart.shutdown().await;
}

// --- A44: visibly unconditional and non-atomic ----------------------

#[tokio::test]
async fn a44_delete_objects_has_no_condition_or_transaction() {
    let (store, keyspace, counterpart) = keyspace_fixture("a44").await;

    // Mixed success plus an earlier-chunk success and a later stop:
    // partial state is recorded and earlier deletions stand.
    const SIZE: usize = 3_001;
    let keys: Vec<String> = (0..SIZE).map(|index| format!("p{index}")).collect();
    for index in [7usize, 1_507] {
        keyspace
            .create(&keys[index], Bytes::from_static(b"v"))
            .await
            .unwrap();
    }
    counterpart
        .arm_multi_delete_entry_fault(
            &control_path("a44", &keys[5]),
            StorageFaultPhase::BeforeEffect,
            "PolicyViolation",
            "mixed batch",
        )
        .await;
    counterpart
        .arm_multi_delete_request_fault(StorageFaultPhase::AfterEffect, 3)
        .await;
    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let outcomes = keyspace.delete_objects(&refs).await.unwrap();
    // Mixed success inside one chunk...
    assert!(outcomes[4].result.is_ok());
    assert!(rejected_diagnostic(&outcomes[5]).code.is_some());
    assert!(outcomes[6].result.is_ok());
    // ...an earlier confirmed chunk that stands, minus its own
    // per-key rejection at index 5...
    assert!(outcomes[..1_000]
        .iter()
        .enumerate()
        .all(|(index, outcome)| index == 5 || outcome.result.is_ok()));
    assert_object_state(&store, &control_path("a44", &keys[7]), false).await;
    // ...and a stopped later chunk with an untouched NotAttempted tail.
    let (reason, _) = unconfirmed_parts(&outcomes[2_000]);
    assert_eq!(reason, DeleteObjectsUnconfirmedReason::RequestFailed);
    assert_not_attempted(&outcomes[3_000]);
    assert_object_state(&store, &control_path("a44", &keys[1_507]), false).await;

    // The request may delete a concurrent replacement: an unconditional
    // batch removes whatever is current — here the CAS replacement v2 —
    // without consulting any token.
    keyspace
        .create("race", Bytes::from_static(b"v1"))
        .await
        .unwrap();
    let (_, etag_v1) = keyspace.get_with_etag("race").await.unwrap().unwrap();
    keyspace
        .compare_exchange("race", &etag_v1, Bytes::from_static(b"v2"))
        .await
        .unwrap();
    let removed = keyspace.delete_objects(&["race"]).await.unwrap();
    assert!(removed[0].result.is_ok());
    assert_object_state(&store, &control_path("a44", "race"), false).await;

    // The companion conditional primitive protects a replacement the
    // unconditional request would have erased: a stale-era token
    // refuses and the value survives.
    keyspace
        .create("guarded", Bytes::from_static(b"v1"))
        .await
        .unwrap();
    let (_, guarded_etag) = keyspace.get_with_etag("guarded").await.unwrap().unwrap();
    keyspace
        .compare_exchange("guarded", &guarded_etag, Bytes::from_static(b"v2"))
        .await
        .unwrap();
    match keyspace.delete_if_match("guarded", &guarded_etag).await {
        Err(KeyspaceError::PreconditionFailed { key, .. }) => assert_eq!(key, "guarded"),
        other => panic!("stale conditional delete must refuse, got {other:?}"),
    }
    assert_object_state(&store, &control_path("a44", "guarded"), true).await;

    // Unconditional wire: no etag, version, or If-Match in any request.
    let snapshot = counterpart.snapshot().await;
    assert!(bulk_posts(&snapshot)
        .iter()
        .all(|post| post.if_match.is_none() && post.if_none_match.is_none()));
    counterpart.shutdown().await;
}

// --- A45: the published deletion surface does not move --------------

#[tokio::test]
async fn a45_delete_many_and_delete_if_match_remain_unchanged() {
    let (store, keyspace, counterpart) = keyspace_fixture("a45").await;

    // Compile-time signatures and types: delete_many still returns
    // boolean DeleteOutcome per key; delete and delete_if_match still
    // return Result<(), KeyspaceError>.
    let legacy_keys: Vec<&str> = vec!["m0", "m1", "m2"];
    for key in &legacy_keys {
        keyspace
            .create(key, Bytes::from_static(b"v"))
            .await
            .unwrap();
    }
    let outcomes: Vec<crate::atomic_keyspace::DeleteOutcome> =
        keyspace.delete_many(&legacy_keys).await.unwrap();
    for (outcome, key) in outcomes.iter().zip(&legacy_keys) {
        let crate::atomic_keyspace::DeleteOutcome {
            key: reported,
            deleted,
        } = outcome;
        assert_eq!((reported.as_str(), *deleted), (*key, true));
    }

    // N one-key DeleteObjects POSTs through the unchanged object_store
    // path — not one multi-key request.
    let snapshot = counterpart.snapshot().await;
    let legacy_posts: Vec<&LoopbackRequestObservation> = bulk_posts(&snapshot)
        .into_iter()
        .filter(|post| {
            post.delete_keys
                .as_deref()
                .is_some_and(|keys| keys.iter().all(|key| key.starts_with("keyspace/a45/m")))
        })
        .collect();
    assert_eq!(legacy_posts.len(), 3);
    assert!(legacy_posts
        .iter()
        .all(|post| post.delete_keys.as_deref().is_some_and(|keys| keys.len() == 1)));

    // G117: an armed legacy KeyspaceDelete AfterEffect cut inside a
    // multi-key POST still reports deleted=false for that key only —
    // the old fault mapping, unchanged.
    keyspace
        .create("g117", Bytes::from_static(b"v"))
        .await
        .unwrap();
    keyspace
        .create("g117b", Bytes::from_static(b"v"))
        .await
        .unwrap();
    counterpart
        .arm_storage_fault(
            StorageFaultCut::KeyspaceDelete,
            StorageFaultPhase::AfterEffect,
            &control_path("a45", "g117"),
        )
        .await;
    let mixed = keyspace.delete_many(&["g117", "g117b"]).await.unwrap();
    assert_eq!(
        (mixed[0].deleted, mixed[1].deleted),
        (false, true),
        "G117 lost-response mapping"
    );
    assert!(
        counterpart
            .snapshot()
            .await
            .faults
            .iter()
            .any(|fault| fault.cut == StorageFaultCut::KeyspaceDelete)
    );

    // G118: delete_if_match is one conditional DELETE per key on the
    // wire, and its taxonomy stands.
    keyspace
        .create("cond", Bytes::from_static(b"v"))
        .await
        .unwrap();
    let (_, etag) = keyspace.get_with_etag("cond").await.unwrap().unwrap();
    keyspace.delete_if_match("cond", &etag).await.unwrap();
    let cond_snapshot = counterpart.snapshot().await;
    let conditional_delete = cond_snapshot
        .requests
        .iter()
        .find(|request| {
            request.method == "DELETE" && request.key.as_deref() == Some(&control_path("a45", "cond"))
        })
        .expect("the conditional delete wire request");
    assert!(conditional_delete.if_match.is_some());
    assert_eq!(conditional_delete.status, 204);
    match keyspace.delete_if_match("cond", &etag).await {
        Err(KeyspaceError::PreconditionFailed { .. }) => {}
        other => panic!("absent conditional delete taxonomy, got {other:?}"),
    }

    // Dual-call trace: the legacy path emits N one-key POSTs while the
    // new primitive emits one bounded multi-key POST — two visibly
    // distinct DeleteObjects implementations, no wrapper either way.
    let before = bulk_posts(&counterpart.snapshot().await).len();
    keyspace.delete_many(&["t1", "t2"]).await.unwrap();
    let after_legacy = bulk_posts(&counterpart.snapshot().await).len();
    assert_eq!(after_legacy - before, 2, "delete_many: N one-key POSTs");
    let new = keyspace.delete_objects(&["t1", "t2"]).await.unwrap();
    assert!(new.iter().all(|outcome| outcome.result.is_ok()));
    let after_new = bulk_posts(&counterpart.snapshot().await).len();
    assert_eq!(after_new - after_legacy, 1, "delete_objects: one POST");

    // No wrapper, deprecation, or alias: delete_objects calls neither
    // delete nor delete_many, and delete_many does not call
    // delete_objects.
    let source = include_str!("atomic_keyspace.rs");
    for segment in source.split("pub async fn ").skip(1) {
        let name = segment.split('(').next().unwrap_or_default();
        let body = segment.split("\n    pub ").next().unwrap_or(segment);
        match name {
            "delete_objects" => {
                assert!(
                    !body.contains("self.delete(") && !body.contains("self.delete_many("),
                    "delete_objects must not wrap the per-key surface"
                );
            }
            "delete_many" => {
                assert!(
                    !body.contains("delete_objects"),
                    "delete_many must not wrap the new surface"
                );
            }
            _ => {}
        }
    }
    assert!(
        !source.contains("deprecated"),
        "no published deletion API is deprecated"
    );

    let _ = store;
    counterpart.shutdown().await;
}
