//! W-suite (batch 6): tombstones — existence witnesses that make
//! "destroyed" distinguishable from "never created." The design is
//! ADR 0001's batch-6 addendum; the parent-aggregate witness
//! convention this mechanizes is the parent data model's
//! existence-and-deletion discipline.

use yeetz_s3_kernel::{AtomicKeyspace, KernelHandle, KeyState, KeyspaceError};

fn keyspace(name: &str) -> AtomicKeyspace {
    KernelHandle::with_in_memory_store(name)
        .atomic_keyspace("tomb/v1")
        .unwrap()
}

fn data_key(seq: u64) -> String {
    format!("data/{seq:020}")
}

/// W1: the three-way lifecycle — create reads `Present` (value +
/// etag); an intentional deletion reads `Destroyed` with the
/// tombstone's cause/actor/generation; a key that never existed
/// reads `Absent` (no fabricated witness).
#[tokio::test]
async fn w1_present_destroyed_absent_lifecycle() {
    let ks = keyspace("w1");

    // Never existed: Absent — not Destroyed, not an error.
    assert_eq!(ks.read_state(&data_key(3)).await.unwrap(), KeyState::Absent);

    // Create → Present with the value and a CAS token.
    ks.create(&data_key(3), bytes::Bytes::from_static(b"v0"))
        .await
        .unwrap();
    match ks.read_state(&data_key(3)).await.unwrap() {
        KeyState::Present { value, etag } => {
            assert_eq!(value.as_ref(), b"v0");
            assert!(!etag.is_empty());
        }
        other => panic!("expected Present, got {other:?}"),
    }

    // Intentional deletion → Destroyed, carrying the witness.
    ks.destroy(&data_key(3), "retention-policy", "reconciler-x")
        .await
        .unwrap();
    match ks.read_state(&data_key(3)).await.unwrap() {
        KeyState::Destroyed { tombstone } => {
            assert_eq!(tombstone.deleted_at_gen, 0, "created at version 0");
            assert_eq!(tombstone.cause, "retention-policy");
            assert_eq!(tombstone.actor, "reconciler-x");
            assert!(tombstone.ts > 0, "the witness is timestamped");
        }
        other => panic!("expected Destroyed, got {other:?}"),
    }

    // A CAS-era value records the generation it was destroyed at.
    ks.create(&data_key(4), bytes::Bytes::from_static(b"v0"))
        .await
        .unwrap();
    let (_, etag) = ks.get_with_etag(&data_key(4)).await.unwrap().unwrap();
    ks.compare_exchange(&data_key(4), &etag, bytes::Bytes::from_static(b"v1"))
        .await
        .unwrap();
    ks.destroy(&data_key(4), "manual", "operator")
        .await
        .unwrap();
    match ks.read_state(&data_key(4)).await.unwrap() {
        KeyState::Destroyed { tombstone } => assert_eq!(tombstone.deleted_at_gen, 1),
        other => panic!("expected Destroyed, got {other:?}"),
    }

    // Destroying an absent key is a no-op — absence stays absence,
    // and no witness is fabricated.
    ks.destroy(&data_key(9), "noop", "nobody").await.unwrap();
    assert_eq!(ks.read_state(&data_key(9)).await.unwrap(), KeyState::Absent);
}

/// W2: create-after-destroy supersedes — the re-create succeeds with
/// a fresh identity (version 0), `read_state` is `Present` (the new
/// existence IS the truth), and the tombstone object remains as
/// historical record.
#[tokio::test]
async fn w2_create_after_destroy_supersedes() {
    let ks = keyspace("w2");
    ks.create(&data_key(5), bytes::Bytes::from_static(b"first"))
        .await
        .unwrap();
    ks.destroy(&data_key(5), "rebuild", "agent").await.unwrap();

    // The re-create: fresh lifetime, version 0.
    ks.create(&data_key(5), bytes::Bytes::from_static(b"second"))
        .await
        .unwrap();
    // The new existence is the truth: Present, not Destroyed.
    match ks.read_state(&data_key(5)).await.unwrap() {
        KeyState::Present { value, .. } => assert_eq!(value.as_ref(), b"second"),
        other => panic!("expected Present after re-create, got {other:?}"),
    }
    // Fresh identity: version 0 (not a continuation of the old era).
    let (_, version, _) = ks
        .get_with_version_for_test(&data_key(5))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(version, 0);

    // The tombstone remains as history until a certified trim
    // retires it (a raw object — observed through the key listing,
    // which is namespace-relative).
    let tombstones = ks.list_after(Some("tombstones"), 1000).await.unwrap();
    assert!(
        tombstones.contains(&"tombstones/data/00000000000000000005".to_string()),
        "the witness outlives the key it named: {tombstones:?}"
    );
}

/// W3: tombstone immutability — the reserved `tombstones/` prefix
/// refuses direct create, compare-and-swap, delete, and bulk delete.
/// Only `destroy` writes witnesses; only a certified trim sweep
/// removes them.
#[tokio::test]
async fn w3_tombstones_are_immutable() {
    let ks = keyspace("w3");
    ks.create(&data_key(2), bytes::Bytes::from_static(b"v"))
        .await
        .unwrap();
    ks.destroy(&data_key(2), "test", "w3").await.unwrap();
    let tombstone_key = "tombstones/data/00000000000000000002";

    assert!(matches!(
        ks.create(tombstone_key, bytes::Bytes::from_static(b"forged"))
            .await
            .unwrap_err(),
        KeyspaceError::TombstoneImmutable(_)
    ));
    assert!(matches!(
        ks.compare_exchange(tombstone_key, "any", bytes::Bytes::from_static(b"x"))
            .await
            .unwrap_err(),
        KeyspaceError::TombstoneImmutable(_)
    ));
    assert!(matches!(
        ks.delete(tombstone_key).await.unwrap_err(),
        KeyspaceError::TombstoneImmutable(_)
    ));
    assert!(matches!(
        ks.delete_many(&[tombstone_key]).await.unwrap_err(),
        KeyspaceError::TombstoneImmutable(_)
    ));

    // The witness is intact, and the read still names it.
    assert!(matches!(
        ks.read_state(&data_key(2)).await.unwrap(),
        KeyState::Destroyed { .. }
    ));
}

/// W4: trim interaction — a tombstone (or its key) below the
/// certified floor reads `OffsetExpired` after the sweep retires it;
/// above the floor it reads `Destroyed`. The certificate rules the
/// history, never object absence.
#[tokio::test]
async fn w4_trim_retires_tombstones_below_the_floor() {
    let ks = keyspace("w4");
    for seq in 0..=9u64 {
        ks.create(&data_key(seq), bytes::Bytes::from_static(b"v"))
            .await
            .unwrap();
    }
    // Witness seq 3's deletion BEFORE trimming (the tombstone must
    // predate the certificate it will be retired by).
    ks.destroy(&data_key(3), "policy", "reconciler")
        .await
        .unwrap();
    ks.propose_trim("", 6).await.unwrap();

    // Below the floor, pre-sweep: the certificate already rules —
    // OffsetExpired, not Destroyed.
    assert_eq!(
        ks.read_state(&data_key(3)).await.unwrap(),
        KeyState::OffsetExpired { first_retained: 6 }
    );

    // The sweep retires values AND tombstones below the floor.
    let report = ks.delete_below("", "data/", 6).await.unwrap();
    // Values 1, 2, 4, 5 (seq 3's value was already destroyed) plus
    // the seq-3 tombstone and its incarnation counter (batch 7);
    // the genesis (seq 0) is immortal.
    assert_eq!(report.deleted, 6);
    let remaining = ks.list_after(Some("tombstones"), 1000).await.unwrap();
    assert!(
        !remaining.contains(&"tombstones/data/00000000000000000003".to_string()),
        "the tombstone retired with the data it witnessed: {remaining:?}"
    );

    // Above the floor, destruction stays legible.
    ks.destroy(&data_key(8), "manual", "operator")
        .await
        .unwrap();
    assert!(matches!(
        ks.read_state(&data_key(8)).await.unwrap(),
        KeyState::Destroyed { .. }
    ));
    // The floor itself is retained and present.
    assert!(matches!(
        ks.read_state(&data_key(6)).await.unwrap(),
        KeyState::Present { .. }
    ));
}

// W5 (the lineage tombstone member) moved in-src to
// `state_kernel::gateway_state_contract` beside the lifecycle
// canaries: `destroy`'s tail now requires the conditional-delete
// wire primitive (lifecycle closure L10), and in-memory handles
// fail closed — the A18 posture. Its public assertions are
// unchanged.
