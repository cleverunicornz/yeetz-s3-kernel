//! I-suite (batch 7): incarnation counters — CAS safety across
//! deletion boundaries. Batch 4's versioned values made versions
//! strictly increase within a key's lifetime; batch 7 makes the
//! LIFETIME itself part of the envelope, so a delete/recreate cycle
//! can never let an era-1 etag match era-2 bytes. The design is ADR
//! 0001's batch-7 addendum.

use bytes::Bytes;
use yeetz_s3_kernel::{AtomicKeyspace, KernelHandle, KeyState, KeyspaceError};

fn keyspace(name: &str) -> AtomicKeyspace {
    KernelHandle::with_in_memory_store(name)
        .atomic_keyspace("incar/v1")
        .unwrap()
}

fn data_key(seq: u64) -> String {
    format!("data/{seq:020}")
}

/// I1: the headline hazard — delete, recreate with IDENTICAL payload
/// bytes, then CAS with the era-1 etag: rejected, with the error
/// naming the era-2 incarnation. The same CAS with a fresh era-2
/// etag succeeds.
#[tokio::test]
async fn i1_stale_era_cas_across_recreate_is_rejected() {
    let ks = keyspace("i1");
    ks.create("cell", Bytes::from_static(b"era-one"))
        .await
        .unwrap();
    let (_, era1_etag) = ks.get_with_etag("cell").await.unwrap().unwrap();

    ks.destroy_in_memory_for_test("cell", "rebuild", "i1")
        .await
        .unwrap();
    // The recreation uses IDENTICAL payload bytes — the exact ABA
    // shape that content-derived etags cannot distinguish on their
    // own.
    ks.create("cell", Bytes::from_static(b"era-one"))
        .await
        .unwrap();

    // Era-1 CAS: rejected, and the error names the era it lost to.
    let err = ks
        .compare_exchange("cell", &era1_etag, Bytes::from_static(b"era-two"))
        .await
        .unwrap_err();
    match err {
        KeyspaceError::PreconditionFailed {
            observed_incarnation: Some(1),
            ..
        } => {}
        other => panic!("expected incarnation-1 rejection, got {other:?}"),
    }
    // The value is untouched.
    assert_eq!(
        ks.get("cell").await.unwrap().unwrap().as_ref(),
        b"era-one".as_slice()
    );

    // Era-2 CAS with a fresh etag succeeds within the incarnation.
    let (_, era2_etag) = ks.get_with_etag("cell").await.unwrap().unwrap();
    ks.compare_exchange("cell", &era2_etag, Bytes::from_static(b"era-two"))
        .await
        .unwrap();
    assert_eq!(
        ks.get("cell").await.unwrap().unwrap().as_ref(),
        b"era-two".as_slice()
    );
}

/// I2: versions are monotone within an incarnation and reset across
/// them — the re-created era is version 0 of a HIGHER incarnation,
/// not a continuation.
#[tokio::test]
async fn i2_versions_reset_across_incarnations_not_within() {
    let ks = keyspace("i2");
    ks.create("cell", Bytes::from_static(b"v0")).await.unwrap();

    // Monotone within incarnation 0: 0 -> 1 -> 2.
    for expected in 1..=2u64 {
        let (_, etag) = ks.get_with_etag("cell").await.unwrap().unwrap();
        ks.compare_exchange("cell", &etag, Bytes::from_static(b"next"))
            .await
            .unwrap();
        let (_, version) = ks
            .get_with_version_for_test("cell")
            .await
            .unwrap()
            .map(|(payload, version, _)| (payload, version))
            .unwrap();
        assert_eq!(version, expected);
    }

    // Across the boundary: version restarts at 0.
    ks.destroy_in_memory_for_test("cell", "cycle", "i2")
        .await
        .unwrap();
    ks.create("cell", Bytes::from_static(b"fresh"))
        .await
        .unwrap();
    let (payload, version) = ks
        .get_with_version_for_test("cell")
        .await
        .unwrap()
        .map(|(payload, version, _)| (payload, version))
        .unwrap();
    assert_eq!(version, 0, "the new era starts at version 0");
    assert_eq!(payload.as_ref(), b"fresh".as_slice());
    assert_eq!(ks.incarnation_for_test("cell").await.unwrap(), 1);
}

/// I3: the incarnation never decreases across many destroy/create
/// cycles (gaps would be fine; decreases are impossible).
#[tokio::test]
async fn i3_incarnation_never_decreases_across_cycles() {
    let ks = keyspace("i3");
    assert_eq!(ks.incarnation_for_test("cell").await.unwrap(), 0);
    for expected in 1..=5u64 {
        ks.create("cell", Bytes::from_static(b"life"))
            .await
            .unwrap();
        ks.destroy_in_memory_for_test("cell", "cycle", "i3")
            .await
            .unwrap();
        assert_eq!(
            ks.incarnation_for_test("cell").await.unwrap(),
            expected,
            "after destroy #{expected}"
        );
    }
    // The counter object itself refuses direct writes.
    let counter_key = "incarnations/cell";
    assert!(matches!(
        ks.delete(counter_key).await.unwrap_err(),
        KeyspaceError::IncarnationCounterImmutable(_)
    ));
    assert!(matches!(
        ks.create(counter_key, Bytes::from_static(b"0"))
            .await
            .unwrap_err(),
        KeyspaceError::IncarnationCounterImmutable(_)
    ));
}

/// I4: the tombstone records the incarnation it closed.
#[tokio::test]
async fn i4_tombstone_records_the_destroyed_incarnation() {
    let ks = keyspace("i4");
    ks.create("cell", Bytes::from_static(b"first"))
        .await
        .unwrap();
    ks.destroy_in_memory_for_test("cell", "cycle", "i4")
        .await
        .unwrap();
    match ks.read_state("cell").await.unwrap() {
        KeyState::Destroyed { tombstone } => {
            assert_eq!(tombstone.incarnation, 0, "the first lifetime");
            assert_eq!(tombstone.deleted_at_gen, 0);
        }
        other => panic!("expected Destroyed, got {other:?}"),
    }
}

/// I5: a certified trim retires incarnation counters with the
/// history they counted — above the floor they persist, below it a
/// recreated key starts at incarnation 0 again, sanctioned by the
/// certificate (readers of that history see `OffsetExpired`).
#[tokio::test]
async fn i5_trim_retires_counters_with_the_history() {
    let ks = keyspace("i5");
    for seq in 0..=9u64 {
        ks.create(&data_key(seq), Bytes::from_static(b"v"))
            .await
            .unwrap();
    }
    // Two lifetimes on seq 3 (counter = 1).
    ks.destroy_in_memory_for_test(&data_key(3), "cycle", "i5")
        .await
        .unwrap();
    ks.create(&data_key(3), Bytes::from_static(b"v2"))
        .await
        .unwrap();
    ks.propose_trim("", 6).await.unwrap();
    let report = ks.delete_below("", "data/", 6).await.unwrap();
    // Values 1..=5 (including seq 3's re-created value) + the seq-3
    // tombstone + the seq-3 incarnation counter.
    assert_eq!(report.deleted, 7);

    // Below the floor: history gone, readers OffsetExpired.
    assert!(matches!(
        ks.read_state(&data_key(3)).await.unwrap(),
        KeyState::OffsetExpired { first_retained: 6 }
    ));
    // Above the floor the counter survives: seq 8's value was never
    // destroyed (incarnation 0); its first destroy moves it to 1.
    assert_eq!(ks.incarnation_for_test(&data_key(8)).await.unwrap(), 0);
    ks.destroy_in_memory_for_test(&data_key(8), "cycle", "i5")
        .await
        .unwrap();
    assert_eq!(ks.incarnation_for_test(&data_key(8)).await.unwrap(), 1);
}

/// I6: batch 4's behavior is preserved — a current-token CAS with an
/// identical payload succeeds and advances the version within the
/// same incarnation (the idempotency convergence shape), and the
/// streams S-suite's append/cursor CAS paths still hold (covered by
/// the standing suite; this pins the keyspace-level semantics).
#[tokio::test]
async fn i6_same_incarnation_cas_semantics_preserved() {
    let ks = keyspace("i6");
    ks.create("cell", Bytes::from_static(b"stable"))
        .await
        .unwrap();
    let incarnation = ks.incarnation_for_test("cell").await.unwrap();
    let (payload, version, etag) = ks
        .get_with_version_for_test("cell")
        .await
        .unwrap()
        .unwrap_or_else(|| unreachable!("just created"));
    assert_eq!(
        (payload.as_ref(), incarnation, version),
        (b"stable".as_slice(), 0, 0)
    );

    // Current token + identical payload: succeeds, version 1, same
    // incarnation (A12's shape, now era-fenced).
    let new_etag = ks
        .compare_exchange("cell", &etag, Bytes::from_static(b"stable"))
        .await
        .unwrap();
    let incarnation_after = ks.incarnation_for_test("cell").await.unwrap();
    let (payload, version, _) = ks
        .get_with_version_for_test("cell")
        .await
        .unwrap()
        .unwrap_or_else(|| unreachable!("just CAS'd"));
    assert_eq!((payload.as_ref(), version), (b"stable".as_slice(), 1));
    assert_eq!(incarnation_after, 0, "CAS never moves the incarnation");
    assert_ne!(new_etag, etag, "each era's bytes are distinct");
}
