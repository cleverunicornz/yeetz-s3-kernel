//! R-suite (batch 5): certified trim and retention on the
//! AtomicKeyspace — the logical boundary (immutable create-once
//! certificates, monotone), the GC sweeper's contract (below-floor
//! only, idempotent, resumable), and the anti-resurrection guarantee
//! the versioned values carry. The design is ADR 0002's trim
//! addendum; Fugu's no-fencing blocker is resolved by batch 4.

use bytes::Bytes;
use yeetz_s3_kernel::{AtomicKeyspace, DeleteBelowReport, KernelHandle, KeyspaceError, TrimState};

fn keyspace(name: &str) -> AtomicKeyspace {
    KernelHandle::with_in_memory_store(name)
        .atomic_keyspace("trim/v1")
        .unwrap()
}

/// A seq-keyed data object under the shared data prefix.
fn data_key(seq: u64) -> String {
    format!("data/{seq:020}")
}

/// R1: trim certificates are monotone — a first proposal certifies,
/// an equal one is idempotent, a LOWER one is rejected by the
/// existing certificate ([`KeyspaceError::TrimNotMonotone`], never by
/// object absence), a higher one advances. Scopes are independent
/// sub-trees; the effective floor is always max-by-key.
#[tokio::test]
async fn r1_trim_certificate_is_monotone() {
    let ks = keyspace("r1");

    let first = ks.propose_trim("", 10).await.unwrap();
    assert_eq!(
        first,
        TrimState {
            first_retained: 10,
            advanced: true
        }
    );
    assert_eq!(ks.trim_floor("").await.unwrap(), Some(10));

    // Idempotent at the same floor.
    let again = ks.propose_trim("", 10).await.unwrap();
    assert_eq!(
        again,
        TrimState {
            first_retained: 10,
            advanced: false
        }
    );

    // Lower is rejected by the certificate.
    let rejected = ks.propose_trim("", 5).await.unwrap_err();
    assert!(matches!(
        rejected,
        KeyspaceError::TrimNotMonotone {
            requested: 5,
            certified: 10
        }
    ));

    // Higher advances; max-by-key is the effective floor.
    let higher = ks.propose_trim("", 20).await.unwrap();
    assert_eq!(
        higher,
        TrimState {
            first_retained: 20,
            advanced: true
        }
    );
    assert_eq!(ks.trim_floor("").await.unwrap(), Some(20));

    // A different scope certifies independently.
    let scoped = ks.propose_trim("stream-a", 3).await.unwrap();
    assert_eq!(scoped.first_retained, 3);
    assert_eq!(ks.trim_floor("stream-a").await.unwrap(), Some(3));
    assert_eq!(ks.trim_floor("").await.unwrap(), Some(20));
    assert_eq!(ks.trim_floor("stream-b").await.unwrap(), None);
}

/// R3: the sweeper deletes only strictly below the certified floor —
/// never the genesis position (seq 0), never at or above the
/// boundary — and only under a covering certificate
/// ([`KeyspaceError::TrimNotCertified`]).
#[tokio::test]
async fn r3_gc_deletes_only_below_floor_never_at_or_above() {
    let ks = keyspace("r3");
    for seq in 0..=9u64 {
        ks.create(&data_key(seq), Bytes::from_static(b"payload"))
            .await
            .unwrap();
    }
    // A sibling non-seq key sorts after the data range and must
    // survive every sweep.
    ks.create("side/cur", Bytes::from_static(b"keep"))
        .await
        .unwrap();
    ks.propose_trim("", 6).await.unwrap();

    // An uncertified bound is refused before any delete.
    let uncertified = ks.delete_below("", "data/", 7).await.unwrap_err();
    assert!(matches!(
        uncertified,
        KeyspaceError::TrimNotCertified { requested: 7, .. }
    ));

    let report = ks.delete_below("", "data/", 6).await.unwrap();
    assert_eq!(
        report,
        DeleteBelowReport {
            examined: 5,
            deleted: 5,
            remaining: 0
        }
    );
    for seq in 0..=9u64 {
        let present = ks.get(&data_key(seq)).await.unwrap().is_some();
        // Seq 0 is the genesis position: immortal. At/above the floor:
        // never collected.
        assert_eq!(present, seq == 0 || seq >= 6, "seq {seq}");
    }
    assert!(ks.get("side/cur").await.unwrap().is_some());
}

/// R4: GC is idempotent — a re-run examines nothing, deletes
/// nothing, changes nothing.
#[tokio::test]
async fn r4_gc_is_idempotent_rerun_no_changes() {
    let ks = keyspace("r4");
    for seq in 0..=9u64 {
        ks.create(&data_key(seq), Bytes::from_static(b"payload"))
            .await
            .unwrap();
    }
    ks.propose_trim("", 6).await.unwrap();
    let first = ks.delete_below("", "data/", 6).await.unwrap();
    assert_eq!(first.deleted, 5);

    let rerun = ks.delete_below("", "data/", 6).await.unwrap();
    assert_eq!(
        rerun,
        DeleteBelowReport {
            examined: 0,
            deleted: 0,
            remaining: 0
        }
    );
    assert_eq!(ks.trim_floor("").await.unwrap(), Some(6));
    for seq in 0..=9u64 {
        let present = ks.get(&data_key(seq)).await.unwrap().is_some();
        assert_eq!(present, seq == 0 || seq >= 6, "seq {seq}");
    }
}

/// R5: an interrupted sweep is resumable. A crash mid-sweep (some
/// below-floor deletes applied, response lost) leaves extra objects —
/// safe by contract; the re-run converges over exactly the remainder
/// without touching the boundary or the genesis.
#[tokio::test]
async fn r5_gc_interrupted_mid_sweep_is_resumable() {
    let ks = keyspace("r5");
    for seq in 0..=19u64 {
        ks.create(&data_key(seq), Bytes::from_static(b"payload"))
            .await
            .unwrap();
    }
    ks.propose_trim("", 15).await.unwrap();

    // Simulate a crash after a partial sweep: seqs 3..=8 already gone.
    for seq in 3..=8u64 {
        ks.delete(&data_key(seq)).await.unwrap();
    }

    // The re-run sweeps exactly the remainder below the floor
    // (1, 2, 9..=14 — eight keys) and converges.
    let report = ks.delete_below("", "data/", 15).await.unwrap();
    assert_eq!(
        report,
        DeleteBelowReport {
            examined: 8,
            deleted: 8,
            remaining: 0
        }
    );
    for seq in 0..=19u64 {
        let present = ks.get(&data_key(seq)).await.unwrap().is_some();
        assert_eq!(present, seq == 0 || seq >= 15, "seq {seq}");
    }
}

/// R7 (keyspace leg): versioned values prevent trim-resurrection. A
/// stale writer CAN recreate a below-floor key (a fresh version-0
/// lifetime — batch 4's documented cross-deletion boundary), but the
/// CERTIFICATE still rules: a lower floor is rejected while the
/// resurrected object exists (rejected by the certificate, not by
/// object absence), and the next sweep re-collects the zombie.
#[tokio::test]
async fn r7_resurrection_rejected_by_certificate_not_absence() {
    let ks = keyspace("r7");
    for seq in 0..=9u64 {
        ks.create(&data_key(seq), Bytes::from_static(b"payload"))
            .await
            .unwrap();
    }
    ks.propose_trim("", 6).await.unwrap();
    ks.delete_below("", "data/", 6).await.unwrap();

    // The stale writer resurrects seq 3 — put-if-absent accepts the
    // fresh lifetime; nothing about object absence protects the floor.
    ks.create(&data_key(3), Bytes::from_static(b"zombie"))
        .await
        .unwrap();

    // The certificate rejects the lower floor even though seq 3 now
    // EXISTS again.
    let rejected = ks.propose_trim("", 4).await.unwrap_err();
    assert!(matches!(
        rejected,
        KeyspaceError::TrimNotMonotone {
            requested: 4,
            certified: 6
        }
    ));

    // The sweeper re-collects the resurrected object: idempotent
    // convergence, boundary untouched.
    let report = ks.delete_below("", "data/", 6).await.unwrap();
    assert_eq!(report.deleted, 1);
    assert!(ks.get(&data_key(3)).await.unwrap().is_none());
    for seq in (6..=9u64).chain(0..=0) {
        assert!(ks.get(&data_key(seq)).await.unwrap().is_some(), "seq {seq}");
    }
}

/// R8 (teardown finding T1, 2026-08-22): trim certificates are
/// structurally immutable. ADR 0002's batch-5 addendum calls the
/// certificate "an immutable, create-once object" — the `trims` path
/// segment is now reserved exactly like `tombstones/` and
/// `incarnations/`: direct create, compare-and-swap, delete, and bulk
/// delete are refused, at any scope depth. A deletable maximum
/// certificate is a regressable floor, and a regressable floor
/// reopens the resurrection attack (retired history reads `Absent`,
/// lower trims are accepted) that the certificate exists to prevent.
#[tokio::test]
async fn r8_trim_certificates_are_immutable() {
    let ks = keyspace("r8");
    for seq in 0..=9u64 {
        ks.create(&data_key(seq), Bytes::from_static(b"payload"))
            .await
            .unwrap();
    }
    ks.propose_trim("", 6).await.unwrap();
    let certificate = "trims/00000000000000000006";

    assert!(matches!(
        ks.create(certificate, Bytes::from_static(b"forged"))
            .await
            .unwrap_err(),
        KeyspaceError::TrimCertificateImmutable(_)
    ));
    let (_, etag) = ks.get_with_etag(certificate).await.unwrap().unwrap();
    assert!(matches!(
        ks.compare_exchange(certificate, &etag, Bytes::from_static(b"x"))
            .await
            .unwrap_err(),
        KeyspaceError::TrimCertificateImmutable(_)
    ));
    assert!(matches!(
        ks.delete(certificate).await.unwrap_err(),
        KeyspaceError::TrimCertificateImmutable(_)
    ));
    assert!(matches!(
        ks.delete_many(&[certificate]).await.unwrap_err(),
        KeyspaceError::TrimCertificateImmutable(_)
    ));

    // The certificate stands: the floor and its monotonicity rule.
    assert_eq!(ks.trim_floor("").await.unwrap(), Some(6));
    assert!(matches!(
        ks.propose_trim("", 3).await.unwrap_err(),
        KeyspaceError::TrimNotMonotone {
            requested: 3,
            certified: 6
        }
    ));

    // The certified sweep still works through its own raw path.
    let report = ks.delete_below("", "data/", 6).await.unwrap();
    assert_eq!(report.deleted, 5);

    // Scoped certificate shapes are guarded at any depth.
    ks.propose_trim("s1", 2).await.unwrap();
    assert!(matches!(
        ks.delete("s1/trims/00000000000000000002")
            .await
            .unwrap_err(),
        KeyspaceError::TrimCertificateImmutable(_)
    ));

    // An ordinary key whose FINAL segment is "trims" is not a
    // certificate and stays writable.
    ks.create("notes/trims", Bytes::from_static(b"ordinary"))
        .await
        .unwrap();
    ks.delete("notes/trims").await.unwrap();
}

/// R9 (teardown finding T2, 2026-08-22): the floor walk steps over
/// keys sorting in the sibling window between its start sentinel
/// (`{scope}/trims`) and the certificate range (`{scope}/trims/`).
/// The first such key used to terminate the walk and hide the
/// certified floor entirely — `OffsetExpired` boundaries lost, GC
/// refused — while the certificates stood.
#[tokio::test]
async fn r9_floor_walk_steps_over_prefix_window_siblings() {
    let ks = keyspace("r9");
    ks.propose_trim("", 10).await.unwrap();
    assert_eq!(ks.trim_floor("").await.unwrap(), Some(10));

    // `trims-x` and `trims.y` are valid keys sorting strictly after
    // the sentinel "trims" and strictly before every "trims/" key.
    ks.create("trims-x", Bytes::from_static(b"sibling"))
        .await
        .unwrap();
    ks.create("trims.y", Bytes::from_static(b"sibling"))
        .await
        .unwrap();
    assert_eq!(
        ks.trim_floor("").await.unwrap(),
        Some(10),
        "the root floor survives prefix-window siblings"
    );

    // The scoped (per-stream) shape, both sibling spellings.
    ks.propose_trim("s1", 7).await.unwrap();
    ks.create("s1/trims-x", Bytes::from_static(b"sibling"))
        .await
        .unwrap();
    ks.create("s1/trims.z", Bytes::from_static(b"sibling"))
        .await
        .unwrap();
    assert_eq!(
        ks.trim_floor("s1").await.unwrap(),
        Some(7),
        "the scoped floor survives prefix-window siblings"
    );

    // The walk still terminates and still tracks a higher floor.
    ks.propose_trim("", 12).await.unwrap();
    assert_eq!(ks.trim_floor("").await.unwrap(), Some(12));
}
