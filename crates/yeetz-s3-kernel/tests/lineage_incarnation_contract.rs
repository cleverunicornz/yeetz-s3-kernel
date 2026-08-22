//! L-suite (batch 9): lineage-head incarnation — the head path joins
//! the era model batches 4-7 built for the keyspace. A destroy/
//! recreate cycle (even with byte-identical genesis bytes) moves the
//! head's incarnation, so a stale era-1 `HeadRead` can never CAS a
//! reborn lineage on any backend — including content-etag backends
//! where identical bytes recur identical etags. The design is ADR
//! 0001's batch-9 addendum. L4 (crash windows), L6 (v1 head compat)
//! and L7 (the deterministic parked-writer canary) live in-src in
//! `state_kernel::gateway_state_contract` — they need the loopback
//! counterpart or the private wire shapes.

use yeetz_s3_kernel::state_kernel::{CanonicalRecord, KernelError, KernelLineage, SuccessorPolicy};
use yeetz_s3_kernel::{KernelHandle, LineageHeadState};

fn kernel(name: &str) -> (yeetz_s3_kernel::state_kernel::StateKernel, KernelLineage) {
    let handle = KernelHandle::with_in_memory_store(format!("l-suite-{name}"));
    let lineage = KernelLineage::new(format!("l9/{name}"), SuccessorPolicy::SuccessorCapable)
        .expect("lineage");
    let kernel = handle.state_kernel(lineage.clone());
    (kernel, lineage)
}

fn genesis(lineage: &KernelLineage, payload: &[u8]) -> CanonicalRecord {
    CanonicalRecord::new(
        lineage,
        0,
        None,
        "l9.create",
        "l9.v1",
        payload.to_vec(),
        "l9-operation",
        "l9-actor",
        "l9-cause",
    )
    .expect("genesis record")
}

fn successor(
    lineage: &KernelLineage,
    head: &yeetz_s3_kernel::state_kernel::HeadRead,
) -> CanonicalRecord {
    CanonicalRecord::new(
        lineage,
        head.generation() + 1,
        Some(head.record_position()),
        "l9.update",
        "l9.v1",
        vec![head.generation() as u8 + 1; 32],
        format!("l9-operation-{}", head.generation() + 1),
        "l9-actor",
        "l9-cause",
    )
    .expect("successor record")
}

/// L1: the headline hazard — destroy, recreate with a byte-identical
/// genesis, then append a successor with the era-1 `HeadRead`: the
/// token is refused (`LineageHeadConflict`), the reborn head is
/// untouched, and the same append with the fresh era-2 token
/// succeeds.
#[tokio::test]
async fn l1_stale_head_token_across_destroy_recreate_is_rejected() {
    let (kernel, lineage) = kernel("l1");
    let payload = b"identical genesis bytes in both eras";
    let era1 = kernel
        .append_genesis(&genesis(&lineage, payload))
        .await
        .expect("era-1 genesis");
    assert_eq!(era1.incarnation(), 0);

    kernel.destroy("rebuild", "l1").await.expect("destroy");

    let era2 = kernel
        .append_genesis(&genesis(&lineage, payload))
        .await
        .expect("era-2 genesis — byte-identical payload");
    assert_eq!(
        era2.incarnation(),
        1,
        "the reborn head must carry a fresh era stamp"
    );
    assert_eq!(era2.generation(), 0);

    // Era-1 token: refused, head untouched.
    match kernel
        .append_successor(&successor(&lineage, &era1), &era1)
        .await
    {
        Err(KernelError::LineageHeadConflict { .. }) => {}
        other => panic!("era-1 token across destroy must conflict, got {other:?}"),
    }
    let after = kernel.read_head().await.expect("head after refusal");
    assert_eq!(after.incarnation(), 1);
    assert_eq!(after.generation(), 0, "the refused append changed nothing");
    assert_eq!(
        after.record_digest(),
        era2.record_digest(),
        "the reborn head still names the era-2 genesis"
    );

    // Era-2 token: accepted within the new lifetime.
    let advanced = kernel
        .append_successor(&successor(&lineage, &era2), &era2)
        .await
        .expect("era-2 append");
    assert_eq!(advanced.generation(), 1);
    assert_eq!(advanced.incarnation(), 1);
}

/// L2: the non-destroy surface is unchanged — genesis and successor
/// CAS within one lifetime behave exactly as before (K1-K7
/// semantics), with the incarnation constant across the lifetime.
#[tokio::test]
async fn l2_one_lifetime_writes_are_unchanged() {
    let (kernel, lineage) = kernel("l2");
    let mut head = kernel
        .append_genesis(&genesis(&lineage, b"genesis"))
        .await
        .expect("genesis");
    for expected_generation in 1..=3 {
        head = kernel
            .append_successor(&successor(&lineage, &head), &head)
            .await
            .expect("successor");
        assert_eq!(head.generation(), expected_generation);
        assert_eq!(head.incarnation(), 0);
    }

    // A stale WITHIN-lifetime token still conflicts (the standing
    // CAS contract), and the etag carried on is the fresh one.
    let stale = kernel.read_head().await.unwrap();
    let fresh = kernel
        .append_successor(&successor(&lineage, &stale), &stale)
        .await
        .unwrap();
    match kernel
        .append_successor(&successor(&lineage, &stale), &stale)
        .await
    {
        Err(KernelError::LineageHeadConflict { .. }) => {}
        other => panic!("stale within-lifetime token must conflict, got {other:?}"),
    }
    assert_eq!(fresh.generation(), 4);
    assert_eq!(kernel.read_head().await.unwrap().generation(), 4);
}

/// L3: the tombstone keeps batch-6 semantics and now records the
/// closed era: witness written before the head delete, the FIRST
/// witness stands across later lifetimes, and the counter keeps
/// counting (gaps sanctioned, decreases impossible).
#[tokio::test]
async fn l3_tombstone_records_the_closed_era_and_first_witness_stands() {
    let (kernel, lineage) = kernel("l3");
    // Absent before anything exists — no fabricated witness.
    assert!(kernel.read_head_state().await.unwrap().is_absent());

    let era1 = kernel
        .append_genesis(&genesis(&lineage, b"era-one"))
        .await
        .unwrap();
    kernel.destroy("first", "l3").await.unwrap();

    let LineageHeadState::Destroyed(tombstone) = kernel.read_head_state().await.unwrap() else {
        panic!("destroyed lineage must read Destroyed");
    };
    assert_eq!(tombstone.incarnation, 0, "witness names era 0");
    assert_eq!(tombstone.deleted_at_gen, era1.generation());

    // Second lifetime: fresh genesis, advance, destroy again — the
    // first witness stands (immutable history), but the counter
    // moved twice: the third lifetime is stamped 2.
    kernel
        .append_genesis(&genesis(&lineage, b"era-two"))
        .await
        .unwrap();
    kernel.destroy("second", "l3").await.unwrap();
    let LineageHeadState::Destroyed(second) = kernel.read_head_state().await.unwrap() else {
        panic!("second destroy must also read Destroyed");
    };
    assert_eq!(
        second.incarnation, 0,
        "the first tombstone stands across lifetimes"
    );

    let era3 = kernel
        .append_genesis(&genesis(&lineage, b"era-three"))
        .await
        .unwrap();
    assert_eq!(
        era3.incarnation(),
        2,
        "two completed destroys — the third lifetime is era 2"
    );
}

/// L5: terminal reads and the existence taxonomy are invariant
/// across the destroy boundary — the same O(1) read shape, and a
/// reborn lineage with byte-identical history presents the identical
/// terminal payload/digest a reader saw before the destroy.
#[tokio::test]
async fn l5_terminal_reads_and_taxonomy_invariant_across_eras() {
    let (kernel, lineage) = kernel("l5");
    let payload = b"same terminal in both eras";

    // Absent → Present.
    assert!(kernel.read_head_state().await.unwrap().is_absent());
    let era1 = kernel
        .append_genesis(&genesis(&lineage, payload))
        .await
        .unwrap();
    let terminal1 = kernel.read_terminal_record().await.unwrap();
    assert_eq!(terminal1.payload(), payload);
    assert_eq!(terminal1.digest(), era1.record_digest());

    // Present → Destroyed → Present.
    kernel.destroy("rebuild", "l5").await.unwrap();
    assert!(matches!(
        kernel.read_head_state().await.unwrap(),
        LineageHeadState::Destroyed(_)
    ));
    // Terminal read on a destroyed lineage is still an integrity
    // error, never absence (law 7).
    match kernel.read_terminal_record().await {
        Err(KernelError::StateHistoryIncomplete { .. }) => {}
        other => panic!("terminal read on destroyed lineage, got {other:?}"),
    }

    kernel
        .append_genesis(&genesis(&lineage, payload))
        .await
        .unwrap();
    let LineageHeadState::Present(reborn) = kernel.read_head_state().await.unwrap() else {
        panic!("reborn lineage must read Present");
    };
    assert_eq!(reborn.incarnation(), 1);
    let terminal2 = kernel.read_terminal_record().await.unwrap();
    assert_eq!(
        terminal2.payload(),
        terminal1.payload(),
        "byte-identical genesis — identical terminal payload"
    );
    assert_eq!(
        terminal2.digest(),
        terminal1.digest(),
        "identical history digests across eras"
    );
    assert_eq!(terminal2.generation(), terminal1.generation());
}
