//! L-suite (batch 9): lineage-head incarnation — the head path joins
//! the era model batches 4-7 built for the keyspace. A destroy/
//! recreate cycle (even with byte-identical genesis bytes) moves the
//! head's incarnation, so a stale era-1 `HeadRead` can never CAS a
//! reborn lineage on any backend — including content-etag backends
//! where identical bytes recur identical etags. The design is ADR
//! 0001's batch-9 addendum. L4 (crash windows), L6 (v1 head compat),
//! L7 (the deterministic parked-writer canary) and — since the
//! lifecycle closure, which made `destroy`'s tail a conditional
//! delete — L1/L3/L5 and the L9/L10/L13/L14 wire canaries live
//! in-src in `state_kernel::gateway_state_contract` on the loopback
//! rig: destroy now requires the conditional-delete wire primitive,
//! and in-memory handles fail closed (the A18 posture).

use yeetz_s3_kernel::KernelHandle;
use yeetz_s3_kernel::state_kernel::{CanonicalRecord, KernelError, KernelLineage, SuccessorPolicy};

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
