//! Rig: kernel batch 9 lineage-incarnation adversary.
//! Run: `cargo run -p yeetz-rigs --example batch9_adversary`
//! (also compiled and executed as rig tests by the workspace suite).
//!
//! This public-surface rig pins the promises that remain observable
//! without access to the kernel's private wire layout: sequential
//! cross-destroy fencing, monotone incarnation convergence under
//! concurrent first destroys, and terminal-read/taxonomy behavior.
//! The private-request interleavings live beside the implementation as
//! L8-L13 in `state_kernel::gateway_state_contract`; those canaries own
//! the eviction wire, post-landing fault cut, destroy wire, exact dual
//! decode, counter race, and GET-count evidence.

use yeetz_s3_kernel::state_kernel::{
    CanonicalRecord, HeadRead, KernelError, KernelLineage, SuccessorPolicy,
};
use yeetz_s3_kernel::{KernelHandle, LineageHeadState};

fn genesis(lineage: &KernelLineage, payload: &[u8]) -> CanonicalRecord {
    CanonicalRecord::new(
        lineage,
        0,
        None,
        "batch9-rig.create",
        "batch9-rig.v1",
        payload.to_vec(),
        "batch9-rig-genesis",
        "batch9-rig",
        "batch9-teardown",
    )
    .expect("valid genesis")
}

fn successor(lineage: &KernelLineage, head: &HeadRead, payload: &[u8]) -> CanonicalRecord {
    CanonicalRecord::new(
        lineage,
        head.generation() + 1,
        Some(head.record_position()),
        "batch9-rig.update",
        "batch9-rig.v1",
        payload.to_vec(),
        format!("batch9-rig-successor-{}", head.generation() + 1),
        "batch9-rig",
        "batch9-teardown",
    )
    .expect("valid successor")
}

/// The sequential headline: a byte-identical rebirth moves the era,
/// the destroyed-era token refuses, and the fresh token advances.
pub async fn p1_sequential_cross_destroy_fence() -> Result<String, String> {
    let handle = KernelHandle::with_in_memory_store("batch9-rig-p1");
    let lineage = KernelLineage::new("batch9-rig/p1", SuccessorPolicy::SuccessorCapable).unwrap();
    let kernel = handle.state_kernel(lineage.clone());
    let record = genesis(&lineage, b"byte-identical genesis");
    let era1 = kernel.append_genesis(&record).await.map_err(debug)?;
    kernel
        .destroy("p1-destroy", "batch9-rig")
        .await
        .map_err(debug)?;
    let era2 = kernel.append_genesis(&record).await.map_err(debug)?;
    if era2.incarnation() <= era1.incarnation() {
        return Err(format!(
            "P1 BROKEN: incarnation did not move ({} -> {})",
            era1.incarnation(),
            era2.incarnation()
        ));
    }
    match kernel
        .append_successor(&successor(&lineage, &era1, b"stale"), &era1)
        .await
    {
        Err(KernelError::LineageHeadConflict { .. }) => {}
        other => return Err(format!("P1 BROKEN: destroyed-era token result {other:?}")),
    }
    kernel
        .append_successor(&successor(&lineage, &era2, b"fresh"), &era2)
        .await
        .map_err(debug)?;
    Ok(format!(
        "P1 HOLDS sequentially: byte-identical rebirth moved incarnation {} -> {}",
        era1.incarnation(),
        era2.incarnation()
    ))
}

/// Concurrent destroy callers may create sanctioned gaps, but cannot
/// regress or wedge the counter; the next head carries the converged
/// era and a later completed destroy moves it again.
pub async fn p2_counter_converges_monotonically() -> Result<String, String> {
    let handle = KernelHandle::with_in_memory_store("batch9-rig-p2");
    let lineage = KernelLineage::new("batch9-rig/p2", SuccessorPolicy::SuccessorCapable).unwrap();
    let record = genesis(&lineage, b"counter-race");
    handle
        .state_kernel(lineage.clone())
        .append_genesis(&record)
        .await
        .map_err(debug)?;

    let mut destroys = Vec::new();
    for actor in 0..6 {
        let handle = handle.clone();
        let lineage = lineage.clone();
        destroys.push(tokio::spawn(async move {
            handle
                .state_kernel(lineage)
                .destroy("concurrent-first-destroy", &format!("actor-{actor}"))
                .await
        }));
    }
    for destroy in destroys {
        destroy
            .await
            .map_err(|error| format!("destroy task: {error}"))?
            .map_err(debug)?;
    }

    let kernel = handle.state_kernel(lineage.clone());
    if !matches!(
        kernel.read_head_state().await.map_err(debug)?,
        LineageHeadState::Destroyed(_)
    ) {
        return Err("P2 BROKEN: concurrent destroys left a live head".to_string());
    }
    let after_race = kernel.append_genesis(&record).await.map_err(debug)?;
    if after_race.incarnation() == 0 {
        return Err("P2 BROKEN: recreated head regressed to incarnation 0".to_string());
    }
    kernel
        .destroy("second-completed-destroy", "batch9-rig")
        .await
        .map_err(debug)?;
    let after_second = kernel.append_genesis(&record).await.map_err(debug)?;
    if after_second.incarnation() <= after_race.incarnation() {
        return Err(format!(
            "P2 BROKEN: counter regressed or wedged ({} -> {})",
            after_race.incarnation(),
            after_second.incarnation()
        ));
    }
    Ok(format!(
        "P2 HOLDS: concurrent era {} advanced to {} after the next destroy",
        after_race.incarnation(),
        after_second.incarnation()
    ))
}

/// Public taxonomy remains head-driven and terminal payloads remain
/// invariant across a byte-identical rebirth.
pub async fn p3_terminal_and_taxonomy_invariant() -> Result<String, String> {
    let handle = KernelHandle::with_in_memory_store("batch9-rig-p3");
    let lineage = KernelLineage::new("batch9-rig/p3", SuccessorPolicy::SuccessorCapable).unwrap();
    let kernel = handle.state_kernel(lineage.clone());
    if !kernel.read_head_state().await.map_err(debug)?.is_absent() {
        return Err("P3 BROKEN: never-created lineage is not Absent".to_string());
    }
    let record = genesis(&lineage, b"same terminal payload");
    kernel.append_genesis(&record).await.map_err(debug)?;
    let before = kernel.read_terminal_record().await.map_err(debug)?;
    kernel
        .destroy("p3-destroy", "batch9-rig")
        .await
        .map_err(debug)?;
    if !matches!(
        kernel.read_head_state().await.map_err(debug)?,
        LineageHeadState::Destroyed(_)
    ) {
        return Err("P3 BROKEN: completed destroy is not Destroyed".to_string());
    }
    kernel.append_genesis(&record).await.map_err(debug)?;
    let after = kernel.read_terminal_record().await.map_err(debug)?;
    if before.payload() != after.payload()
        || before.digest() != after.digest()
        || before.generation() != after.generation()
    {
        return Err("P3 BROKEN: terminal read changed across identical rebirth".to_string());
    }
    Ok("P3 HOLDS: Absent -> Present -> Destroyed -> Present; terminal unchanged".to_string())
}

fn debug(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

pub async fn run() -> Result<Vec<String>, String> {
    Ok(vec![
        p1_sequential_cross_destroy_fence().await?,
        p2_counter_converges_monotonically().await?,
        p3_terminal_and_taxonomy_invariant().await?,
    ])
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn rig_p1_sequential_cross_destroy_fence() {
        super::p1_sequential_cross_destroy_fence()
            .await
            .expect("P1");
    }

    #[tokio::test]
    async fn rig_p2_counter_converges_monotonically() {
        super::p2_counter_converges_monotonically()
            .await
            .expect("P2");
    }

    #[tokio::test]
    async fn rig_p3_terminal_and_taxonomy_invariant() {
        super::p3_terminal_and_taxonomy_invariant()
            .await
            .expect("P3");
    }
}
