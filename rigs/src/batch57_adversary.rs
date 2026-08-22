//! Rig: kernel batches 5–7 adversary — teardown pass (2026-08-22).
//! Run: `cargo run -p yeetz-rigs --example batch57_adversary`
//! (also compiled and executed as rig tests by the workspace suite).
//!
//! Promises under attack (ADR 0002 §"Ruled addendum: Batch 5",
//! ADR 0001 §"Ruled addendum: Batch 5" as cited by batches 6–7):
//!
//! - **P1 — certificates are immutable.** "An immutable, create-once
//!   object at `{scope}/trims/{first_retained:020}` … the certificate,
//!   never object absence, is the boundary." If any public keyspace
//!   API can remove a certificate, the certified floor regresses and
//!   the anti-resurrection boundary becomes object absence again.
//! - **P2 — `trim_floor` is max-by-key over the scope's certificates.**
//!   The floor walk must not be terminable by namespace keys that
//!   merely sort near the certificate prefix (`{scope}/trims-{…}`,
//!   `{scope}/trims.{…}` sit strictly between the walk's start
//!   sentinel and the certificate range).
//!
//! Each leg FAILS (Err / failing test) while its defect stands and
//! turns green under the corresponding fix PR.

use bytes::Bytes;
use yeetz_s3_kernel::{AtomicKeyspace, KernelHandle, KeyState};

fn keyspace(name: &str) -> AtomicKeyspace {
    KernelHandle::with_in_memory_store(name)
        .atomic_keyspace("batch57-adv/v1")
        .expect("valid adversary namespace")
}

fn data_key(seq: u64) -> String {
    format!("data/{seq:020}")
}

/// P1: trim certificates are immutable through every keyspace API.
/// The attack is a plain `delete` of the maximum certificate after a
/// certified sweep; the consequence chain is the resurrection attack
/// reopening (floor regression → lower re-trim accepted → retired
/// history reading as `Absent` instead of `OffsetExpired`).
pub async fn p1_trim_certificates_are_immutable() -> Result<String, String> {
    let ks = keyspace("p1");
    for seq in 0..=9u64 {
        ks.create(&data_key(seq), Bytes::from_static(b"payload"))
            .await
            .expect("seed seq key");
    }
    ks.propose_trim("", 6).await.expect("certify floor 6");
    ks.delete_below("", "data/", 6)
        .await
        .expect("certified sweep");
    let baseline = ks.read_state(&data_key(3)).await.expect("read below floor");
    assert_eq!(
        baseline,
        KeyState::OffsetExpired { first_retained: 6 },
        "baseline: the certificate rules below-floor reads"
    );

    // The attack: remove the maximum certificate through the public
    // delete path (and the bulk path). Both must refuse.
    let certificate = "trims/00000000000000000006";
    if ks.delete(certificate).await.is_err() {
        return Ok("P1 HOLDS: the certificate prefix refuses direct delete".to_string());
    }
    if ks.delete_many(&[certificate]).await.is_err() {
        return Ok("P1 HOLDS (bulk): the certificate prefix refuses delete_many".to_string());
    }

    // Defect chain, observed through the public surface.
    let floor_after = ks.trim_floor("").await.expect("floor read after delete");
    let resurrection = ks.propose_trim("", 3).await;
    let state_after = ks
        .read_state(&data_key(3))
        .await
        .expect("read below retired floor");
    Err(format!(
        "P1 BROKEN: a public delete removed the maximum trim certificate — \
         floor {floor_after:?}, lower re-trim {resurrection:?}, below-floor \
         read_state {state_after:?} (was OffsetExpired {{ first_retained: 6 }}). \
         The anti-resurrection boundary is the certificate, and the \
         certificate is deletable through the keyspace API"
    ))
}

/// P2: `trim_floor` returns the scope's maximum certificate regardless
/// of inert sibling keys sorting inside the (`{scope}/trims`,
/// `{scope}/trims/`) byte window the floor walk starts from.
pub async fn p2_trim_floor_ignores_prefix_window_siblings() -> Result<String, String> {
    let ks = keyspace("p2");

    // Root scope.
    ks.propose_trim("", 10).await.expect("certify root floor");
    assert_eq!(
        ks.trim_floor("").await.expect("root floor baseline"),
        Some(10),
        "baseline: floor is the maximum certificate"
    );
    // `trims-x` is a valid namespace key sorting strictly after the
    // walk sentinel "trims" and strictly before every "trims/" key.
    ks.create("trims-x", Bytes::from_static(b"inert sibling"))
        .await
        .expect("sibling key is a valid identifier");
    match ks.trim_floor("").await {
        Ok(Some(10)) => {}
        Ok(witness) => {
            return Err(format!(
                "P2 BROKEN (root scope): an inert sibling key hid the \
                 certified floor — trim_floor(\"\") = {witness:?} while the \
                 certificate for 10 stands"
            ));
        }
        Err(error) => {
            return Err(format!(
                "P2 BROKEN (root scope): trim_floor errored: {error}"
            ));
        }
    }

    // Scoped (per-stream shape): `{scope}/trims.x` — same window.
    ks.propose_trim("s1", 7)
        .await
        .expect("certify scoped floor");
    assert_eq!(
        ks.trim_floor("s1").await.expect("scoped floor baseline"),
        Some(7)
    );
    ks.create("s1/trims.x", Bytes::from_static(b"inert sibling"))
        .await
        .expect("scoped sibling key is a valid identifier");
    match ks.trim_floor("s1").await {
        Ok(Some(7)) => {}
        Ok(witness) => {
            return Err(format!(
                "P2 BROKEN (scope s1): an inert sibling key hid the \
                 certified floor — trim_floor(\"s1\") = {witness:?} while the \
                 certificate for 7 stands"
            ));
        }
        Err(error) => {
            return Err(format!("P2 BROKEN (scope s1): trim_floor errored: {error}"));
        }
    }
    Ok("P2 HOLDS: the floor walk steps over prefix-window siblings".to_string())
}

/// Fire every leg; collect PASS verdicts or fail with the first BROKEN
/// narrative.
pub async fn run() -> Result<Vec<String>, String> {
    let legs = [
        ("p1", p1_trim_certificates_are_immutable().await?),
        ("p2", p2_trim_floor_ignores_prefix_window_siblings().await?),
    ];
    Ok(legs
        .into_iter()
        .map(|(name, verdict)| format!("{name}: {verdict}"))
        .collect())
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn rig_p1_trim_certificates_are_immutable() {
        super::p1_trim_certificates_are_immutable()
            .await
            .expect("P1");
    }

    #[tokio::test]
    async fn rig_p2_trim_floor_ignores_prefix_window_siblings() {
        super::p2_trim_floor_ignores_prefix_window_siblings()
            .await
            .expect("P2");
    }
}
