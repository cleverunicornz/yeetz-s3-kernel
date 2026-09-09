//! Rig: streams crate core contracts — yeetz-s3-streams PR (ADR 0017).
//! Run: cargo run -p yeetz-rigs --example streams_contracts
//!
//! Re-fires the durable promises in one process over the in-memory
//! kernel: one-winner-per-seq under concurrent appends (S1), dense
//! replay with LIST-qualified completeness (S2), idempotent re-append
//! (S3), damage loudness with named seqs (S4), cursor monotonicity —
//! plus the conditional-write surface: caller-owned stream creation
//! (Created/Existing convergence, typed ConfigurationConflict with the
//! incumbent intact), predecessor-expected append (exact successor,
//! exact retry before and after suffix advance, same-id and position
//! conflicts that never move to another slot, verified identity
//! agreeing with the receipt), and a paginated fixed strict window
//! that stays complete and byte-identical after suffix growth.

#[tokio::main]
async fn main() {
    let verdicts = match yeetz_rigs::streams_contracts::run().await {
        Ok(verdicts) => verdicts,
        Err(failure) => {
            eprintln!("FAIL: {failure}");
            std::process::exit(1);
        }
    };
    for verdict in verdicts {
        println!("PASS: {verdict}");
    }
}
