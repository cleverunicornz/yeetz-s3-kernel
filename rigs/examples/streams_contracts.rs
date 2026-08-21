//! Rig: streams crate core contracts — yeetz-s3-streams PR (ADR 0017).
//! Run: cargo run -p yeetz-rigs --example streams_contracts
//!
//! Re-fires the S-suite's durable promises in one process:
//! one-winner-per-seq under concurrent appends (S1), dense replay
//! with LIST-qualified completeness (S2), idempotent re-append (S3),
//! damage loudness with named seqs (S4), and cursor monotonicity.

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
