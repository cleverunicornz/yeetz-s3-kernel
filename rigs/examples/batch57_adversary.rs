//! Rig: kernel batches 5–7 adversary — teardown pass (2026-08-22).
//! Run: cargo run -p yeetz-rigs --example batch57_adversary
//!
//! Certificate immutability (P1) and floor-walk robustness (P2) against
//! the public AtomicKeyspace surface. See `rigs/src/batch57_adversary.rs`
//! for the promise statements.

#[tokio::main]
async fn main() {
    let verdicts = match yeetz_rigs::batch57_adversary::run().await {
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
