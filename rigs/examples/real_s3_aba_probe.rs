//! Rig: real-backend ABA probe — ruling #3 (ADR 0016/0017 addendum).
//! Run: EXO_S3_KEY/EXO_S3_SECRET/EXO_S3_BUCKET set, then
//! `cargo run -p yeetz-rigs --example real_s3_aba_probe`
//!
//! Measures what the loopback cannot model on a real S3 backend
//! (Exoscale SOS): etag recurrence for identical content, conditional
//! PUT semantics (create race, CAS, the ABA case), LIST-after-write
//! visibility. Prints the verdict table; fails loudly on surprise.

#[tokio::main]
async fn main() {
    let verdicts = match yeetz_rigs::real_s3_aba_probe::run().await {
        Ok(verdicts) => verdicts,
        Err(failure) => {
            eprintln!("FAIL: {failure}");
            std::process::exit(1);
        }
    };
    // Verdict rows stream as they are measured; this is the summary.
    println!("PASS: {} verdict rows", verdicts.len());
    println!("real-s3 ABA probe: battery complete");
}
