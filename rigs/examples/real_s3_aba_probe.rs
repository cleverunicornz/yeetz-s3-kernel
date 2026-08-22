//! Rig: real-backend kernel capability probe — ABA rulings plus the
//! streaming-value conditional-multipart design witness.
//! Run: EXO_S3_KEY/EXO_S3_SECRET/EXO_S3_BUCKET set, then
//! `cargo run -p yeetz-rigs --example real_s3_aba_probe`
//!
//! Measures what the loopback cannot establish on Exoscale SOS: etag
//! recurrence, conditional PUTs, LIST-after-write, conditional multipart
//! completion, incomplete-upload abort/listing, and part-addressed reads.
//! Prints every verdict row; integrity and cleanup surprises fail loudly.

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
