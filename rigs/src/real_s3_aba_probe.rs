//! Rig entry point for the kernel-owned real-backend ABA probe.

use yeetz_s3_kernel::{S3Config, run_real_s3_aba_probe};

/// Exoscale SOS zone endpoint (ch-gva-2).
const ENDPOINT: &str = "https://sos-ch-gva-2.exo.io";
const REGION: &str = "ch-gva-2";

pub async fn run() -> Result<Vec<String>, String> {
    let key = env("EXO_S3_KEY")?;
    let secret = env("EXO_S3_SECRET")?;
    let bucket = env("EXO_S3_BUCKET")?;
    let config = S3Config::custom(&bucket, REGION, ENDPOINT, &key, &secret);

    run_real_s3_aba_probe(&config).await
}

fn env(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| {
        format!("env {name} not set — the probe needs EXO_S3_KEY, EXO_S3_SECRET, EXO_S3_BUCKET")
    })
}
