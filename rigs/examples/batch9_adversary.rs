//! Rig: kernel batch 9 lineage-incarnation adversary.
//! Run: `cargo run -p yeetz-rigs --example batch9_adversary`

#[tokio::main]
async fn main() {
    let verdicts = match yeetz_rigs::batch9_adversary::run().await {
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
