//! yeetz-rigs — durable verification rigs for the S3 kernel closure.
//!
//! Every rig is a witness: executed evidence backing a claim. Rigs
//! live here as `examples/<name>.rs` (compiled and linted in CI, run
//! on demand — `cargo run -p yeetz-rigs --example <name>`) and are
//! mapped to their promises in `INDEX.md`. Each rig's oracle is its
//! named ADR contract; the real-backend probe additionally cites its
//! ci-dev run URL as the durable witness.

pub mod batch57_adversary;
pub mod real_s3_aba_probe;
pub mod streams_contracts;
