#![allow(dead_code)]
//! Shared test support: in-memory wiring and a hand-rolled envelope
//! encoder for pre-seeding the log at arbitrary seqs.

//! Shared test support: helpers each integration-test binary links
//! separately; unused-per-binary entries are expected.

use base64::Engine as _;
use sha2::{Digest, Sha256};
use yeetz_s3_kernel::KernelHandle;
use yeetz_s3_streams::{StreamId, Streams};

/// Streams over an opaque in-memory kernel handle.
pub fn streams_on_in_memory_store() -> Streams {
    let kernel = KernelHandle::with_in_memory_store("streams-contract");
    Streams::new(&kernel).expect("streams on in-memory store")
}

pub fn streams_on_store(kernel: &KernelHandle) -> Streams {
    Streams::new(kernel).expect("streams on store")
}

/// Encode an envelope by hand — the wire format the tests depend on
/// (JSON, base64 payload, sha256 digest), independent of the crate's
/// own encoder.
pub fn hand_envelope(
    stream_id: &str,
    seq: u64,
    stable_event_id: &str,
    schema_id: &str,
    payload: &[u8],
) -> bytes::Bytes {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    let digest = hex::encode(hasher.finalize());
    let json = serde_json::json!({
        "format_version": 1,
        "stream_id": stream_id,
        "seq": seq,
        "stable_event_id": stable_event_id,
        "schema_id": schema_id,
        "payload_len": payload.len(),
        "payload_sha256": digest,
        "payload": base64::engine::general_purpose::STANDARD.encode(payload),
    });
    bytes::Bytes::from(serde_json::to_vec(&json).unwrap())
}

/// The keyspace the crate uses (for test-side seeding/damage).
pub fn streams_keyspace(kernel: &KernelHandle) -> yeetz_s3_kernel::atomic_keyspace::AtomicKeyspace {
    kernel.atomic_keyspace("streams/v1").unwrap()
}

#[allow(dead_code)]
pub fn stream_id(value: &str) -> StreamId {
    StreamId::new(value).unwrap()
}

pub mod loopback;
