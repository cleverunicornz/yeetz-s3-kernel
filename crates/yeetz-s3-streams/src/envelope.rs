//! The event envelope: `{format_version, stream_id, seq,
//! stable_event_id, schema_id, payload_len, payload_sha256, payload}`.
//! The digest lives in the envelope, never the key; key↔envelope
//! agreement (stream id and seq) is verified on every read, and a
//! decode or verification failure is an error, never a skip.

use crate::{SchemaId, StableEventId, StreamId, StreamsError};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const ENVELOPE_FORMAT_VERSION: u32 = 1;

/// The schema id of the immutable genesis/config record at seq 0.
pub const GENESIS_SCHEMA_ID: &str = "stream.genesis.v1";

#[derive(Debug, Serialize, Deserialize)]
struct WireEnvelope {
    format_version: u32,
    stream_id: String,
    seq: u64,
    stable_event_id: String,
    schema_id: String,
    payload_len: u64,
    payload_sha256: String,
    payload: String,
}

/// A verified envelope. Constructed only through
/// [`Envelope::encode`] (deterministic serialization — byte-identical
/// retries encode identically) or [`Envelope::decode_and_verify`].
#[derive(Debug, Clone)]
pub struct Envelope {
    pub format_version: u32,
    pub stream_id: StreamId,
    pub seq: u64,
    pub stable_event_id: StableEventId,
    pub schema_id: SchemaId,
    pub payload: bytes::Bytes,
    /// The canonical encoded object bytes.
    pub(crate) encoded: bytes::Bytes,
}

#[derive(Debug)]
pub(crate) struct EnvelopeMismatch;

impl Envelope {
    /// Deterministically encode an envelope (byte-identical inputs
    /// produce byte-identical objects — the idempotent-retry
    /// comparison relies on this).
    pub(crate) fn encode(
        stream: &StreamId,
        seq: u64,
        schema_id: &SchemaId,
        stable_event_id: &StableEventId,
        payload: &[u8],
    ) -> Result<Self, StreamsError> {
        let wire = WireEnvelope {
            format_version: ENVELOPE_FORMAT_VERSION,
            stream_id: stream.as_str().to_string(),
            seq,
            stable_event_id: stable_event_id.as_str().to_string(),
            schema_id: schema_id.as_str().to_string(),
            payload_len: payload.len() as u64,
            payload_sha256: sha256_hex(payload),
            payload: base64::engine::general_purpose::STANDARD.encode(payload),
        };
        Ok(Self {
            format_version: wire.format_version,
            stream_id: stream.clone(),
            seq,
            stable_event_id: stable_event_id.clone(),
            schema_id: schema_id.clone(),
            payload: bytes::Bytes::copy_from_slice(payload),
            encoded: bytes::Bytes::from(
                serde_json::to_vec(&wire)
                    .map_err(|_| StreamsError::InvalidArgument("envelope serialization".into()))?,
            ),
        })
    }

    /// The genesis/config envelope at seq 0.
    pub(crate) fn genesis(stream: &StreamId, config: &[u8]) -> Result<Self, StreamsError> {
        Self::encode(
            stream,
            0,
            &SchemaId::new(GENESIS_SCHEMA_ID).expect("static schema id"),
            &StableEventId::new("genesis").expect("static event id"),
            config,
        )
    }

    /// Digest of the encoded object — the `terminal_record_digest` a
    /// verified tail hint carries.
    pub(crate) fn digest_hex(&self) -> String {
        sha256_hex(&self.encoded)
    }

    /// Decode and fully verify: format version, key↔envelope
    /// agreement (stream id, seq), payload length, payload digest.
    pub(crate) fn decode_and_verify(
        expected_stream: &StreamId,
        expected_seq: u64,
        bytes: &[u8],
    ) -> Result<Self, EnvelopeMismatch> {
        let verify = || -> Result<Self, EnvelopeMismatch> {
            let wire: WireEnvelope = serde_json::from_slice(bytes).map_err(|_| EnvelopeMismatch)?;
            if wire.format_version != ENVELOPE_FORMAT_VERSION {
                return Err(EnvelopeMismatch);
            }
            if wire.stream_id != expected_stream.as_str() || wire.seq != expected_seq {
                return Err(EnvelopeMismatch); // key↔envelope disagreement
            }
            let payload = base64::engine::general_purpose::STANDARD
                .decode(&wire.payload)
                .map_err(|_| EnvelopeMismatch)?;
            if payload.len() as u64 != wire.payload_len {
                return Err(EnvelopeMismatch);
            }
            if sha256_hex(&payload) != wire.payload_sha256 {
                return Err(EnvelopeMismatch);
            }
            Ok(Self {
                format_version: wire.format_version,
                stream_id: StreamId::new(&wire.stream_id).map_err(|_| EnvelopeMismatch)?,
                seq: wire.seq,
                stable_event_id: StableEventId::new(&wire.stable_event_id)
                    .map_err(|_| EnvelopeMismatch)?,
                schema_id: SchemaId::new(&wire.schema_id).map_err(|_| EnvelopeMismatch)?,
                payload: bytes::Bytes::from(payload),
                encoded: bytes::Bytes::copy_from_slice(bytes),
            })
        };
        verify()
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}
