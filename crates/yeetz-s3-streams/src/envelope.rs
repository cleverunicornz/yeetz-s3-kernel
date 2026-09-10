//! The event envelope: `{format_version, stream_id, seq,
//! stable_event_id, schema_id, payload_len, payload_sha256, payload}`.
//! The digest lives in the envelope, never the key; key↔envelope
//! agreement (stream id and seq) is verified on every read, and a
//! decode or verification failure is an error, never a skip.
//!
//! An [`Envelope`] is a verified value: only [`Envelope::encode`] and
//! [`Envelope::decode_and_verify`] construct one, every field is
//! private behind allocation-free getters, and the event's identity
//! is stored once as the [`EventRef`] served by
//! [`Envelope::event_ref`].

use crate::{SchemaId, Seq, StableEventId, StreamId, StreamsError};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const ENVELOPE_FORMAT_VERSION: u32 = 1;

/// The schema id of the immutable genesis/config record at seq 0.
pub const GENESIS_SCHEMA_ID: &str = "stream.genesis.v1";

/// The structural encoded-envelope bound (ADR 0004 §3.4, S11):
/// every envelope written by create, append, or migration — genesis
/// and config included — is at most 16 MiB after canonical
/// JSON/base64 encoding, independent of the kernel's `INLINE_MAX`
/// threshold, so streams stay inline and the chunk root is never
/// touched by a streams write.
pub const MAX_ENCODED_ENVELOPE_BYTES: usize = 16 * 1024 * 1024;

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

/// Forgeable identifying data for one landed event: the stream, seq,
/// stable event id, and payload digest that name a record for
/// revalidation. A reference is identifying data, not proof: anyone
/// can construct one, it carries no capability, and it certifies
/// neither the record's bytes nor any prefix of the log — only a
/// verified `Envelope` vouches for the event it names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRef {
    pub stream_id: StreamId,
    pub seq: Seq,
    pub stable_event_id: StableEventId,
    pub payload_sha256: String,
}

/// A verified envelope. Constructed only through
/// [`Envelope::encode`] (deterministic serialization — byte-identical
/// retries encode identically) or [`Envelope::decode_and_verify`],
/// and immutable afterwards: every field is private behind getters.
/// The identity (stream, seq, stable event id, payload digest) is
/// carried once as the [`EventRef`] returned by [`Envelope::event_ref`].
#[derive(Debug, Clone)]
pub struct Envelope {
    format_version: u32,
    identity: EventRef,
    schema_id: SchemaId,
    payload: bytes::Bytes,
    /// The canonical encoded object bytes.
    encoded: bytes::Bytes,
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
        let encoded = bytes::Bytes::from(
            serde_json::to_vec(&wire)
                .map_err(|_| StreamsError::InvalidArgument("envelope serialization".into()))?,
        );
        // The structural bound (ADR 0004 §3.4/S11): enforced after
        // canonical encoding, before the envelope exists as a value —
        // therefore before any keyspace effect. Oversize is typed.
        if encoded.len() > MAX_ENCODED_ENVELOPE_BYTES {
            return Err(StreamsError::EnvelopeTooLarge {
                encoded_len: encoded.len() as u64,
                max_encoded_len: MAX_ENCODED_ENVELOPE_BYTES as u64,
            });
        }
        Ok(Self {
            format_version: wire.format_version,
            identity: EventRef {
                stream_id: stream.clone(),
                seq,
                stable_event_id: stable_event_id.clone(),
                // The digest just encoded into the wire object —
                // retained, identical to the persisted bytes.
                payload_sha256: wire.payload_sha256,
            },
            schema_id: schema_id.clone(),
            payload: bytes::Bytes::copy_from_slice(payload),
            encoded,
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
    /// verified tail hint carries. Distinct from the payload digest:
    /// `payload_sha256` names the payload bytes; this names the
    /// whole encoded envelope, and neither substitutes for the other.
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
                identity: EventRef {
                    stream_id: StreamId::new(&wire.stream_id).map_err(|_| EnvelopeMismatch)?,
                    seq: wire.seq,
                    stable_event_id: StableEventId::new(&wire.stable_event_id)
                        .map_err(|_| EnvelopeMismatch)?,
                    // The persisted digest this decode just verified
                    // against the payload — retained, never recomputed.
                    payload_sha256: wire.payload_sha256,
                },
                schema_id: SchemaId::new(&wire.schema_id).map_err(|_| EnvelopeMismatch)?,
                payload: bytes::Bytes::from(payload),
                encoded: bytes::Bytes::copy_from_slice(bytes),
            })
        };
        verify()
    }
}

impl Envelope {
    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    pub fn stream_id(&self) -> &StreamId {
        &self.identity.stream_id
    }

    pub fn seq(&self) -> Seq {
        self.identity.seq
    }

    pub fn stable_event_id(&self) -> &StableEventId {
        &self.identity.stable_event_id
    }

    pub fn schema_id(&self) -> &SchemaId {
        &self.schema_id
    }

    pub fn payload(&self) -> &bytes::Bytes {
        &self.payload
    }

    /// The payload digest computed by `encode` and verified by
    /// `decode_and_verify` — retained on the value, not recomputed.
    pub fn payload_sha256(&self) -> &str {
        &self.identity.payload_sha256
    }

    /// The identity of the landed event this envelope verified.
    pub fn event_ref(&self) -> &EventRef {
        &self.identity
    }

    /// The canonical encoded object bytes (the persisted wire form).
    pub(crate) fn encoded(&self) -> &bytes::Bytes {
        &self.encoded
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}
