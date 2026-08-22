//! Migration support (ADR 0017's migration contract): copy a
//! pre-existing log into a stream at EXPLICIT seqs, verify density,
//! and seal the migration immutably.
//!
//! `migrate_log` is the only API that writes caller-chosen seqs — it
//! exists for one-shot ingestion of an already-ordered history (the
//! quiesced ADR-0011 → ADR-0017 migration). It uses the same
//! conditional-create allocation as `append`; re-running it over a
//! completed migration is idempotent, and any disagreement with the
//! landed bytes is a typed error, never an overwrite.

use crate::{
    ENVELOPE_FORMAT_VERSION, Envelope, Replay, SchemaId, StableEventId, StreamId, Streams,
    StreamsError,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// One migrated event: an explicit seq plus the event's identity.
#[derive(Debug, Clone)]
pub struct MigrationEntry<'a> {
    pub seq: u64,
    pub schema_id: &'a SchemaId,
    pub stable_event_id: &'a StableEventId,
    pub payload: &'a [u8],
}

/// The immutable migration seal: old head digest, count, and the new
/// event-root digest (sha256 over the seq-ordered envelope digests).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationSeal {
    pub format_version: u32,
    /// The source lineage the events were copied from (e.g.
    /// `events/<owner>/<repo>`).
    pub source_lineage: String,
    /// Digest of the source lineage's terminal head record at
    /// migration time — the exact head the canonical traversal folded.
    pub source_head_digest: String,
    pub event_count: u64,
    /// sha256 over `sha256(envelope_1) ∥ … ∥ sha256(envelope_N)` in
    /// seq order — the new event root.
    pub event_root_digest: String,
}

/// What `migrate_log` did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationReceipt {
    pub count: u64,
    pub event_root_digest: String,
}

impl Streams {
    /// Copy `entries` into `stream` at their explicit seqs. The stream
    /// must exist (genesis at seq 0 — `create_stream`). Entries must
    /// be dense from 1; density is verified before anything is
    /// written, and again after. Idempotent: a re-run over identical
    /// landed bytes succeeds; any disagreement (different payload,
    /// schema, or stable id at a seq) is a typed error — the
    /// destination is never overwritten.
    pub async fn migrate_log(
        &self,
        stream: &StreamId,
        entries: &[MigrationEntry<'_>],
    ) -> Result<MigrationReceipt, StreamsError> {
        // Pre-conditions: dense 1..=N, strictly ordered.
        let mut ordered: Vec<&MigrationEntry<'_>> = entries.iter().collect();
        ordered.sort_by_key(|entry| entry.seq);
        for (position, entry) in ordered.iter().enumerate() {
            if entry.seq != position as u64 + 1 {
                return Err(StreamsError::InvalidArgument(format!(
                    "migration entries must be dense from 1 (seq {} at position {})",
                    entry.seq, position
                )));
            }
        }

        // Genesis is the destination's existence and integrity
        // witness. Verify it before the first explicit-seq write so a
        // damaged destination cannot acquire a partial migration.
        self.verify_migration_genesis(stream).await?;

        // Write each entry at its explicit seq: conditional create;
        // AlreadyExists is idempotent-success only for byte-identical
        // envelopes, a typed error otherwise.
        let mut terminal: Option<Envelope> = None;
        let mut root = Sha256::new();
        for entry in &ordered {
            let envelope = Envelope::encode(
                stream,
                entry.seq,
                entry.schema_id,
                entry.stable_event_id,
                entry.payload,
            )?;
            let key = crate::Streams::log_key_of(stream, entry.seq);
            match self.keyspace().create(&key, envelope.encoded.clone()).await {
                Ok(()) => {}
                Err(yeetz_s3_kernel::KeyspaceError::AlreadyExists(_)) => {
                    let existing =
                        self.keyspace_get(stream, entry.seq).await?.ok_or_else(|| {
                            StreamsError::BackendUnqualified {
                                witness: format!(
                                    "migration: create conflict at seq {} but readback absent",
                                    entry.seq
                                ),
                            }
                        })?;
                    if existing != envelope.encoded {
                        Envelope::decode_and_verify(stream, entry.seq, &existing).map_err(
                            |_| StreamsError::Corrupt {
                                stream: stream.clone(),
                                missing_or_mismatched: vec![entry.seq],
                            },
                        )?;
                        return Err(StreamsError::InvalidArgument(format!(
                            "migration conflict at seq {}: destination holds different bytes",
                            entry.seq
                        )));
                    }
                }
                Err(_) => {
                    return Err(StreamsError::Unavailable {
                        operation: "migrate_log: create",
                    });
                }
            }
            root.update(hex::decode(envelope.digest_hex()).expect("envelope digest hex"));
            terminal = Some(envelope);
        }

        // Post-condition: density 1..=N with exactly the written
        // digests (read back through the public read path).
        let expected_count = ordered.len() as u64;
        let mut after = 0u64;
        loop {
            match self.read(stream, after, 256).await {
                Replay::Page { events, complete } => {
                    for envelope in &events {
                        if envelope.seq != after + 1 {
                            return Err(StreamsError::BackendUnqualified {
                                witness: format!(
                                    "migration verification: expected seq {} got {}",
                                    after + 1,
                                    envelope.seq
                                ),
                            });
                        }
                        after = envelope.seq;
                    }
                    if complete {
                        break;
                    }
                }
                Replay::Empty => break,
                Replay::Corrupt {
                    missing_or_mismatched,
                } => {
                    return Err(StreamsError::Corrupt {
                        stream: stream.clone(),
                        missing_or_mismatched,
                    });
                }
                Replay::Unavailable { operation } => {
                    return Err(StreamsError::Unavailable { operation });
                }
                Replay::BackendUnqualified { witness } => {
                    return Err(StreamsError::BackendUnqualified { witness });
                }
                Replay::NotFound { .. } => {
                    return Err(StreamsError::StreamNotFound(stream.clone()));
                }
                Replay::OffsetExpired { first_retained } => {
                    // Migration verifies density from seq 1; a
                    // trimmed stream cannot satisfy that post-condition.
                    return Err(StreamsError::InvalidArgument(format!(
                        "stream is trimmed below {first_retained}; migration requires the full log"
                    )));
                }
            }
        }
        if after != expected_count {
            return Err(StreamsError::BackendUnqualified {
                witness: format!(
                    "migration verification: count {after} != expected {expected_count}"
                ),
            });
        }

        // The pass above validated contiguous truth (density +
        // digests through the public read path): write the tail hint
        // as the completeness witness — a migrated stream certifies
        // reads exactly like an append-built one. Monotone CAS, so a
        // re-run over identical bytes is a no-op here.
        if let Some(terminal) = terminal {
            self.advance_tail_hint(stream, expected_count, &terminal)
                .await;
        }

        Ok(MigrationReceipt {
            count: expected_count,
            event_root_digest: hex::encode(root.finalize()),
        })
    }

    /// Write the immutable migration seal (create-once; a second write
    /// with different content is a typed error, identical content is
    /// idempotent).
    pub async fn write_migration_seal(
        &self,
        stream: &StreamId,
        seal: &MigrationSeal,
    ) -> Result<(), StreamsError> {
        if seal.format_version != ENVELOPE_FORMAT_VERSION {
            return Err(StreamsError::InvalidArgument(format!(
                "unsupported migration seal format {}",
                seal.format_version
            )));
        }
        self.verify_migration_genesis(stream).await?;
        let key = format!("{}/migration-seal", stream.as_str());
        let bytes = bytes::Bytes::from(
            serde_json::to_vec(seal)
                .map_err(|_| StreamsError::InvalidArgument("seal JSON".into()))?,
        );
        match self.keyspace().create(&key, bytes).await {
            Ok(()) => Ok(()),
            Err(yeetz_s3_kernel::KeyspaceError::AlreadyExists(_)) => {
                let existing = match self.keyspace().get(&key).await {
                    Ok(Some(existing)) => existing,
                    Ok(None) => {
                        return Err(StreamsError::BackendUnqualified {
                            witness: "seal create conflict but readback absent".into(),
                        });
                    }
                    Err(yeetz_s3_kernel::KeyspaceError::ValueEnvelopeMalformed(_)) => {
                        return Err(StreamsError::MigrationSealCorrupt {
                            stream: stream.clone(),
                            reason: "keyspace value envelope is malformed",
                        });
                    }
                    Err(_) => {
                        return Err(StreamsError::Unavailable {
                            operation: "write_migration_seal: readback",
                        });
                    }
                };
                let landed = Self::decode_migration_seal(stream, &existing)?;
                if landed == *seal {
                    Ok(())
                } else {
                    Err(StreamsError::InvalidArgument(
                        "migration seal already exists with different content".into(),
                    ))
                }
            }
            Err(_) => Err(StreamsError::Unavailable {
                operation: "write_migration_seal",
            }),
        }
    }

    /// Read the stream's migration seal (`None` when unmigrated).
    pub async fn read_migration_seal(
        &self,
        stream: &StreamId,
    ) -> Result<Option<MigrationSeal>, StreamsError> {
        let key = format!("{}/migration-seal", stream.as_str());
        match self.keyspace().get(&key).await {
            Ok(Some(bytes)) => {
                self.verify_migration_genesis(stream).await?;
                Self::decode_migration_seal(stream, &bytes).map(Some)
            }
            Ok(None) => Ok(None),
            Err(yeetz_s3_kernel::KeyspaceError::ValueEnvelopeMalformed(_)) => {
                Err(StreamsError::MigrationSealCorrupt {
                    stream: stream.clone(),
                    reason: "keyspace value envelope is malformed",
                })
            }
            Err(_) => Err(StreamsError::Unavailable {
                operation: "read_migration_seal",
            }),
        }
    }

    async fn verify_migration_genesis(&self, stream: &StreamId) -> Result<(), StreamsError> {
        let key = crate::Streams::log_key_of(stream, 0);
        let bytes = match self.keyspace().get(&key).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Err(StreamsError::StreamNotFound(stream.clone())),
            Err(yeetz_s3_kernel::KeyspaceError::ValueEnvelopeMalformed(_)) => {
                return Err(StreamsError::Corrupt {
                    stream: stream.clone(),
                    missing_or_mismatched: vec![0],
                });
            }
            Err(_) => {
                return Err(StreamsError::Unavailable {
                    operation: "migration: read genesis",
                });
            }
        };
        Envelope::decode_and_verify(stream, 0, &bytes).map_err(|_| StreamsError::Corrupt {
            stream: stream.clone(),
            missing_or_mismatched: vec![0],
        })?;
        Ok(())
    }

    fn decode_migration_seal(
        stream: &StreamId,
        bytes: &[u8],
    ) -> Result<MigrationSeal, StreamsError> {
        let seal: MigrationSeal =
            serde_json::from_slice(bytes).map_err(|_| StreamsError::MigrationSealCorrupt {
                stream: stream.clone(),
                reason: "payload is malformed",
            })?;
        if seal.format_version != ENVELOPE_FORMAT_VERSION {
            return Err(StreamsError::MigrationSealCorrupt {
                stream: stream.clone(),
                reason: "format version is unsupported",
            });
        }
        Ok(seal)
    }

    // Internal accessors (the keyspace and log-key layout stay private
    // to the crate; migration lives inside the crate for exactly this
    // reason).
    fn keyspace(&self) -> &yeetz_s3_kernel::AtomicKeyspace {
        &self.keyspace
    }

    async fn keyspace_get(
        &self,
        stream: &StreamId,
        seq: u64,
    ) -> Result<Option<bytes::Bytes>, StreamsError> {
        self.keyspace()
            .get(&crate::Streams::log_key_of(stream, seq))
            .await
            .map_err(|_| StreamsError::Unavailable {
                operation: "migrate_log: get",
            })
    }
}

/// Marker so `ENVELOPE_FORMAT_VERSION` stays referenced in doc
/// contexts without a separate const import site.
#[allow(dead_code)]
const MIGRATION_SEAL_FORMAT_VERSION: u32 = ENVELOPE_FORMAT_VERSION;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seal_shape_is_stable() {
        let seal = MigrationSeal {
            format_version: 1,
            source_lineage: "events/demo/hello".into(),
            source_head_digest: "abc".into(),
            event_count: 3,
            event_root_digest: "def".into(),
        };
        let json = serde_json::to_value(&seal).unwrap();
        assert_eq!(json["source_lineage"], "events/demo/hello");
        assert_eq!(json["event_count"], 3);
    }
}
