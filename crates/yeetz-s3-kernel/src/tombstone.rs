//! Existence tombstones (batch 6): the immutable witness written
//! BEFORE an intentional deletion, mechanizing the parent-aggregate
//! witness convention — "destroyed" becomes distinguishable from
//! "never created" inside the kernel itself.
//!
//! One shape serves both surfaces: keyspace tombstones at
//! `tombstones/{key}` (per deleted key) and lineage tombstones at
//! `{lineage}/tombstone` (per deleted head). Put-if-absent, never
//! overwritten, never deleted except by a certified trim sweep; a
//! re-create supersedes it (the new existence IS the truth) while
//! the tombstone remains as history until trimmed.

use crate::KeyspaceError;

pub const TOMBSTONE_FORMAT_VERSION: u32 = 1;

/// The existence witness: proof that a key existed and was
/// deliberately deleted, by whom, why, and at which generation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Tombstone {
    pub format_version: u32,
    /// The destroyed value's generation: the keyspace version (or
    /// lineage head generation) at destroy time.
    pub deleted_at_gen: u64,
    /// Why the deletion happened (caller-supplied, auditable).
    pub cause: String,
    /// Who performed the deletion (caller-supplied, auditable).
    pub actor: String,
    /// When: unix milliseconds.
    pub ts: u64,
}

impl Tombstone {
    /// Mint a witness for a deliberate deletion now.
    #[must_use]
    pub fn new(deleted_at_gen: u64, cause: &str, actor: &str) -> Self {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_millis() as u64)
            .unwrap_or(0);
        Self {
            format_version: TOMBSTONE_FORMAT_VERSION,
            deleted_at_gen,
            cause: cause.to_string(),
            actor: actor.to_string(),
            ts,
        }
    }

    /// Canonical bytes (deterministic serialization, like heads).
    pub(crate) fn encode(&self) -> Result<Vec<u8>, KeyspaceError> {
        serde_json::to_vec(self).map_err(|_| KeyspaceError::Unavailable {
            operation: "tombstone encode",
        })
    }

    /// Parse and structurally verify (version check included).
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, KeyspaceError> {
        let tombstone: Self = serde_json::from_slice(bytes)
            .map_err(|_| KeyspaceError::ValueEnvelopeMalformed("tombstone payload".to_string()))?;
        if tombstone.format_version != TOMBSTONE_FORMAT_VERSION {
            return Err(KeyspaceError::ValueEnvelopeMalformed(
                "tombstone format version".to_string(),
            ));
        }
        Ok(tombstone)
    }
}
