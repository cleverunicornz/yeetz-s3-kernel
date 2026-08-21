//! AtomicKeyspace — namespace-scoped validated keyed I/O over the
//! object store (ADR 0016; Sol's cross-review §1 spec).
//!
//! The assured keyed-I/O surface: `create` is put-if-absent with a
//! typed `AlreadyExists` on a lost race; `compare_exchange` is If-Match
//! CAS; `list_after` is exclusive-start-after, strictly ordered,
//! bounded; deletes are namespaced and idempotent; `delete_many`
//! reports per-key outcomes for resumable sweeps. Values are stored in
//! an internal versioned envelope so byte-identical payloads in
//! different CAS eras have different object bytes. **No unconditional
//! overwrite exists in this module.**
//!
//! Key layout: every object lives under the kernel-reserved root
//! `keyspace/` — `keyspace/{namespace}/{key}` — structurally disjoint
//! from lineage keys, so a namespace can never collide with a lineage
//! name regardless of naming. Listing is prefix-scoped to the
//! namespace root and cannot observe other namespaces.

use std::sync::Arc;

use bytes::Bytes;
use yeetz_sdk_s3::{ObjectStoreClient, ObjectStoreError};

/// Reserved key root for the keyspace (kernel-owned, like `objects/`
/// and `head`).
pub const KEYSPACE_ROOT: &str = "keyspace";

/// Canonical binary value envelope. The prefix makes unversioned or
/// differently encoded objects fail closed; the big-endian version
/// gives every successful CAS era distinct bytes without constraining
/// the caller's opaque payload.
const VALUE_ENVELOPE_PREFIX: &[u8] = b"yeetz-keyspace-value/v1\0";
const VALUE_VERSION_BYTES: usize = size_of::<u64>();

#[derive(Debug)]
struct ValueEnvelope {
    version: u64,
    payload: Bytes,
}

impl ValueEnvelope {
    fn new(version: u64, payload: Bytes) -> Self {
        Self { version, payload }
    }

    fn encode(self) -> Bytes {
        let mut encoded = Vec::with_capacity(
            VALUE_ENVELOPE_PREFIX.len() + VALUE_VERSION_BYTES + self.payload.len(),
        );
        encoded.extend_from_slice(VALUE_ENVELOPE_PREFIX);
        encoded.extend_from_slice(&self.version.to_be_bytes());
        encoded.extend_from_slice(&self.payload);
        Bytes::from(encoded)
    }

    fn decode(key: &str, encoded: &Bytes) -> Result<Self, KeyspaceError> {
        let version_start = VALUE_ENVELOPE_PREFIX.len();
        let payload_start = version_start + VALUE_VERSION_BYTES;
        if encoded.len() < payload_start || !encoded.starts_with(VALUE_ENVELOPE_PREFIX) {
            return Err(KeyspaceError::ValueEnvelopeMalformed(key.to_string()));
        }
        let version = u64::from_be_bytes(
            encoded[version_start..payload_start]
                .try_into()
                .expect("version slice has fixed width"),
        );
        Ok(Self {
            version,
            payload: encoded.slice(payload_start..),
        })
    }
}

/// Errors from the keyspace surface. Its own type (additive — no
/// `KernelError` variants moved or added).
#[derive(Debug, thiserror::Error)]
pub enum KeyspaceError {
    /// The namespace or key failed validation.
    #[error("invalid keyspace identifier: {0}")]
    InvalidIdentifier(String),
    /// `create` lost the put-if-absent race; the key already exists.
    #[error("key already exists: {0}")]
    AlreadyExists(String),
    /// `compare_exchange` failed: the observed etag differs from
    /// `expected_etag`. Carries the etag observed at conflict time
    /// when the store reported one.
    #[error(
        "compare_exchange precondition failed for {key}: expected {expected_etag}, observed {observed:?}"
    )]
    PreconditionFailed {
        key: String,
        expected_etag: String,
        observed: Option<String>,
    },
    /// A stored keyspace value is not the canonical versioned
    /// envelope. Integrity failure is distinct from absence.
    #[error("keyspace value envelope malformed: {0}")]
    ValueEnvelopeMalformed(String),
    /// The current value has no representable successor version.
    #[error("keyspace value version exhausted: {0}")]
    VersionExhausted(String),
    /// The backing store failed in a way the caller should retry.
    #[error("keyspace store unavailable: {operation}")]
    Unavailable { operation: &'static str },
}

/// Per-key outcome of a `delete_many` sweep — idempotent and
/// resumable: a failed sweep tells the caller exactly which keys
/// remain (deleted=false) and which are confirmed gone, so a re-run
/// of the not-deleted set converges without side effects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteOutcome {
    pub key: String,
    pub deleted: bool,
}

impl DeleteOutcome {
    /// The subset of outcomes not confirmed deleted — the resumable
    /// remainder of an interrupted sweep.
    #[must_use]
    pub fn remaining(outcomes: &[DeleteOutcome]) -> Vec<String> {
        outcomes
            .iter()
            .filter(|outcome| !outcome.deleted)
            .map(|outcome| outcome.key.clone())
            .collect()
    }
}

/// Conservative identifier rule: non-empty slash-joined segments of
/// `[A-Za-z0-9][A-Za-z0-9._-]*`, total length ≤ 255, no leading or
/// trailing slash, no empty segments.
fn validate_identifier(kind: &str, value: &str) -> Result<(), KeyspaceError> {
    let invalid = || KeyspaceError::InvalidIdentifier(format!("{kind} {value:?}"));
    if value.is_empty() || value.len() > 255 || value.starts_with('/') || value.ends_with('/') {
        return Err(invalid());
    }
    for segment in value.split('/') {
        if segment.is_empty() {
            return Err(invalid());
        }
        let mut bytes = segment.bytes();
        let Some(first) = bytes.next() else {
            return Err(invalid());
        };
        if !first.is_ascii_alphanumeric() {
            return Err(invalid());
        }
        if !bytes.all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.') {
            return Err(invalid());
        }
    }
    Ok(())
}

/// A namespace-scoped atomic keyspace over the kernel-owned object store.
/// Construction stays behind [`crate::KernelHandle`].
///
/// ```compile_fail
/// use std::sync::Arc;
/// use yeetz_s3_kernel::AtomicKeyspace;
/// use yeetz_sdk_s3::ObjectStoreClient;
///
/// fn bypass(store: Arc<ObjectStoreClient>) {
///     let _ = AtomicKeyspace::new(store, "raw");
/// }
/// ```
#[derive(Debug)]
pub struct AtomicKeyspace {
    store: Arc<ObjectStoreClient>,
    namespace: String,
}

impl AtomicKeyspace {
    /// Bind a keyspace to a namespace. The namespace is validated
    /// once; every subsequent key is validated against the same rule.
    pub(crate) fn new(
        store: Arc<ObjectStoreClient>,
        namespace: &str,
    ) -> Result<Self, KeyspaceError> {
        validate_identifier("namespace", namespace)?;
        Ok(Self {
            store,
            namespace: namespace.to_string(),
        })
    }

    /// Full object-store key for a keyspace key:
    /// `keyspace/{namespace}/{key}`.
    fn object_key(&self, key: &str) -> Result<String, KeyspaceError> {
        validate_identifier("key", key)?;
        Ok(format!("{KEYSPACE_ROOT}/{}/{}", self.namespace, key))
    }

    /// Put-if-absent. A lost race returns
    /// [`KeyspaceError::AlreadyExists`]; the winner's bytes are
    /// untouched (the loser never overwrites).
    pub async fn create(&self, key: &str, value: Bytes) -> Result<(), KeyspaceError> {
        let object_key = self.object_key(key)?;
        let value = ValueEnvelope::new(0, value).encode();
        match self
            .store
            .upload_conditional(&object_key, value, None)
            .await
        {
            Ok(_) => Ok(()),
            Err(ObjectStoreError::PreconditionFailed(_)) => {
                Err(KeyspaceError::AlreadyExists(key.to_string()))
            }
            Err(_) => Err(KeyspaceError::Unavailable {
                operation: "keyspace create",
            }),
        }
    }

    /// Read a key's value (`None` when absent).
    pub async fn get(&self, key: &str) -> Result<Option<Bytes>, KeyspaceError> {
        let object_key = self.object_key(key)?;
        match self.store.download(&object_key).await {
            Ok(bytes) => Ok(Some(ValueEnvelope::decode(key, &bytes)?.payload)),
            Err(ObjectStoreError::NotFound(_)) => Ok(None),
            Err(_) => Err(KeyspaceError::Unavailable {
                operation: "keyspace get",
            }),
        }
    }

    /// Read a key's value together with its etag — the token a
    /// subsequent `compare_exchange` must present. The pair is
    /// atomically consistent: the etag names exactly the returned
    /// bytes (single-object read).
    pub async fn get_with_etag(&self, key: &str) -> Result<Option<(Bytes, String)>, KeyspaceError> {
        let object_key = self.object_key(key)?;
        match self.store.download_with_etag(&object_key).await {
            Ok(meta) => match meta.etag {
                Some(etag) => Ok(Some((
                    ValueEnvelope::decode(key, &meta.data)?.payload,
                    etag,
                ))),
                // An object whose store reports no etag cannot be CAS'd
                // against; surface it rather than hand back a token
                // that would silently never match.
                None => Err(KeyspaceError::Unavailable {
                    operation: "keyspace get_with_etag (no etag reported)",
                }),
            },
            Err(ObjectStoreError::NotFound(_)) => Ok(None),
            Err(_) => Err(KeyspaceError::Unavailable {
                operation: "keyspace get_with_etag",
            }),
        }
    }

    /// Read a payload with its internal version and store etag inside
    /// the kernel closure. Production callers remain payload-shaped.
    pub(crate) async fn get_with_version(
        &self,
        key: &str,
    ) -> Result<Option<(Bytes, u64, String)>, KeyspaceError> {
        let object_key = self.object_key(key)?;
        match self.store.download_with_etag(&object_key).await {
            Ok(meta) => {
                let Some(etag) = meta.etag else {
                    return Err(KeyspaceError::Unavailable {
                        operation: "keyspace get_with_version (no etag reported)",
                    });
                };
                let envelope = ValueEnvelope::decode(key, &meta.data)?;
                Ok(Some((envelope.payload, envelope.version, etag)))
            }
            Err(ObjectStoreError::NotFound(_)) => Ok(None),
            Err(_) => Err(KeyspaceError::Unavailable {
                operation: "keyspace get_with_version",
            }),
        }
    }

    /// Test/probe view of the internal version. The production surface
    /// exposes only caller payloads and opaque etags.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub async fn get_with_version_for_test(
        &self,
        key: &str,
    ) -> Result<Option<(Bytes, u64, String)>, KeyspaceError> {
        self.get_with_version(key).await
    }

    /// If-Match compare-and-swap: replaces the value only when the
    /// stored object's etag equals `expected_etag`. Returns the new
    /// etag. A mismatch returns [`KeyspaceError::PreconditionFailed`]
    /// carrying the currently observed etag when available — the
    /// caller re-reads, re-derives, retries (law 4).
    pub async fn compare_exchange(
        &self,
        key: &str,
        expected_etag: &str,
        value: Bytes,
    ) -> Result<String, KeyspaceError> {
        let object_key = self.object_key(key)?;
        let current = match self.store.download_with_etag(&object_key).await {
            Ok(meta) => meta,
            Err(ObjectStoreError::NotFound(_)) => {
                return Err(KeyspaceError::PreconditionFailed {
                    key: key.to_string(),
                    expected_etag: expected_etag.to_string(),
                    observed: None,
                });
            }
            Err(_) => {
                return Err(KeyspaceError::Unavailable {
                    operation: "keyspace compare_exchange read",
                });
            }
        };
        let Some(observed_etag) = current.etag else {
            return Err(KeyspaceError::Unavailable {
                operation: "keyspace compare_exchange read (no etag reported)",
            });
        };
        if observed_etag != expected_etag {
            return Err(KeyspaceError::PreconditionFailed {
                key: key.to_string(),
                expected_etag: expected_etag.to_string(),
                observed: Some(observed_etag),
            });
        }
        let current = ValueEnvelope::decode(key, &current.data)?;
        let next_version = current
            .version
            .checked_add(1)
            .ok_or_else(|| KeyspaceError::VersionExhausted(key.to_string()))?;
        let next = ValueEnvelope::new(next_version, value).encode();
        match self
            .store
            .upload_conditional(&object_key, next, Some(expected_etag))
            .await
        {
            Ok(etag) => etag.ok_or(KeyspaceError::Unavailable {
                operation: "keyspace compare_exchange (no etag)",
            }),
            Err(ObjectStoreError::PreconditionFailed(_)) => {
                let observed = match self.store.download_with_etag(&object_key).await {
                    Ok(meta) => meta.etag,
                    Err(_) => None,
                };
                Err(KeyspaceError::PreconditionFailed {
                    key: key.to_string(),
                    expected_etag: expected_etag.to_string(),
                    observed,
                })
            }
            Err(_) => Err(KeyspaceError::Unavailable {
                operation: "keyspace compare_exchange",
            }),
        }
    }

    /// List the namespace's keys strictly ordered (byte order), after
    /// the exclusive `start_after` key (namespace-relative), at most
    /// `limit`. For a stable key set, passing the last returned key
    /// walks every key exactly once with no boundary duplicate or skip.
    ///
    /// This is a weakly consistent cursor, not a snapshot: a concurrent
    /// insert that sorts after the cursor is eligible for a later page;
    /// one that sorts at or before the cursor is outside the remaining
    /// walk. Concurrent deletes simply vanish from later pages.
    pub async fn list_after(
        &self,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>, KeyspaceError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if let Some(after) = start_after {
            validate_identifier("start_after", after)?;
        }
        let prefix = format!("{KEYSPACE_ROOT}/{}/", self.namespace);
        let after = start_after.map(|after| format!("{prefix}{after}"));
        let keys = self
            .store
            .list_prefix_after(&prefix, after.as_deref(), limit)
            .await
            .map_err(|_| KeyspaceError::Unavailable {
                operation: "keyspace list_after",
            })?;
        // Defensive: strip the namespace prefix and refuse anything
        // that is not a plain key under it (the store's prefix filter
        // already guarantees this; the projection makes the scoping
        // structural rather than assumed).
        Ok(keys
            .into_iter()
            .filter_map(|key| key.strip_prefix(&prefix).map(str::to_string))
            .collect())
    }

    /// Delete a key (namespaced). Idempotent: deleting an absent key
    /// succeeds — object stores treat it that way and so do we.
    pub async fn delete(&self, key: &str) -> Result<(), KeyspaceError> {
        let object_key = self.object_key(key)?;
        self.store
            .delete(&object_key)
            .await
            .map_err(|_| KeyspaceError::Unavailable {
                operation: "keyspace delete",
            })
    }

    /// Delete many keys (namespaced, batch, idempotent). Returns a
    /// per-key outcome; a key is `deleted: true` only when its delete
    /// was confirmed applied, so an interrupted sweep resumes by
    /// re-running exactly the `deleted: false` remainder
    /// ([`DeleteOutcome::remaining`]) with no side effects on the
    /// confirmed set.
    pub async fn delete_many(&self, keys: &[&str]) -> Result<Vec<DeleteOutcome>, KeyspaceError> {
        // Reject malformed batches before the first effect. Returning an
        // identifier error after earlier deletes would discard the outcome
        // report the caller needs to resume safely.
        for key in keys {
            self.object_key(key)?;
        }

        let mut outcomes = Vec::with_capacity(keys.len());
        for key in keys {
            match self.delete(key).await {
                Ok(()) => outcomes.push(DeleteOutcome {
                    key: key.to_string(),
                    deleted: true,
                }),
                Err(KeyspaceError::Unavailable { operation }) => {
                    // Record the failure and continue: partial progress
                    // is the resumability contract.
                    let _ = operation;
                    outcomes.push(DeleteOutcome {
                        key: key.to_string(),
                        deleted: false,
                    });
                }
                Err(err) => return Err(err),
            }
        }
        Ok(outcomes)
    }
}
