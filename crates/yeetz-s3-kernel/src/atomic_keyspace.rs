//! AtomicKeyspace — namespace-scoped validated keyed I/O over the
//! object store (ADR 0016; Sol's cross-review §1 spec).
//!
//! The assured keyed-I/O surface: `create` is put-if-absent with a
//! typed `AlreadyExists` on a lost race; `compare_exchange` is If-Match
//! CAS; `list_after` is exclusive-start-after, strictly ordered,
//! bounded; deletes are namespaced and idempotent; `delete_many`
//! reports per-key outcomes for resumable sweeps. Trim and retention
//! (batch 5): an immutable create-once certificate at
//! `{scope}/trims/{first_retained:020}` bounds a scope's retained
//! prefix (max-by-key is the floor); [`AtomicKeyspace::delete_below`]
//! is the certified, idempotent, resumable GC primitive; the
//! certificate keys themselves are reserved (`trims` path segment)
//! and refuse direct caller writes. Values are
//! stored in an internal versioned envelope so byte-identical
//! payloads in different CAS eras have different object bytes. **No
//! unconditional overwrite exists in this module.**
//!
//! Key layout: every object lives under the kernel-reserved root
//! `keyspace/` — `keyspace/{namespace}/{key}` — structurally disjoint
//! from lineage keys, so a namespace can never collide with a lineage
//! name regardless of naming. Listing is prefix-scoped to the
//! namespace root and cannot observe other namespaces.
use std::sync::Arc;

use bytes::Bytes;
use yeetz_sdk_s3::{ObjectStoreClient, ObjectStoreError};

use crate::tombstone::Tombstone;

/// Reserved key root for the keyspace (kernel-owned, like `objects/`
/// and `head`).
pub const KEYSPACE_ROOT: &str = "keyspace";

/// Canonical binary value envelope (v2, batch 7). The prefix makes
/// unversioned or differently encoded objects fail closed — v1
/// envelopes included: a deliberate, documented format break at 0.x
/// (pre-release stores only). The big-endian **incarnation** gives
/// byte-identical payloads in different deletion lifetimes distinct
/// object bytes (closing batch 4's delete/recreate gap: an era-1
/// etag can never match an era-2 value, because the eras disagree
/// in the envelope even when the payload agrees); the **version**
/// gives every CAS era within one lifetime distinct bytes.
const VALUE_ENVELOPE_PREFIX: &[u8] = b"yeetz-keyspace-value/v2\0";
const VALUE_INCARNATION_BYTES: usize = size_of::<u64>();
const VALUE_VERSION_BYTES: usize = size_of::<u64>();

#[derive(Debug)]
struct ValueEnvelope {
    incarnation: u64,
    version: u64,
    payload: Bytes,
}

impl ValueEnvelope {
    fn new(incarnation: u64, version: u64, payload: Bytes) -> Self {
        Self {
            incarnation,
            version,
            payload,
        }
    }

    fn encode(self) -> Bytes {
        let mut encoded = Vec::with_capacity(
            VALUE_ENVELOPE_PREFIX.len()
                + VALUE_INCARNATION_BYTES
                + VALUE_VERSION_BYTES
                + self.payload.len(),
        );
        encoded.extend_from_slice(VALUE_ENVELOPE_PREFIX);
        encoded.extend_from_slice(&self.incarnation.to_be_bytes());
        encoded.extend_from_slice(&self.version.to_be_bytes());
        encoded.extend_from_slice(&self.payload);
        Bytes::from(encoded)
    }

    fn decode(key: &str, encoded: &Bytes) -> Result<Self, KeyspaceError> {
        let incarnation_start = VALUE_ENVELOPE_PREFIX.len();
        let version_start = incarnation_start + VALUE_INCARNATION_BYTES;
        let payload_start = version_start + VALUE_VERSION_BYTES;
        if encoded.len() < payload_start || !encoded.starts_with(VALUE_ENVELOPE_PREFIX) {
            return Err(KeyspaceError::ValueEnvelopeMalformed(key.to_string()));
        }
        let incarnation = u64::from_be_bytes(
            encoded[incarnation_start..version_start]
                .try_into()
                .expect("incarnation slice has fixed width"),
        );
        let version = u64::from_be_bytes(
            encoded[version_start..payload_start]
                .try_into()
                .expect("version slice has fixed width"),
        );
        Ok(Self {
            incarnation,
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
    /// when the store reported one, and — since batch 7 — the
    /// incarnation and version the current value carries, so a
    /// stale-era CAS names the era it lost to (an era-1 etag against
    /// an era-2 value of identical payload bytes reports the
    /// incarnation mismatch).
    #[error(
        "compare_exchange precondition failed for {key}: expected {expected_etag}, observed {observed:?} (incarnation {observed_incarnation:?}, version {observed_version:?})"
    )]
    PreconditionFailed {
        key: String,
        expected_etag: String,
        observed: Option<String>,
        observed_incarnation: Option<u64>,
        observed_version: Option<u64>,
    },
    /// A stored keyspace value is not the canonical versioned
    /// envelope. Integrity failure is distinct from absence.
    #[error("keyspace value envelope malformed: {0}")]
    ValueEnvelopeMalformed(String),
    /// The current value has no representable successor version.
    #[error("keyspace value version exhausted: {0}")]
    VersionExhausted(String),
    /// A proposed trim floor is below the certified maximum: a lower
    /// trim cannot supersede a higher one. The certificate, not
    /// object absence, is the boundary.
    #[error("trim not monotone: requested first_retained {requested}, certified {certified}")]
    TrimNotMonotone { requested: u64, certified: u64 },
    /// The requested GC bound is not covered by a trim certificate
    /// for the scope.
    #[error(
        "trim not certified for {scope:?}: requested first_retained {requested}, certified {certified:?}"
    )]
    TrimNotCertified {
        scope: String,
        requested: u64,
        certified: Option<u64>,
    },
    /// The tombstone namespace (`tombstones/`) is reserved and
    /// immutable: tombstones are written only by [`AtomicKeyspace::destroy`],
    /// never overwritten, never deleted except by a certified trim
    /// sweep.
    #[error("tombstone namespace is immutable: {0}")]
    TombstoneImmutable(String),
    /// The incarnation-counter namespace (`incarnations/`) is
    /// reserved: counters are advanced only by the kernel's
    /// `destroy`, never written or deleted by callers, and removed
    /// only by a certified trim sweep.
    #[error("incarnation counter namespace is reserved: {0}")]
    IncarnationCounterImmutable(String),
    /// The trim-certificate namespace (any `{scope}/trims/{seq}` key)
    /// is reserved and immutable: certificates are written only by
    /// [`AtomicKeyspace::propose_trim`], never overwritten, never
    /// deleted by callers. The maximum certificate IS the certified
    /// floor — a deletable certificate is a regressable floor, which
    /// reopens the resurrection attack the certificate exists to
    /// close (teardown finding T1, 2026-08-22).
    #[error("trim certificate namespace is immutable: {0}")]
    TrimCertificateImmutable(String),
    /// The backing store failed in a way the caller should retry.
    #[error("keyspace store unavailable: {operation}")]
    Unavailable { operation: &'static str },
}

/// The state of a trim scope after [`AtomicKeyspace::propose_trim`]:
/// the effective (maximum) certified floor and whether this call
/// advanced it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrimState {
    /// The maximum certified first-retained seq — the effective
    /// floor; a concurrent higher proposal may have overtaken the
    /// caller's request.
    pub first_retained: u64,
    /// Whether this proposal is the effective floor (`false` when
    /// idempotent or overtaken).
    pub advanced: bool,
}

/// The four-state existence read (batch 6): [`KeyState::Present`] /
/// [`KeyState::Destroyed`] / [`KeyState::OffsetExpired`] /
/// [`KeyState::Absent`].
#[derive(Debug, Clone, PartialEq)]
pub enum KeyState {
    /// The key exists; value plus the CAS token.
    Present { value: Bytes, etag: String },
    /// The key existed and was deliberately deleted; the tombstone
    /// is the witness (cause, actor, generation, timestamp).
    Destroyed { tombstone: Tombstone },
    /// The key's seq position is below the namespace's certified
    /// trim floor: history the certificate retired (batch 5) —
    /// neither absent nor destroyed, and never a zombie's Present.
    OffsetExpired { first_retained: u64 },
    /// The key never existed: no value, no tombstone.
    Absent,
}

/// The outcome of a [`AtomicKeyspace::delete_below`] sweep. A crash
/// mid-sweep leaves `remaining > 0` — extra objects, which are safe;
/// a re-run converges to `remaining == 0` with no other effect.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeleteBelowReport {
    /// Keys below the floor the sweep attempted.
    pub examined: u64,
    /// Keys whose delete was confirmed applied.
    pub deleted: u64,
    /// Keys below the floor still present after the sweep — the
    /// interrupted remainder; re-run to converge.
    pub remaining: u64,
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

    /// Reserved tombstone sub-root: `tombstones/{key}` mirrors the
    /// key it witnesses. Kernel-owned, like `trims/`.
    const TOMBSTONE_ROOT: &str = "tombstones";

    /// The tombstone key mirroring a data key.
    fn tombstone_key(key: &str) -> String {
        format!("{}/{key}", Self::TOMBSTONE_ROOT)
    }

    /// Reserved incarnation-counter sub-root: `incarnations/{key}`
    /// mirrors the key whose deletion lifetimes it counts.
    /// Kernel-owned, like `tombstones/`.
    const INCARNATION_ROOT: &str = "incarnations";

    /// Reserved prefixes refuse every direct write/delete path:
    /// tombstones are written only by `destroy`, incarnation
    /// counters only by `destroy`'s bump, trim certificates only by
    /// `propose_trim`; all three are removed only by a certified
    /// trim sweep. The certificate guard is structural: a key with a
    /// `trims` path segment naming a FOLLOWING component is a
    /// certificate (`{scope}/trims/{seq:020}` at any scope depth),
    /// and the maximum certificate is the certified floor — a
    /// deletable certificate is a regressable floor (teardown
    /// finding T1).
    fn ensure_not_reserved_key(key: &str) -> Result<(), KeyspaceError> {
        if key.starts_with(Self::TOMBSTONE_ROOT) && key.len() > Self::TOMBSTONE_ROOT.len() {
            return Err(KeyspaceError::TombstoneImmutable(key.to_string()));
        }
        if key.starts_with(Self::INCARNATION_ROOT) && key.len() > Self::INCARNATION_ROOT.len() {
            return Err(KeyspaceError::IncarnationCounterImmutable(key.to_string()));
        }
        let segments: Vec<&str> = key.split('/').collect();
        if segments[..segments.len() - 1].contains(&Self::TRIMS_SEGMENT) {
            return Err(KeyspaceError::TrimCertificateImmutable(key.to_string()));
        }
        Ok(())
    }

    /// The reserved certificate path segment (ADR 0002 batch 5):
    /// `{scope}/trims/{first_retained:020}` at any scope depth.
    const TRIMS_SEGMENT: &str = "trims";

    /// The incarnation-counter object mirroring a data key:
    /// `incarnations/{key}`. Raw big-endian u64: created once
    /// (put-if-absent at 1 on the first destroy), advanced only by
    /// CAS, never decreased, removed only by a certified trim sweep.
    /// An absent counter means incarnation 0 — the key's first
    /// lifetime.
    fn incarnation_key(key: &str) -> String {
        format!("{}/{key}", Self::INCARNATION_ROOT)
    }

    /// The key's current incarnation (0 when no counter exists).
    async fn current_incarnation(&self, key: &str) -> Result<u64, KeyspaceError> {
        let object_key = format!(
            "{KEYSPACE_ROOT}/{}/{}",
            self.namespace,
            Self::incarnation_key(key)
        );
        match self.store.download(&object_key).await {
            Ok(bytes) => {
                let raw = bytes.as_ref();
                if raw.len() != size_of::<u64>() {
                    return Err(KeyspaceError::ValueEnvelopeMalformed(
                        Self::incarnation_key(key),
                    ));
                }
                Ok(u64::from_be_bytes(raw.try_into().expect("fixed width")))
            }
            Err(ObjectStoreError::NotFound(_)) => Ok(0),
            Err(_) => Err(KeyspaceError::Unavailable {
                operation: "incarnation counter read",
            }),
        }
    }

    /// Advance the counter to at least `current + 1` and return the
    /// new incarnation. Monotone: a lost create/CAS race re-reads and
    /// re-derives (gaps are fine; decreases are impossible — the CAS
    /// target always exceeds the observed counter).
    async fn bump_incarnation(&self, key: &str) -> Result<u64, KeyspaceError> {
        let object_key = format!(
            "{KEYSPACE_ROOT}/{}/{}",
            self.namespace,
            Self::incarnation_key(key)
        );
        for _ in 0..8 {
            match self.store.download_with_etag(&object_key).await {
                Ok(meta) => {
                    let Some(etag) = meta.etag else {
                        return Err(KeyspaceError::Unavailable {
                            operation: "incarnation counter read (no etag)",
                        });
                    };
                    let raw = meta.data.as_ref();
                    if raw.len() != size_of::<u64>() {
                        return Err(KeyspaceError::ValueEnvelopeMalformed(
                            Self::incarnation_key(key),
                        ));
                    }
                    let current = u64::from_be_bytes(raw.try_into().expect("fixed width"));
                    let next = current
                        .checked_add(1)
                        .ok_or_else(|| KeyspaceError::VersionExhausted(key.to_string()))?;
                    match self
                        .store
                        .upload_conditional(
                            &object_key,
                            Bytes::from(next.to_be_bytes().to_vec()),
                            Some(&etag),
                        )
                        .await
                    {
                        Ok(_) => return Ok(next),
                        // Contention or a concurrent first-create: the loop
                        // iteration ends and re-reads.
                        Err(ObjectStoreError::PreconditionFailed(_)) => {}
                        Err(_) => {
                            return Err(KeyspaceError::Unavailable {
                                operation: "incarnation counter bump",
                            });
                        }
                    }
                }
                Err(ObjectStoreError::NotFound(_)) => {
                    // First bump: create-once at 1. A lost race means
                    // someone else created it — re-read.
                    match self
                        .store
                        .upload_conditional(
                            &object_key,
                            Bytes::from(1u64.to_be_bytes().to_vec()),
                            None,
                        )
                        .await
                    {
                        Ok(_) => return Ok(1),
                        Err(ObjectStoreError::PreconditionFailed(_)) => {}
                        Err(_) => {
                            return Err(KeyspaceError::Unavailable {
                                operation: "incarnation counter create",
                            });
                        }
                    }
                }
                Err(_) => {
                    return Err(KeyspaceError::Unavailable {
                        operation: "incarnation counter read",
                    });
                }
            }
        }
        Err(KeyspaceError::Unavailable {
            operation: "incarnation counter bump budget exhausted",
        })
    }

    /// Put-if-absent. A lost race returns
    /// [`KeyspaceError::AlreadyExists`]; the winner's bytes are
    /// untouched (the loser never overwrites). A re-create after
    /// destroy starts at version 0 of the key's CURRENT incarnation
    /// (batch 7) — a fresh lifetime, never a versioned continuation
    /// of the destroyed era.
    pub async fn create(&self, key: &str, value: Bytes) -> Result<(), KeyspaceError> {
        Self::ensure_not_reserved_key(key)?;
        let object_key = self.object_key(key)?;
        let incarnation = self.current_incarnation(key).await?;
        let value = ValueEnvelope::new(incarnation, 0, value).encode();
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
    ) -> Result<Option<(Bytes, u64, u64, String)>, KeyspaceError> {
        let object_key = self.object_key(key)?;
        match self.store.download_with_etag(&object_key).await {
            Ok(meta) => {
                let Some(etag) = meta.etag else {
                    return Err(KeyspaceError::Unavailable {
                        operation: "keyspace get_with_version (no etag reported)",
                    });
                };
                let envelope = ValueEnvelope::decode(key, &meta.data)?;
                Ok(Some((
                    envelope.payload,
                    envelope.incarnation,
                    envelope.version,
                    etag,
                )))
            }
            Err(ObjectStoreError::NotFound(_)) => Ok(None),
            Err(_) => Err(KeyspaceError::Unavailable {
                operation: "keyspace get_with_version",
            }),
        }
    }

    pub async fn get_with_version_for_test(
        &self,
        key: &str,
    ) -> Result<Option<(Bytes, u64, String)>, KeyspaceError> {
        Ok(self
            .get_with_version(key)
            .await?
            .map(|(payload, _, version, etag)| (payload, version, etag)))
    }

    /// Test/probe view of the key's current incarnation (batch 7).
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub async fn incarnation_for_test(&self, key: &str) -> Result<u64, KeyspaceError> {
        self.current_incarnation(key).await
    }
    pub async fn compare_exchange(
        &self,
        key: &str,
        expected_etag: &str,
        value: Bytes,
    ) -> Result<String, KeyspaceError> {
        Self::ensure_not_reserved_key(key)?;
        let object_key = self.object_key(key)?;
        let current = match self.store.download_with_etag(&object_key).await {
            Ok(meta) => meta,
            Err(ObjectStoreError::NotFound(_)) => {
                return Err(KeyspaceError::PreconditionFailed {
                    key: key.to_string(),
                    expected_etag: expected_etag.to_string(),
                    observed: None,
                    observed_incarnation: None,
                    observed_version: None,
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
            // Batch 7: name the era the caller lost to. Identical
            // payload bytes across a deletion boundary still produce
            // distinct envelope bytes (distinct incarnations), so an
            // era-1 etag cannot match an era-2 value — and the error
            // says so.
            let (observed_incarnation, observed_version) =
                match ValueEnvelope::decode(key, &current.data) {
                    Ok(envelope) => (Some(envelope.incarnation), Some(envelope.version)),
                    Err(_) => (None, None),
                };
            return Err(KeyspaceError::PreconditionFailed {
                key: key.to_string(),
                expected_etag: expected_etag.to_string(),
                observed: Some(observed_etag),
                observed_incarnation,
                observed_version,
            });
        }
        let current = ValueEnvelope::decode(key, &current.data)?;
        // CAS advances the version WITHIN the current incarnation;
        // only destroy moves the incarnation.
        let next_version = current
            .version
            .checked_add(1)
            .ok_or_else(|| KeyspaceError::VersionExhausted(key.to_string()))?;
        let next = ValueEnvelope::new(current.incarnation, next_version, value).encode();
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
                let (observed_incarnation, observed_version) =
                    match self.store.download(&object_key).await {
                        Ok(bytes) => match ValueEnvelope::decode(key, &bytes) {
                            Ok(envelope) => (Some(envelope.incarnation), Some(envelope.version)),
                            Err(_) => (None, None),
                        },
                        Err(_) => (None, None),
                    };
                Err(KeyspaceError::PreconditionFailed {
                    key: key.to_string(),
                    expected_etag: expected_etag.to_string(),
                    observed,
                    observed_incarnation,
                    observed_version,
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
        Self::ensure_not_reserved_key(key)?;
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
            Self::ensure_not_reserved_key(key)?;
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

    /// Intentional deletion with an existence witness: an immutable
    /// tombstone (`{deleted_at_gen, cause, actor, ts}`) is written
    /// at `tombstones/{key}` BEFORE the value is deleted — enough to
    /// prove the key existed and was deliberately deleted. Idempotent
    /// for an already-destroyed key (the first tombstone stands);
    /// a no-op for a never-created key (no fabricated history). A
    /// re-create supersedes the tombstone: the new existence IS the
    /// truth, the witness remains as history until a certified trim
    /// sweep retires it.
    pub async fn destroy(&self, key: &str, cause: &str, actor: &str) -> Result<(), KeyspaceError> {
        let object_key = self.object_key(key)?;
        let Some((_, incarnation, version, _)) = self.get_with_version(key).await? else {
            return Ok(()); // nothing to witness — absence is already the truth
        };
        let tombstone = Tombstone::new(incarnation, version, cause, actor);
        let tombstone_object_key = format!(
            "{KEYSPACE_ROOT}/{}/{}",
            self.namespace,
            Self::tombstone_key(key)
        );
        // Put-if-absent through the raw path (the public create is
        // guarded against the reserved prefix); AlreadyExists means
        // an earlier lifetime's tombstone stands — immutable history.
        match self
            .store
            .upload_conditional(&tombstone_object_key, tombstone.encode()?.into(), None)
            .await
        {
            Ok(_) | Err(ObjectStoreError::PreconditionFailed(_)) => {}
            Err(_) => {
                return Err(KeyspaceError::Unavailable {
                    operation: "keyspace destroy: tombstone create",
                });
            }
        }
        // Witness in place — advance the incarnation (the next
        // lifetime is a NEW era: version 0 of a higher incarnation,
        // so an era-1 etag can never match era-2 bytes), then delete.
        self.bump_incarnation(key).await?;
        self.store
            .delete(&object_key)
            .await
            .map_err(|_| KeyspaceError::Unavailable {
                operation: "keyspace destroy: delete",
            })
    }

    /// The existence read (batch 6): `Present` / `Destroyed` /
    /// `OffsetExpired` / `Absent` — destroyed is never conflated with
    /// never-created, and trimmed history is never conflated with
    /// either. A seq-shaped key below the namespace's root trim
    /// certificate reads `OffsetExpired` regardless of zombies or
    /// unswept tombstones (the certificate rules, per batch 5);
    /// scoped certificates (per-stream) surface through the scoped
    /// APIs. Seq 0 — the genesis position — is exempt (immortal).
    pub async fn read_state(&self, key: &str) -> Result<KeyState, KeyspaceError> {
        self.object_key(key)?;
        if let Some(seq) = key.rsplit('/').next().and_then(Self::parse_seq_component)
            && seq > 0
            && let Some(first_retained) = self.trim_floor("").await?
            && seq < first_retained
        {
            return Ok(KeyState::OffsetExpired { first_retained });
        }
        if let Some((value, etag)) = self.get_with_etag(key).await? {
            return Ok(KeyState::Present { value, etag });
        }
        // Tombstones are raw objects (not versioned envelopes — they
        // are create-once by construction), read through the raw path.
        let tombstone_object_key = format!(
            "{KEYSPACE_ROOT}/{}/{}",
            self.namespace,
            Self::tombstone_key(key)
        );
        match self.store.download(&tombstone_object_key).await {
            Ok(bytes) => {
                return Ok(KeyState::Destroyed {
                    tombstone: Tombstone::decode(bytes.as_ref())?,
                });
            }
            Err(ObjectStoreError::NotFound(_)) => {}
            Err(_) => {
                return Err(KeyspaceError::Unavailable {
                    operation: "read_state: tombstone",
                });
            }
        }
        Ok(KeyState::Absent)
    }

    /// The 20-digit zero-padded key component of a seq (the seq-key
    /// convention shared with the streams layout).
    fn seq_component(seq: u64) -> String {
        format!("{seq:020}")
    }

    /// Parse a 20-digit zero-padded seq key component.
    fn parse_seq_component(component: &str) -> Option<u64> {
        (component.len() == 20 && component.bytes().all(|byte| byte.is_ascii_digit()))
            .then(|| component.parse().ok())
            .flatten()
    }

    /// The certificate prefix of a scope: `{scope}/trims/` (or
    /// `trims/` at the namespace root).
    fn trim_cert_prefix(scope: &str) -> Result<String, KeyspaceError> {
        if !scope.is_empty() {
            validate_identifier("trim scope", scope)?;
        }
        Ok(if scope.is_empty() {
            "trims/".to_string()
        } else {
            format!("{scope}/trims/")
        })
    }

    /// Propose a trim floor for a scope ("" = the namespace root; a
    /// non-empty scope carves a sub-tree, e.g. one stream). The
    /// certificate is an immutable create-once object at
    /// `{scope}/trims/{first_retained:020}`; the effective floor is
    /// the maximum certificate (zero-padded keys sort with their
    /// seqs). Monotone by law: a proposal below the current floor is
    /// rejected with [`KeyspaceError::TrimNotMonotone`] — the
    /// certificate, never object absence, is the boundary, so a stale
    /// writer cannot resurrect a lower floor by recreating what the
    /// sweeper deleted (the batch-4 versioned values make the
    /// certificate itself ABA-proof). An equal proposal is idempotent.
    /// A concurrent higher proposal may overtake; the returned
    /// [`TrimState`] names the effective floor.
    pub async fn propose_trim(
        &self,
        scope: &str,
        first_retained: u64,
    ) -> Result<TrimState, KeyspaceError> {
        let cert_prefix = Self::trim_cert_prefix(scope)?;
        if let Some(certified) = self.trim_floor(scope).await? {
            if first_retained < certified {
                return Err(KeyspaceError::TrimNotMonotone {
                    requested: first_retained,
                    certified,
                });
            }
            if first_retained == certified {
                return Ok(TrimState {
                    first_retained: certified,
                    advanced: false,
                });
            }
        }
        let key = format!("{cert_prefix}{}", Self::seq_component(first_retained));
        // The certificate key is structurally guarded against caller
        // writes; propose itself writes through the raw put-if-absent
        // path (the same discipline `destroy` uses for tombstones).
        // The value is the canonical v2 envelope so certified keys
        // stay readable through the value surface.
        let certificate =
            ValueEnvelope::new(0, 0, Bytes::from_static(b"trim-certificate")).encode();
        let object_key = self.object_key(&key)?;
        match self
            .store
            .upload_conditional(&object_key, certificate, None)
            .await
        {
            Ok(_) | Err(ObjectStoreError::PreconditionFailed(_)) => {}
            Err(_) => {
                return Err(KeyspaceError::Unavailable {
                    operation: "propose_trim: certificate create",
                });
            }
        }
        // Max-by-key is the truth: report the effective floor, not
        // the caller's proposal (a concurrent higher cert wins).
        let effective = self.trim_floor(scope).await?.unwrap_or(first_retained);
        Ok(TrimState {
            first_retained: effective,
            advanced: effective == first_retained,
        })
    }

    /// The effective trim floor of a scope: the maximum certificate,
    /// or `None` when the scope was never trimmed. One bounded LIST
    /// page per call for any realistic certificate count.
    pub async fn trim_floor(&self, scope: &str) -> Result<Option<u64>, KeyspaceError> {
        let cert_prefix = Self::trim_cert_prefix(scope)?;
        // `start_after` must be a valid identifier: the bare
        // `{scope}/trims` segment sorts immediately before every
        // `{scope}/trims/{seq}` certificate key.
        let start = if scope.is_empty() {
            "trims".to_string()
        } else {
            format!("{scope}/trims")
        };
        let mut after = Some(start);
        let mut floor = None;
        loop {
            let keys = self.list_after(after.as_deref(), 1000).await?;
            for key in &keys {
                // The certificate prefix's key range is contiguous in
                // byte order; the first key outside it ends the walk.
                let Some(rest) = key.strip_prefix(&cert_prefix) else {
                    return Ok(floor);
                };
                if let Some(seq) = Self::parse_seq_component(rest) {
                    floor = Some(floor.map_or(seq, |current: u64| current.max(seq)));
                }
                after = Some(key.clone());
            }
            if keys.len() < 1000 {
                return Ok(floor);
            }
        }
    }

    /// Certified GC: delete `{data_prefix}{seq:020}` keys with
    /// `1 <= seq < first_retained`, conditional on a certificate
    /// covering the bound (`trim_floor(scope) >= first_retained`;
    /// otherwise [`KeyspaceError::TrimNotCertified`]). Seq 0 — the
    /// genesis position of the seq-key convention — is never
    /// collectable: it anchors existence, and deleting it is stream
    /// deletion, a different operation. Never deletes at or above the
    /// boundary. Bounded batches; idempotent and resumable: a crash
    /// mid-sweep leaves extra objects (safe), and a re-run converges
    /// through the same certificate.
    pub async fn delete_below(
        &self,
        scope: &str,
        data_prefix: &str,
        first_retained: u64,
    ) -> Result<DeleteBelowReport, KeyspaceError> {
        // Validates the scope and derives the certificate prefix.
        Self::trim_cert_prefix(scope)?;
        let certified = self.trim_floor(scope).await?;
        if certified.is_none_or(|floor| first_retained > floor) {
            return Err(KeyspaceError::TrimNotCertified {
                scope: scope.to_string(),
                requested: first_retained,
                certified,
            });
        }

        // Validate the data prefix by composing its first legal key.
        let start = format!("{data_prefix}{}", Self::seq_component(0));
        validate_identifier("delete_below data prefix", &start)?;

        // Ascending walk: the seq-key range is contiguous in byte
        // order, so the first key outside the prefix (or at/above the
        // floor) ends the sweep. The certificate prefix itself sorts
        // before every `data_prefix` continuation, so `trims/` keys
        // encountered before the data range are stepped over.
        let mut report = DeleteBelowReport::default();
        let mut after = Some(start);
        let mut pending: Vec<String> = Vec::new();
        'walk: loop {
            let keys = self.list_after(after.as_deref(), 1000).await?;
            if keys.is_empty() {
                break;
            }
            for key in &keys {
                let Some(seq) = key
                    .strip_prefix(data_prefix)
                    .and_then(Self::parse_seq_component)
                else {
                    // Past the contiguous data range: ascending order
                    // guarantees no further data keys exist.
                    break 'walk;
                };
                if seq == 0 {
                    // The genesis position is immortal.
                    after = Some(key.clone());
                    continue;
                }
                if seq >= first_retained {
                    // At or above the boundary: never collected, and
                    // ascending order means the sweep is done.
                    break 'walk;
                }
                pending.push(key.clone());
                after = Some(key.clone());
            }
        }

        // The same bounded walk over the mirrored tombstones below
        // the floor: history retires WITH the data it witnessed —
        // the trim certificate carries the witness from here on.
        let tombstone_prefix = Self::tombstone_key(data_prefix);
        let tombstone_start = format!("{tombstone_prefix}{}", Self::seq_component(0));
        let mut after = Some(tombstone_start);
        'tombstones: loop {
            let keys = self.list_after(after.as_deref(), 1000).await?;
            if keys.is_empty() {
                break;
            }
            for key in &keys {
                let Some(seq) = key
                    .strip_prefix(&tombstone_prefix)
                    .and_then(Self::parse_seq_component)
                else {
                    break 'tombstones;
                };
                if seq == 0 || seq >= first_retained {
                    break 'tombstones;
                }
                pending.push(key.clone());
                after = Some(key.clone());
            }
        }

        // And the same walk over the mirrored incarnation counters
        // below the floor: a trimmed-and-recreated key starts at
        // incarnation 0 again, sanctioned because the trim
        // certificate witnesses the erased lifetimes and
        // `OffsetExpired` protects readers of that history.
        let incarnation_prefix = Self::incarnation_key(data_prefix);
        let incarnation_start = format!("{incarnation_prefix}{}", Self::seq_component(0));
        let mut after = Some(incarnation_start);
        'incarnations: loop {
            let keys = self.list_after(after.as_deref(), 1000).await?;
            if keys.is_empty() {
                break;
            }
            for key in &keys {
                let Some(seq) = key
                    .strip_prefix(&incarnation_prefix)
                    .and_then(Self::parse_seq_component)
                else {
                    break 'incarnations;
                };
                if seq == 0 || seq >= first_retained {
                    break 'incarnations;
                }
                pending.push(key.clone());
                after = Some(key.clone());
            }
        }

        // Bounded batches. Tombstone and incarnation keys bypass the
        // guarded bulk primitive (immutability applies to callers,
        // not to the certified sweep), so the sweep deletes through
        // the raw object path with the same per-key accounting.
        for key in &pending {
            report.examined += 1;
            let object_key = self.object_key(key)?;
            match self.store.delete(&object_key).await {
                Ok(()) => report.deleted += 1,
                Err(_) => report.remaining += 1,
            }
        }
        Ok(report)
    }
}
