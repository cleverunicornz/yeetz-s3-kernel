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
//! is the certified, idempotent, resumable GC primitive. Values are
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
        self.create(&key, Bytes::from_static(b"trim-certificate"))
            .await?;
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

        // Bounded batches through the resumable bulk primitive.
        for chunk in pending.chunks(64) {
            let refs: Vec<&str> = chunk.iter().map(String::as_str).collect();
            for outcome in self.delete_many(&refs).await? {
                report.examined += 1;
                if outcome.deleted {
                    report.deleted += 1;
                } else {
                    report.remaining += 1;
                }
            }
        }
        Ok(report)
    }
}
