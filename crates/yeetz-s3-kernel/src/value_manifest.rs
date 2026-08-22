//! The chunked-value v3 control format (ADR 0004): the canonical
//! binary manifest committed at the logical key, the fixed-size
//! content-addressed chunk objects under the kernel-private
//! `keyspace-chunks` root, and the v2/v3-aware control decoder every
//! control-metadata path shares.
//!
//! Representation law (ADR 0004 §1): a logical `AtomicKeyspace` value
//! is EITHER an inline v2 envelope (every new encoded payload at or
//! below `INLINE_MAX`, plus every legacy v2 object regardless of
//! size) OR a v3 manifest at the logical key naming immutable,
//! fixed 16 MiB SHA-256-addressed chunks in a separate kernel-private
//! root. The control object — inline envelope or manifest — is the
//! only CAS/delete unit; the successful conditional manifest PUT is
//! the commit point, and no chunk state is visible through the
//! logical surface before it.

use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::atomic_keyspace::{KeyspaceError, ValueEnvelope};

/// Reserved physical root for chunk objects (kernel-owned, like
/// `keyspace`): `keyspace-chunks/v1/{namespace}/...`. Lineages cannot
/// occupy either root (ADR 0004 §1.4).
pub const CHUNK_ROOT: &str = "keyspace-chunks";

/// Private layout version under [`CHUNK_ROOT`].
const CHUNK_ROOT_VERSION: &str = "v1";

/// Fixed chunk size: the cache/integrity unit (ADR 0004 ruling 2).
pub const CHUNK_BYTES: usize = 16 * 1024 * 1024;

/// Encoded-payload threshold above which whole-value writes take the
/// chunked representation; at or below it they stay one inline v2
/// object (ADR 0004 ruling 2 — `INLINE_MAX = 64 MiB`).
pub const INLINE_MAX: usize = 64 * 1024 * 1024;

/// Canonical v3 floor: a one-chunk v3 is rejected because inline is
/// canonical (ADR 0004 §1.2).
pub const MIN_CHUNKS: u32 = 2;

/// Chunk-count ceiling: bounds the manifest; ordinals `0..=u16::MAX`.
pub const MAX_CHUNKS: u32 = 65_536;

/// Maximum logical encoded value: 16 MiB × 65,536 = 1 TiB.
pub const MAX_LOGICAL_BYTES: u64 = CHUNK_BYTES as u64 * MAX_CHUNKS as u64;

/// Maximum encoded manifest: the entry table is ~2.25 MiB at maximum
/// count; the bound is structural headroom (ADR 0004 §6).
pub const MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;

/// Maximum in-flight chunk transfers on the read/write path: a
/// 64 MiB payload window, matching `INLINE_MAX` peak memory.
pub const MAX_IN_FLIGHT_CHUNKS: usize = 4;

/// Manifest binary magic. The prefix makes non-v3 (and future)
/// controls fail closed, exactly like the v2 envelope prefix.
const MANIFEST_MAGIC: &[u8] = b"yeetz-keyspace-value/v3\0";

/// The only supported manifest `kind`: chunked-v1.
const MANIFEST_KIND_CHUNKED_V1: u32 = 1;

/// Domain separator for `value_root_sha256` (ADR 0004 §1.2):
/// `SHA-256(domain || logical_len || chunk_bytes || chunk_count ||
/// ordered (encoded_len, chunk_sha256) entries)`.
const VALUE_ROOT_DOMAIN: &[u8] = b"yeetz-keyspace-value-root/v1";

const U64_BYTES: usize = std::mem::size_of::<u64>();
const U32_BYTES: usize = std::mem::size_of::<u32>();
const COMMIT_ID_BYTES: usize = 16;
const SHA256_BYTES: usize = 32;

/// magic(25) + incarnation(8) + version(8) + kind(4) + commit_id(16)
/// + logical_len(8) + value_root(32) + chunk_bytes(4) + chunk_count(4).
const MANIFEST_HEADER_BYTES: usize = MANIFEST_MAGIC.len()
    + U64_BYTES
    + U64_BYTES
    + U32_BYTES
    + COMMIT_ID_BYTES
    + U64_BYTES
    + SHA256_BYTES
    + U32_BYTES
    + U32_BYTES;

/// One chunk-table entry: `encoded_len(4) + sha256(32)`.
const MANIFEST_ENTRY_BYTES: usize = U32_BYTES + SHA256_BYTES;

/// One chunk-table entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ManifestEntry {
    pub(crate) encoded_len: u32,
    pub(crate) sha256: [u8; SHA256_BYTES],
}

impl ManifestEntry {
    /// The opaque digest cache identity of a verified chunk — a
    /// digest, never a storage capability (ADR 0004 §3.3).
    #[must_use]
    pub(crate) fn digest_hex(&self) -> String {
        hex::encode(self.sha256)
    }
}

/// The canonical v3 manifest (ADR 0004 §1.2). `incarnation` and
/// `version` live here — in the control envelope, never in chunks;
/// partial uploads alter neither.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ValueManifest {
    pub(crate) incarnation: u64,
    pub(crate) version: u64,
    pub(crate) commit_id: [u8; COMMIT_ID_BYTES],
    pub(crate) logical_len: u64,
    pub(crate) value_root_sha256: [u8; SHA256_BYTES],
    /// Always exactly `CHUNK_BYTES` in a canonical manifest.
    pub(crate) chunk_bytes: u32,
    pub(crate) entries: Vec<ManifestEntry>,
}

impl ValueManifest {
    #[must_use]
    pub(crate) fn chunk_count(&self) -> u32 {
        self.entries.len() as u32
    }

    /// `value_root_sha256` commits to boundaries, order, and every
    /// chunk while permitting a range reader to validate the table
    /// without fetching unrelated chunks.
    #[must_use]
    pub(crate) fn compute_value_root(
        logical_len: u64,
        chunk_bytes: u32,
        entries: &[ManifestEntry],
    ) -> [u8; SHA256_BYTES] {
        let mut hasher = Sha256::new();
        hasher.update(VALUE_ROOT_DOMAIN);
        hasher.update(logical_len.to_be_bytes());
        hasher.update(chunk_bytes.to_be_bytes());
        hasher.update((entries.len() as u32).to_be_bytes());
        for entry in entries {
            hasher.update(entry.encoded_len.to_be_bytes());
            hasher.update(entry.sha256);
        }
        hasher.finalize().into()
    }

    /// Canonical binary encode. The caller guarantees canonicality
    /// (entries derived from the chunk pipeline); [`Self::decode`]
    /// refuses everything else.
    pub(crate) fn encode(&self) -> Bytes {
        let mut encoded =
            Vec::with_capacity(MANIFEST_HEADER_BYTES + MANIFEST_ENTRY_BYTES * self.entries.len());
        encoded.extend_from_slice(MANIFEST_MAGIC);
        encoded.extend_from_slice(&self.incarnation.to_be_bytes());
        encoded.extend_from_slice(&self.version.to_be_bytes());
        encoded.extend_from_slice(&MANIFEST_KIND_CHUNKED_V1.to_be_bytes());
        encoded.extend_from_slice(&self.commit_id);
        encoded.extend_from_slice(&self.logical_len.to_be_bytes());
        encoded.extend_from_slice(&self.value_root_sha256);
        encoded.extend_from_slice(&self.chunk_bytes.to_be_bytes());
        encoded.extend_from_slice(&self.chunk_count().to_be_bytes());
        for entry in &self.entries {
            encoded.extend_from_slice(&entry.encoded_len.to_be_bytes());
            encoded.extend_from_slice(&entry.sha256);
        }
        Bytes::from(encoded)
    }

    /// Canonical decode with the full ADR 0004 §1.2 rule set. Length
    /// agreement is checked BEFORE count-derived allocation; bad root,
    /// unsupported kind, non-canonical fields, oversized manifests,
    /// count overflow, and length disagreement are integrity
    /// failures — never absence.
    pub(crate) fn decode(key: &str, encoded: &[u8]) -> Result<Self, KeyspaceError> {
        let malformed = || KeyspaceError::ManifestMalformed(key.to_string());
        if encoded.len() > MAX_MANIFEST_BYTES {
            return Err(KeyspaceError::ManifestTooLarge {
                key: key.to_string(),
                len: encoded.len() as u64,
                max: MAX_MANIFEST_BYTES as u64,
            });
        }
        if encoded.len() < MANIFEST_HEADER_BYTES || !encoded.starts_with(MANIFEST_MAGIC) {
            return Err(malformed());
        }
        let be_u64 =
            |slice: &[u8]| u64::from_be_bytes(slice.try_into().expect("fixed-width slice"));
        let be_u32 =
            |slice: &[u8]| u32::from_be_bytes(slice.try_into().expect("fixed-width slice"));
        let mut cursor = MANIFEST_MAGIC.len();
        let incarnation = be_u64(&encoded[cursor..cursor + U64_BYTES]);
        cursor += U64_BYTES;
        let version = be_u64(&encoded[cursor..cursor + U64_BYTES]);
        cursor += U64_BYTES;
        let kind = be_u32(&encoded[cursor..cursor + U32_BYTES]);
        cursor += U32_BYTES;
        let mut commit_id = [0u8; COMMIT_ID_BYTES];
        commit_id.copy_from_slice(&encoded[cursor..cursor + COMMIT_ID_BYTES]);
        cursor += COMMIT_ID_BYTES;
        let logical_len = be_u64(&encoded[cursor..cursor + U64_BYTES]);
        cursor += U64_BYTES;
        let mut value_root_sha256 = [0u8; SHA256_BYTES];
        value_root_sha256.copy_from_slice(&encoded[cursor..cursor + SHA256_BYTES]);
        cursor += SHA256_BYTES;
        let chunk_bytes = be_u32(&encoded[cursor..cursor + U32_BYTES]);
        cursor += U32_BYTES;
        let chunk_count = be_u32(&encoded[cursor..cursor + U32_BYTES]);
        cursor += U32_BYTES;

        if kind != MANIFEST_KIND_CHUNKED_V1 {
            return Err(malformed());
        }
        if chunk_bytes != CHUNK_BYTES as u32 {
            return Err(malformed());
        }
        if !(MIN_CHUNKS..=MAX_CHUNKS).contains(&chunk_count) {
            return Err(KeyspaceError::ChunkCountInvalid {
                key: key.to_string(),
                count: chunk_count,
            });
        }
        // Length agreement before count-derived allocation.
        let expected_len = MANIFEST_HEADER_BYTES + MANIFEST_ENTRY_BYTES * chunk_count as usize;
        if encoded.len() != expected_len {
            return Err(malformed());
        }
        let mut entries = Vec::with_capacity(chunk_count as usize);
        let mut summed_len = 0u64;
        for index in 0..chunk_count as usize {
            let entry_start = cursor + index * MANIFEST_ENTRY_BYTES;
            let encoded_len = be_u32(&encoded[entry_start..entry_start + U32_BYTES]);
            let mut sha256 = [0u8; SHA256_BYTES];
            sha256.copy_from_slice(
                &encoded[entry_start + U32_BYTES..entry_start + MANIFEST_ENTRY_BYTES],
            );
            // Every non-final chunk is exactly CHUNK_BYTES; the final
            // is 1..=CHUNK_BYTES. Empty values are inline.
            let is_final = index == chunk_count as usize - 1;
            let canonical_len = if is_final {
                (1..=CHUNK_BYTES as u32).contains(&encoded_len)
            } else {
                encoded_len == CHUNK_BYTES as u32
            };
            if !canonical_len {
                return Err(malformed());
            }
            summed_len += u64::from(encoded_len);
            entries.push(ManifestEntry {
                encoded_len,
                sha256,
            });
        }
        if summed_len != logical_len {
            return Err(malformed());
        }
        if logical_len > MAX_LOGICAL_BYTES {
            return Err(KeyspaceError::ValueTooLarge {
                key: key.to_string(),
                len: logical_len,
                max: MAX_LOGICAL_BYTES,
            });
        }
        if Self::compute_value_root(logical_len, chunk_bytes, &entries) != value_root_sha256 {
            return Err(KeyspaceError::ManifestRootMismatch(key.to_string()));
        }
        Ok(Self {
            incarnation,
            version,
            commit_id,
            logical_len,
            value_root_sha256,
            chunk_bytes,
            entries,
        })
    }
}

/// The physical chunk-object key (ADR 0004 §1.3):
/// `keyspace-chunks/v1/{namespace}/{hex(key)}/{incarnation:020}/{version:020}/{sha256}`.
/// The logical key is hex-encoded: reversible, exactly 2× expansion,
/// never percent encoding's 3× worst case — 892 bytes at the
/// identifier-rule maximum (255-byte namespace, 255-byte key), below
/// S3's 1,024-byte key limit.
#[must_use]
pub(crate) fn chunk_object_key(
    namespace: &str,
    key: &str,
    incarnation: u64,
    version: u64,
    digest_hex: &str,
) -> String {
    format!(
        "{CHUNK_ROOT}/{CHUNK_ROOT_VERSION}/{namespace}/{}/{:020}/{:020}/{digest_hex}",
        hex::encode(key),
        incarnation,
        version
    )
}

/// A parsed private chunk path. The format decoder refuses any other
/// key encoding (ADR 0004 §1.3): a path under the chunk root that
/// does not parse exactly classifies as unresolved, never as a
/// deletable or referencable chunk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChunkObjectPath {
    pub(crate) namespace: String,
    pub(crate) logical_key: String,
    pub(crate) incarnation: u64,
    pub(crate) version: u64,
    pub(crate) digest_hex: String,
}

/// Parse a physical chunk key. Namespace segments may contain slashes
/// (`streams/v1`), so the path parses from the right: digest (64
/// lowercase hex), version (20 digits), incarnation (20 digits), then
/// the single hex-encoded logical-key segment; the remainder is the
/// namespace.
#[must_use]
pub(crate) fn parse_chunk_object_key(object_key: &str) -> Option<ChunkObjectPath> {
    let rest = object_key.strip_prefix(format!("{CHUNK_ROOT}/{CHUNK_ROOT_VERSION}/").as_str())?;
    let mut segments: Vec<&str> = rest.split('/').collect();
    if segments.len() < 5 {
        return None;
    }
    let digest_hex = segments.pop()?.to_string();
    let version = parse_zero_padded(segments.pop()?)?;
    let incarnation = parse_zero_padded(segments.pop()?)?;
    let encoded_key = segments.pop()?;
    if !is_lower_hex(digest_hex.as_str()) || digest_hex.len() != SHA256_BYTES * 2 {
        return None;
    }
    if encoded_key.is_empty() || encoded_key.len() % 2 != 0 || !is_lower_hex(encoded_key) {
        return None;
    }
    let logical_key = String::from_utf8(hex::decode(encoded_key).ok()?).ok()?;
    let namespace = segments.join("/");
    if namespace.is_empty() {
        return None;
    }
    Some(ChunkObjectPath {
        namespace,
        logical_key,
        incarnation,
        version,
        digest_hex,
    })
}

fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn parse_zero_padded(segment: &str) -> Option<u64> {
    (segment.len() == 20 && segment.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| segment.parse().ok())
        .flatten()
}

/// A decoded control object: the inline v2 envelope or the v3
/// manifest. Every control-metadata path — CAS conflict enrichment,
/// `delete_if_match` conflict enrichment, `destroy`'s era read, the
/// read path's representation dispatch — decodes through this one
/// v2/v3-aware decoder (ADR 0004 §2.3). Decoding a v3 manifest never
/// fetches chunks.
pub(crate) enum ControlEnvelope {
    Inline(ValueEnvelope),
    Chunked(ValueManifest),
}

impl ControlEnvelope {
    pub(crate) fn decode(key: &str, encoded: &Bytes) -> Result<Self, KeyspaceError> {
        if encoded.starts_with(MANIFEST_MAGIC) {
            Ok(Self::Chunked(ValueManifest::decode(key, encoded)?))
        } else {
            Ok(Self::Inline(ValueEnvelope::decode(key, encoded)?))
        }
    }

    pub(crate) fn incarnation(&self) -> u64 {
        match self {
            Self::Inline(envelope) => envelope.incarnation,
            Self::Chunked(manifest) => manifest.incarnation,
        }
    }

    pub(crate) fn version(&self) -> u64 {
        match self {
            Self::Inline(envelope) => envelope.version,
            Self::Chunked(manifest) => manifest.version,
        }
    }
}

/// Mint a writer-scoped commit ID. Not logical content identity: it
/// distinguishes concurrent contenders for one target generation and
/// is retained only across retries of the same pending write
/// (ADR 0004 §1.2).
pub(crate) fn mint_commit_id() -> [u8; COMMIT_ID_BYTES] {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut hasher = Sha256::new();
    hasher.update(nanos.to_be_bytes());
    hasher.update(counter.to_be_bytes());
    hasher.update(std::process::id().to_be_bytes());
    let digest: [u8; 32] = hasher.finalize().into();
    let mut commit_id = [0u8; COMMIT_ID_BYTES];
    commit_id.copy_from_slice(&digest[..COMMIT_ID_BYTES]);
    commit_id
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_fixture(count: u32, final_len: u32) -> ValueManifest {
        let entries: Vec<ManifestEntry> = (0..count)
            .map(|index| {
                let encoded_len = if index == count - 1 {
                    final_len
                } else {
                    CHUNK_BYTES as u32
                };
                ManifestEntry {
                    encoded_len,
                    sha256: [index as u8; SHA256_BYTES],
                }
            })
            .collect();
        let logical_len: u64 = (count - 1) as u64 * CHUNK_BYTES as u64 + u64::from(final_len);
        ValueManifest {
            incarnation: 3,
            version: 7,
            commit_id: [0xAB; COMMIT_ID_BYTES],
            logical_len,
            value_root_sha256: ValueManifest::compute_value_root(
                logical_len,
                CHUNK_BYTES as u32,
                &entries,
            ),
            chunk_bytes: CHUNK_BYTES as u32,
            entries,
        }
    }

    #[test]
    fn manifest_round_trips_canonically() {
        let manifest = manifest_fixture(2, 512);
        let encoded = manifest.encode();
        assert_eq!(
            encoded.len(),
            MANIFEST_HEADER_BYTES + 2 * MANIFEST_ENTRY_BYTES
        );
        assert_eq!(ValueManifest::decode("k", &encoded).unwrap(), manifest);
    }

    #[test]
    fn manifest_rejects_one_chunk_and_zero() {
        for count in [0u32, 1] {
            let manifest = ValueManifest {
                incarnation: 0,
                version: 0,
                commit_id: [0; COMMIT_ID_BYTES],
                logical_len: u64::from(count) * CHUNK_BYTES as u64,
                value_root_sha256: [0; SHA256_BYTES],
                chunk_bytes: CHUNK_BYTES as u32,
                entries: Vec::new(),
            };
            let mut encoded = manifest.encode().to_vec();
            // Forgive the hand-built inconsistency: re-stamp the count
            // field only (entries empty), which decode must reject on
            // count before length.
            let count_at = MANIFEST_MAGIC.len()
                + U64_BYTES
                + U64_BYTES
                + U32_BYTES
                + COMMIT_ID_BYTES
                + U64_BYTES
                + SHA256_BYTES
                + U32_BYTES;
            encoded[count_at..count_at + U32_BYTES].copy_from_slice(&count.to_be_bytes());
            match ValueManifest::decode("k", &Bytes::from(encoded)) {
                Err(KeyspaceError::ChunkCountInvalid {
                    count: observed, ..
                }) => {
                    assert_eq!(observed, count);
                }
                other => panic!("count {count} must reject, got {other:?}"),
            }
        }
    }

    #[test]
    fn manifest_length_disagreement_rejects_before_allocation() {
        let manifest = manifest_fixture(MAX_CHUNKS, 512);
        let mut encoded = manifest.encode().to_vec();
        assert_eq!(
            encoded.len(),
            MANIFEST_HEADER_BYTES + MANIFEST_ENTRY_BYTES * MAX_CHUNKS as usize
        ); // header + 65,536 entries, ~2.25 MiB
        // Truncate the entry table: count stays MAX, bytes disagree.
        encoded.truncate(encoded.len() - MANIFEST_ENTRY_BYTES);
        assert!(matches!(
            ValueManifest::decode("k", &Bytes::from(encoded)),
            Err(KeyspaceError::ManifestMalformed(_))
        ));
    }

    #[test]
    fn manifest_size_and_logical_bounds_reject_before_parsing() {
        // Oversized manifest: refused on total length before any
        // count-derived allocation (A32 allocation table).
        let mut oversized = Vec::with_capacity(MAX_MANIFEST_BYTES + 1);
        oversized.extend_from_slice(MANIFEST_MAGIC);
        oversized.resize(MAX_MANIFEST_BYTES + 1, 0);
        match ValueManifest::decode("k", &Bytes::from(oversized)) {
            Err(KeyspaceError::ManifestTooLarge { len, max, .. }) => {
                assert_eq!(max as usize, MAX_MANIFEST_BYTES);
                assert_eq!(len as usize, MAX_MANIFEST_BYTES + 1);
            }
            other => panic!("oversized manifest must refuse typed, got {other:?}"),
        }
        // A logical length beyond the 1 TiB ceiling: the canonicality
        // sum cannot reach it (entries are bounded), so the check is
        // structural — exercised through the writer-side bound and the
        // chunk-count ceiling; the decode-side length agreement makes
        // an oversized logical_len unreachable in a canonical encode.
        let manifest = manifest_fixture(MAX_CHUNKS, CHUNK_BYTES as u32);
        assert_eq!(
            manifest.logical_len, MAX_LOGICAL_BYTES,
            "the maximum canonical encode is exactly the 1 TiB bound"
        );
        assert!(ValueManifest::decode("k", &manifest.encode()).is_ok());
    }

    #[test]
    fn manifest_bad_root_rejects() {
        let mut manifest = manifest_fixture(2, 512);
        manifest.value_root_sha256 = [0xFF; SHA256_BYTES];
        let encoded = manifest.encode();
        assert!(matches!(
            ValueManifest::decode("k", &encoded),
            Err(KeyspaceError::ManifestRootMismatch(_))
        ));
    }

    #[test]
    fn manifest_rejects_wrong_kind_chunk_bytes_and_final_len() {
        let manifest = manifest_fixture(3, 128);
        let encoded = manifest.encode();
        let kind_at = MANIFEST_MAGIC.len() + U64_BYTES + U64_BYTES;
        let mut wrong_kind = encoded.to_vec();
        wrong_kind[kind_at..kind_at + U32_BYTES].copy_from_slice(&99u32.to_be_bytes());
        assert!(matches!(
            ValueManifest::decode("k", &Bytes::from(wrong_kind)),
            Err(KeyspaceError::ManifestMalformed(_))
        ));
        let chunk_bytes_at = MANIFEST_MAGIC.len()
            + U64_BYTES
            + U64_BYTES
            + U32_BYTES
            + COMMIT_ID_BYTES
            + U64_BYTES
            + SHA256_BYTES;
        let mut wrong_chunk_bytes = encoded.to_vec();
        wrong_chunk_bytes[chunk_bytes_at..chunk_bytes_at + U32_BYTES]
            .copy_from_slice(&8u32.to_be_bytes());
        assert!(matches!(
            ValueManifest::decode("k", &Bytes::from(wrong_chunk_bytes)),
            Err(KeyspaceError::ManifestMalformed(_))
        ));
        // A non-final entry shorter than CHUNK_BYTES is non-canonical.
        let first_entry_len_at = MANIFEST_HEADER_BYTES;
        let mut short_entry = encoded.to_vec();
        short_entry[first_entry_len_at..first_entry_len_at + U32_BYTES]
            .copy_from_slice(&7u32.to_be_bytes());
        assert!(matches!(
            ValueManifest::decode("k", &Bytes::from(short_entry)),
            Err(KeyspaceError::ManifestMalformed(_))
        ));
    }

    #[test]
    fn chunk_paths_round_trip_and_refuse_other_encodings() {
        let key = chunk_object_key("ns", "a/b", 4, 9, &"ab".repeat(32));
        assert_eq!(
            key,
            "keyspace-chunks/v1/ns/612f62/00000000000000000004/00000000000000000009/".to_string()
                + &"ab".repeat(32)
        );
        let parsed = parse_chunk_object_key(&key).unwrap();
        assert_eq!(parsed.namespace, "ns");
        assert_eq!(parsed.logical_key, "a/b");
        assert_eq!(parsed.incarnation, 4);
        assert_eq!(parsed.version, 9);
        // Multi-segment namespaces parse (the namespace remainder).
        let key = chunk_object_key("streams/v1", "k", 0, 0, &"cd".repeat(32));
        let parsed = parse_chunk_object_key(&key).unwrap();
        assert_eq!(parsed.namespace, "streams/v1");
        assert_eq!(parsed.logical_key, "k");
        // Refusals: wrong digest width, non-hex key, missing
        // generation, empty namespace, odd-length hex.
        assert!(
            parse_chunk_object_key(
                "keyspace-chunks/v1/ns/612f/00000000000000000000/00000000000000000000/abcd"
            )
            .is_none()
        );
        let digest = "ab".repeat(32);
        assert!(
            parse_chunk_object_key(&format!(
                "keyspace-chunks/v1/ns/zz/00000000000000000000/00000000000000000000/{digest}"
            ))
            .is_none()
        );
        assert!(
            parse_chunk_object_key(&format!(
                "keyspace-chunks/v1/612f/00000000000000000000/{digest}"
            ))
            .is_none()
        );
        assert!(
            parse_chunk_object_key(&format!(
                "other-root/v1/ns/612f/00000000000000000000/00000000000000000000/{digest}"
            ))
            .is_none()
        );
    }

    #[test]
    fn worst_case_physical_chunk_key_is_892_bytes() {
        // 255-byte namespace + 255-byte key: the identifier-rule
        // maximum. 19 root/version + 255 ns + 1 + 510 hex key + 1 +
        // 20 incarnation + 1 + 20 version + 1 + 64 digest = 892.
        let namespace = "n".repeat(255);
        let key = "k".repeat(255);
        assert_eq!(key.len(), 255);
        let physical = chunk_object_key(&namespace, &key, u64::MAX, u64::MAX, &"0f".repeat(32));
        assert_eq!(physical.len(), 892);
        assert!(physical.len() <= 1024, "S3 key limit");
    }

    #[test]
    fn control_envelope_sniffs_v2_and_v3() {
        let v2 = ValueEnvelope::new(1, 2, Bytes::from_static(b"payload")).encode();
        match ControlEnvelope::decode("k", &v2).unwrap() {
            ControlEnvelope::Inline(envelope) => {
                assert_eq!(envelope.incarnation, 1);
                assert_eq!(envelope.version, 2);
                assert_eq!(envelope.payload, Bytes::from_static(b"payload"));
            }
            ControlEnvelope::Chunked(_) => panic!("v2 sniffed as v3"),
        }
        let v3 = manifest_fixture(2, 64).encode();
        match ControlEnvelope::decode("k", &v3).unwrap() {
            ControlEnvelope::Chunked(manifest) => assert_eq!(manifest.incarnation, 3),
            ControlEnvelope::Inline(_) => panic!("v3 sniffed as v2"),
        }
    }
}
