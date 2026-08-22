use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use yeetz_sdk_s3::{ObjectStoreClient, ObjectStoreError, S3Config};

use crate::tombstone::Tombstone;

const SUPPORTED_PROTOCOL_EPOCH: u16 = 1;
const RECORD_ENVELOPE: &str = "llm_gateway_state_record/v1";
const HEAD_ENVELOPE: &str = "llm_gateway_state_head/v1";
const CHECKPOINT_ENVELOPE: &str = "llm_gateway_state_checkpoint/v1";
const PROJECTION_ENVELOPE: &str = "llm_gateway_state_projection/v1";
const MAX_IDENTIFIER_BYTES: usize = 512;
static KERNEL_DIAGNOSTIC_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// The owning vocabulary chooses whether this lineage can advance after genesis.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SuccessorPolicy {
    GenesisOnly,
    SuccessorCapable,
}

/// A caller-owned lineage name and its closed update policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KernelLineage {
    value: String,
    successor_policy: SuccessorPolicy,
}

impl KernelLineage {
    pub fn new(
        value: impl Into<String>,
        successor_policy: SuccessorPolicy,
    ) -> Result<Self, KernelError> {
        let value = value.into();
        if !is_valid_lineage(&value) {
            return Err(KernelError::StateRecordMalformed {
                reference: SafeReference::invalid(),
            });
        }

        Ok(Self {
            value,
            successor_policy,
        })
    }

    fn same_identity(&self, other: &Self) -> bool {
        self.value == other.value
    }

    fn object_key(&self, digest: &RecordDigest) -> String {
        format!("{}/objects/{}", self.value, digest.as_str())
    }

    fn head_key(&self) -> String {
        format!("{}/head", self.value)
    }

    /// The existence witness for an intentionally deleted head
    pub(crate) fn tombstone_key(&self) -> String {
        format!("{}/tombstone", self.value)
    }

    fn checkpoint_key(&self, digest: &RecordDigest) -> String {
        format!("{}/checkpoints/{}", self.value, digest.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordDigest(String);

impl RecordDigest {
    fn of(bytes: &[u8]) -> Self {
        Self(hex::encode(Sha256::digest(bytes)))
    }

    fn parse(value: &str) -> Option<Self> {
        if value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            Some(Self(value.to_owned()))
        } else {
            None
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordPosition {
    generation: u64,
    digest: RecordDigest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalRecord {
    lineage: KernelLineage,
    sequence: u64,
    prior: Option<RecordPosition>,
    transition_type: String,
    transition_schema: String,
    payload: Vec<u8>,
    operation_id: String,
    actor_id: String,
    cause_id: String,
}

impl CanonicalRecord {
    /// The record's payload bytes (crate surface for the O(1)
    /// terminal read; ADR 0016).
    pub(crate) fn record_payload(&self) -> &[u8] {
        &self.payload
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        lineage: &KernelLineage,
        sequence: u64,
        prior: Option<RecordPosition>,
        transition_type: impl Into<String>,
        transition_schema: impl Into<String>,
        payload: Vec<u8>,
        operation_id: impl Into<String>,
        actor_id: impl Into<String>,
        cause_id: impl Into<String>,
    ) -> Result<Self, KernelError> {
        let transition_type = transition_type.into();
        let transition_schema = transition_schema.into();
        let operation_id = operation_id.into();
        let actor_id = actor_id.into();
        let cause_id = cause_id.into();

        if !is_valid_identifier(&transition_type)
            || !is_valid_identifier(&transition_schema)
            || !is_valid_identifier(&operation_id)
            || !is_valid_identifier(&actor_id)
            || !is_valid_identifier(&cause_id)
            || (sequence == 0 && prior.is_some())
            || (sequence > 0
                && prior
                    .as_ref()
                    .is_none_or(|position| position.generation != sequence - 1))
        {
            return Err(KernelError::StateRecordMalformed {
                reference: SafeReference::for_lineage(lineage),
            });
        }

        Ok(Self {
            lineage: lineage.clone(),
            sequence,
            prior,
            transition_type,
            transition_schema,
            payload,
            operation_id,
            actor_id,
            cause_id,
        })
    }

    fn canonical_bytes(&self) -> Result<Vec<u8>, KernelError> {
        serde_json::to_vec(&RecordWire {
            actor_id: self.actor_id.clone(),
            cause_id: self.cause_id.clone(),
            envelope: RECORD_ENVELOPE.to_owned(),
            lineage: self.lineage.value.clone(),
            operation_id: self.operation_id.clone(),
            payload_hex: hex::encode(&self.payload),
            prior: self.prior.as_ref().map(PositionWire::from),
            protocol_epoch: SUPPORTED_PROTOCOL_EPOCH,
            sequence: self.sequence,
            transition_schema: self.transition_schema.clone(),
            transition_type: self.transition_type.clone(),
        })
        .map_err(|_| KernelError::StateRecordMalformed {
            reference: self.reference(None),
        })
    }

    pub(crate) fn digest(&self) -> Result<RecordDigest, KernelError> {
        Ok(RecordDigest::of(&self.canonical_bytes()?))
    }

    pub fn record_position(&self) -> Result<RecordPosition, KernelError> {
        Ok(RecordPosition {
            generation: self.sequence,
            digest: self.digest()?,
        })
    }

    fn reference(&self, digest: Option<RecordDigest>) -> SafeReference {
        SafeReference {
            lineage: self.lineage.value.clone(),
            generation: Some(self.sequence),
            digest,
        }
    }

    fn fold_record(&self) -> FoldRecord<'_> {
        FoldRecord { record: self }
    }

    fn from_bytes(
        expected_lineage: &KernelLineage,
        expected_digest: &RecordDigest,
        bytes: &[u8],
    ) -> Result<Self, KernelError> {
        let reference = SafeReference::for_digest(expected_lineage, expected_digest.clone());
        if RecordDigest::of(bytes) != *expected_digest {
            return Err(KernelError::DigestMismatch { reference });
        }

        let wire: RecordWire =
            serde_json::from_slice(bytes).map_err(|_| KernelError::StateRecordMalformed {
                reference: SafeReference::for_digest(expected_lineage, expected_digest.clone()),
            })?;
        if wire.envelope != RECORD_ENVELOPE {
            return Err(KernelError::StateRecordMalformed {
                reference: SafeReference::for_digest(expected_lineage, expected_digest.clone()),
            });
        }
        if wire.protocol_epoch != SUPPORTED_PROTOCOL_EPOCH {
            return Err(KernelError::ProtocolEpochUnsupported {
                reference: SafeReference::for_digest(expected_lineage, expected_digest.clone()),
                observed: wire.protocol_epoch,
            });
        }
        if wire.lineage != expected_lineage.value {
            return Err(KernelError::StateRecordMalformed {
                reference: SafeReference::for_digest(expected_lineage, expected_digest.clone()),
            });
        }

        let prior = wire
            .prior
            .map(|position| position.into_position(expected_lineage, expected_digest))
            .transpose()?;
        let payload =
            hex::decode(&wire.payload_hex).map_err(|_| KernelError::StateRecordMalformed {
                reference: SafeReference::for_digest(expected_lineage, expected_digest.clone()),
            })?;
        let record = Self::new(
            expected_lineage,
            wire.sequence,
            prior,
            wire.transition_type,
            wire.transition_schema,
            payload,
            wire.operation_id,
            wire.actor_id,
            wire.cause_id,
        )?;
        if record.canonical_bytes()? != bytes {
            return Err(KernelError::StateRecordMalformed {
                reference: SafeReference::for_digest(expected_lineage, expected_digest.clone()),
            });
        }

        Ok(record)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalCheckpoint {
    lineage: KernelLineage,
    transition_schema: String,
    source_generation: u64,
    source_head_digest: RecordDigest,
    last_included_record_digest: RecordDigest,
    state_bytes: Vec<u8>,
}

impl CanonicalCheckpoint {
    pub fn new(
        lineage: &KernelLineage,
        transition_schema: impl Into<String>,
        source_generation: u64,
        source_head_digest: RecordDigest,
        last_included_record_digest: RecordDigest,
        state_bytes: Vec<u8>,
    ) -> Result<Self, KernelError> {
        let transition_schema = transition_schema.into();
        if !is_valid_identifier(&transition_schema) {
            return Err(KernelError::StateRecordMalformed {
                reference: SafeReference::for_lineage(lineage),
            });
        }

        Ok(Self {
            lineage: lineage.clone(),
            transition_schema,
            source_generation,
            source_head_digest,
            last_included_record_digest,
            state_bytes,
        })
    }

    fn canonical_bytes(&self) -> Result<Vec<u8>, KernelError> {
        serde_json::to_vec(&CheckpointWire {
            envelope: CHECKPOINT_ENVELOPE.to_owned(),
            last_included_record_digest: self.last_included_record_digest.0.clone(),
            lineage: self.lineage.value.clone(),
            protocol_epoch: SUPPORTED_PROTOCOL_EPOCH,
            source_generation: self.source_generation,
            source_head_digest: self.source_head_digest.0.clone(),
            state_hex: hex::encode(&self.state_bytes),
            transition_schema: self.transition_schema.clone(),
        })
        .map_err(|_| KernelError::StateRecordMalformed {
            reference: SafeReference::for_lineage(&self.lineage),
        })
    }

    pub(crate) fn digest(&self) -> Result<RecordDigest, KernelError> {
        Ok(RecordDigest::of(&self.canonical_bytes()?))
    }

    fn from_bytes(
        expected_lineage: &KernelLineage,
        expected_digest: &RecordDigest,
        bytes: &[u8],
    ) -> Result<Self, KernelError> {
        let reference = SafeReference::for_digest(expected_lineage, expected_digest.clone());
        if RecordDigest::of(bytes) != *expected_digest {
            return Err(KernelError::DigestMismatch { reference });
        }

        let wire: CheckpointWire =
            serde_json::from_slice(bytes).map_err(|_| KernelError::StateRecordMalformed {
                reference: SafeReference::for_digest(expected_lineage, expected_digest.clone()),
            })?;
        if wire.envelope != CHECKPOINT_ENVELOPE || wire.lineage != expected_lineage.value {
            return Err(KernelError::StateRecordMalformed {
                reference: SafeReference::for_digest(expected_lineage, expected_digest.clone()),
            });
        }
        if wire.protocol_epoch != SUPPORTED_PROTOCOL_EPOCH {
            return Err(KernelError::ProtocolEpochUnsupported {
                reference: SafeReference::for_digest(expected_lineage, expected_digest.clone()),
                observed: wire.protocol_epoch,
            });
        }

        let source_head_digest =
            RecordDigest::parse(&wire.source_head_digest).ok_or_else(|| {
                KernelError::StateRecordMalformed {
                    reference: SafeReference::for_digest(expected_lineage, expected_digest.clone()),
                }
            })?;
        let last_included_record_digest = RecordDigest::parse(&wire.last_included_record_digest)
            .ok_or_else(|| KernelError::StateRecordMalformed {
                reference: SafeReference::for_digest(expected_lineage, expected_digest.clone()),
            })?;
        let state_bytes =
            hex::decode(&wire.state_hex).map_err(|_| KernelError::StateRecordMalformed {
                reference: SafeReference::for_digest(expected_lineage, expected_digest.clone()),
            })?;
        let checkpoint = Self::new(
            expected_lineage,
            wire.transition_schema,
            wire.source_generation,
            source_head_digest,
            last_included_record_digest,
            state_bytes,
        )?;
        if checkpoint.canonical_bytes()? != bytes {
            return Err(KernelError::StateRecordMalformed {
                reference: SafeReference::for_digest(expected_lineage, expected_digest.clone()),
            });
        }

        Ok(checkpoint)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CanonicalHead {
    lineage: KernelLineage,
    generation: u64,
    pub(crate) record_digest: RecordDigest,
    prior: Option<RecordPosition>,
}

impl CanonicalHead {
    fn canonical_bytes(&self) -> Result<Vec<u8>, KernelError> {
        serde_json::to_vec(&HeadWire {
            envelope: HEAD_ENVELOPE.to_owned(),
            generation: self.generation,
            lineage: self.lineage.value.clone(),
            prior: self.prior.as_ref().map(PositionWire::from),
            record_digest: self.record_digest.0.clone(),
        })
        .map_err(|_| KernelError::StateRecordMalformed {
            reference: self.reference(),
        })
    }

    fn reference(&self) -> SafeReference {
        SafeReference {
            lineage: self.lineage.value.clone(),
            generation: Some(self.generation),
            digest: Some(self.record_digest.clone()),
        }
    }

    fn position(&self) -> RecordPosition {
        RecordPosition {
            generation: self.generation,
            digest: self.record_digest.clone(),
        }
    }

    fn same_identity(&self, other: &Self) -> bool {
        self.lineage.same_identity(&other.lineage)
            && self.generation == other.generation
            && self.record_digest == other.record_digest
            && self.prior == other.prior
    }

    fn from_bytes(expected_lineage: &KernelLineage, bytes: &[u8]) -> Result<Self, KernelError> {
        let wire: HeadWire =
            serde_json::from_slice(bytes).map_err(|_| KernelError::StateRecordMalformed {
                reference: SafeReference::for_lineage(expected_lineage),
            })?;
        if wire.envelope != HEAD_ENVELOPE || wire.lineage != expected_lineage.value {
            return Err(KernelError::StateRecordMalformed {
                reference: SafeReference::for_lineage(expected_lineage),
            });
        }
        let record_digest = RecordDigest::parse(&wire.record_digest).ok_or_else(|| {
            KernelError::StateRecordMalformed {
                reference: SafeReference::for_lineage(expected_lineage),
            }
        })?;
        let prior = wire
            .prior
            .map(|position| position.into_position(expected_lineage, &record_digest))
            .transpose()?;
        if (wire.generation == 0 && prior.is_some())
            || (wire.generation > 0
                && prior
                    .as_ref()
                    .is_none_or(|position| position.generation != wire.generation - 1))
        {
            return Err(KernelError::StateRecordMalformed {
                reference: SafeReference::for_lineage(expected_lineage),
            });
        }

        let head = Self {
            lineage: expected_lineage.clone(),
            generation: wire.generation,
            record_digest,
            prior,
        };
        if head.canonical_bytes()? != bytes {
            return Err(KernelError::StateRecordMalformed {
                reference: SafeReference::for_lineage(expected_lineage),
            });
        }

        Ok(head)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeadRead {
    pub(crate) head: CanonicalHead,
    pub(crate) etag: String,
}

impl HeadRead {
    pub fn generation(&self) -> u64 {
        self.head.generation
    }

    pub fn record_digest(&self) -> &RecordDigest {
        &self.head.record_digest
    }

    pub fn record_position(&self) -> RecordPosition {
        self.head.position()
    }

    pub fn reference(&self) -> SafeReference {
        self.head.reference()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SafeReference {
    lineage: String,
    generation: Option<u64>,
    digest: Option<RecordDigest>,
}

impl SafeReference {
    fn invalid() -> Self {
        Self {
            lineage: "invalid".to_owned(),
            generation: None,
            digest: None,
        }
    }

    pub(crate) fn for_lineage(lineage: &KernelLineage) -> Self {
        Self {
            lineage: lineage.value.clone(),
            generation: None,
            digest: None,
        }
    }

    pub(crate) fn for_digest(lineage: &KernelLineage, digest: RecordDigest) -> Self {
        Self {
            lineage: lineage.value.clone(),
            generation: None,
            digest: Some(digest),
        }
    }

    pub fn lineage(&self) -> &str {
        &self.lineage
    }

    pub fn generation(&self) -> Option<u64> {
        self.generation
    }

    pub fn digest(&self) -> Option<&RecordDigest> {
        self.digest.as_ref()
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum KernelError {
    #[error("DIGEST_MISMATCH")]
    DigestMismatch { reference: SafeReference },
    #[error("LINEAGE_HEAD_CONFLICT")]
    LineageHeadConflict { current: Option<SafeReference> },
    #[error("STATE_RECORD_MALFORMED")]
    StateRecordMalformed { reference: SafeReference },
    #[error("STATE_HISTORY_INCOMPLETE")]
    StateHistoryIncomplete { reference: SafeReference },
    #[error("PROTOCOL_EPOCH_UNSUPPORTED")]
    ProtocolEpochUnsupported {
        reference: SafeReference,
        observed: u16,
    },
    #[error("SUCCESSOR_NOT_ALLOWED")]
    SuccessorNotAllowed { reference: SafeReference },
    /// A lineage tombstone is present but malformed or uses an
    /// unsupported format — stored evidence corruption, distinct
    /// from absence (batch 6).
    #[error("TOMBSTONE_CORRUPT")]
    TombstoneCorrupt { reference: SafeReference },
    #[error("GATEWAY_STATE_UNAVAILABLE")]
    StateUnavailable { operation: &'static str },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointRejectionCode {
    DigestMismatch,
    StateRecordMalformed,
    StateHistoryIncomplete,
    ProtocolEpochUnsupported,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointRejection {
    pub code: CheckpointRejectionCode,
    pub reference: SafeReference,
}

pub struct FoldRecord<'a> {
    record: &'a CanonicalRecord,
}

impl FoldRecord<'_> {
    pub fn sequence(&self) -> u64 {
        self.record.sequence
    }

    pub fn transition_schema(&self) -> &str {
        &self.record.transition_schema
    }

    pub fn transition_type(&self) -> &str {
        &self.record.transition_type
    }

    pub fn payload(&self) -> &[u8] {
        &self.record.payload
    }

    pub fn reference(&self) -> SafeReference {
        self.record.reference(None)
    }
}

/// The gateway vocabulary owns schema validation and state meaning.
pub trait LineageFold {
    type State;

    fn validate_transition(&self, record: &FoldRecord<'_>) -> Result<(), ()>;
    fn initial_state(&self) -> Self::State;
    fn apply(&self, state: &mut Self::State, record: &FoldRecord<'_>) -> Result<(), ()>;
    fn canonical_state(&self, state: &Self::State) -> Result<Vec<u8>, ()>;
    fn restore_checkpoint(
        &self,
        transition_schema: &str,
        state_bytes: &[u8],
    ) -> Result<Self::State, ()>;
}

#[derive(Debug)]
pub struct FoldOutcome<S> {
    pub state: S,
    pub canonical_state: Vec<u8>,
    pub records: Vec<SafeReference>,
    pub checkpoint_rejections: Vec<CheckpointRejection>,
    pub projection: ProjectionDisposition,
}

/// A projection can only accelerate a fold when its content-addressed history
/// still agrees with the authoritative head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionDisposition {
    Absent,
    Used,
    Discarded,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionSource {
    lineage: String,
    generation: u64,
    digest: RecordDigest,
}

impl ProjectionSource {
    fn from_head(head: &CanonicalHead) -> Self {
        Self {
            lineage: head.lineage.value.clone(),
            generation: head.generation,
            digest: head.record_digest.clone(),
        }
    }

    fn matches(&self, head: &CanonicalHead) -> bool {
        self.lineage == head.lineage.value
            && self.generation == head.generation
            && self.digest == head.record_digest
    }

    pub fn lineage(&self) -> &str {
        &self.lineage
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn digest(&self) -> &RecordDigest {
        &self.digest
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProjectedRecord {
    digest: RecordDigest,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectionWire {
    envelope: String,
    records: Vec<ProjectedRecordWire>,
    source_digest: String,
    source_generation: u64,
    source_lineage: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectedRecordWire {
    bytes_hex: String,
    digest: String,
}

/// A disposable, content-addressed copy of one authoritative lineage history.
///
/// The projection never becomes a source of truth: it is accepted only when
/// every copied record validates against the just-read authoritative head.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FoldProjection {
    source: ProjectionSource,
    records: Vec<ProjectedRecord>,
}

impl FoldProjection {
    fn from_authoritative(
        head: &CanonicalHead,
        records: &[CanonicalRecord],
    ) -> Result<Self, KernelError> {
        let records = records
            .iter()
            .map(|record| {
                let bytes = record.canonical_bytes()?;
                Ok(ProjectedRecord {
                    digest: RecordDigest::of(&bytes),
                    bytes,
                })
            })
            .collect::<Result<Vec<_>, KernelError>>()?;
        Ok(Self {
            source: ProjectionSource::from_head(head),
            records,
        })
    }

    pub fn source(&self) -> &ProjectionSource {
        &self.source
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, KernelError> {
        let lineage = KernelLineage::new(
            self.source.lineage.clone(),
            SuccessorPolicy::SuccessorCapable,
        )
        .map_err(|_| KernelError::StateRecordMalformed {
            reference: SafeReference::invalid(),
        })?;
        self.records_for_source(&lineage)?;
        serde_json::to_vec(&ProjectionWire {
            envelope: PROJECTION_ENVELOPE.to_owned(),
            records: self
                .records
                .iter()
                .map(|record| ProjectedRecordWire {
                    bytes_hex: hex::encode(&record.bytes),
                    digest: record.digest.as_str().to_owned(),
                })
                .collect(),
            source_digest: self.source.digest.as_str().to_owned(),
            source_generation: self.source.generation,
            source_lineage: self.source.lineage.clone(),
        })
        .map_err(|_| KernelError::StateRecordMalformed {
            reference: SafeReference::for_lineage(&lineage),
        })
    }

    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, KernelError> {
        let wire: ProjectionWire =
            serde_json::from_slice(bytes).map_err(|_| KernelError::StateRecordMalformed {
                reference: SafeReference::invalid(),
            })?;
        if wire.envelope != PROJECTION_ENVELOPE {
            return Err(KernelError::StateRecordMalformed {
                reference: SafeReference::invalid(),
            });
        }

        let lineage = KernelLineage::new(wire.source_lineage, SuccessorPolicy::SuccessorCapable)
            .map_err(|_| KernelError::StateRecordMalformed {
                reference: SafeReference::invalid(),
            })?;
        let source_digest = RecordDigest::parse(&wire.source_digest).ok_or_else(|| {
            KernelError::StateRecordMalformed {
                reference: SafeReference::for_lineage(&lineage),
            }
        })?;
        let reference = SafeReference::for_digest(&lineage, source_digest.clone());
        let records = wire
            .records
            .into_iter()
            .map(|record| {
                let digest = RecordDigest::parse(&record.digest).ok_or_else(|| {
                    KernelError::StateRecordMalformed {
                        reference: reference.clone(),
                    }
                })?;
                let bytes = hex::decode(&record.bytes_hex).map_err(|_| {
                    KernelError::StateRecordMalformed {
                        reference: SafeReference::for_digest(&lineage, digest.clone()),
                    }
                })?;
                Ok(ProjectedRecord { digest, bytes })
            })
            .collect::<Result<Vec<_>, KernelError>>()?;
        let projection = Self {
            source: ProjectionSource {
                lineage: lineage.value.clone(),
                generation: wire.source_generation,
                digest: source_digest,
            },
            records,
        };
        if projection.canonical_bytes()? != bytes {
            return Err(KernelError::StateRecordMalformed { reference });
        }
        projection.records_for_source(&lineage)?;
        Ok(projection)
    }

    fn records_for_source(
        &self,
        lineage: &KernelLineage,
    ) -> Result<Vec<CanonicalRecord>, KernelError> {
        if self.source.lineage != lineage.value {
            return Err(KernelError::StateHistoryIncomplete {
                reference: SafeReference::for_lineage(lineage),
            });
        }

        let expected_count = usize::try_from(self.source.generation)
            .ok()
            .and_then(|generation| generation.checked_add(1))
            .ok_or_else(|| KernelError::StateHistoryIncomplete {
                reference: SafeReference::for_lineage(lineage),
            })?;
        let mut indexed = BTreeMap::new();
        for projected in &self.records {
            if indexed
                .insert(projected.digest.clone(), projected)
                .is_some()
            {
                return Err(KernelError::StateHistoryIncomplete {
                    reference: SafeReference::for_digest(lineage, projected.digest.clone()),
                });
            }
        }
        if indexed.len() != expected_count {
            return Err(KernelError::StateHistoryIncomplete {
                reference: SafeReference::for_lineage(lineage),
            });
        }

        let mut records = Vec::new();
        let mut seen = BTreeSet::new();
        let mut expected_generation = self.source.generation;
        let mut next_digest = self.source.digest.clone();
        loop {
            if !seen.insert(next_digest.clone()) {
                return Err(KernelError::StateHistoryIncomplete {
                    reference: SafeReference::for_digest(lineage, next_digest),
                });
            }
            let projected =
                indexed
                    .get(&next_digest)
                    .ok_or_else(|| KernelError::StateHistoryIncomplete {
                        reference: SafeReference::for_digest(lineage, next_digest.clone()),
                    })?;
            let record = CanonicalRecord::from_bytes(lineage, &next_digest, &projected.bytes)?;
            let record_digest = record.digest()?;
            if record_digest != next_digest || record.sequence != expected_generation {
                return Err(KernelError::StateHistoryIncomplete {
                    reference: record.reference(Some(next_digest)),
                });
            }

            match &record.prior {
                None if expected_generation == 0 => {
                    records.push(record);
                    break;
                }
                Some(prior)
                    if expected_generation > 0 && prior.generation == expected_generation - 1 =>
                {
                    next_digest = prior.digest.clone();
                    expected_generation -= 1;
                    records.push(record);
                }
                _ => {
                    return Err(KernelError::StateHistoryIncomplete {
                        reference: record.reference(Some(record_digest)),
                    });
                }
            }
        }
        if seen.len() != self.records.len() {
            return Err(KernelError::StateHistoryIncomplete {
                reference: SafeReference::for_lineage(lineage),
            });
        }

        records.reverse();
        for (expected_generation, (record, projected)) in
            records.iter().zip(&self.records).enumerate()
        {
            let expected_generation = u64::try_from(expected_generation).map_err(|_| {
                KernelError::StateHistoryIncomplete {
                    reference: SafeReference::for_lineage(lineage),
                }
            })?;
            if record.sequence != expected_generation || record.digest()? != projected.digest {
                return Err(KernelError::StateHistoryIncomplete {
                    reference: SafeReference::for_digest(lineage, projected.digest.clone()),
                });
            }
        }
        Ok(records)
    }
}

/// One validated lineage bound to the kernel-owned store. Construction
/// stays behind [`KernelHandle`].
///
/// ```compile_fail
/// use std::sync::Arc;
/// use yeetz_s3_kernel::state_kernel::{KernelLineage, StateKernel};
/// use yeetz_sdk_s3::ObjectStoreClient;
///
/// fn bypass(store: Arc<ObjectStoreClient>, lineage: KernelLineage) {
///     let _ = StateKernel::new(store, lineage);
/// }
/// ```
pub struct StateKernel {
    pub(crate) object_store: Arc<ObjectStoreClient>,
    pub(crate) lineage: KernelLineage,
}

/// Opaque access to one kernel-owned object store.
///
/// Applications can bind lineages and keyspaces through this handle, but the
/// backing adapter type never crosses into application code.
#[derive(Clone)]
pub struct KernelHandle {
    object_store: Arc<ObjectStoreClient>,
}

impl std::fmt::Debug for KernelHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KernelHandle")
            .finish_non_exhaustive()
    }
}

/// Failure to construct the kernel-owned object-store adapter.
#[derive(Debug, Error)]
#[error("kernel store initialization failed: {message}")]
pub struct KernelInitError {
    message: String,
}

impl KernelHandle {
    /// Construct the storage adapter inside the kernel closure.
    pub fn from_s3_config(config: &S3Config) -> Result<Self, KernelInitError> {
        let object_store = ObjectStoreClient::new(config).map_err(|error| KernelInitError {
            message: error.to_string(),
        })?;
        Ok(Self {
            object_store: Arc::new(object_store),
        })
    }

    /// Construct an isolated in-memory kernel store for tests and rigs.
    #[cfg(feature = "test-support")]
    #[must_use]
    pub fn with_in_memory_store(name: impl Into<String>) -> Self {
        Self {
            object_store: Arc::new(ObjectStoreClient::in_memory(name)),
        }
    }

    /// Bind one validated lineage to this store.
    #[must_use]
    pub fn state_kernel(&self, lineage: KernelLineage) -> StateKernel {
        StateKernel::new(Arc::clone(&self.object_store), lineage)
    }

    /// Bind one validated atomic keyspace to this store.
    pub fn atomic_keyspace(
        &self,
        namespace: &str,
    ) -> Result<crate::AtomicKeyspace, crate::KeyspaceError> {
        crate::AtomicKeyspace::new(Arc::clone(&self.object_store), namespace)
    }

    /// Whether any object exists below a lineage prefix. Test inspection stays
    /// inside the kernel so callers never parse or list its private layout.
    #[cfg(feature = "test-support")]
    pub async fn has_lineage_prefix_for_test(&self, prefix: &str) -> Result<bool, KernelError> {
        let keys = self.object_store.list_prefix(prefix).await.map_err(|_| {
            KernelError::StateUnavailable {
                operation: "test lineage-prefix inspection",
            }
        })?;
        Ok(!keys.is_empty())
    }
}

impl StateKernel {
    fn new(object_store: Arc<ObjectStoreClient>, lineage: KernelLineage) -> Self {
        Self {
            object_store,
            lineage,
        }
    }

    /// Intentional deletion with an existence witness (batch 6): an
    /// immutable tombstone at `{lineage}/tombstone` —
    /// `{deleted_at_gen, cause, actor, ts}`, `deleted_at_gen` = the
    /// head's generation — is written BEFORE the head object is
    /// deleted. Idempotent for an already-destroyed lineage (the
    /// first tombstone stands); a no-op for a never-created one. A
    /// re-created lineage (fresh genesis) supersedes the tombstone:
    /// the new head's existence IS the truth; the witness remains as
    /// history. Records are NOT deleted — repair-by-replay stays
    /// possible for a reborn lineage; sweeping them is a separate,
    /// deliberately-unshipped operation.
    pub async fn destroy(&self, cause: &str, actor: &str) -> Result<(), KernelError> {
        let loaded = match self.load_head().await {
            Ok(loaded) => loaded,
            Err(KernelError::StateHistoryIncomplete { .. }) => return Ok(()),
            Err(err) => return Err(err),
        };
        let tombstone = Tombstone::new(0, loaded.head.generation, cause, actor);
        let bytes = tombstone
            .encode()
            .map_err(|_| KernelError::StateUnavailable {
                operation: "tombstone encode",
            })?;
        match self
            .object_store
            .upload_conditional(&self.lineage.tombstone_key(), bytes.into(), None)
            .await
        {
            // Put-if-absent; PreconditionFailed = an earlier
            // lifetime's tombstone stands (immutable history).
            Ok(_) | Err(ObjectStoreError::PreconditionFailed(_)) => {}
            Err(_) => {
                return Err(KernelError::StateUnavailable {
                    operation: "tombstone conditional create",
                });
            }
        }
        self.object_store
            .delete(&self.lineage.head_key())
            .await
            .map_err(|_| KernelError::StateUnavailable {
                operation: "head deletion",
            })
    }

    /// Remove every object in this lineage for a rebuild/corruption test.
    /// Production code cannot enable this surface.
    #[cfg(feature = "test-support")]
    pub async fn destroy_lineage_for_test(&self) -> Result<(), KernelError> {
        let prefix = format!("{}/", self.lineage.value);
        let keys = self.object_store.list_prefix(&prefix).await.map_err(|_| {
            KernelError::StateUnavailable {
                operation: "test lineage listing",
            }
        })?;
        for key in keys {
            self.object_store
                .delete(&key)
                .await
                .map_err(|_| KernelError::StateUnavailable {
                    operation: "test lineage deletion",
                })?;
        }
        Ok(())
    }

    /// Remove the terminal record while retaining its head, producing the
    /// canonical incomplete-history shape for negative tests.
    #[cfg(feature = "test-support")]
    pub async fn destroy_terminal_record_for_test(&self) -> Result<(), KernelError> {
        let head = self.read_head().await?;
        let key = self.lineage.object_key(head.record_digest());
        self.object_store
            .delete(&key)
            .await
            .map_err(|_| KernelError::StateUnavailable {
                operation: "test terminal-record deletion",
            })
    }

    pub async fn publish_record(
        &self,
        record: &CanonicalRecord,
    ) -> Result<RecordDigest, KernelError> {
        self.ensure_record_lineage(record)?;
        let bytes = record.canonical_bytes()?;
        let digest = RecordDigest::of(&bytes);
        self.publish_immutable(
            self.lineage.object_key(&digest),
            bytes,
            record.reference(Some(digest.clone())),
        )
        .await?;
        Ok(digest)
    }

    pub async fn publish_checkpoint(
        &self,
        checkpoint: &CanonicalCheckpoint,
    ) -> Result<RecordDigest, KernelError> {
        if !self.lineage.same_identity(&checkpoint.lineage) {
            return Err(KernelError::StateRecordMalformed {
                reference: SafeReference::for_lineage(&self.lineage),
            });
        }
        let bytes = checkpoint.canonical_bytes()?;
        let digest = RecordDigest::of(&bytes);
        self.publish_immutable(
            self.lineage.checkpoint_key(&digest),
            bytes,
            SafeReference::for_digest(&self.lineage, digest.clone()),
        )
        .await?;
        Ok(digest)
    }

    pub async fn append_genesis(&self, record: &CanonicalRecord) -> Result<HeadRead, KernelError> {
        self.ensure_record_lineage(record)?;
        if record.sequence != 0 || record.prior.is_some() {
            return Err(KernelError::StateRecordMalformed {
                reference: record.reference(None),
            });
        }

        let record_digest = self.publish_record(record).await?;
        let head = CanonicalHead {
            lineage: self.lineage.clone(),
            generation: 0,
            record_digest,
            prior: None,
        };
        self.create_head(head).await
    }

    /// Publish a contiguous immutable record batch, then make the terminal
    /// record current with one genesis-head conditional create. A failed head
    /// create can leave immutable objects behind, but none becomes reachable
    /// without the one winning head update.
    pub async fn append_genesis_batch(
        &self,
        records: &[CanonicalRecord],
    ) -> Result<HeadRead, KernelError> {
        let terminal = self.validate_record_batch(records, 0, None)?;
        for record in records {
            self.publish_record(record).await?;
        }
        let head = CanonicalHead {
            lineage: self.lineage.clone(),
            generation: terminal.generation,
            record_digest: terminal.digest,
            prior: records.last().and_then(|record| record.prior.clone()),
        };
        self.create_head(head).await
    }

    pub async fn read_head(&self) -> Result<HeadRead, KernelError> {
        let loaded = self.load_head().await?;
        let etag = loaded.etag.ok_or(KernelError::StateUnavailable {
            operation: "head read did not return an ETag",
        })?;
        Ok(HeadRead {
            head: loaded.head,
            etag,
        })
    }

    pub async fn append_successor(
        &self,
        record: &CanonicalRecord,
        expected: &HeadRead,
    ) -> Result<HeadRead, KernelError> {
        if self.lineage.successor_policy == SuccessorPolicy::GenesisOnly {
            return Err(KernelError::SuccessorNotAllowed {
                reference: SafeReference::for_lineage(&self.lineage),
            });
        }
        self.ensure_record_lineage(record)?;
        if !self.lineage.same_identity(&expected.head.lineage) {
            return Err(self.head_conflict().await);
        }

        let current = self.load_head().await?;
        let current_etag = current
            .etag
            .as_deref()
            .ok_or(KernelError::StateUnavailable {
                operation: "head read did not return an ETag",
            })?;
        if !current.head.same_identity(&expected.head) || current_etag != expected.etag {
            return Err(KernelError::LineageHeadConflict {
                current: Some(current.head.reference()),
            });
        }

        let next_generation = expected.head.generation.checked_add(1).ok_or_else(|| {
            KernelError::StateRecordMalformed {
                reference: expected.head.reference(),
            }
        })?;
        let expected_prior = expected.head.position();
        if record.sequence != next_generation || record.prior.as_ref() != Some(&expected_prior) {
            return Err(KernelError::StateRecordMalformed {
                reference: record.reference(None),
            });
        }

        let record_digest = self.publish_record(record).await?;
        let head = CanonicalHead {
            lineage: self.lineage.clone(),
            generation: next_generation,
            record_digest,
            prior: Some(expected_prior),
        };
        self.replace_head(head, &expected.etag).await
    }

    /// Publish a contiguous immutable successor batch, then advance the head
    /// exactly once to its terminal record. The batch shares the same expected
    /// head and ETag, so a loser has no partially visible successor state.
    pub async fn append_successor_batch(
        &self,
        records: &[CanonicalRecord],
        expected: &HeadRead,
    ) -> Result<HeadRead, KernelError> {
        if self.lineage.successor_policy == SuccessorPolicy::GenesisOnly {
            return Err(KernelError::SuccessorNotAllowed {
                reference: SafeReference::for_lineage(&self.lineage),
            });
        }
        if !self.lineage.same_identity(&expected.head.lineage) {
            return Err(self.head_conflict().await);
        }

        let current = self.load_head().await?;
        let current_etag = current
            .etag
            .as_deref()
            .ok_or(KernelError::StateUnavailable {
                operation: "head read did not return an ETag",
            })?;
        if !current.head.same_identity(&expected.head) || current_etag != expected.etag {
            return Err(KernelError::LineageHeadConflict {
                current: Some(current.head.reference()),
            });
        }

        let start_generation = expected.head.generation.checked_add(1).ok_or_else(|| {
            KernelError::StateRecordMalformed {
                reference: expected.head.reference(),
            }
        })?;
        let terminal =
            self.validate_record_batch(records, start_generation, Some(expected.head.position()))?;
        for record in records {
            self.publish_record(record).await?;
        }
        let head = CanonicalHead {
            lineage: self.lineage.clone(),
            generation: terminal.generation,
            record_digest: terminal.digest,
            prior: records.last().and_then(|record| record.prior.clone()),
        };
        self.replace_head(head, &expected.etag).await
    }

    fn validate_record_batch(
        &self,
        records: &[CanonicalRecord],
        first_generation: u64,
        first_prior: Option<RecordPosition>,
    ) -> Result<RecordPosition, KernelError> {
        let mut generation = first_generation;
        let mut prior = first_prior;
        for record in records {
            self.ensure_record_lineage(record)?;
            if record.sequence != generation || record.prior != prior {
                return Err(KernelError::StateRecordMalformed {
                    reference: record.reference(None),
                });
            }
            prior = Some(RecordPosition {
                generation,
                digest: record.digest()?,
            });
            generation =
                generation
                    .checked_add(1)
                    .ok_or_else(|| KernelError::StateRecordMalformed {
                        reference: record.reference(None),
                    })?;
        }
        prior.ok_or_else(|| KernelError::StateRecordMalformed {
            reference: SafeReference::for_lineage(&self.lineage),
        })
    }

    pub async fn fold<F>(
        &self,
        checkpoint_digest: Option<&RecordDigest>,
        folder: &F,
    ) -> Result<FoldOutcome<F::State>, KernelError>
    where
        F: LineageFold,
    {
        let loaded_head = self.load_head().await?;
        let records = self.load_history(&loaded_head.head).await?;
        self.fold_records(
            &loaded_head.head,
            records,
            checkpoint_digest,
            folder,
            ProjectionDisposition::Absent,
        )
        .await
    }

    /// Fold an immutable prefix at an exact committed generation. The caller
    /// owns the meaning of that coordinate; the kernel only rebuilds the
    /// validated canonical prefix and never treats a newer head as equivalent.
    pub async fn fold_at_generation<F>(
        &self,
        generation: u64,
        folder: &F,
    ) -> Result<FoldOutcome<F::State>, KernelError>
    where
        F: LineageFold,
    {
        let loaded_head = self.load_head().await?;
        if generation > loaded_head.head.generation {
            return Err(KernelError::StateHistoryIncomplete {
                reference: loaded_head.head.reference(),
            });
        }

        let mut records = self.load_history(&loaded_head.head).await?;
        let prefix_len = usize::try_from(generation)
            .ok()
            .and_then(|generation| generation.checked_add(1))
            .ok_or_else(|| KernelError::StateHistoryIncomplete {
                reference: loaded_head.head.reference(),
            })?;
        records.truncate(prefix_len);
        let terminal = records
            .last()
            .ok_or_else(|| KernelError::StateHistoryIncomplete {
                reference: loaded_head.head.reference(),
            })?;
        let head = CanonicalHead {
            lineage: self.lineage.clone(),
            generation,
            record_digest: terminal.digest()?,
            prior: terminal.prior.clone(),
        };
        self.fold_records(&head, records, None, folder, ProjectionDisposition::Absent)
            .await
    }

    /// Rebuild a disposable candidate from the canonical S3 head and history.
    pub async fn rebuild_projection(&self) -> Result<FoldProjection, KernelError> {
        let loaded_head = self.load_head().await?;
        let records = self.load_history(&loaded_head.head).await?;
        FoldProjection::from_authoritative(&loaded_head.head, &records)
    }

    /// Fold through a projection only when it is an exact, validated copy of
    /// the just-read canonical lineage. Any defect discards it and rereads S3.
    pub async fn fold_with_projection<F>(
        &self,
        projection: Option<&FoldProjection>,
        folder: &F,
    ) -> Result<FoldOutcome<F::State>, KernelError>
    where
        F: LineageFold,
    {
        let loaded_head = self.load_head().await?;
        let (records, projection) = match projection {
            Some(projection) => match self.load_projected_history(projection, &loaded_head.head) {
                Ok(records) => (records, ProjectionDisposition::Used),
                Err(_) => (
                    self.load_history(&loaded_head.head).await?,
                    ProjectionDisposition::Discarded,
                ),
            },
            None => (
                self.load_history(&loaded_head.head).await?,
                ProjectionDisposition::Absent,
            ),
        };
        self.fold_records(&loaded_head.head, records, None, folder, projection)
            .await
    }

    async fn fold_records<F>(
        &self,
        head: &CanonicalHead,
        records: Vec<CanonicalRecord>,
        checkpoint_digest: Option<&RecordDigest>,
        folder: &F,
        projection: ProjectionDisposition,
    ) -> Result<FoldOutcome<F::State>, KernelError>
    where
        F: LineageFold,
    {
        self.validate_transitions(&records, folder)?;

        // The complete genesis fold is authoritative even when a checkpoint is valid.
        let (state, canonical_state) = self.fold_full(&records, folder)?;
        let mut checkpoint_rejections = Vec::new();
        if let Some(checkpoint_digest) = checkpoint_digest {
            self.check_checkpoint(
                checkpoint_digest,
                head,
                &records,
                &canonical_state,
                folder,
                &mut checkpoint_rejections,
            )
            .await?;
        }

        let references = records
            .iter()
            .map(|record| record.reference(record.digest().ok()))
            .collect();
        Ok(FoldOutcome {
            state,
            canonical_state,
            records: references,
            checkpoint_rejections,
            projection,
        })
    }

    fn ensure_record_lineage(&self, record: &CanonicalRecord) -> Result<(), KernelError> {
        if self.lineage.same_identity(&record.lineage) {
            Ok(())
        } else {
            Err(KernelError::StateRecordMalformed {
                reference: record.reference(None),
            })
        }
    }

    async fn publish_immutable(
        &self,
        key: String,
        bytes: Vec<u8>,
        reference: SafeReference,
    ) -> Result<(), KernelError> {
        match self
            .object_store
            .upload_conditional(&key, bytes.clone().into(), None)
            .await
        {
            Ok(_) | Err(ObjectStoreError::PreconditionFailed(_)) => {
                let existing = self.object_store.download(&key).await.map_err(|_| {
                    KernelError::StateUnavailable {
                        operation: "immutable readback after conditional create",
                    }
                })?;
                if existing.as_ref() == bytes.as_slice()
                    && RecordDigest::of(existing.as_ref()) == RecordDigest::of(&bytes)
                {
                    Ok(())
                } else {
                    Err(KernelError::DigestMismatch { reference })
                }
            }
            Err(_) => Err(KernelError::StateUnavailable {
                operation: "immutable conditional create",
            }),
        }
    }

    async fn create_head(&self, head: CanonicalHead) -> Result<HeadRead, KernelError> {
        let bytes = head.canonical_bytes()?;
        match self
            .object_store
            .upload_conditional(&self.lineage.head_key(), bytes.into(), None)
            .await
        {
            Ok(etag) => Ok(HeadRead {
                head,
                etag: etag.ok_or(KernelError::StateUnavailable {
                    operation: "head create did not return an ETag",
                })?,
            }),
            Err(ObjectStoreError::PreconditionFailed(_)) => Err(self.head_conflict().await),
            Err(_) => Err(KernelError::StateUnavailable {
                operation: "head conditional create",
            }),
        }
    }

    async fn replace_head(
        &self,
        head: CanonicalHead,
        expected_etag: &str,
    ) -> Result<HeadRead, KernelError> {
        let bytes = head.canonical_bytes()?;
        match self
            .object_store
            .upload_conditional(&self.lineage.head_key(), bytes.into(), Some(expected_etag))
            .await
        {
            Ok(etag) => Ok(HeadRead {
                head,
                etag: etag.ok_or(KernelError::StateUnavailable {
                    operation: "head update did not return an ETag",
                })?,
            }),
            Err(ObjectStoreError::PreconditionFailed(_)) => Err(self.head_conflict().await),
            Err(_) => Err(KernelError::StateUnavailable {
                operation: "head conditional update",
            }),
        }
    }

    async fn head_conflict(&self) -> KernelError {
        let current = self
            .load_head()
            .await
            .ok()
            .map(|loaded| loaded.head.reference());
        KernelError::LineageHeadConflict { current }
    }

    pub(crate) async fn load_head(&self) -> Result<LoadedHead, KernelError> {
        let download = self
            .object_store
            .download_with_etag(&self.lineage.head_key())
            .await
            .map_err(|error| match error {
                ObjectStoreError::NotFound(_) => KernelError::StateHistoryIncomplete {
                    reference: SafeReference::for_lineage(&self.lineage),
                },
                _ => KernelError::StateUnavailable {
                    operation: "head read",
                },
            })?;
        let head = CanonicalHead::from_bytes(&self.lineage, &download.data)?;
        Ok(LoadedHead {
            head,
            etag: download.etag,
        })
    }

    pub(crate) async fn load_record(
        &self,
        digest: &RecordDigest,
    ) -> Result<CanonicalRecord, KernelError> {
        let bytes = self
            .object_store
            .download(&self.lineage.object_key(digest))
            .await
            .map_err(|error| match error {
                ObjectStoreError::NotFound(_) => KernelError::StateHistoryIncomplete {
                    reference: SafeReference::for_digest(&self.lineage, digest.clone()),
                },
                _ => KernelError::StateUnavailable {
                    operation: "record read",
                },
            })?;
        CanonicalRecord::from_bytes(&self.lineage, digest, &bytes)
    }

    async fn load_checkpoint(
        &self,
        digest: &RecordDigest,
    ) -> Result<CanonicalCheckpoint, KernelError> {
        let bytes = self
            .object_store
            .download(&self.lineage.checkpoint_key(digest))
            .await
            .map_err(|error| match error {
                ObjectStoreError::NotFound(_) => KernelError::StateHistoryIncomplete {
                    reference: SafeReference::for_digest(&self.lineage, digest.clone()),
                },
                _ => KernelError::StateUnavailable {
                    operation: "checkpoint read",
                },
            })?;
        CanonicalCheckpoint::from_bytes(&self.lineage, digest, &bytes)
    }

    async fn load_history(
        &self,
        head: &CanonicalHead,
    ) -> Result<Vec<CanonicalRecord>, KernelError> {
        let mut records = Vec::new();
        let mut seen = BTreeSet::new();
        let mut expected_generation = head.generation;
        let mut next_digest = head.record_digest.clone();

        loop {
            self.mark_history_digest(&mut seen, &next_digest)?;
            let record = self.load_record(&next_digest).await?;
            let record_digest = record.digest()?;
            if record_digest != next_digest || record.sequence != expected_generation {
                return Err(KernelError::StateHistoryIncomplete {
                    reference: record.reference(Some(next_digest)),
                });
            }
            if records.is_empty() && head.prior != record.prior {
                return Err(KernelError::StateHistoryIncomplete {
                    reference: record.reference(Some(record_digest)),
                });
            }

            match &record.prior {
                None if expected_generation == 0 => {
                    records.push(record);
                    break;
                }
                Some(prior)
                    if expected_generation > 0 && prior.generation == expected_generation - 1 =>
                {
                    next_digest = prior.digest.clone();
                    expected_generation -= 1;
                    records.push(record);
                }
                _ => {
                    return Err(KernelError::StateHistoryIncomplete {
                        reference: record.reference(Some(record_digest)),
                    });
                }
            }
        }

        records.reverse();
        Ok(records)
    }

    fn load_projected_history(
        &self,
        projection: &FoldProjection,
        head: &CanonicalHead,
    ) -> Result<Vec<CanonicalRecord>, KernelError> {
        if !projection.source.matches(head) {
            return Err(KernelError::StateHistoryIncomplete {
                reference: SafeReference::for_lineage(&self.lineage),
            });
        }
        let records = projection.records_for_source(&self.lineage)?;
        let terminal = records
            .last()
            .ok_or_else(|| KernelError::StateHistoryIncomplete {
                reference: SafeReference::for_lineage(&self.lineage),
            })?;
        if head.prior != terminal.prior {
            return Err(KernelError::StateHistoryIncomplete {
                reference: terminal.reference(Some(head.record_digest.clone())),
            });
        }
        Ok(records)
    }

    fn mark_history_digest(
        &self,
        seen: &mut BTreeSet<RecordDigest>,
        digest: &RecordDigest,
    ) -> Result<(), KernelError> {
        if seen.insert(digest.clone()) {
            Ok(())
        } else {
            Err(KernelError::StateHistoryIncomplete {
                reference: SafeReference::for_digest(&self.lineage, digest.clone()),
            })
        }
    }

    fn validate_transitions<F>(
        &self,
        records: &[CanonicalRecord],
        folder: &F,
    ) -> Result<(), KernelError>
    where
        F: LineageFold,
    {
        for record in records {
            folder
                .validate_transition(&record.fold_record())
                .map_err(|_| KernelError::StateRecordMalformed {
                    reference: record.reference(record.digest().ok()),
                })?;
        }
        Ok(())
    }

    fn fold_full<F>(
        &self,
        records: &[CanonicalRecord],
        folder: &F,
    ) -> Result<(F::State, Vec<u8>), KernelError>
    where
        F: LineageFold,
    {
        let mut state = folder.initial_state();
        for record in records {
            folder
                .apply(&mut state, &record.fold_record())
                .map_err(|_| KernelError::StateRecordMalformed {
                    reference: record.reference(record.digest().ok()),
                })?;
        }
        let bytes =
            folder
                .canonical_state(&state)
                .map_err(|_| KernelError::StateRecordMalformed {
                    reference: SafeReference::for_lineage(&self.lineage),
                })?;
        Ok((state, bytes))
    }

    #[allow(clippy::too_many_arguments)]
    async fn check_checkpoint<F>(
        &self,
        checkpoint_digest: &RecordDigest,
        head: &CanonicalHead,
        records: &[CanonicalRecord],
        full_state: &[u8],
        folder: &F,
        rejections: &mut Vec<CheckpointRejection>,
    ) -> Result<(), KernelError>
    where
        F: LineageFold,
    {
        let checkpoint = match self.load_checkpoint(checkpoint_digest).await {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                if let Some(rejection) = CheckpointRejection::from_error(&error) {
                    rejections.push(rejection);
                    return Ok(());
                }
                return Err(error);
            }
        };
        if let Err(error) = self.validate_checkpoint_basis(&checkpoint, head, records) {
            rejections.push(CheckpointRejection::from_integrity_error(error)?);
            return Ok(());
        }

        let Ok(checkpoint_state) =
            folder.restore_checkpoint(&checkpoint.transition_schema, &checkpoint.state_bytes)
        else {
            rejections.push(CheckpointRejection {
                code: CheckpointRejectionCode::StateRecordMalformed,
                reference: SafeReference::for_digest(&self.lineage, checkpoint_digest.clone()),
            });
            return Ok(());
        };
        let start = checkpoint
            .source_generation
            .checked_add(1)
            .and_then(|generation| usize::try_from(generation).ok())
            .unwrap_or(records.len());
        let mut state = checkpoint_state;
        for record in records.get(start..).unwrap_or_default() {
            if folder.apply(&mut state, &record.fold_record()).is_err() {
                rejections.push(CheckpointRejection {
                    code: CheckpointRejectionCode::StateRecordMalformed,
                    reference: record.reference(record.digest().ok()),
                });
                return Ok(());
            }
        }
        match folder.canonical_state(&state) {
            Ok(bytes) if bytes == full_state => Ok(()),
            Ok(_) => {
                rejections.push(CheckpointRejection {
                    code: CheckpointRejectionCode::StateHistoryIncomplete,
                    reference: SafeReference::for_digest(&self.lineage, checkpoint_digest.clone()),
                });
                Ok(())
            }
            Err(()) => {
                rejections.push(CheckpointRejection {
                    code: CheckpointRejectionCode::StateRecordMalformed,
                    reference: SafeReference::for_digest(&self.lineage, checkpoint_digest.clone()),
                });
                Ok(())
            }
        }
    }

    fn validate_checkpoint_basis(
        &self,
        checkpoint: &CanonicalCheckpoint,
        head: &CanonicalHead,
        records: &[CanonicalRecord],
    ) -> Result<(), KernelError> {
        if !self.lineage.same_identity(&checkpoint.lineage) {
            return Err(KernelError::StateRecordMalformed {
                reference: SafeReference::for_lineage(&self.lineage),
            });
        }
        if checkpoint.source_generation > head.generation {
            return Err(KernelError::StateHistoryIncomplete {
                reference: SafeReference::for_lineage(&self.lineage),
            });
        }
        let source = usize::try_from(checkpoint.source_generation)
            .ok()
            .and_then(|index| records.get(index))
            .ok_or_else(|| KernelError::StateHistoryIncomplete {
                reference: SafeReference::for_lineage(&self.lineage),
            })?;
        let source_digest = source.digest()?;
        if checkpoint.source_head_digest != source_digest
            || checkpoint.last_included_record_digest != source_digest
        {
            return Err(KernelError::StateHistoryIncomplete {
                reference: source.reference(Some(source_digest)),
            });
        }
        Ok(())
    }
}

/// Fixed, separately-authorized probes used only to observe the private
/// kernel through the activated gateway process. They do not accept lineage,
/// record, or payload input from the caller.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KernelDiagnosticProbe {
    Core,
    FaultBeforeEffect,
    FaultAfterEffect,
}

impl KernelDiagnosticProbe {
    fn name(self) -> &'static str {
        match self {
            Self::Core => "core",
            Self::FaultBeforeEffect => "fault_before_effect",
            Self::FaultAfterEffect => "fault_after_effect",
        }
    }
}

#[derive(Debug, Serialize)]
pub struct KernelDiagnosticClaimObservation {
    passed: bool,
    facts: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct KernelDiagnosticObservation {
    probe: &'static str,
    claims: BTreeMap<String, KernelDiagnosticClaimObservation>,
}

fn diagnostic_claim(passed: bool, facts: serde_json::Value) -> KernelDiagnosticClaimObservation {
    KernelDiagnosticClaimObservation { passed, facts }
}

fn diagnostic_kernel_error_code(error: &KernelError) -> &'static str {
    match error {
        KernelError::DigestMismatch { .. } => "DIGEST_MISMATCH",
        KernelError::LineageHeadConflict { .. } => "LINEAGE_HEAD_CONFLICT",
        KernelError::StateRecordMalformed { .. } => "STATE_RECORD_MALFORMED",
        KernelError::StateHistoryIncomplete { .. } => "STATE_HISTORY_INCOMPLETE",
        KernelError::ProtocolEpochUnsupported { .. } => "PROTOCOL_EPOCH_UNSUPPORTED",
        KernelError::SuccessorNotAllowed { .. } => "SUCCESSOR_NOT_ALLOWED",
        KernelError::StateUnavailable { .. } => "GATEWAY_STATE_UNAVAILABLE",
        KernelError::TombstoneCorrupt { .. } => "TOMBSTONE_CORRUPT",
    }
}

fn diagnostic_lineage(
    state_set_id: &str,
    probe: KernelDiagnosticProbe,
    policy: SuccessorPolicy,
) -> Result<KernelLineage, KernelError> {
    let state_set_digest = hex::encode(Sha256::digest(state_set_id.as_bytes()));
    let sequence = KERNEL_DIAGNOSTIC_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    KernelLineage::new(
        format!(
            "state/v1/diagnostic/{state_set_digest}/{}-{sequence}",
            probe.name()
        ),
        policy,
    )
}

fn diagnostic_record(
    lineage: &KernelLineage,
    sequence: u64,
    prior: Option<RecordPosition>,
    payload: &str,
) -> Result<CanonicalRecord, KernelError> {
    CanonicalRecord::new(
        lineage,
        sequence,
        prior,
        "diagnostic",
        "diagnostic.v1",
        payload.as_bytes().to_vec(),
        format!("diagnostic-operation-{sequence}"),
        "diagnostic-actor",
        "diagnostic-cause",
    )
}

#[derive(Default)]
struct DiagnosticFold;

impl LineageFold for DiagnosticFold {
    type State = Vec<String>;

    fn validate_transition(&self, record: &FoldRecord<'_>) -> Result<(), ()> {
        (record.transition_type() == "diagnostic"
            && record.transition_schema() == "diagnostic.v1"
            && std::str::from_utf8(record.payload()).is_ok())
        .then_some(())
        .ok_or(())
    }

    fn initial_state(&self) -> Self::State {
        Vec::new()
    }

    fn apply(&self, state: &mut Self::State, record: &FoldRecord<'_>) -> Result<(), ()> {
        state.push(
            std::str::from_utf8(record.payload())
                .map_err(|_| ())?
                .to_owned(),
        );
        Ok(())
    }

    fn canonical_state(&self, state: &Self::State) -> Result<Vec<u8>, ()> {
        serde_json::to_vec(state).map_err(|_| ())
    }

    fn restore_checkpoint(
        &self,
        transition_schema: &str,
        state_bytes: &[u8],
    ) -> Result<Self::State, ()> {
        if transition_schema == "diagnostic.v1" {
            serde_json::from_slice(state_bytes).map_err(|_| ())
        } else {
            Err(())
        }
    }
}

async fn diagnostic_upload(
    object_store: &ObjectStoreClient,
    key: &str,
    bytes: Vec<u8>,
    operation: &'static str,
) -> Result<(), KernelError> {
    object_store
        .upload(key, bytes.into())
        .await
        .map_err(|_| KernelError::StateUnavailable { operation })
}

async fn diagnostic_download(
    object_store: &ObjectStoreClient,
    key: &str,
    operation: &'static str,
) -> Result<Vec<u8>, KernelError> {
    object_store
        .download(key)
        .await
        .map(|bytes| bytes.to_vec())
        .map_err(|_| KernelError::StateUnavailable { operation })
}

/// Exercise a fixed kernel relation set through an activated handle's store.
/// The output is intentionally sanitized to booleans, hashes, generation
/// numbers, and typed outcomes.
///
/// ```compile_fail
/// use std::sync::Arc;
/// use yeetz_s3_kernel::state_kernel::{run_kernel_diagnostic, KernelDiagnosticProbe};
/// use yeetz_sdk_s3::ObjectStoreClient;
///
/// fn bypass(store: Arc<ObjectStoreClient>) {
///     let _ = run_kernel_diagnostic(store, "probe", KernelDiagnosticProbe::Core);
/// }
/// ```
pub async fn run_kernel_diagnostic(
    handle: &KernelHandle,
    state_set_id: &str,
    probe: KernelDiagnosticProbe,
) -> Result<KernelDiagnosticObservation, KernelError> {
    let object_store = Arc::clone(&handle.object_store);
    match probe {
        KernelDiagnosticProbe::Core => run_core_kernel_diagnostic(object_store, state_set_id).await,
        KernelDiagnosticProbe::FaultBeforeEffect | KernelDiagnosticProbe::FaultAfterEffect => {
            run_fault_kernel_diagnostic(object_store, state_set_id, probe).await
        }
    }
}

async fn run_core_kernel_diagnostic(
    object_store: Arc<ObjectStoreClient>,
    state_set_id: &str,
) -> Result<KernelDiagnosticObservation, KernelError> {
    let lineage = diagnostic_lineage(
        state_set_id,
        KernelDiagnosticProbe::Core,
        SuccessorPolicy::SuccessorCapable,
    )?;
    let kernel = StateKernel::new(Arc::clone(&object_store), lineage.clone());
    let folder = DiagnosticFold;
    let first = diagnostic_record(&lineage, 0, None, "first")?;
    let first_bytes = first.canonical_bytes()?;
    let first_digest = kernel.publish_record(&first).await?;
    let repeat_digest = kernel.publish_record(&first).await?;
    let first_key = lineage.object_key(&first_digest);
    let first_readback =
        diagnostic_download(&object_store, &first_key, "diagnostic immutable readback").await?;

    let divergent_lineage = diagnostic_lineage(
        state_set_id,
        KernelDiagnosticProbe::Core,
        SuccessorPolicy::SuccessorCapable,
    )?;
    let divergent_kernel = StateKernel::new(Arc::clone(&object_store), divergent_lineage.clone());
    let divergent_record = diagnostic_record(&divergent_lineage, 0, None, "expected")?;
    let divergent_digest = divergent_record.digest()?;
    let divergent_key = divergent_lineage.object_key(&divergent_digest);
    let divergent_bytes = b"diagnostic divergent immutable bytes".to_vec();
    diagnostic_upload(
        &object_store,
        &divergent_key,
        divergent_bytes.clone(),
        "diagnostic divergent immutable seed",
    )
    .await?;
    let divergent_result = divergent_kernel.publish_record(&divergent_record).await;
    let divergent_readback = diagnostic_download(
        &object_store,
        &divergent_key,
        "diagnostic divergent immutable readback",
    )
    .await?;

    let (left, right) = tokio::join!(kernel.append_genesis(&first), kernel.append_genesis(&first));
    let winner_count = usize::from(left.is_ok()) + usize::from(right.is_ok());
    let loser = if left.is_err() { &left } else { &right };
    let loser_code = loser
        .as_ref()
        .err()
        .map(diagnostic_kernel_error_code)
        .unwrap_or("ACKNOWLEDGED");
    let initial_head = kernel.read_head().await?;

    let genesis_only_lineage = diagnostic_lineage(
        state_set_id,
        KernelDiagnosticProbe::Core,
        SuccessorPolicy::GenesisOnly,
    )?;
    let genesis_only = StateKernel::new(Arc::clone(&object_store), genesis_only_lineage.clone());
    let genesis = diagnostic_record(&genesis_only_lineage, 0, None, "genesis-only")?;
    let genesis_head = genesis_only.append_genesis(&genesis).await?;
    let forbidden = diagnostic_record(
        &genesis_only_lineage,
        1,
        Some(genesis_head.record_position()),
        "forbidden-successor",
    )?;
    let forbidden_key = genesis_only_lineage.object_key(&forbidden.digest()?);
    let forbidden_result = genesis_only
        .append_successor(&forbidden, &genesis_head)
        .await;
    let forbidden_absent = matches!(
        object_store.download(&forbidden_key).await,
        Err(ObjectStoreError::NotFound(_))
    );

    let second = diagnostic_record(&lineage, 1, Some(initial_head.record_position()), "second")?;
    let second_head = kernel.append_successor(&second, &initial_head).await?;
    let full_fold = kernel.fold(None, &folder).await?;
    let checkpoint = CanonicalCheckpoint::new(
        &lineage,
        "diagnostic.v1",
        second_head.generation(),
        second_head.record_digest().clone(),
        second_head.record_digest().clone(),
        full_fold.canonical_state.clone(),
    )?;
    let checkpoint_digest = kernel.publish_checkpoint(&checkpoint).await?;
    let checkpoint_fold = kernel.fold(Some(&checkpoint_digest), &folder).await?;

    let malformed_checkpoint_bytes = b"{diagnostic malformed checkpoint".to_vec();
    let malformed_checkpoint_digest = RecordDigest::of(&malformed_checkpoint_bytes);
    diagnostic_upload(
        &object_store,
        &lineage.checkpoint_key(&malformed_checkpoint_digest),
        malformed_checkpoint_bytes,
        "diagnostic malformed checkpoint seed",
    )
    .await?;
    let malformed_checkpoint_fold = kernel
        .fold(Some(&malformed_checkpoint_digest), &folder)
        .await?;
    let checkpoint_rejection = malformed_checkpoint_fold
        .checkpoint_rejections
        .first()
        .map(|rejection| rejection.code == CheckpointRejectionCode::StateRecordMalformed)
        .unwrap_or(false);

    let fresh_kernel = StateKernel::new(Arc::clone(&object_store), lineage.clone());
    let fresh_fold = fresh_kernel.fold(None, &folder).await?;
    let mut poisoned_projection = kernel.rebuild_projection().await?;
    poisoned_projection.source.generation = 0;
    let projection_fold = kernel
        .fold_with_projection(Some(&poisoned_projection), &folder)
        .await?;

    let corrupt_lineage = diagnostic_lineage(
        state_set_id,
        KernelDiagnosticProbe::Core,
        SuccessorPolicy::SuccessorCapable,
    )?;
    let corrupt_kernel = StateKernel::new(Arc::clone(&object_store), corrupt_lineage.clone());
    let corrupt_record = diagnostic_record(&corrupt_lineage, 0, None, "corruptible")?;
    let corrupt_head = corrupt_kernel.append_genesis(&corrupt_record).await?;
    diagnostic_upload(
        &object_store,
        &corrupt_lineage.object_key(corrupt_head.record_digest()),
        b"diagnostic corrupt record".to_vec(),
        "diagnostic corrupt record seed",
    )
    .await?;
    let corrupt_record_result = corrupt_kernel.fold(None, &folder).await;

    let malformed_head_lineage = diagnostic_lineage(
        state_set_id,
        KernelDiagnosticProbe::Core,
        SuccessorPolicy::SuccessorCapable,
    )?;
    let malformed_head_kernel =
        StateKernel::new(Arc::clone(&object_store), malformed_head_lineage.clone());
    diagnostic_upload(
        &object_store,
        &malformed_head_lineage.head_key(),
        b"diagnostic malformed head".to_vec(),
        "diagnostic malformed head seed",
    )
    .await?;
    let malformed_head_result = malformed_head_kernel.read_head().await;

    let state_digest = |state: &[u8]| hex::encode(Sha256::digest(state));
    let mut claims = BTreeMap::new();
    claims.insert(
        "C-001".to_owned(),
        diagnostic_claim(
            first_digest == repeat_digest && first_readback == first_bytes,
            serde_json::json!({
                "record_digest": first_digest.as_str(),
                "readback_sha256": hex::encode(Sha256::digest(&first_readback)),
            }),
        ),
    );
    claims.insert(
        "C-002".to_owned(),
        diagnostic_claim(
            matches!(
                divergent_result.as_ref(),
                Err(KernelError::DigestMismatch { .. })
            ) && divergent_readback == divergent_bytes,
            serde_json::json!({
                "result": divergent_result.as_ref().err().map(diagnostic_kernel_error_code),
                "original_readback_sha256": hex::encode(Sha256::digest(&divergent_readback)),
            }),
        ),
    );
    claims.insert(
        "C-003".to_owned(),
        diagnostic_claim(
            winner_count == 1 && initial_head.generation() == 0,
            serde_json::json!({"winner_count": winner_count, "read_generation": initial_head.generation()}),
        ),
    );
    claims.insert(
        "C-004".to_owned(),
        diagnostic_claim(
            matches!(
                forbidden_result.as_ref(),
                Err(KernelError::SuccessorNotAllowed { .. })
            ) && forbidden_absent
                && genesis_only.read_head().await?.generation() == 0,
            serde_json::json!({
                "result": forbidden_result.as_ref().err().map(diagnostic_kernel_error_code),
                "candidate_object_absent": forbidden_absent,
            }),
        ),
    );
    claims.insert(
        "C-005".to_owned(),
        diagnostic_claim(
            winner_count == 1 && initial_head.generation() == 0,
            serde_json::json!({"winner_count": winner_count, "read_generation": initial_head.generation()}),
        ),
    );
    claims.insert(
        "C-006".to_owned(),
        diagnostic_claim(
            loser_code == "LINEAGE_HEAD_CONFLICT",
            serde_json::json!({"loser_result": loser_code, "retry_count": 0}),
        ),
    );
    claims.insert(
        "C-007".to_owned(),
        diagnostic_claim(
            checkpoint_fold.canonical_state == full_fold.canonical_state
                && checkpoint_fold.checkpoint_rejections.is_empty(),
            serde_json::json!({
                "generation": second_head.generation(),
                "state_sha256": state_digest(&checkpoint_fold.canonical_state),
                "checkpoint_rejections": checkpoint_fold.checkpoint_rejections.len(),
            }),
        ),
    );
    claims.insert(
        "C-008".to_owned(),
        diagnostic_claim(
            checkpoint_rejection
                && malformed_checkpoint_fold.canonical_state == full_fold.canonical_state,
            serde_json::json!({
                "rejected": checkpoint_rejection,
                "authoritative_state_sha256": state_digest(&malformed_checkpoint_fold.canonical_state),
            }),
        ),
    );
    claims.insert(
        "C-009".to_owned(),
        diagnostic_claim(
            fresh_fold.canonical_state == full_fold.canonical_state
                && fresh_fold.projection == ProjectionDisposition::Absent,
            serde_json::json!({
                "projection": "absent",
                "state_sha256": state_digest(&fresh_fold.canonical_state),
            }),
        ),
    );
    claims.insert(
        "C-010".to_owned(),
        diagnostic_claim(
            projection_fold.canonical_state == full_fold.canonical_state
                && projection_fold.projection == ProjectionDisposition::Discarded,
            serde_json::json!({
                "projection": "discarded",
                "state_sha256": state_digest(&projection_fold.canonical_state),
            }),
        ),
    );
    claims.insert(
        "C-013".to_owned(),
        diagnostic_claim(
            full_fold.state == vec!["first".to_owned(), "second".to_owned()]
                && full_fold.records.len() == 2,
            serde_json::json!({
                "record_count": full_fold.records.len(),
                "state_sha256": state_digest(&full_fold.canonical_state),
            }),
        ),
    );
    claims.insert(
        "C-014".to_owned(),
        diagnostic_claim(
            matches!(
                corrupt_record_result.as_ref(),
                Err(KernelError::DigestMismatch { .. })
            ) && matches!(
                malformed_head_result.as_ref(),
                Err(KernelError::StateRecordMalformed { .. })
            )
                && checkpoint_rejection,
            serde_json::json!({
                "record_result": corrupt_record_result.as_ref().err().map(diagnostic_kernel_error_code),
                "head_result": malformed_head_result.as_ref().err().map(diagnostic_kernel_error_code),
                "checkpoint_rejected": checkpoint_rejection,
            }),
        ),
    );

    Ok(KernelDiagnosticObservation {
        probe: KernelDiagnosticProbe::Core.name(),
        claims,
    })
}

async fn run_fault_kernel_diagnostic(
    object_store: Arc<ObjectStoreClient>,
    state_set_id: &str,
    probe: KernelDiagnosticProbe,
) -> Result<KernelDiagnosticObservation, KernelError> {
    let lineage = diagnostic_lineage(state_set_id, probe, SuccessorPolicy::SuccessorCapable)?;
    let kernel = StateKernel::new(Arc::clone(&object_store), lineage.clone());
    let folder = DiagnosticFold;
    let first = diagnostic_record(&lineage, 0, None, "fault-prior")?;
    let first_head = kernel.append_genesis(&first).await?;
    let candidate = diagnostic_record(
        &lineage,
        1,
        Some(first_head.record_position()),
        "fault-candidate",
    )?;
    let append_result = kernel.append_successor(&candidate, &first_head).await;
    let final_head = kernel.read_head().await?;
    let folded = kernel.fold(None, &folder).await?;
    let expected_generation = match probe {
        KernelDiagnosticProbe::FaultBeforeEffect => 0,
        KernelDiagnosticProbe::FaultAfterEffect => 1,
        KernelDiagnosticProbe::Core => unreachable!("core takes its own diagnostic path"),
    };
    let expected_state = match probe {
        KernelDiagnosticProbe::FaultBeforeEffect => vec!["fault-prior".to_owned()],
        KernelDiagnosticProbe::FaultAfterEffect => {
            vec!["fault-prior".to_owned(), "fault-candidate".to_owned()]
        }
        KernelDiagnosticProbe::Core => unreachable!("core takes its own diagnostic path"),
    };
    let append_error = append_result
        .as_ref()
        .err()
        .map(diagnostic_kernel_error_code);
    let completed_or_prior_only = final_head.generation() == expected_generation
        && folded.state == expected_state
        && folded.records.len() == usize::try_from(expected_generation + 1).unwrap_or_default();
    let mut claims = BTreeMap::new();
    claims.insert(
        "C-011".to_owned(),
        diagnostic_claim(
            append_error == Some("GATEWAY_STATE_UNAVAILABLE") && completed_or_prior_only,
            serde_json::json!({
                "append_result": append_error,
                "read_generation": final_head.generation(),
                "expected_generation": expected_generation,
                "state_sha256": hex::encode(Sha256::digest(&folded.canonical_state)),
            }),
        ),
    );
    claims.insert(
        "C-012".to_owned(),
        diagnostic_claim(
            append_error == Some("GATEWAY_STATE_UNAVAILABLE") && completed_or_prior_only,
            serde_json::json!({
                "append_result": append_error,
                "record_count": folded.records.len(),
                "expected_record_count": expected_generation + 1,
                "partial_transition_observed": !completed_or_prior_only,
            }),
        ),
    );
    Ok(KernelDiagnosticObservation {
        probe: probe.name(),
        claims,
    })
}

pub(crate) struct LoadedHead {
    pub(crate) head: CanonicalHead,
    pub(crate) etag: Option<String>,
}

impl CheckpointRejection {
    fn from_error(error: &KernelError) -> Option<Self> {
        match error {
            KernelError::DigestMismatch { reference } => Some(Self {
                code: CheckpointRejectionCode::DigestMismatch,
                reference: reference.clone(),
            }),
            KernelError::StateRecordMalformed { reference } => Some(Self {
                code: CheckpointRejectionCode::StateRecordMalformed,
                reference: reference.clone(),
            }),
            KernelError::StateHistoryIncomplete { reference } => Some(Self {
                code: CheckpointRejectionCode::StateHistoryIncomplete,
                reference: reference.clone(),
            }),
            KernelError::ProtocolEpochUnsupported { reference, .. } => Some(Self {
                code: CheckpointRejectionCode::ProtocolEpochUnsupported,
                reference: reference.clone(),
            }),
            KernelError::LineageHeadConflict { .. }
            | KernelError::SuccessorNotAllowed { .. }
            | KernelError::TombstoneCorrupt { .. }
            | KernelError::StateUnavailable { .. } => None,
        }
    }

    fn from_integrity_error(error: KernelError) -> Result<Self, KernelError> {
        Self::from_error(&error).ok_or(error)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PositionWire {
    digest: String,
    generation: u64,
}

impl From<&RecordPosition> for PositionWire {
    fn from(position: &RecordPosition) -> Self {
        Self {
            digest: position.digest.0.clone(),
            generation: position.generation,
        }
    }
}

impl PositionWire {
    fn into_position(
        self,
        lineage: &KernelLineage,
        expected_digest: &RecordDigest,
    ) -> Result<RecordPosition, KernelError> {
        let digest =
            RecordDigest::parse(&self.digest).ok_or_else(|| KernelError::StateRecordMalformed {
                reference: SafeReference::for_digest(lineage, expected_digest.clone()),
            })?;
        Ok(RecordPosition {
            generation: self.generation,
            digest,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordWire {
    actor_id: String,
    cause_id: String,
    envelope: String,
    lineage: String,
    operation_id: String,
    payload_hex: String,
    prior: Option<PositionWire>,
    protocol_epoch: u16,
    sequence: u64,
    transition_schema: String,
    transition_type: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HeadWire {
    envelope: String,
    generation: u64,
    lineage: String,
    prior: Option<PositionWire>,
    record_digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointWire {
    envelope: String,
    last_included_record_digest: String,
    lineage: String,
    protocol_epoch: u16,
    source_generation: u64,
    source_head_digest: String,
    state_hex: String,
    transition_schema: String,
}

fn is_valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn is_valid_lineage(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value
            .split('/')
            .all(|segment| is_valid_identifier(segment) && segment != "." && segment != "..")
}

#[cfg(test)]
pub mod gateway_state_contract {
    use std::collections::BTreeMap;
    use std::fs::File;
    use std::path::{Path, PathBuf};
    use std::process::Stdio;
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };
    use std::time::Duration;

    use axum::{
        Json, Router,
        body::{Body, to_bytes},
        extract::{Request, State},
        http::{
            HeaderValue, Method, StatusCode,
            header::{ETAG, IF_MATCH, IF_NONE_MATCH},
        },
        response::Response,
        routing::{any as any_route, get, post},
    };
    use proptest::prelude::*;
    use serde::{Deserialize, Serialize};
    use sha2::Digest as _;
    use yeetz_sdk_s3::{ObjectStoreClient, S3Config};

    use super::*;

    const T001_COUNTERPART_BUCKET: &str = "kernel-contract";
    const T001_COUNTERPART_READY_PATH: &str = "T001_S3_COUNTERPART_READY_PATH";
    const T001_COUNTERPART_TIMEOUT: Duration = Duration::from_secs(5);
    const T002_FAULT_STATUS: StatusCode = StatusCode::BAD_REQUEST;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct CounterpartReady {
        endpoint: String,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct LoopbackRequestObservation {
        sequence: u64,
        method: String,
        key: Option<String>,
        if_match: Option<String>,
        if_none_match: Option<String>,
        status: u16,
        fault: Option<StorageFaultObservation>,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum StorageFaultCut {
        ImmutableWrite,
        ImmutableChecksum,
        ImmutableReadback,
        HeadCreate,
        HeadUpdate,
        /// A DELETE against a keyspace object (ADR 0016 batch 2):
        /// BeforeEffect refuses the delete (nothing applied);
        /// AfterEffect applies it and cuts the response — the lost
        /// response a GC sweep must tolerate (G117).
        KeyspaceDelete,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum StorageFaultPhase {
        BeforeEffect,
        AfterEffect,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct StorageFaultObservation {
        cut: StorageFaultCut,
        phase: StorageFaultPhase,
        key: String,
        request_sequence: u64,
        effect_applied: bool,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct ArmStorageFault {
        cut: StorageFaultCut,
        phase: StorageFaultPhase,
        key: String,
    }

    struct ArmedStorageFault {
        cut: StorageFaultCut,
        phase: StorageFaultPhase,
        key: String,
    }

    impl ArmedStorageFault {
        fn matches(
            &self,
            method: &Method,
            key: &str,
            if_match: Option<&str>,
            if_none_match: Option<&str>,
        ) -> bool {
            if self.key != key {
                return false;
            }
            let immutable_key = key.contains("/objects/") || key.contains("/checkpoints/");
            match self.cut {
                StorageFaultCut::ImmutableWrite | StorageFaultCut::ImmutableChecksum => {
                    *method == Method::PUT && immutable_key && if_none_match == Some("*")
                }
                StorageFaultCut::ImmutableReadback => *method == Method::GET && immutable_key,
                StorageFaultCut::HeadCreate => {
                    *method == Method::PUT && key.ends_with("/head") && if_none_match == Some("*")
                }
                StorageFaultCut::HeadUpdate => {
                    *method == Method::PUT && key.ends_with("/head") && if_match.is_some()
                }
                StorageFaultCut::KeyspaceDelete => {
                    *method == Method::DELETE && key.starts_with("keyspace/")
                }
            }
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct ConditionalHeadBarrierObservation {
        key: String,
        expected_arrivals: usize,
        arrivals: usize,
        passes: usize,
        request_sequences: Vec<u64>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct CounterpartSnapshot {
        requests: Vec<LoopbackRequestObservation>,
        barrier: Option<ConditionalHeadBarrierObservation>,
        faults: Vec<StorageFaultObservation>,
    }

    #[derive(Clone)]
    struct LoopbackObject {
        bytes: Vec<u8>,
        etag: String,
    }

    struct ConditionalHeadBarrier {
        key: String,
        expected_arrivals: usize,
        gate: Arc<tokio::sync::Barrier>,
        request_sequences: Vec<u64>,
        passes: usize,
    }

    #[derive(Clone)]
    struct CounterpartState {
        objects: Arc<tokio::sync::Mutex<BTreeMap<String, LoopbackObject>>>,
        requests: Arc<tokio::sync::Mutex<Vec<LoopbackRequestObservation>>>,
        faults: Arc<tokio::sync::Mutex<Vec<StorageFaultObservation>>>,
        next_request_sequence: Arc<AtomicU64>,
        conditional_head_barrier: Arc<tokio::sync::Mutex<Option<ConditionalHeadBarrier>>>,
        storage_fault: Arc<tokio::sync::Mutex<Option<ArmedStorageFault>>>,
        shutdown: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    }

    impl CounterpartState {
        fn new(shutdown: tokio::sync::oneshot::Sender<()>) -> Self {
            Self {
                objects: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
                requests: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                faults: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                next_request_sequence: Arc::new(AtomicU64::new(1)),
                conditional_head_barrier: Arc::new(tokio::sync::Mutex::new(None)),
                storage_fault: Arc::new(tokio::sync::Mutex::new(None)),
                shutdown: Arc::new(tokio::sync::Mutex::new(Some(shutdown))),
            }
        }

        async fn arm_conditional_head_barrier(&self, key: String) {
            *self.conditional_head_barrier.lock().await = Some(ConditionalHeadBarrier {
                key,
                expected_arrivals: 2,
                gate: Arc::new(tokio::sync::Barrier::new(2)),
                request_sequences: Vec::new(),
                passes: 0,
            });
        }

        async fn wait_for_conditional_head(
            &self,
            key: &str,
            request_sequence: u64,
            is_conditional: bool,
        ) {
            if !is_conditional {
                return;
            }

            let gate = {
                let mut barrier = self.conditional_head_barrier.lock().await;
                let Some(barrier) = barrier.as_mut() else {
                    return;
                };
                if barrier.key != key
                    || barrier.request_sequences.len() >= barrier.expected_arrivals
                {
                    return;
                }
                barrier.request_sequences.push(request_sequence);
                Arc::clone(&barrier.gate)
            };

            gate.wait().await;
            let mut barrier = self.conditional_head_barrier.lock().await;
            if let Some(barrier) = barrier.as_mut()
                && barrier.key == key
                && barrier.request_sequences.contains(&request_sequence)
            {
                barrier.passes += 1;
            }
        }

        async fn record(&self, observation: LoopbackRequestObservation) {
            self.requests.lock().await.push(observation);
        }

        async fn arm_storage_fault(&self, command: ArmStorageFault) {
            *self.storage_fault.lock().await = Some(ArmedStorageFault {
                cut: command.cut,
                phase: command.phase,
                key: command.key,
            });
        }

        async fn take_storage_fault(
            &self,
            method: &Method,
            key: &str,
            if_match: Option<&str>,
            if_none_match: Option<&str>,
        ) -> Option<ArmedStorageFault> {
            let mut fault = self.storage_fault.lock().await;
            if fault
                .as_ref()
                .is_some_and(|fault| fault.matches(method, key, if_match, if_none_match))
            {
                fault.take()
            } else {
                None
            }
        }

        async fn record_fault(&self, observation: StorageFaultObservation) {
            self.faults.lock().await.push(observation);
        }

        async fn corrupt_object(&self, key: &str) {
            let mut objects = self.objects.lock().await;
            let object = objects
                .get_mut(key)
                .expect("immutable checksum fault requires a stored object");
            object.bytes.push(b'!');
        }

        async fn snapshot(&self) -> CounterpartSnapshot {
            let requests = self.requests.lock().await.clone();
            let barrier = self
                .conditional_head_barrier
                .lock()
                .await
                .as_ref()
                .map(|barrier| ConditionalHeadBarrierObservation {
                    key: barrier.key.clone(),
                    expected_arrivals: barrier.expected_arrivals,
                    arrivals: barrier.request_sequences.len(),
                    passes: barrier.passes,
                    request_sequences: barrier.request_sequences.clone(),
                });
            let faults = self.faults.lock().await.clone();
            CounterpartSnapshot {
                requests,
                barrier,
                faults,
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    struct ArmConditionalHeadBarrier {
        key: String,
    }

    fn counterpart_key(path: &str) -> Option<String> {
        path.strip_prefix(&format!("/{T001_COUNTERPART_BUCKET}/"))
            .filter(|key| !key.is_empty())
            .map(ToOwned::to_owned)
    }

    fn unquoted_etag(value: &str) -> &str {
        value.trim_matches('"')
    }

    fn counterpart_response(status: StatusCode, etag: Option<&str>, body: Vec<u8>) -> Response {
        let mut response = Response::new(Body::from(body));
        *response.status_mut() = status;
        if let Some(etag) = etag {
            response.headers_mut().insert(
                ETAG,
                HeaderValue::try_from(format!("\"{etag}\"")).expect("loopback ETag header"),
            );
        }
        response
    }

    fn query_param(parts: &axum::http::request::Parts, name: &str) -> Option<String> {
        let query = parts.uri.query()?;
        for pair in query.split('&') {
            let (k, v) = pair.split_once('=')?;
            if k == name {
                return Some(percent_decode(v));
            }
        }
        None
    }

    /// <Key> values from a bulk-delete body.
    fn extract_delete_keys(body: &[u8]) -> Vec<String> {
        let text = String::from_utf8_lossy(body);
        text.split("<Key>")
            .skip(1)
            .filter_map(|rest| rest.split_once("</Key>").map(|(key, _)| key.to_string()))
            .collect()
    }

    fn percent_decode(value: &str) -> String {
        let bytes = value.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut index = 0;
        while index < bytes.len() {
            match bytes[index] {
                b'%' if index + 2 < bytes.len() => {
                    let hex = |b: u8| (b as char).to_digit(16);
                    match (hex(bytes[index + 1]), hex(bytes[index + 2])) {
                        (Some(high), Some(low)) => {
                            out.push((high * 16 + low) as u8);
                            index += 3;
                        }
                        _ => {
                            out.push(bytes[index]);
                            index += 1;
                        }
                    }
                }
                byte => {
                    out.push(byte);
                    index += 1;
                }
            }
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    /// ListObjectsV2 subset XML with real pagination fidelity:
    /// `IsTruncated`, `KeyCount`, and `NextContinuationToken` (the
    /// last returned key — the continuation subset this rig's clients
    /// use; `continuation-token` is honored as the exclusive resume
    /// point on the next request).
    fn list_objects_xml(
        prefix: &str,
        entries: &[(String, String, usize)],
        truncated: bool,
        next_token: Option<&str>,
    ) -> String {
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        xml.push_str(&format!(
            "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>t001</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><IsTruncated>{}</IsTruncated>",
            prefix,
            entries.len(),
            truncated
        ));
        if let Some(token) = next_token {
            xml.push_str(&format!(
                "<NextContinuationToken>{token}</NextContinuationToken>"
            ));
        }
        for (key, etag, size) in entries {
            xml.push_str(&format!(
                "<Contents><Key>{}</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>&quot;{}&quot;</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                key, etag, size
            ));
        }
        xml.push_str("</ListBucketResult>");
        xml
    }

    async fn loopback_s3_request(
        State(state): State<CounterpartState>,
        request: Request,
    ) -> Response {
        let (parts, body) = request.into_parts();
        let method = parts.method.clone();
        let key = counterpart_key(parts.uri.path());
        let list_request = parts
            .uri
            .path()
            .trim_end_matches('/')
            .ends_with(&format!("/{T001_COUNTERPART_BUCKET}"))
            && method == Method::GET
            && parts.uri.query().is_some_and(|q| q.contains("list-type=2"));
        // Bulk delete (object_store's delete_stream → POST ?delete=; the
        // single-object DELETE arm stays for direct callers).
        let bulk_delete_request = parts
            .uri
            .path()
            .trim_end_matches('/')
            .ends_with(&format!("/{T001_COUNTERPART_BUCKET}"))
            && method == Method::POST
            && parts.uri.query().is_some_and(|q| q.contains("delete"));
        let if_match = parts
            .headers
            .get(IF_MATCH)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);
        let if_none_match = parts
            .headers
            .get(IF_NONE_MATCH)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);
        let sequence = state.next_request_sequence.fetch_add(1, Ordering::SeqCst);
        let bytes = match to_bytes(body, 8 * 1024 * 1024).await {
            Ok(bytes) => bytes.to_vec(),
            Err(_) => {
                state
                    .record(LoopbackRequestObservation {
                        sequence,
                        method: method.to_string(),
                        key,
                        if_match,
                        if_none_match,
                        status: StatusCode::PAYLOAD_TOO_LARGE.as_u16(),
                        fault: None,
                    })
                    .await;
                return counterpart_response(StatusCode::PAYLOAD_TOO_LARGE, None, Vec::new());
            }
        };

        let fault = match key.as_deref() {
            Some(key) => {
                state
                    .take_storage_fault(&method, key, if_match.as_deref(), if_none_match.as_deref())
                    .await
            }
            None => None,
        };
        if let Some(fault) = fault
            .as_ref()
            .filter(|fault| fault.phase == StorageFaultPhase::BeforeEffect)
        {
            let observation = StorageFaultObservation {
                cut: fault.cut,
                phase: fault.phase,
                key: fault.key.clone(),
                request_sequence: sequence,
                effect_applied: false,
            };
            state.record_fault(observation.clone()).await;
            state
                .record(LoopbackRequestObservation {
                    sequence,
                    method: method.to_string(),
                    key,
                    if_match,
                    if_none_match,
                    status: T002_FAULT_STATUS.as_u16(),
                    fault: Some(observation),
                })
                .await;
            return counterpart_response(T002_FAULT_STATUS, None, Vec::new());
        }

        let (mut status, mut etag, mut response_body) = if bulk_delete_request {
            let mut objects = state.objects.lock().await;
            let mut deleted = Vec::new();
            let mut errored = Vec::new();
            for key in extract_delete_keys(&bytes) {
                // The bulk endpoint carries no single path, so the
                // armed fault is taken per key here (matches() already
                // filters cut/method/prefix).
                let armed = state
                    .take_storage_fault(&Method::DELETE, &key, None, None)
                    .await
                    .map(|fault| fault.phase);
                if let Some(phase) = armed {
                    if phase == StorageFaultPhase::AfterEffect {
                        // Applied server-side; the response is lost —
                        // surfaces to the caller as a failed entry (the
                        // G117 lost-response shape).
                        objects.remove(&key);
                        errored.push(key.clone());
                    } else {
                        // Refused entirely.
                        errored.push(key.clone());
                    }
                    state
                        .record_fault(StorageFaultObservation {
                            cut: StorageFaultCut::KeyspaceDelete,
                            phase,
                            key: key.clone(),
                            request_sequence: sequence,
                            effect_applied: phase == StorageFaultPhase::AfterEffect,
                        })
                        .await;
                } else {
                    objects.remove(&key);
                    deleted.push(key);
                }
            }
            let mut xml =
                String::from("<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
            for key in &deleted {
                xml.push_str(&format!("<Deleted><Key>{key}</Key></Deleted>"));
            }
            for key in &errored {
                xml.push_str(&format!(
                    "<Error><Key>{key}</Key><Code>InternalError</Code><Message>lost response</Message></Error>"
                ));
            }
            xml.push_str("</DeleteResult>");
            (StatusCode::OK, None, xml.into_bytes())
        } else if list_request {
            // LIST (ListObjectsV2 subset): prefix filter, exclusive
            // start-after, bounded by max-keys; strictly ordered (the
            // BTreeMap iteration is byte order).
            let prefix = query_param(&parts, "prefix").unwrap_or_default();
            let start_after = query_param(&parts, "start-after");
            // continuation-token (the last key of the prior page) and
            // start-after are both exclusive resume points; the token
            // wins when both are present.
            let resume_after = query_param(&parts, "continuation-token")
                .or(start_after)
                .filter(|token| !token.is_empty());
            let max_keys: usize = query_param(&parts, "max-keys")
                .and_then(|value| value.parse().ok())
                .unwrap_or(1000);
            let objects = state.objects.lock().await;
            let mut matching: Vec<(String, String, usize)> = objects
                .iter()
                .filter(|(key, _entry)| {
                    key.starts_with(&prefix)
                        && resume_after
                            .as_deref()
                            .is_none_or(|after| key.as_str() > after)
                })
                .map(|(key, entry)| (key.clone(), entry.etag.clone(), entry.bytes.len()))
                .collect();
            matching.sort();
            let truncated = max_keys > 0 && matching.len() > max_keys;
            let next_token = truncated.then(|| matching[max_keys - 1].0.clone());
            let entries: Vec<(String, String, usize)> =
                matching.into_iter().take(max_keys).collect();
            let xml = list_objects_xml(&prefix, &entries, truncated, next_token.as_deref());
            (StatusCode::OK, None, xml.into_bytes())
        } else {
            match key.as_deref() {
                None => (StatusCode::NOT_FOUND, None, Vec::new()),
                // DELETE: idempotent removal; absent key is still a
                // success (S3 semantics — delete of a missing object is
                // 204).
                Some(key) if method == Method::DELETE => {
                    let mut objects = state.objects.lock().await;
                    objects.remove(key);
                    (StatusCode::NO_CONTENT, None, Vec::new())
                }
                Some(key) if method == Method::PUT => {
                    state
                        .wait_for_conditional_head(
                            key,
                            sequence,
                            if_match.is_some() || if_none_match.is_some(),
                        )
                        .await;
                    let mut objects = state.objects.lock().await;
                    let existing = objects.get(key);
                    let condition_matches = match (&if_match, &if_none_match) {
                        (_, Some(value)) if value == "*" => existing.is_none(),
                        (Some(expected), _) => {
                            existing.is_some_and(|entry| entry.etag == unquoted_etag(expected))
                        }
                        _ => true,
                    };
                    if !condition_matches {
                        (StatusCode::PRECONDITION_FAILED, None, Vec::new())
                    } else {
                        // Match the measured Exoscale SOS behavior:
                        // single-PUT etags derive from content, so
                        // byte-identical writes recur across eras.
                        let etag = hex::encode(Sha256::digest(&bytes));
                        objects.insert(
                            key.to_owned(),
                            LoopbackObject {
                                bytes,
                                etag: etag.clone(),
                            },
                        );
                        (StatusCode::OK, Some(etag), Vec::new())
                    }
                }
                Some(key) if method == Method::GET || method == Method::HEAD => {
                    let objects = state.objects.lock().await;
                    match objects.get(key) {
                        Some(entry) => (
                            StatusCode::OK,
                            Some(entry.etag.clone()),
                            if method == Method::GET {
                                entry.bytes.clone()
                            } else {
                                Vec::new()
                            },
                        ),
                        None => (StatusCode::NOT_FOUND, None, Vec::new()),
                    }
                }
                Some(_) => (StatusCode::METHOD_NOT_ALLOWED, None, Vec::new()),
            }
        };
        let fault_observation = if let Some(fault) = fault {
            let effect_applied = status.is_success();
            if fault.phase == StorageFaultPhase::AfterEffect && effect_applied {
                match fault.cut {
                    StorageFaultCut::ImmutableChecksum => {
                        state.corrupt_object(&fault.key).await;
                    }
                    StorageFaultCut::ImmutableReadback => {
                        response_body = b"corrupt immutable readback".to_vec();
                    }
                    StorageFaultCut::ImmutableWrite
                    | StorageFaultCut::HeadCreate
                    | StorageFaultCut::HeadUpdate
                    // Delete applied; the response is lost (G117).
                    | StorageFaultCut::KeyspaceDelete => {
                        status = T002_FAULT_STATUS;
                        etag = None;
                        response_body.clear();
                    }
                }
            }
            let observation = StorageFaultObservation {
                cut: fault.cut,
                phase: fault.phase,
                key: fault.key,
                request_sequence: sequence,
                effect_applied,
            };
            state.record_fault(observation.clone()).await;
            Some(observation)
        } else {
            None
        };
        state
            .record(LoopbackRequestObservation {
                sequence,
                method: method.to_string(),
                key,
                if_match,
                if_none_match,
                status: status.as_u16(),
                fault: fault_observation,
            })
            .await;
        counterpart_response(status, etag.as_deref(), response_body)
    }

    async fn arm_loopback_head_barrier(
        State(state): State<CounterpartState>,
        Json(command): Json<ArmConditionalHeadBarrier>,
    ) -> StatusCode {
        state.arm_conditional_head_barrier(command.key).await;
        StatusCode::NO_CONTENT
    }

    async fn arm_loopback_storage_fault(
        State(state): State<CounterpartState>,
        Json(command): Json<ArmStorageFault>,
    ) -> StatusCode {
        state.arm_storage_fault(command).await;
        StatusCode::NO_CONTENT
    }

    async fn loopback_snapshot(State(state): State<CounterpartState>) -> Json<CounterpartSnapshot> {
        Json(state.snapshot().await)
    }

    async fn shutdown_loopback_counterpart(State(state): State<CounterpartState>) -> StatusCode {
        if let Some(shutdown) = state.shutdown.lock().await.take() {
            let _ = shutdown.send(());
        }
        StatusCode::NO_CONTENT
    }

    #[tokio::test]
    #[ignore = "separate loopback S3 counterpart process for gateway state contract tests"]
    async fn s3_loopback_counterpart_process() {
        let ready_path = std::env::var_os(T001_COUNTERPART_READY_PATH)
            .map(std::path::PathBuf::from)
            .expect("counterpart ready path");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback S3 counterpart");
        let address = listener
            .local_addr()
            .expect("loopback S3 counterpart address");
        let (shutdown, shutdown_receiver) = tokio::sync::oneshot::channel();
        let state = CounterpartState::new(shutdown);
        let app = Router::new()
            .route("/__t001__/barrier", post(arm_loopback_head_barrier))
            .route("/__t001__/fault", post(arm_loopback_storage_fault))
            .route("/__t001__/snapshot", get(loopback_snapshot))
            .route("/__t001__/shutdown", post(shutdown_loopback_counterpart))
            .fallback(any_route(loopback_s3_request))
            .with_state(state);
        let ready = CounterpartReady {
            endpoint: format!("http://{address}"),
        };
        std::fs::write(
            ready_path,
            serde_json::to_vec(&ready).expect("serialize counterpart readiness"),
        )
        .expect("write counterpart readiness");
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_receiver.await;
            })
            .await
            .expect("serve loopback S3 counterpart");
    }

    struct ReapedChild {
        child: Option<std::process::Child>,
    }

    impl ReapedChild {
        fn new(child: std::process::Child) -> Self {
            Self { child: Some(child) }
        }

        fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
            let Some(child) = self.child.as_mut() else {
                return Ok(None);
            };
            let status = child.try_wait()?;
            if status.is_some() {
                self.child.take();
            }
            Ok(status)
        }

        fn is_reaped(&self) -> bool {
            self.child.is_none()
        }

        async fn wait_for_exit(
            &mut self,
            timeout: Duration,
        ) -> std::io::Result<std::process::ExitStatus> {
            let deadline = tokio::time::Instant::now() + timeout;
            loop {
                if let Some(status) = self.try_wait()? {
                    return Ok(status);
                }
                if tokio::time::Instant::now() >= deadline {
                    return self.kill_and_wait();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        fn kill_and_wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
            let Some(mut child) = self.child.take() else {
                return Err(std::io::Error::other(
                    "loopback counterpart child already reaped",
                ));
            };
            if let Ok(Some(status)) = child.try_wait() {
                return Ok(status);
            }
            let kill_error = child.kill().err();
            match child.wait() {
                Ok(status) => Ok(status),
                Err(wait_error) => {
                    self.child = Some(child);
                    Err(kill_error.unwrap_or(wait_error))
                }
            }
        }

        fn reap_synchronously(&mut self) {
            let Some(mut child) = self.child.take() else {
                return;
            };
            match child.try_wait() {
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
        }
    }

    impl Drop for ReapedChild {
        fn drop(&mut self) {
            self.reap_synchronously();
        }
    }

    struct CounterpartOutput {
        stdout_path: PathBuf,
        stderr_path: PathBuf,
    }

    impl CounterpartOutput {
        fn in_directory(directory: &Path) -> Self {
            Self {
                stdout_path: directory.join("counterpart-stdout.log"),
                stderr_path: directory.join("counterpart-stderr.log"),
            }
        }

        fn stdout(&self) -> File {
            File::create(&self.stdout_path).expect("create loopback counterpart stdout log")
        }

        fn stderr(&self) -> File {
            File::create(&self.stderr_path).expect("create loopback counterpart stderr log")
        }

        fn diagnostics(&self) -> String {
            let stdout = std::fs::read_to_string(&self.stdout_path)
                .unwrap_or_else(|error| format!("<unavailable: {error}>"));
            let stderr = std::fs::read_to_string(&self.stderr_path)
                .unwrap_or_else(|error| format!("<unavailable: {error}>"));
            if stdout.is_empty() && stderr.is_empty() {
                String::new()
            } else {
                format!("\ncounterpart stdout:\n{stdout}\ncounterpart stderr:\n{stderr}")
            }
        }
    }

    pub struct LoopbackCounterpart {
        endpoint: String,
        control: reqwest::Client,
        child: ReapedChild,
        child_output: CounterpartOutput,
        _directory: tempfile::TempDir,
    }

    impl LoopbackCounterpart {
        pub async fn start() -> (Self, Arc<ObjectStoreClient>) {
            let directory = tempfile::tempdir().expect("counterpart temporary directory");
            let ready_path = directory.path().join("counterpart-ready.json");
            let child_output = CounterpartOutput::in_directory(directory.path());
            let test_binary = std::env::current_exe().expect("current test binary");
            let mut command = std::process::Command::new(test_binary);
            command
                .arg("--ignored")
                .arg("--exact")
                .arg("state_kernel::gateway_state_contract::s3_loopback_counterpart_process")
                .env(T001_COUNTERPART_READY_PATH, &ready_path)
                // Keep the child outside nextest's captured pipe graph while
                // retaining its failure output in the fixture's private directory.
                .stdin(Stdio::null())
                .stdout(child_output.stdout())
                .stderr(child_output.stderr());
            let mut child =
                ReapedChild::new(command.spawn().expect("spawn loopback S3 counterpart"));
            let ready = match wait_for_counterpart_ready(&mut child, &ready_path).await {
                Ok(ready) => ready,
                Err(error) => {
                    child.reap_synchronously();
                    panic!(
                        "loopback S3 counterpart failed before ready: {error}{}",
                        child_output.diagnostics()
                    );
                }
            };
            let counterpart = Self {
                endpoint: ready.endpoint.clone(),
                control: reqwest::Client::new(),
                child,
                child_output,
                _directory: directory,
            };
            let config = S3Config::custom_with_insecure_http(
                T001_COUNTERPART_BUCKET,
                "us-east-1",
                ready.endpoint,
                "t001-loopback-access-key",
                "t001-loopback-secret-key",
                true,
            );
            let store = Arc::new(
                ObjectStoreClient::new(&config).expect("construct real loopback ObjectStoreClient"),
            );
            (counterpart, store)
        }

        pub async fn arm_conditional_head_barrier(&self, key: &str) {
            self.control
                .post(format!("{}/__t001__/barrier", self.endpoint))
                .json(&ArmConditionalHeadBarrier {
                    key: key.to_owned(),
                })
                .send()
                .await
                .expect("arm loopback head barrier")
                .error_for_status()
                .expect("loopback head barrier response");
        }

        async fn arm_storage_fault(
            &self,
            cut: StorageFaultCut,
            phase: StorageFaultPhase,
            key: &str,
        ) {
            self.control
                .post(format!("{}/__t001__/fault", self.endpoint))
                .json(&ArmStorageFault {
                    cut,
                    phase,
                    key: key.to_owned(),
                })
                .send()
                .await
                .expect("arm loopback storage fault")
                .error_for_status()
                .expect("loopback storage fault response");
        }

        async fn snapshot(&self) -> CounterpartSnapshot {
            self.control
                .get(format!("{}/__t001__/snapshot", self.endpoint))
                .send()
                .await
                .expect("read loopback counterpart snapshot")
                .error_for_status()
                .expect("loopback counterpart snapshot response")
                .json()
                .await
                .expect("decode loopback counterpart snapshot")
        }

        pub async fn assert_conditional_head_race(&self, key: &str, create: bool) {
            let snapshot = self.snapshot().await;
            let barrier = snapshot.barrier.expect("armed head barrier observation");
            assert_eq!(barrier.key, key);
            assert_eq!(barrier.expected_arrivals, 2);
            assert_eq!(barrier.arrivals, 2);
            assert_eq!(barrier.passes, 2);

            let mut head_requests: Vec<_> = snapshot
                .requests
                .into_iter()
                .filter(|request| barrier.request_sequences.contains(&request.sequence))
                .collect();
            head_requests.sort_by_key(|request| request.sequence);
            assert_eq!(head_requests.len(), 2);
            assert!(
                head_requests
                    .windows(2)
                    .all(|requests| { requests[0].sequence < requests[1].sequence })
            );
            assert!(head_requests.iter().all(|request| {
                request.method == Method::PUT.as_str()
                    && request.key.as_deref() == Some(key)
                    && if create {
                        request.if_match.is_none() && request.if_none_match.as_deref() == Some("*")
                    } else {
                        request
                            .if_match
                            .as_deref()
                            .is_some_and(|etag| !etag.is_empty())
                            && request.if_none_match.is_none()
                    }
            }));
            let mut statuses: Vec<_> = head_requests.iter().map(|request| request.status).collect();
            statuses.sort_unstable();
            assert_eq!(
                statuses,
                vec![
                    StatusCode::OK.as_u16(),
                    StatusCode::PRECONDITION_FAILED.as_u16()
                ]
            );
        }

        pub async fn shutdown(mut self) {
            let graceful_shutdown = tokio::time::timeout(
                Duration::from_secs(1),
                self.control
                    .post(format!("{}/__t001__/shutdown", self.endpoint))
                    .send(),
            )
            .await
            .ok()
            .and_then(Result::ok)
            .is_some_and(|response| response.status().is_success());
            let status = self
                .child
                .wait_for_exit(T001_COUNTERPART_TIMEOUT)
                .await
                .expect("wait for loopback S3 counterpart");
            assert!(
                self.child.is_reaped(),
                "loopback S3 counterpart child must be reaped before fixture teardown"
            );
            if !status.success() {
                panic!(
                    "loopback S3 counterpart exited unsuccessfully after graceful={graceful_shutdown}: {status}{}",
                    self.child_output.diagnostics()
                );
            }
        }
    }

    async fn wait_for_counterpart_ready(
        child: &mut ReapedChild,
        ready_path: &Path,
    ) -> Result<CounterpartReady, String> {
        let deadline = tokio::time::Instant::now() + T001_COUNTERPART_TIMEOUT;
        let mut readiness_error = None;
        loop {
            if ready_path.exists() {
                match std::fs::read(ready_path)
                    .map_err(|error| format!("read counterpart readiness: {error}"))
                    .and_then(|bytes| {
                        serde_json::from_slice(&bytes)
                            .map_err(|error| format!("decode counterpart readiness: {error}"))
                    }) {
                    Ok(ready) => return Ok(ready),
                    Err(error) => readiness_error = Some(error),
                }
            }
            if let Some(status) = child
                .try_wait()
                .map_err(|error| format!("inspect loopback counterpart: {error}"))?
            {
                return Err(format!(
                    "loopback counterpart exited before ready: {status}"
                ));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(readiness_error.unwrap_or_else(|| {
                    "timed out waiting for loopback counterpart readiness".to_owned()
                }));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[derive(Clone, Copy)]
    enum HeadCondition {
        Create,
        Update,
    }

    async fn assert_barriered_head_race(fixture: &Fixture, condition: HeadCondition) {
        let snapshot = fixture.counterpart.snapshot().await;
        let barrier = snapshot.barrier.expect("armed head barrier observation");
        assert_eq!(barrier.key, fixture.lineage.head_key());
        assert_eq!(barrier.expected_arrivals, 2);
        assert_eq!(barrier.arrivals, 2);
        assert_eq!(barrier.passes, 2);

        let mut head_requests: Vec<_> = snapshot
            .requests
            .into_iter()
            .filter(|request| barrier.request_sequences.contains(&request.sequence))
            .collect();
        head_requests.sort_by_key(|request| request.sequence);
        assert_eq!(head_requests.len(), 2);
        assert!(
            head_requests
                .windows(2)
                .all(|requests| requests[0].sequence < requests[1].sequence),
            "counterpart request sequence remains strictly ordered"
        );
        assert!(head_requests.iter().all(|request| {
            request.method == Method::PUT.as_str()
                && request.key.as_deref() == Some(fixture.lineage.head_key().as_str())
        }));
        match condition {
            HeadCondition::Create => {
                assert!(head_requests.iter().all(|request| {
                    request.if_match.is_none() && request.if_none_match.as_deref() == Some("*")
                }));
            }
            HeadCondition::Update => {
                assert!(head_requests.iter().all(|request| {
                    request
                        .if_match
                        .as_deref()
                        .is_some_and(|etag| !etag.is_empty())
                        && request.if_none_match.is_none()
                }));
            }
        }
        let mut statuses: Vec<_> = head_requests.iter().map(|request| request.status).collect();
        statuses.sort_unstable();
        assert_eq!(
            statuses,
            vec![
                StatusCode::OK.as_u16(),
                StatusCode::PRECONDITION_FAILED.as_u16()
            ]
        );
    }

    struct Fixture {
        store: Arc<ObjectStoreClient>,
        lineage: KernelLineage,
        kernel: StateKernel,
        counterpart: LoopbackCounterpart,
    }

    impl Fixture {
        async fn shutdown(self) {
            // Drop the S3 client before asking its loopback server to drain.
            // A live keep-alive connection can otherwise keep the child alive
            // after the shutdown request has been accepted.
            let Self {
                store,
                lineage,
                kernel,
                counterpart,
            } = self;
            drop(kernel);
            drop(lineage);
            drop(store);
            counterpart.shutdown().await;
        }
    }

    async fn new_fixture(policy: SuccessorPolicy) -> Fixture {
        new_named_fixture("state/v1/kernel-contract", policy).await
    }

    async fn new_named_fixture(lineage_name: &str, policy: SuccessorPolicy) -> Fixture {
        let (counterpart, store) = LoopbackCounterpart::start().await;
        let lineage = KernelLineage::new(lineage_name, policy).expect("lineage");
        let kernel = StateKernel::new(Arc::clone(&store), lineage.clone());
        Fixture {
            store,
            lineage,
            kernel,
            counterpart,
        }
    }

    fn record(
        lineage: &KernelLineage,
        sequence: u64,
        prior: Option<RecordPosition>,
        payload: &[u8],
    ) -> CanonicalRecord {
        CanonicalRecord::new(
            lineage,
            sequence,
            prior,
            "opaque",
            "opaque.v1",
            payload.to_vec(),
            format!("operation-{sequence}"),
            "actor-test",
            "cause-test",
        )
        .expect("record")
    }

    async fn raw_upload(store: &ObjectStoreClient, key: &str, bytes: Vec<u8>) {
        store
            .upload(key, bytes.into())
            .await
            .expect("controlled counterpart upload");
    }

    async fn raw_create(store: &ObjectStoreClient, key: &str, bytes: Vec<u8>) {
        store
            .upload_conditional(key, bytes.into(), None)
            .await
            .expect("controlled counterpart create");
    }

    async fn seed_head(
        fixture: &Fixture,
        generation: u64,
        digest: RecordDigest,
        prior: Option<RecordPosition>,
    ) {
        let head = CanonicalHead {
            lineage: fixture.lineage.clone(),
            generation,
            record_digest: digest,
            prior,
        };
        raw_create(
            &fixture.store,
            &fixture.lineage.head_key(),
            head.canonical_bytes().expect("head bytes"),
        )
        .await;
    }

    async fn two_record_history(fixture: &Fixture) -> (CanonicalRecord, CanonicalRecord, HeadRead) {
        let first = record(&fixture.lineage, 0, None, b"first");
        let first_head = fixture
            .kernel
            .append_genesis(&first)
            .await
            .expect("genesis");
        let second = record(
            &fixture.lineage,
            1,
            Some(first_head.record_position()),
            b"second",
        );
        let second_head = fixture
            .kernel
            .append_successor(&second, &first_head)
            .await
            .expect("successor");
        (first, second, second_head)
    }

    #[derive(Default)]
    struct ByteFold;

    impl ByteFold {
        fn encode(parts: &[Vec<u8>]) -> Vec<u8> {
            let mut encoded = Vec::new();
            for part in parts {
                let length = u32::try_from(part.len()).expect("bounded fixture payload");
                encoded.extend_from_slice(&length.to_be_bytes());
                encoded.extend_from_slice(part);
            }
            encoded
        }

        fn decode(bytes: &[u8]) -> Result<Vec<Vec<u8>>, ()> {
            let mut cursor = 0;
            let mut parts = Vec::new();
            while cursor < bytes.len() {
                let length = bytes.get(cursor..cursor + 4).ok_or(())?;
                let length = u32::from_be_bytes(length.try_into().map_err(|_| ())?) as usize;
                cursor += 4;
                let part = bytes.get(cursor..cursor + length).ok_or(())?;
                parts.push(part.to_vec());
                cursor += length;
            }
            Ok(parts)
        }
    }

    impl LineageFold for ByteFold {
        type State = Vec<Vec<u8>>;

        fn validate_transition(&self, record: &FoldRecord<'_>) -> Result<(), ()> {
            (record.transition_type() == "opaque" && record.transition_schema() == "opaque.v1")
                .then_some(())
                .ok_or(())
        }

        fn initial_state(&self) -> Self::State {
            Vec::new()
        }

        fn apply(&self, state: &mut Self::State, record: &FoldRecord<'_>) -> Result<(), ()> {
            state.push(record.payload().to_vec());
            Ok(())
        }

        fn canonical_state(&self, state: &Self::State) -> Result<Vec<u8>, ()> {
            Ok(Self::encode(state))
        }

        fn restore_checkpoint(
            &self,
            transition_schema: &str,
            state_bytes: &[u8],
        ) -> Result<Self::State, ()> {
            if transition_schema != "opaque.v1" {
                return Err(());
            }
            Self::decode(state_bytes)
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum StorageFaultState {
        Prior,
        Orphan,
        Complete,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct StorageFaultCase {
        cut: StorageFaultCut,
        phase: StorageFaultPhase,
        expected_state: StorageFaultState,
    }

    const STORAGE_FAULT_CASES: [StorageFaultCase; 10] = [
        StorageFaultCase {
            cut: StorageFaultCut::ImmutableWrite,
            phase: StorageFaultPhase::BeforeEffect,
            expected_state: StorageFaultState::Prior,
        },
        StorageFaultCase {
            cut: StorageFaultCut::ImmutableWrite,
            phase: StorageFaultPhase::AfterEffect,
            expected_state: StorageFaultState::Orphan,
        },
        StorageFaultCase {
            cut: StorageFaultCut::ImmutableChecksum,
            phase: StorageFaultPhase::BeforeEffect,
            expected_state: StorageFaultState::Prior,
        },
        StorageFaultCase {
            cut: StorageFaultCut::ImmutableChecksum,
            phase: StorageFaultPhase::AfterEffect,
            expected_state: StorageFaultState::Orphan,
        },
        StorageFaultCase {
            cut: StorageFaultCut::ImmutableReadback,
            phase: StorageFaultPhase::BeforeEffect,
            expected_state: StorageFaultState::Orphan,
        },
        StorageFaultCase {
            cut: StorageFaultCut::ImmutableReadback,
            phase: StorageFaultPhase::AfterEffect,
            expected_state: StorageFaultState::Orphan,
        },
        StorageFaultCase {
            cut: StorageFaultCut::HeadCreate,
            phase: StorageFaultPhase::BeforeEffect,
            expected_state: StorageFaultState::Orphan,
        },
        StorageFaultCase {
            cut: StorageFaultCut::HeadCreate,
            phase: StorageFaultPhase::AfterEffect,
            expected_state: StorageFaultState::Complete,
        },
        StorageFaultCase {
            cut: StorageFaultCut::HeadUpdate,
            phase: StorageFaultPhase::BeforeEffect,
            expected_state: StorageFaultState::Orphan,
        },
        StorageFaultCase {
            cut: StorageFaultCut::HeadUpdate,
            phase: StorageFaultPhase::AfterEffect,
            expected_state: StorageFaultState::Complete,
        },
    ];

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum FaultCallResult {
        Acknowledged,
        DigestMismatch,
        StateUnavailable,
    }

    struct StorageFaultReceipt {
        case: StorageFaultCase,
        result: FaultCallResult,
        actual_state: StorageFaultState,
        candidate_exists: bool,
        candidate_reachable: bool,
        folded: Option<Vec<u8>>,
        prior_fold: Option<Vec<u8>>,
        complete_fold: Vec<u8>,
        fault: StorageFaultObservation,
        fault_status: u16,
        fault_recorded_on_request: bool,
        put_count: usize,
    }

    fn classify_storage_fault_result(result: Result<HeadRead, KernelError>) -> FaultCallResult {
        match result {
            Ok(_) => FaultCallResult::Acknowledged,
            Err(KernelError::DigestMismatch { .. }) => FaultCallResult::DigestMismatch,
            Err(KernelError::StateUnavailable { .. }) => FaultCallResult::StateUnavailable,
            Err(error) => panic!("unexpected storage-fault result: {error:?}"),
        }
    }

    fn expected_storage_fault_result(case: StorageFaultCase) -> FaultCallResult {
        match (case.cut, case.phase) {
            (StorageFaultCut::ImmutableChecksum, StorageFaultPhase::AfterEffect)
            | (StorageFaultCut::ImmutableReadback, StorageFaultPhase::AfterEffect) => {
                FaultCallResult::DigestMismatch
            }
            _ => FaultCallResult::StateUnavailable,
        }
    }

    fn expected_storage_fault_status(case: StorageFaultCase) -> u16 {
        match (case.cut, case.phase) {
            // KeyspaceDelete AfterEffect (response lost, G117) shares
            // the fault status of the other lost-response cuts; proven
            // by its own contract.
            (_, StorageFaultPhase::BeforeEffect)
            | (StorageFaultCut::ImmutableWrite, StorageFaultPhase::AfterEffect)
            | (StorageFaultCut::HeadCreate, StorageFaultPhase::AfterEffect)
            | (StorageFaultCut::HeadUpdate, StorageFaultPhase::AfterEffect)
            | (StorageFaultCut::KeyspaceDelete, StorageFaultPhase::AfterEffect) => {
                T002_FAULT_STATUS.as_u16()
            }
            (StorageFaultCut::ImmutableChecksum, StorageFaultPhase::AfterEffect)
            | (StorageFaultCut::ImmutableReadback, StorageFaultPhase::AfterEffect) => {
                StatusCode::OK.as_u16()
            }
        }
    }

    async fn record_storage_fault(
        fixture: &Fixture,
        before: &CounterpartSnapshot,
        case: StorageFaultCase,
    ) -> (StorageFaultObservation, u16, bool, usize) {
        let snapshot = fixture.counterpart.snapshot().await;
        let faults = &snapshot.faults[before.faults.len()..];
        assert_eq!(faults.len(), 1, "one-shot fault must record exactly once");
        let fault = faults[0].clone();
        assert_eq!(fault.cut, case.cut);
        assert_eq!(fault.phase, case.phase);
        assert_eq!(
            fault.effect_applied,
            case.phase == StorageFaultPhase::AfterEffect
        );
        let request = snapshot
            .requests
            .iter()
            .find(|request| request.sequence == fault.request_sequence)
            .expect("counterpart must retain the controlled request");
        let recorded_on_request = request.fault.as_ref() == Some(&fault);
        let put_count = snapshot.requests[before.requests.len()..]
            .iter()
            .filter(|request| request.method == Method::PUT.as_str())
            .count();
        (fault, request.status, recorded_on_request, put_count)
    }

    async fn run_successor_storage_fault(case: StorageFaultCase) -> StorageFaultReceipt {
        let fixture = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let first = record(&fixture.lineage, 0, None, b"first");
        let first_head = fixture
            .kernel
            .append_genesis(&first)
            .await
            .expect("initial head");
        let candidate = record(
            &fixture.lineage,
            1,
            Some(first_head.record_position()),
            b"candidate",
        );
        let candidate_digest = candidate.digest().expect("candidate digest");
        let candidate_key = fixture.lineage.object_key(&candidate_digest);
        let fault_key = if case.cut == StorageFaultCut::HeadUpdate {
            fixture.lineage.head_key()
        } else {
            candidate_key.clone()
        };
        let before = fixture.counterpart.snapshot().await;
        fixture
            .counterpart
            .arm_storage_fault(case.cut, case.phase, &fault_key)
            .await;
        let result = classify_storage_fault_result(
            fixture
                .kernel
                .append_successor(&candidate, &first_head)
                .await,
        );
        let candidate_exists = fixture.store.download(&candidate_key).await.is_ok();
        let head = fixture
            .kernel
            .read_head()
            .await
            .expect("readable canonical head");
        let candidate_reachable = head.record_digest() == &candidate_digest;
        let folded = fixture
            .kernel
            .fold(None, &ByteFold)
            .await
            .expect("canonical fold after storage cut")
            .canonical_state;
        let actual_state = if candidate_reachable {
            StorageFaultState::Complete
        } else if candidate_exists {
            StorageFaultState::Orphan
        } else {
            StorageFaultState::Prior
        };
        let (fault, fault_status, fault_recorded_on_request, put_count) =
            record_storage_fault(&fixture, &before, case).await;
        let receipt = StorageFaultReceipt {
            case,
            result,
            actual_state,
            candidate_exists,
            candidate_reachable,
            folded: Some(folded),
            prior_fold: Some(ByteFold::encode(&[b"first".to_vec()])),
            complete_fold: ByteFold::encode(&[b"first".to_vec(), b"candidate".to_vec()]),
            fault,
            fault_status,
            fault_recorded_on_request,
            put_count,
        };
        fixture.shutdown().await;
        receipt
    }

    async fn run_head_create_storage_fault(case: StorageFaultCase) -> StorageFaultReceipt {
        let fixture = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let candidate = record(&fixture.lineage, 0, None, b"candidate");
        let candidate_digest = candidate.digest().expect("candidate digest");
        let candidate_key = fixture.lineage.object_key(&candidate_digest);
        let before = fixture.counterpart.snapshot().await;
        fixture
            .counterpart
            .arm_storage_fault(case.cut, case.phase, &fixture.lineage.head_key())
            .await;
        let result = classify_storage_fault_result(fixture.kernel.append_genesis(&candidate).await);
        let candidate_exists = fixture.store.download(&candidate_key).await.is_ok();
        let (actual_state, folded) = match fixture.kernel.read_head().await {
            Ok(head) => {
                assert_eq!(head.record_digest(), &candidate_digest);
                let folded = fixture
                    .kernel
                    .fold(None, &ByteFold)
                    .await
                    .expect("complete genesis fold")
                    .canonical_state;
                (StorageFaultState::Complete, Some(folded))
            }
            Err(KernelError::StateHistoryIncomplete { .. }) => {
                let error = fixture
                    .kernel
                    .fold(None, &ByteFold)
                    .await
                    .expect_err("orphan genesis cannot enter a fold");
                assert!(matches!(error, KernelError::StateHistoryIncomplete { .. }));
                (
                    if candidate_exists {
                        StorageFaultState::Orphan
                    } else {
                        StorageFaultState::Prior
                    },
                    None,
                )
            }
            Err(error) => panic!("unexpected canonical head result: {error:?}"),
        };
        let candidate_reachable = actual_state == StorageFaultState::Complete;
        let (fault, fault_status, fault_recorded_on_request, put_count) =
            record_storage_fault(&fixture, &before, case).await;
        let receipt = StorageFaultReceipt {
            case,
            result,
            actual_state,
            candidate_exists,
            candidate_reachable,
            folded,
            prior_fold: None,
            complete_fold: ByteFold::encode(&[b"candidate".to_vec()]),
            fault,
            fault_status,
            fault_recorded_on_request,
            put_count,
        };
        fixture.shutdown().await;
        receipt
    }

    async fn run_storage_fault_case(case: StorageFaultCase) -> StorageFaultReceipt {
        match case.cut {
            StorageFaultCut::HeadCreate => run_head_create_storage_fault(case).await,
            StorageFaultCut::ImmutableWrite
            | StorageFaultCut::ImmutableChecksum
            | StorageFaultCut::ImmutableReadback
            | StorageFaultCut::HeadUpdate => run_successor_storage_fault(case).await,
            // Keyspace deletes are proven by their own loopback
            // contract (a9_delete_many_resumes_after_lost_response_cut);
            // they carry no lineage semantics.
            StorageFaultCut::KeyspaceDelete => {
                unreachable!("keyspace delete cut has no lineage receipt")
            }
        }
    }

    async fn assert_checkpoint_discarded(
        fixture: &Fixture,
        checkpoint_digest: &RecordDigest,
        expected_code: CheckpointRejectionCode,
    ) {
        let result = fixture
            .kernel
            .fold(Some(checkpoint_digest), &ByteFold)
            .await
            .expect("valid history remains authoritative");
        assert_eq!(
            result.canonical_state,
            ByteFold::encode(&[b"first".to_vec(), b"second".to_vec()])
        );
        assert_eq!(result.checkpoint_rejections.len(), 1);
        assert_eq!(result.checkpoint_rejections[0].code, expected_code);
    }

    async fn seed_checkpoint_rejection_cases(
        fixture: &Fixture,
        source_digest: &RecordDigest,
    ) -> Vec<(RecordDigest, CheckpointRejectionCode)> {
        let foreign_lineage = KernelLineage::new(
            "state/v1/foreign-checkpoint",
            SuccessorPolicy::SuccessorCapable,
        )
        .expect("foreign lineage");
        let foreign = CanonicalCheckpoint::new(
            &foreign_lineage,
            "opaque.v1",
            0,
            source_digest.clone(),
            source_digest.clone(),
            ByteFold::encode(&[b"poison".to_vec()]),
        )
        .expect("foreign checkpoint");
        let foreign_digest = foreign.digest().expect("foreign checkpoint digest");
        raw_upload(
            &fixture.store,
            &fixture.lineage.checkpoint_key(&foreign_digest),
            foreign.canonical_bytes().expect("foreign checkpoint bytes"),
        )
        .await;

        let malformed_bytes = b"{malformed checkpoint".to_vec();
        let malformed_digest = RecordDigest::of(&malformed_bytes);
        raw_upload(
            &fixture.store,
            &fixture.lineage.checkpoint_key(&malformed_digest),
            malformed_bytes,
        )
        .await;

        let noncanonical = CanonicalCheckpoint::new(
            &fixture.lineage,
            "opaque.v1",
            0,
            source_digest.clone(),
            source_digest.clone(),
            ByteFold::encode(&[b"source".to_vec()]),
        )
        .expect("noncanonical checkpoint source");
        let mut noncanonical_bytes = noncanonical
            .canonical_bytes()
            .expect("noncanonical checkpoint bytes");
        noncanonical_bytes.push(b' ');
        let noncanonical_digest = RecordDigest::of(&noncanonical_bytes);
        raw_upload(
            &fixture.store,
            &fixture.lineage.checkpoint_key(&noncanonical_digest),
            noncanonical_bytes,
        )
        .await;

        let digest_mismatch = RecordDigest::of(b"expected checkpoint");
        raw_upload(
            &fixture.store,
            &fixture.lineage.checkpoint_key(&digest_mismatch),
            b"different checkpoint bytes".to_vec(),
        )
        .await;

        let missing_source = RecordDigest::of(b"missing checkpoint source");
        let missing_basis = CanonicalCheckpoint::new(
            &fixture.lineage,
            "opaque.v1",
            0,
            missing_source.clone(),
            missing_source,
            ByteFold::encode(&[b"source".to_vec()]),
        )
        .expect("missing basis checkpoint");
        let missing_basis_digest = fixture
            .kernel
            .publish_checkpoint(&missing_basis)
            .await
            .expect("publish missing basis checkpoint");

        let future_source = RecordDigest::of(b"future checkpoint source");
        let stale = CanonicalCheckpoint::new(
            &fixture.lineage,
            "opaque.v1",
            2,
            future_source.clone(),
            future_source,
            ByteFold::encode(&[b"source".to_vec(), b"future".to_vec()]),
        )
        .expect("future checkpoint");
        let stale_digest = fixture
            .kernel
            .publish_checkpoint(&stale)
            .await
            .expect("publish future checkpoint");

        let disagreeing = CanonicalCheckpoint::new(
            &fixture.lineage,
            "opaque.v1",
            0,
            source_digest.clone(),
            source_digest.clone(),
            ByteFold::encode(&[b"poison".to_vec()]),
        )
        .expect("disagreeing checkpoint");
        let disagreeing_digest = fixture
            .kernel
            .publish_checkpoint(&disagreeing)
            .await
            .expect("publish disagreeing checkpoint");

        vec![
            (
                foreign_digest,
                CheckpointRejectionCode::StateRecordMalformed,
            ),
            (
                malformed_digest,
                CheckpointRejectionCode::StateRecordMalformed,
            ),
            (
                noncanonical_digest,
                CheckpointRejectionCode::StateRecordMalformed,
            ),
            (digest_mismatch, CheckpointRejectionCode::DigestMismatch),
            (
                missing_basis_digest,
                CheckpointRejectionCode::StateHistoryIncomplete,
            ),
            (
                stale_digest,
                CheckpointRejectionCode::StateHistoryIncomplete,
            ),
            (
                disagreeing_digest,
                CheckpointRejectionCode::StateHistoryIncomplete,
            ),
        ]
    }

    #[tokio::test]
    async fn k1_immutable_append_positive() {
        let fixture = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let first = record(&fixture.lineage, 0, None, b"immutable");
        let digest = fixture
            .kernel
            .publish_record(&first)
            .await
            .expect("publish");
        let retry = fixture
            .kernel
            .publish_record(&first)
            .await
            .expect("exact retry converges");

        assert_eq!(digest, retry);
        let stored = fixture
            .store
            .download(&fixture.lineage.object_key(&digest))
            .await
            .expect("readback");
        assert_eq!(
            stored.as_ref(),
            first.canonical_bytes().expect("bytes").as_slice()
        );

        let checkpoint = CanonicalCheckpoint::new(
            &fixture.lineage,
            "opaque.v1",
            0,
            digest.clone(),
            digest,
            b"checkpoint-state".to_vec(),
        )
        .expect("checkpoint");
        let checkpoint_digest = fixture
            .kernel
            .publish_checkpoint(&checkpoint)
            .await
            .expect("checkpoint publish");
        let checkpoint_retry = fixture
            .kernel
            .publish_checkpoint(&checkpoint)
            .await
            .expect("checkpoint exact retry converges");
        assert_eq!(checkpoint_digest, checkpoint_retry);
        assert_eq!(
            fixture
                .store
                .download(&fixture.lineage.checkpoint_key(&checkpoint_digest))
                .await
                .expect("checkpoint readback")
                .as_ref(),
            checkpoint
                .canonical_bytes()
                .expect("checkpoint bytes")
                .as_slice()
        );
        fixture.shutdown().await;
    }

    #[tokio::test]
    async fn k1_immutable_append_negative() {
        let fixture = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let genesis = record(&fixture.lineage, 0, None, b"genesis");
        let head = fixture
            .kernel
            .append_genesis(&genesis)
            .await
            .expect("genesis");
        let first = record(
            &fixture.lineage,
            1,
            Some(head.record_position()),
            b"immutable",
        );
        let digest = first.digest().expect("digest");
        let key = fixture.lineage.object_key(&digest);
        raw_upload(&fixture.store, &key, b"different".to_vec()).await;

        let error = fixture
            .kernel
            .append_successor(&first, &head)
            .await
            .expect_err("divergence refuses");
        assert!(matches!(error, KernelError::DigestMismatch { .. }));
        assert_eq!(
            fixture
                .store
                .download(&key)
                .await
                .expect("readback")
                .as_ref(),
            b"different"
        );
        assert_eq!(
            fixture.kernel.read_head().await.expect("head").head,
            head.head
        );

        let checkpoint = CanonicalCheckpoint::new(
            &fixture.lineage,
            "opaque.v1",
            0,
            head.record_digest().clone(),
            head.record_digest().clone(),
            b"checkpoint".to_vec(),
        )
        .expect("checkpoint");
        let checkpoint_digest = checkpoint.digest().expect("checkpoint digest");
        let checkpoint_key = fixture.lineage.checkpoint_key(&checkpoint_digest);
        raw_upload(
            &fixture.store,
            &checkpoint_key,
            b"different checkpoint".to_vec(),
        )
        .await;
        let error = fixture
            .kernel
            .publish_checkpoint(&checkpoint)
            .await
            .expect_err("checkpoint divergence refuses");
        assert!(matches!(error, KernelError::DigestMismatch { .. }));
        assert_eq!(
            fixture
                .store
                .download(&checkpoint_key)
                .await
                .expect("checkpoint readback")
                .as_ref(),
            b"different checkpoint"
        );

        let malformed_digest = RecordDigest::of(b"claimed bytes");
        raw_upload(
            &fixture.store,
            &fixture.lineage.object_key(&malformed_digest),
            b"other bytes".to_vec(),
        )
        .await;
        let error = fixture
            .kernel
            .load_record(&malformed_digest)
            .await
            .expect_err("claimed digest mismatch refuses");
        assert!(matches!(error, KernelError::DigestMismatch { .. }));
        fixture.shutdown().await;
    }

    #[tokio::test]
    async fn k2_canonical_head_cas_positive() {
        let fixture = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let first = record(&fixture.lineage, 0, None, b"first");
        let first_head = fixture
            .kernel
            .append_genesis(&first)
            .await
            .expect("genesis");
        let second = record(
            &fixture.lineage,
            1,
            Some(first_head.record_position()),
            b"second",
        );
        let second_head = fixture
            .kernel
            .append_successor(&second, &first_head)
            .await
            .expect("successor");

        assert_eq!(second_head.generation(), 1);
        assert_eq!(
            second_head.record_digest(),
            &second.digest().expect("digest")
        );
        assert_eq!(second_head.head.prior, Some(first_head.record_position()));
        let reread = fixture.kernel.read_head().await.expect("head reread");
        assert_eq!(reread.head, second_head.head);
        assert_eq!(
            fixture
                .store
                .download(&fixture.lineage.head_key())
                .await
                .expect("head bytes")
                .as_ref(),
            second_head
                .head
                .canonical_bytes()
                .expect("canonical head")
                .as_slice()
        );
        fixture.shutdown().await;
    }

    #[tokio::test]
    async fn k2_canonical_head_cas_negative() {
        let genesis_only = new_fixture(SuccessorPolicy::GenesisOnly).await;
        let first = record(&genesis_only.lineage, 0, None, b"first");
        let first_head = genesis_only
            .kernel
            .append_genesis(&first)
            .await
            .expect("genesis");
        let forbidden = record(
            &genesis_only.lineage,
            1,
            Some(first_head.record_position()),
            b"forbidden",
        );
        let forbidden_digest = forbidden.digest().expect("digest");
        let error = genesis_only
            .kernel
            .append_successor(&forbidden, &first_head)
            .await
            .expect_err("genesis-only lineages refuse before update");
        assert!(matches!(error, KernelError::SuccessorNotAllowed { .. }));
        assert!(matches!(
            genesis_only
                .store
                .download(&genesis_only.lineage.object_key(&forbidden_digest))
                .await,
            Err(ObjectStoreError::NotFound(_))
        ));
        genesis_only.shutdown().await;

        let fixture = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let first = record(&fixture.lineage, 0, None, b"first");
        let first_head = fixture
            .kernel
            .append_genesis(&first)
            .await
            .expect("genesis");
        let duplicate_genesis = record(&fixture.lineage, 0, None, b"duplicate genesis");
        let error = fixture
            .kernel
            .append_genesis(&duplicate_genesis)
            .await
            .expect_err("only one genesis head may win");
        assert!(matches!(error, KernelError::LineageHeadConflict { .. }));
        assert_eq!(
            fixture.kernel.read_head().await.expect("head").head,
            first_head.head
        );
        let second = record(
            &fixture.lineage,
            1,
            Some(first_head.record_position()),
            b"second",
        );

        let mut wrong_generation = first_head.clone();
        wrong_generation.head.generation = 9;
        let error = fixture
            .kernel
            .append_successor(&second, &wrong_generation)
            .await
            .expect_err("wrong expected generation refuses");
        assert!(matches!(error, KernelError::LineageHeadConflict { .. }));

        let mut wrong_etag = first_head.clone();
        wrong_etag.etag = "wrong-etag".to_owned();
        let error = fixture
            .kernel
            .append_successor(&second, &wrong_etag)
            .await
            .expect_err("wrong expected etag refuses");
        assert!(matches!(error, KernelError::LineageHeadConflict { .. }));

        let other = new_named_fixture(
            "state/v1/foreign-lineage",
            SuccessorPolicy::SuccessorCapable,
        )
        .await;
        let other_record = record(&other.lineage, 0, None, b"other");
        let other_head = other
            .kernel
            .append_genesis(&other_record)
            .await
            .expect("other head");
        let error = fixture
            .kernel
            .append_successor(&second, &other_head)
            .await
            .expect_err("wrong lineage refuses");
        assert!(matches!(error, KernelError::LineageHeadConflict { .. }));
        other.shutdown().await;

        let winning = fixture
            .kernel
            .append_successor(&second, &first_head)
            .await
            .expect("winner");
        let stale = record(
            &fixture.lineage,
            1,
            Some(first_head.record_position()),
            b"stale",
        );
        let error = fixture
            .kernel
            .append_successor(&stale, &first_head)
            .await
            .expect_err("stale fence refuses");
        let current = match error {
            KernelError::LineageHeadConflict {
                current: Some(reference),
            } => reference,
            other => panic!("expected readable lineage head conflict, got {other:?}"),
        };
        assert_eq!(current.lineage(), fixture.lineage.value);
        assert_eq!(current.generation(), Some(winning.generation()));
        assert_eq!(current.digest(), Some(winning.record_digest()));
        assert_eq!(
            fixture.kernel.read_head().await.expect("head").head,
            winning.head
        );
        fixture.shutdown().await;
    }

    #[tokio::test]
    async fn k3_typed_contention_positive() {
        let exact = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let exact_record = record(&exact.lineage, 0, None, b"exact");
        exact
            .counterpart
            .arm_conditional_head_barrier(&exact.lineage.head_key())
            .await;
        let (left_result, right_result) = tokio::join!(
            exact.kernel.append_genesis(&exact_record),
            exact.kernel.append_genesis(&exact_record)
        );
        let winners = [left_result.as_ref().ok(), right_result.as_ref().ok()];
        assert_eq!(winners.iter().filter(|winner| winner.is_some()).count(), 1);
        let loser = if left_result.is_err() {
            &left_result
        } else {
            &right_result
        };
        let current = match loser {
            Err(KernelError::LineageHeadConflict {
                current: Some(reference),
            }) => reference,
            other => panic!("exact genesis loser must be typed, got {other:?}"),
        };
        let exact_digest = exact_record.digest().expect("exact digest");
        assert_eq!(current.lineage(), exact.lineage.value);
        assert_eq!(current.generation(), Some(0));
        assert_eq!(current.digest(), Some(&exact_digest));
        assert_eq!(
            exact
                .kernel
                .read_head()
                .await
                .expect("winner head")
                .generation(),
            0
        );
        assert_barriered_head_race(&exact, HeadCondition::Create).await;
        exact.shutdown().await;

        let divergent = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let left = record(&divergent.lineage, 0, None, b"left");
        let right = record(&divergent.lineage, 0, None, b"right");
        divergent
            .counterpart
            .arm_conditional_head_barrier(&divergent.lineage.head_key())
            .await;
        let (left_result, right_result) = tokio::join!(
            divergent.kernel.append_genesis(&left),
            divergent.kernel.append_genesis(&right)
        );
        assert_eq!(
            [left_result.as_ref().ok(), right_result.as_ref().ok()]
                .iter()
                .filter(|winner| winner.is_some())
                .count(),
            1
        );
        let loser = if left_result.is_err() {
            &left_result
        } else {
            &right_result
        };
        let current = match loser {
            Err(KernelError::LineageHeadConflict {
                current: Some(reference),
            }) => reference,
            other => panic!("divergent genesis loser must be typed, got {other:?}"),
        };
        assert_eq!(current.lineage(), divergent.lineage.value);
        assert_eq!(current.generation(), Some(0));
        let current = divergent.kernel.read_head().await.expect("winner head");
        assert!(
            current.record_digest() == &left.digest().expect("left digest")
                || current.record_digest() == &right.digest().expect("right digest")
        );
        assert_barriered_head_race(&divergent, HeadCondition::Create).await;
        divergent.shutdown().await;
    }

    #[tokio::test]
    async fn k3_typed_contention_negative() {
        let exact = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let first = record(&exact.lineage, 0, None, b"first");
        let head = exact.kernel.append_genesis(&first).await.expect("genesis");
        let exact_record = record(&exact.lineage, 1, Some(head.record_position()), b"exact");
        exact
            .counterpart
            .arm_conditional_head_barrier(&exact.lineage.head_key())
            .await;
        let (left_result, right_result) = tokio::join!(
            exact.kernel.append_successor(&exact_record, &head),
            exact.kernel.append_successor(&exact_record, &head)
        );
        assert_eq!(
            [left_result.as_ref().ok(), right_result.as_ref().ok()]
                .iter()
                .filter(|winner| winner.is_some())
                .count(),
            1
        );
        let loser = if left_result.is_err() {
            &left_result
        } else {
            &right_result
        };
        let conflict = match loser {
            Err(KernelError::LineageHeadConflict {
                current: Some(reference),
            }) => reference,
            other => panic!("exact successor loser must be typed, got {other:?}"),
        };
        let exact_digest = exact_record.digest().expect("exact digest");
        assert_eq!(conflict.lineage(), exact.lineage.value);
        assert_eq!(conflict.generation(), Some(1));
        assert_eq!(conflict.digest(), Some(&exact_digest));
        let current = exact.kernel.read_head().await.expect("current head");
        assert_eq!(current.generation(), 1);
        assert_eq!(
            current.record_digest(),
            &exact_record.digest().expect("exact digest")
        );
        assert_barriered_head_race(&exact, HeadCondition::Update).await;
        exact.shutdown().await;

        let divergent = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let first = record(&divergent.lineage, 0, None, b"first");
        let head = divergent
            .kernel
            .append_genesis(&first)
            .await
            .expect("genesis");
        let left = record(&divergent.lineage, 1, Some(head.record_position()), b"left");
        let right = record(
            &divergent.lineage,
            1,
            Some(head.record_position()),
            b"right",
        );
        divergent
            .counterpart
            .arm_conditional_head_barrier(&divergent.lineage.head_key())
            .await;
        let (left_result, right_result) = tokio::join!(
            divergent.kernel.append_successor(&left, &head),
            divergent.kernel.append_successor(&right, &head)
        );
        assert_eq!(
            [left_result.as_ref().ok(), right_result.as_ref().ok()]
                .iter()
                .filter(|winner| winner.is_some())
                .count(),
            1
        );
        let loser = if left_result.is_err() {
            &left_result
        } else {
            &right_result
        };
        let conflict = match loser {
            Err(KernelError::LineageHeadConflict {
                current: Some(reference),
            }) => reference,
            other => panic!("divergent successor loser must be typed, got {other:?}"),
        };
        assert_eq!(conflict.lineage(), divergent.lineage.value);
        assert_eq!(conflict.generation(), Some(1));
        let current = divergent.kernel.read_head().await.expect("current head");
        assert!(
            current.record_digest() == &left.digest().expect("left digest")
                || current.record_digest() == &right.digest().expect("right digest")
        );
        assert_barriered_head_race(&divergent, HeadCondition::Update).await;
        divergent.shutdown().await;
    }

    #[tokio::test]
    async fn batch_publication_has_one_head_visibility_boundary() {
        let genesis = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let first = record(&genesis.lineage, 0, None, b"first");
        let second = record(
            &genesis.lineage,
            1,
            Some(RecordPosition {
                generation: 0,
                digest: first.digest().expect("first digest"),
            }),
            b"second",
        );
        let head = genesis
            .kernel
            .append_genesis_batch(&[first.clone(), second.clone()])
            .await
            .expect("batch genesis");
        assert_eq!(head.generation(), 1);
        assert_eq!(
            head.record_digest(),
            &second.digest().expect("second digest")
        );
        let folded = genesis
            .kernel
            .fold(None, &ByteFold)
            .await
            .expect("batch fold");
        assert_eq!(folded.state, vec![b"first".to_vec(), b"second".to_vec()]);
        genesis.shutdown().await;

        let fixture = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let base = record(&fixture.lineage, 0, None, b"base");
        let base_head = fixture
            .kernel
            .append_genesis(&base)
            .await
            .expect("base genesis");
        let left_first = record(
            &fixture.lineage,
            1,
            Some(base_head.record_position()),
            b"left first",
        );
        let left_last = record(
            &fixture.lineage,
            2,
            Some(RecordPosition {
                generation: 1,
                digest: left_first.digest().expect("left first digest"),
            }),
            b"left last",
        );
        let right_first = record(
            &fixture.lineage,
            1,
            Some(base_head.record_position()),
            b"right first",
        );
        let right_last = record(
            &fixture.lineage,
            2,
            Some(RecordPosition {
                generation: 1,
                digest: right_first.digest().expect("right first digest"),
            }),
            b"right last",
        );
        fixture
            .counterpart
            .arm_conditional_head_barrier(&fixture.lineage.head_key())
            .await;
        let left_batch = [left_first.clone(), left_last.clone()];
        let right_batch = [right_first.clone(), right_last.clone()];
        let (left, right) = tokio::join!(
            fixture
                .kernel
                .append_successor_batch(&left_batch, &base_head),
            fixture
                .kernel
                .append_successor_batch(&right_batch, &base_head)
        );
        assert_eq!(
            [left.as_ref().ok(), right.as_ref().ok()]
                .iter()
                .filter(|winner| winner.is_some())
                .count(),
            1
        );
        assert!(matches!(
            left.as_ref().err().or(right.as_ref().err()),
            Some(KernelError::LineageHeadConflict { .. })
        ));
        let current = fixture
            .kernel
            .read_head()
            .await
            .expect("current batch head");
        assert_eq!(current.generation(), 2);
        let left_last_digest = left_last.digest().expect("left terminal digest");
        let right_last_digest = right_last.digest().expect("right terminal digest");
        assert!(
            current.record_digest() == &left_last_digest
                || current.record_digest() == &right_last_digest
        );
        for digest in [&left_last_digest, &right_last_digest] {
            fixture
                .store
                .download(&fixture.lineage.object_key(digest))
                .await
                .expect("each immutable terminal record remains retained");
        }
        let folded = fixture
            .kernel
            .fold(None, &ByteFold)
            .await
            .expect("winning batch fold");
        assert_eq!(folded.state.len(), 3);
        assert_eq!(folded.state[0], b"base".to_vec());
        assert!(
            folded.state[1..] == [b"left first".to_vec(), b"left last".to_vec()]
                || folded.state[1..] == [b"right first".to_vec(), b"right last".to_vec()]
        );
        assert_barriered_head_race(&fixture, HeadCondition::Update).await;
        fixture.shutdown().await;
    }

    #[tokio::test]
    async fn k4_deterministic_fold_positive() {
        let fixture = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let (first, _second, _head) = two_record_history(&fixture).await;
        let first_digest = first.digest().expect("first digest");
        let checkpoint = CanonicalCheckpoint::new(
            &fixture.lineage,
            "opaque.v1",
            0,
            first_digest.clone(),
            first_digest,
            ByteFold::encode(&[b"first".to_vec()]),
        )
        .expect("checkpoint");
        let checkpoint_digest = fixture
            .kernel
            .publish_checkpoint(&checkpoint)
            .await
            .expect("checkpoint publish");

        let first_process = fixture
            .kernel
            .fold(Some(&checkpoint_digest), &ByteFold)
            .await
            .expect("checkpoint fold");
        let fresh_process = StateKernel::new(Arc::clone(&fixture.store), fixture.lineage.clone());
        let second_process = fresh_process
            .fold(None, &ByteFold)
            .await
            .expect("fresh genesis fold");
        assert_eq!(
            first_process.canonical_state,
            second_process.canonical_state
        );
        assert_eq!(first_process.records, second_process.records);
        assert!(first_process.checkpoint_rejections.is_empty());
        fixture.shutdown().await;
    }

    #[tokio::test]
    async fn k4_checkpoint_authority_negative() {
        let fixture = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let (first, _second, _head) = two_record_history(&fixture).await;
        let first_digest = first.digest().expect("first digest");
        for (checkpoint_digest, rejection) in
            seed_checkpoint_rejection_cases(&fixture, &first_digest).await
        {
            assert_checkpoint_discarded(&fixture, &checkpoint_digest, rejection).await;
        }
        fixture.shutdown().await;

        let corrupt = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let canonical_but_invalid = CanonicalRecord::new(
            &corrupt.lineage,
            0,
            None,
            "opaque",
            "unknown.v9",
            b"corrupt-history".to_vec(),
            "operation",
            "actor",
            "cause",
        )
        .expect("structurally canonical corrupt history record");
        let corrupt_digest = canonical_but_invalid
            .digest()
            .expect("corrupt history digest");
        corrupt
            .kernel
            .append_genesis(&canonical_but_invalid)
            .await
            .expect("publish canonically encoded corrupt history");
        for (checkpoint_digest, _) in
            seed_checkpoint_rejection_cases(&corrupt, &corrupt_digest).await
        {
            let error = corrupt
                .kernel
                .fold(Some(&checkpoint_digest), &ByteFold)
                .await
                .expect_err("canonically corrupt history must refuse before checkpoint state");
            assert!(matches!(error, KernelError::StateRecordMalformed { .. }));
        }
        corrupt.shutdown().await;
    }

    #[tokio::test]
    async fn k7_record_history_integrity_positive() {
        let fixture = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let (first, _second, _head) = two_record_history(&fixture).await;
        let first_digest = first.digest().expect("digest");
        let rejected = CanonicalCheckpoint::new(
            &fixture.lineage,
            "opaque.v1",
            0,
            first_digest.clone(),
            first_digest,
            ByteFold::encode(&[b"wrong".to_vec()]),
        )
        .expect("checkpoint");
        let rejected_digest = fixture
            .kernel
            .publish_checkpoint(&rejected)
            .await
            .expect("checkpoint publish");
        let result = fixture
            .kernel
            .fold(Some(&rejected_digest), &ByteFold)
            .await
            .expect("full history remains valid");

        assert_eq!(result.records.len(), 2);
        assert_eq!(
            result.canonical_state,
            ByteFold::encode(&[b"first".to_vec(), b"second".to_vec()])
        );
        assert_eq!(result.checkpoint_rejections.len(), 1);
        assert_eq!(
            result.checkpoint_rejections[0].code,
            CheckpointRejectionCode::StateHistoryIncomplete
        );
        fixture.shutdown().await;
    }

    #[tokio::test]
    async fn k7_record_history_integrity_negative() {
        let missing = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let missing_digest = RecordDigest::of(b"missing record");
        seed_head(&missing, 0, missing_digest, None).await;
        let error = missing
            .kernel
            .fold(None, &ByteFold)
            .await
            .expect_err("missing history refuses");
        assert!(matches!(error, KernelError::StateHistoryIncomplete { .. }));

        let mismatch = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let mismatched_digest = RecordDigest::of(b"expected record");
        raw_upload(
            &mismatch.store,
            &mismatch.lineage.object_key(&mismatched_digest),
            b"changed record".to_vec(),
        )
        .await;
        seed_head(&mismatch, 0, mismatched_digest, None).await;
        let error = mismatch
            .kernel
            .fold(None, &ByteFold)
            .await
            .expect_err("digest mismatch refuses");
        assert!(matches!(error, KernelError::DigestMismatch { .. }));

        let malformed = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let malformed_bytes = br#"{\"not\":\"a record\"}"#.to_vec();
        let malformed_digest = RecordDigest::of(&malformed_bytes);
        raw_upload(
            &malformed.store,
            &malformed.lineage.object_key(&malformed_digest),
            malformed_bytes,
        )
        .await;
        seed_head(&malformed, 0, malformed_digest, None).await;
        let error = malformed
            .kernel
            .fold(None, &ByteFold)
            .await
            .expect_err("malformed record refuses");
        assert!(matches!(error, KernelError::StateRecordMalformed { .. }));

        let noncanonical = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let canonical_record = record(&noncanonical.lineage, 0, None, b"canonical");
        let mut noncanonical_bytes = canonical_record.canonical_bytes().expect("bytes");
        noncanonical_bytes.push(b' ');
        let noncanonical_digest = RecordDigest::of(&noncanonical_bytes);
        raw_upload(
            &noncanonical.store,
            &noncanonical.lineage.object_key(&noncanonical_digest),
            noncanonical_bytes,
        )
        .await;
        seed_head(&noncanonical, 0, noncanonical_digest, None).await;
        let error = noncanonical
            .kernel
            .fold(None, &ByteFold)
            .await
            .expect_err("noncanonical record refuses");
        assert!(matches!(error, KernelError::StateRecordMalformed { .. }));

        let wrong_envelope = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let canonical_record = record(&wrong_envelope.lineage, 0, None, b"envelope");
        let mut wire: RecordWire = serde_json::from_slice(
            &canonical_record
                .canonical_bytes()
                .expect("canonical record bytes"),
        )
        .expect("record wire");
        wire.envelope = "unsupported-record-envelope".to_owned();
        let wrong_envelope_bytes = serde_json::to_vec(&wire).expect("record bytes");
        let wrong_envelope_digest = RecordDigest::of(&wrong_envelope_bytes);
        raw_upload(
            &wrong_envelope.store,
            &wrong_envelope.lineage.object_key(&wrong_envelope_digest),
            wrong_envelope_bytes,
        )
        .await;
        seed_head(&wrong_envelope, 0, wrong_envelope_digest, None).await;
        let error = wrong_envelope
            .kernel
            .fold(None, &ByteFold)
            .await
            .expect_err("unsupported envelope refuses");
        assert!(matches!(error, KernelError::StateRecordMalformed { .. }));

        let unsupported_epoch = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let canonical_record = record(&unsupported_epoch.lineage, 0, None, b"epoch");
        let mut wire: RecordWire = serde_json::from_slice(
            &canonical_record
                .canonical_bytes()
                .expect("canonical record bytes"),
        )
        .expect("record wire");
        wire.protocol_epoch = SUPPORTED_PROTOCOL_EPOCH + 1;
        let unsupported_epoch_bytes = serde_json::to_vec(&wire).expect("record bytes");
        let unsupported_epoch_digest = RecordDigest::of(&unsupported_epoch_bytes);
        raw_upload(
            &unsupported_epoch.store,
            &unsupported_epoch
                .lineage
                .object_key(&unsupported_epoch_digest),
            unsupported_epoch_bytes,
        )
        .await;
        seed_head(&unsupported_epoch, 0, unsupported_epoch_digest, None).await;
        let error = unsupported_epoch
            .kernel
            .fold(None, &ByteFold)
            .await
            .expect_err("unsupported epoch refuses");
        assert!(matches!(
            error,
            KernelError::ProtocolEpochUnsupported { .. }
        ));

        let foreign = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let foreign_lineage =
            KernelLineage::new("state/v1/foreign-record", SuccessorPolicy::SuccessorCapable)
                .expect("lineage");
        let foreign_record = record(&foreign_lineage, 0, None, b"foreign");
        let foreign_digest = foreign_record.digest().expect("digest");
        raw_upload(
            &foreign.store,
            &foreign.lineage.object_key(&foreign_digest),
            foreign_record.canonical_bytes().expect("foreign bytes"),
        )
        .await;
        seed_head(&foreign, 0, foreign_digest, None).await;
        let error = foreign
            .kernel
            .fold(None, &ByteFold)
            .await
            .expect_err("foreign record refuses");
        assert!(matches!(error, KernelError::StateRecordMalformed { .. }));

        let gapped = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let absent_predecessor = RecordDigest::of(b"absent predecessor");
        let gapped_record = record(
            &gapped.lineage,
            2,
            Some(RecordPosition {
                generation: 1,
                digest: absent_predecessor,
            }),
            b"gap",
        );
        let gapped_digest = gapped_record.digest().expect("digest");
        raw_upload(
            &gapped.store,
            &gapped.lineage.object_key(&gapped_digest),
            gapped_record.canonical_bytes().expect("bytes"),
        )
        .await;
        seed_head(
            &gapped,
            2,
            gapped_digest.clone(),
            gapped_record.prior.clone(),
        )
        .await;
        let error = gapped
            .kernel
            .fold(None, &ByteFold)
            .await
            .expect_err("sequence gap refuses");
        assert!(matches!(error, KernelError::StateHistoryIncomplete { .. }));

        let broken_link = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let genesis = record(&broken_link.lineage, 0, None, b"first");
        let genesis_digest = genesis.digest().expect("genesis digest");
        raw_upload(
            &broken_link.store,
            &broken_link.lineage.object_key(&genesis_digest),
            genesis.canonical_bytes().expect("genesis bytes"),
        )
        .await;
        let successor = record(
            &broken_link.lineage,
            1,
            Some(RecordPosition {
                generation: 0,
                digest: genesis_digest,
            }),
            b"second",
        );
        let successor_digest = successor.digest().expect("successor digest");
        raw_upload(
            &broken_link.store,
            &broken_link.lineage.object_key(&successor_digest),
            successor.canonical_bytes().expect("successor bytes"),
        )
        .await;
        seed_head(
            &broken_link,
            1,
            successor_digest,
            Some(RecordPosition {
                generation: 0,
                digest: RecordDigest::of(b"wrong prior"),
            }),
        )
        .await;
        let error = broken_link
            .kernel
            .fold(None, &ByteFold)
            .await
            .expect_err("broken head prior refuses");
        assert!(matches!(error, KernelError::StateHistoryIncomplete { .. }));

        let cycle = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let cycle_digest = RecordDigest::of(b"cycle relation digest");
        let cycle_relation = RecordWire {
            actor_id: "actor".to_owned(),
            cause_id: "cause".to_owned(),
            envelope: RECORD_ENVELOPE.to_owned(),
            lineage: cycle.lineage.value.clone(),
            operation_id: "operation".to_owned(),
            payload_hex: hex::encode(b"cycle"),
            prior: Some(PositionWire {
                digest: cycle_digest.as_str().to_owned(),
                generation: 0,
            }),
            protocol_epoch: SUPPORTED_PROTOCOL_EPOCH,
            sequence: 1,
            transition_schema: "opaque.v1".to_owned(),
            transition_type: "opaque".to_owned(),
        };
        raw_upload(
            &cycle.store,
            &cycle.lineage.object_key(&cycle_digest),
            serde_json::to_vec(&cycle_relation).expect("cycle relation bytes"),
        )
        .await;
        seed_head(
            &cycle,
            1,
            cycle_digest.clone(),
            Some(RecordPosition {
                generation: 0,
                digest: cycle_digest.clone(),
            }),
        )
        .await;
        // The self-prior relation is supplied through the fold boundary. A SHA-256
        // fixed point is unavailable, so digest validation is the first valid fence.
        let error = cycle
            .kernel
            .fold(None, &ByteFold)
            .await
            .expect_err("cycle relation refuses through fold");
        assert!(matches!(error, KernelError::DigestMismatch { .. }));

        let invalid_schema = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let invalid = CanonicalRecord::new(
            &invalid_schema.lineage,
            0,
            None,
            "opaque",
            "unknown.v9",
            b"payload".to_vec(),
            "operation",
            "actor",
            "cause",
        )
        .expect("structurally valid record");
        let invalid_digest = invalid.digest().expect("digest");
        raw_upload(
            &invalid_schema.store,
            &invalid_schema.lineage.object_key(&invalid_digest),
            invalid.canonical_bytes().expect("bytes"),
        )
        .await;
        seed_head(&invalid_schema, 0, invalid_digest, None).await;
        let error = invalid_schema
            .kernel
            .fold(None, &ByteFold)
            .await
            .expect_err("unsupported transition schema refuses");
        assert!(matches!(error, KernelError::StateRecordMalformed { .. }));

        missing.shutdown().await;
        mismatch.shutdown().await;
        malformed.shutdown().await;
        noncanonical.shutdown().await;
        wrong_envelope.shutdown().await;
        unsupported_epoch.shutdown().await;
        foreign.shutdown().await;
        gapped.shutdown().await;
        broken_link.shutdown().await;
        cycle.shutdown().await;
        invalid_schema.shutdown().await;
    }

    #[tokio::test]
    async fn k5_no_local_authority_positive() {
        let fixture = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let (_first, _second, head) = two_record_history(&fixture).await;
        let canonical = fixture
            .kernel
            .fold(None, &ByteFold)
            .await
            .expect("canonical fold")
            .canonical_state;
        let projection = fixture
            .kernel
            .rebuild_projection()
            .await
            .expect("rebuild projection from S3");
        assert_eq!(projection.source().lineage(), fixture.lineage.value);
        assert_eq!(projection.source().generation(), head.generation());
        assert_eq!(projection.source().digest(), head.record_digest());

        let before = fixture.counterpart.snapshot().await;
        let accelerated = fixture
            .kernel
            .fold_with_projection(Some(&projection), &ByteFold)
            .await
            .expect("validated projection fold");
        assert_eq!(accelerated.projection, ProjectionDisposition::Used);
        assert_eq!(accelerated.canonical_state, canonical);
        let after = fixture.counterpart.snapshot().await;
        let projection_reads = &after.requests[before.requests.len()..];
        assert!(!projection_reads.is_empty());
        assert!(projection_reads.iter().all(|request| {
            request.method == Method::GET.as_str()
                && request.key.as_deref() == Some(fixture.lineage.head_key().as_str())
        }));

        let local_directory = tempfile::tempdir().expect("local projection directory");
        let local_projection = local_directory.path().join("projection.bin");
        std::fs::write(
            &local_projection,
            projection
                .canonical_bytes()
                .expect("encode canonical disk projection"),
        )
        .expect("write disk projection");
        let disk_projection = FoldProjection::from_canonical_bytes(
            &std::fs::read(&local_projection).expect("read disk projection"),
        )
        .expect("decode canonical disk projection");
        assert_eq!(
            disk_projection.source().lineage(),
            projection.source().lineage()
        );
        assert_eq!(
            disk_projection.source().generation(),
            projection.source().generation()
        );
        assert_eq!(
            disk_projection.source().digest(),
            projection.source().digest()
        );
        let disk_before = fixture.counterpart.snapshot().await;
        let from_disk = fixture
            .kernel
            .fold_with_projection(Some(&disk_projection), &ByteFold)
            .await
            .expect("validated disk projection fold");
        assert_eq!(from_disk.projection, ProjectionDisposition::Used);
        assert_eq!(from_disk.canonical_state, canonical);
        let disk_after = fixture.counterpart.snapshot().await;
        assert!(
            disk_after.requests[disk_before.requests.len()..]
                .iter()
                .all(|request| {
                    request.method == Method::GET.as_str()
                        && request.key.as_deref() == Some(fixture.lineage.head_key().as_str())
                })
        );
        drop(disk_projection);
        let mut memory_projection = Some(projection);
        assert!(memory_projection.take().is_some());
        std::fs::remove_file(&local_projection).expect("delete disposable local projection");
        assert!(!local_projection.exists());

        let fresh = StateKernel::new(Arc::clone(&fixture.store), fixture.lineage.clone());
        let rebuilt = fresh
            .fold_with_projection(None, &ByteFold)
            .await
            .expect("fresh fold from S3 alone");
        assert_eq!(rebuilt.projection, ProjectionDisposition::Absent);
        assert_eq!(rebuilt.canonical_state, canonical);
        assert_eq!(
            fresh
                .read_head()
                .await
                .expect("fresh canonical head")
                .record_digest(),
            head.record_digest()
        );
        fixture.shutdown().await;
    }

    #[tokio::test]
    async fn k5_projection_poisoning_negative() {
        let fixture = new_fixture(SuccessorPolicy::SuccessorCapable).await;
        let (_first, _second, head) = two_record_history(&fixture).await;
        let canonical = fixture
            .kernel
            .fold(None, &ByteFold)
            .await
            .expect("canonical fold")
            .canonical_state;
        let projection = fixture
            .kernel
            .rebuild_projection()
            .await
            .expect("rebuild projection from S3");
        let projection_bytes = projection
            .canonical_bytes()
            .expect("canonical projection encoding");
        let decoded = FoldProjection::from_canonical_bytes(&projection_bytes)
            .expect("strict projection decoding");
        assert_eq!(decoded.source().lineage(), projection.source().lineage());
        assert_eq!(
            decoded.source().generation(),
            projection.source().generation()
        );
        assert_eq!(decoded.source().digest(), projection.source().digest());

        let mut noncanonical = vec![b' '];
        noncanonical.extend_from_slice(&projection_bytes);
        let error = FoldProjection::from_canonical_bytes(&noncanonical)
            .expect_err("noncanonical projection bytes refuse");
        assert!(matches!(error, KernelError::StateRecordMalformed { .. }));

        let mut unknown_field: serde_json::Value =
            serde_json::from_slice(&projection_bytes).expect("projection JSON");
        unknown_field["unexpected"] = serde_json::Value::Bool(true);
        let error = FoldProjection::from_canonical_bytes(
            &serde_json::to_vec(&unknown_field).expect("unknown projection JSON"),
        )
        .expect_err("unknown projection field refuses");
        assert!(matches!(error, KernelError::StateRecordMalformed { .. }));

        let mut invalid_source_digest: serde_json::Value =
            serde_json::from_slice(&projection_bytes).expect("projection JSON");
        invalid_source_digest["source_digest"] = serde_json::Value::String("invalid".to_owned());
        let error = FoldProjection::from_canonical_bytes(
            &serde_json::to_vec(&invalid_source_digest).expect("invalid source digest JSON"),
        )
        .expect_err("invalid source digest refuses");
        assert!(matches!(error, KernelError::StateRecordMalformed { .. }));

        let mut invalid_source_generation: serde_json::Value =
            serde_json::from_slice(&projection_bytes).expect("projection JSON");
        invalid_source_generation["source_generation"] = serde_json::Value::from(0_u64);
        let error = FoldProjection::from_canonical_bytes(
            &serde_json::to_vec(&invalid_source_generation)
                .expect("invalid source generation JSON"),
        )
        .expect_err("incomplete source generation refuses");
        assert!(matches!(error, KernelError::StateHistoryIncomplete { .. }));

        let mut invalid_record_digest: serde_json::Value =
            serde_json::from_slice(&projection_bytes).expect("projection JSON");
        invalid_record_digest["records"][0]["digest"] =
            serde_json::Value::String("invalid".to_owned());
        let error = FoldProjection::from_canonical_bytes(
            &serde_json::to_vec(&invalid_record_digest).expect("invalid record digest JSON"),
        )
        .expect_err("invalid record digest refuses");
        assert!(matches!(error, KernelError::StateRecordMalformed { .. }));

        let mut invalid_record_bytes: serde_json::Value =
            serde_json::from_slice(&projection_bytes).expect("projection JSON");
        invalid_record_bytes["records"][0]["bytes_hex"] =
            serde_json::Value::String("00".to_owned());
        let error = FoldProjection::from_canonical_bytes(
            &serde_json::to_vec(&invalid_record_bytes).expect("invalid record bytes JSON"),
        )
        .expect_err("invalid record bytes refuse");
        assert!(matches!(error, KernelError::DigestMismatch { .. }));

        let mut stale = projection.clone();
        stale.source.generation = 0;
        let mut foreign = projection.clone();
        foreign.source.lineage = "state/v1/foreign-projection".to_owned();
        let mut mismatched = projection.clone();
        mismatched.source.digest = RecordDigest::of(b"mismatched projection source");
        let mut poisoned = projection;
        poisoned
            .records
            .last_mut()
            .expect("projection has a terminal record")
            .bytes
            .push(b'!');

        for (name, candidate) in [
            ("stale", stale),
            ("foreign", foreign),
            ("mismatched", mismatched),
            ("poisoned", poisoned),
        ] {
            let before = fixture.counterpart.snapshot().await;
            let fresh = StateKernel::new(Arc::clone(&fixture.store), fixture.lineage.clone());
            let outcome = fresh
                .fold_with_projection(Some(&candidate), &ByteFold)
                .await
                .expect("canonical fallback");
            assert_eq!(
                outcome.projection,
                ProjectionDisposition::Discarded,
                "{name} projection must be discarded"
            );
            assert_eq!(
                outcome.canonical_state, canonical,
                "{name} cannot alter fold"
            );
            assert_eq!(
                fresh
                    .read_head()
                    .await
                    .expect("canonical head")
                    .record_digest(),
                head.record_digest(),
                "{name} cannot alter head"
            );
            let after = fixture.counterpart.snapshot().await;
            let fallback_reads = &after.requests[before.requests.len()..];
            assert!(
                fallback_reads
                    .iter()
                    .all(|request| request.method == Method::GET.as_str())
            );
            assert!(fallback_reads.iter().any(|request| {
                request
                    .key
                    .as_deref()
                    .is_some_and(|key| key.contains("/objects/"))
            }));
        }
        fixture.shutdown().await;
    }

    #[tokio::test]
    async fn k6_storage_fault_cut_positive() {
        for case in STORAGE_FAULT_CASES {
            let receipt = run_storage_fault_case(case).await;
            assert_eq!(receipt.case, case);
            assert_eq!(
                receipt.result,
                expected_storage_fault_result(case),
                "fault case: {case:?}"
            );
            assert_eq!(receipt.actual_state, case.expected_state);
            assert_eq!(
                receipt.candidate_exists,
                case.expected_state != StorageFaultState::Prior
            );
            assert_eq!(
                receipt.candidate_reachable,
                case.expected_state == StorageFaultState::Complete
            );
            let expected_fold = match case.expected_state {
                StorageFaultState::Prior | StorageFaultState::Orphan => receipt.prior_fold.as_ref(),
                StorageFaultState::Complete => Some(&receipt.complete_fold),
            };
            assert_eq!(receipt.folded.as_ref(), expected_fold);
            assert_eq!(receipt.fault.cut, case.cut);
            assert_eq!(receipt.fault.phase, case.phase);
            assert_eq!(receipt.fault_status, expected_storage_fault_status(case));
            assert!(receipt.fault_recorded_on_request);
            assert_eq!(
                receipt.put_count,
                if matches!(
                    case.cut,
                    StorageFaultCut::HeadCreate | StorageFaultCut::HeadUpdate
                ) {
                    2
                } else {
                    1
                }
            );
        }
    }

    #[tokio::test]
    async fn k6_partial_state_negative() {
        for case in STORAGE_FAULT_CASES {
            let receipt = run_storage_fault_case(case).await;
            assert_ne!(receipt.result, FaultCallResult::Acknowledged);
            assert!(receipt.fault_recorded_on_request);
            assert_eq!(receipt.actual_state, case.expected_state);
            assert_eq!(
                receipt.candidate_reachable,
                case.expected_state == StorageFaultState::Complete
            );
            match case.expected_state {
                StorageFaultState::Prior | StorageFaultState::Orphan => {
                    assert_eq!(receipt.folded.as_ref(), receipt.prior_fold.as_ref());
                }
                StorageFaultState::Complete => {
                    assert_eq!(receipt.folded.as_ref(), Some(&receipt.complete_fold));
                }
            }
            assert_eq!(
                receipt.put_count,
                if matches!(
                    case.cut,
                    StorageFaultCut::HeadCreate | StorageFaultCut::HeadUpdate
                ) {
                    2
                } else {
                    1
                }
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn bounded_record_canonical_bytes_are_stable(
            lineage_suffix in "[a-z0-9]{1,16}",
            transition_type_suffix in "[a-z0-9]{1,8}",
            schema_suffix in "[a-z0-9]{1,8}",
            payload in prop::collection::vec(any::<u8>(), 0..64),
        ) {
            let lineage = KernelLineage::new(
                format!("state/v1/property/{lineage_suffix}"),
                SuccessorPolicy::SuccessorCapable,
            ).expect("lineage");
            let record = CanonicalRecord::new(
                &lineage,
                0,
                None,
                format!("{transition_type_suffix}.kind"),
                format!("{schema_suffix}.v1"),
                payload,
                "operation",
                "actor",
                "cause",
            ).expect("record");
            let bytes = record.canonical_bytes().expect("canonical bytes");
            let digest = RecordDigest::of(&bytes);
            let decoded = CanonicalRecord::from_bytes(&lineage, &digest, &bytes)
                .expect("canonical record decodes");
            prop_assert_eq!(decoded.canonical_bytes().expect("reencoded bytes"), bytes);
        }
    }
    // --- ADR 0016 batch 2: loopback-backed keyspace contracts ----------

    use crate::atomic_keyspace::AtomicKeyspace;
    use bytes::Bytes;

    async fn keyspace_fixture(
        namespace: &str,
    ) -> (Arc<ObjectStoreClient>, AtomicKeyspace, LoopbackCounterpart) {
        let (counterpart, store) = LoopbackCounterpart::start().await;
        let keyspace =
            AtomicKeyspace::new(Arc::clone(&store), namespace).expect("valid keyspace namespace");
        (store, keyspace, counterpart)
    }

    /// G117: `delete_many` survives a lost-response DELETE cut
    /// mid-batch and resumes exactly-once-per-key. The AfterEffect cut
    /// applies the delete then loses the response — the caller cannot
    /// know — so the resumable remainder must re-run that key
    /// idempotently (it is already gone) and finish the rest.
    #[tokio::test]
    async fn g117_delete_many_resumes_after_lost_response_cut() {
        let (store, keyspace, counterpart) = keyspace_fixture("g117").await;
        for index in 0..6 {
            keyspace
                .create(&format!("k{index}"), Bytes::new())
                .await
                .expect("seed keys");
        }
        // Arm the lost-response cut on k2 — mid-batch.
        counterpart
            .arm_storage_fault(
                StorageFaultCut::KeyspaceDelete,
                StorageFaultPhase::AfterEffect,
                "keyspace/g117/k2",
            )
            .await;
        let targets: Vec<String> = (0..6).map(|i| format!("k{i}")).collect();
        let borrowed: Vec<&str> = targets.iter().map(String::as_str).collect();
        let outcomes = keyspace.delete_many(&borrowed).await.expect("sweep");
        // The cut key reports NOT deleted (its response was lost) —
        // the resumable remainder.
        let remaining = crate::atomic_keyspace::DeleteOutcome::remaining(&outcomes);
        assert_eq!(remaining, vec!["k2".to_string()]);
        // Everything else is confirmed gone.
        for outcome in &outcomes {
            if outcome.key != "k2" {
                assert!(outcome.deleted);
            }
        }
        // The AfterEffect cut DID apply the delete server-side: the
        // resumed sweep finds it already gone (idempotent success) and
        // the remainder converges to empty.
        let resumed = keyspace
            .delete_many(&[remaining[0].as_str()])
            .await
            .unwrap();
        assert!(resumed[0].deleted, "resume is idempotent");
        let list = keyspace.list_after(None, 100).await.unwrap();
        assert!(list.is_empty(), "sweep complete after resume");
        let _ = store;
        let _ = counterpart.shutdown().await;
    }

    /// A9 (batch 2): multi-page LIST continuation across the loopback's
    /// real truncation — every key exactly once across pages, tokens
    /// resume exclusively, IsTruncated flips only when more remain.
    #[tokio::test]
    async fn a9_list_multi_page_continuation_fidelity() {
        let (_store, keyspace, counterpart) = keyspace_fixture("a9").await;
        for index in 0..7 {
            keyspace
                .create(&format!("key-{index:02}"), Bytes::new())
                .await
                .expect("seed");
        }
        // Walk in pages of 2 against the counterpart's max-keys
        // truncation: use the loopback's pagination through raw
        // requests? The keyspace limit is the caller's bound; the
        // loopback truncates at its own max-keys (default 1000). Prove
        // the continuation contract at the keyspace layer with pages
        // of 2 — exactly-once across five pages (7 keys, 2+2+2+1).
        let mut seen: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = keyspace
                .list_after(cursor.as_deref(), 2)
                .await
                .expect("page");
            if page.is_empty() {
                break;
            }
            seen.extend(page.iter().cloned());
            cursor = Some(page.last().expect("nonempty page").clone());
        }
        let expected: Vec<String> = (0..7).map(|i| format!("key-{i:02}")).collect();
        assert_eq!(seen, expected, "exactly-once across pages, byte order");

        // Real truncation fidelity against the counterpart itself:
        // pages of 2 (below the 1000 default), IsTruncated flips, the
        // continuation token resumes exclusively, KeyCount reports the
        // page size, and the walk terminates exactly once per key.
        let http = reqwest::Client::new();
        let base = format!("{}/{}", counterpart.endpoint, T001_COUNTERPART_BUCKET);
        let mut page_keys: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        for expected_remaining in [5usize, 3, 1, 0] {
            let mut url = format!("{base}?list-type=2&prefix=keyspace/a9/&max-keys=2");
            if let Some(token) = &token {
                url.push_str(&format!("&continuation-token={token}"));
            }
            let body = http
                .get(&url)
                .send()
                .await
                .expect("page request")
                .text()
                .await
                .expect("page body");
            let page: Vec<String> = body
                .split("<Key>")
                .skip(1)
                .filter_map(|rest| rest.split_once("</Key>").map(|(key, _)| key.to_string()))
                .collect();
            let truncated = body.contains("<IsTruncated>true</IsTruncated>");
            let key_count = body
                .split("<KeyCount>")
                .nth(1)
                .and_then(|rest| rest.split_once("</KeyCount>"))
                .and_then(|(count, _)| count.parse::<usize>().ok())
                .expect("KeyCount present");
            assert_eq!(key_count, page.len());
            assert_eq!(
                truncated,
                expected_remaining > 0,
                "IsTruncated flips exactly when keys remain"
            );
            page_keys.extend(page);
            token = body
                .split("<NextContinuationToken>")
                .nth(1)
                .and_then(|rest| {
                    rest.split_once("</NextContinuationToken>")
                        .map(|(token, _)| token.to_string())
                });
            assert_eq!(token.is_some(), expected_remaining > 0);
        }
        let expected_prefixed: Vec<String> = expected
            .iter()
            .map(|key| format!("keyspace/a9/{key}"))
            .collect();
        assert_eq!(page_keys, expected_prefixed, "multi-page walk exact");

        let zero = http
            .get(format!("{base}?list-type=2&prefix=keyspace/a9/&max-keys=0"))
            .send()
            .await
            .expect("zero page request")
            .text()
            .await
            .expect("zero page body");
        assert!(zero.contains("<KeyCount>0</KeyCount>"));
        assert!(zero.contains("<IsTruncated>false</IsTruncated>"));
        assert!(!zero.contains("<NextContinuationToken>"));
        let _ = counterpart.shutdown().await;
    }

    /// A10 (batch 2): the G116 weak-cursor boundary against the
    /// loopback — an insert at/before the cursor after a page is
    /// outside the remaining walk; one after it appears.
    #[tokio::test]
    async fn a10_weak_cursor_boundary_on_loopback() {
        let (_store, keyspace, counterpart) = keyspace_fixture("a10").await;
        keyspace.create("b", Bytes::new()).await.unwrap();
        keyspace.create("d", Bytes::new()).await.unwrap();
        assert_eq!(keyspace.list_after(None, 1).await.unwrap(), vec!["b"]);
        keyspace.create("a", Bytes::new()).await.unwrap();
        keyspace.create("c", Bytes::new()).await.unwrap();
        assert_eq!(
            keyspace.list_after(Some("b"), 10).await.unwrap(),
            vec!["c", "d"],
            "inserts at/before the cursor never appear; after it does"
        );
        let _ = counterpart.shutdown().await;
    }

    /// A14 (batch 4): replay the real-S3 content-etag ABA semantics on
    /// the loopback. Raw A -> B -> A recurs the first A etag and accepts
    /// its stale CAS; AtomicKeyspace stores A(v0) -> B(v1) -> A(v2), so
    /// the wrapped bytes and etags do not recur through the module.
    #[tokio::test]
    async fn a14_loopback_content_etag_probe_raw_hazard_wrapped_closure() {
        let (store, keyspace, counterpart) = keyspace_fixture("a14").await;
        let payload_a = Bytes::from_static(b"A");
        let payload_b = Bytes::from_static(b"B");
        let raw_key = "aba-probe/a14/raw-cycle";

        let raw_a1 = store
            .upload_conditional(raw_key, payload_a.clone(), None)
            .await
            .unwrap()
            .expect("loopback returns etag");
        let raw_b = store
            .upload_conditional(raw_key, payload_b.clone(), Some(&raw_a1))
            .await
            .unwrap()
            .expect("loopback returns etag");
        let raw_a2 = store
            .upload_conditional(raw_key, payload_a.clone(), Some(&raw_b))
            .await
            .unwrap()
            .expect("loopback returns etag");
        assert_eq!(
            raw_a2, raw_a1,
            "content-etag loopback reproduces raw ABA recurrence"
        );
        store
            .upload_conditional(raw_key, Bytes::from_static(b"stale-writer"), Some(&raw_a1))
            .await
            .expect("raw stale token is accepted after etag recurrence");

        keyspace.create("cell", payload_a.clone()).await.unwrap();
        let (_, _incarnation_a1, version_a1, wrapped_a1) =
            keyspace.get_with_version("cell").await.unwrap().unwrap();
        let wrapped_b = keyspace
            .compare_exchange("cell", &wrapped_a1, payload_b)
            .await
            .unwrap();
        let wrapped_a2 = keyspace
            .compare_exchange("cell", &wrapped_b, payload_a.clone())
            .await
            .unwrap();
        let (observed_a2, _incarnation_a2, version_a2, observed_etag) =
            keyspace.get_with_version("cell").await.unwrap().unwrap();
        assert_eq!(version_a1, 0);
        assert_eq!(version_a2, 2);
        assert_eq!(observed_a2, payload_a);
        assert_eq!(observed_etag, wrapped_a2);
        assert_ne!(
            wrapped_a2, wrapped_a1,
            "versioned envelopes prevent content-etag recurrence across CAS eras"
        );
        assert!(matches!(
            keyspace
                .compare_exchange("cell", &wrapped_a1, Bytes::from_static(b"stale-writer"))
                .await,
            Err(crate::KeyspaceError::PreconditionFailed { .. })
        ));

        store.delete(raw_key).await.unwrap();
        keyspace.delete("cell").await.unwrap();
        let _ = counterpart.shutdown().await;
    }

    /// A15 (teardown pass 2026-08-22, batch 7): a `create` racing a
    /// `destroy` must never mint a value envelope from the destroyed
    /// era's incarnation. The window is `create`'s incarnation read
    /// landing before the destroy's counter bump while its
    /// put-if-absent lands after the destroy's value delete. The
    /// counterpart's conditional-PUT barrier parks the racing
    /// create's PUT server-side (its incarnation read completes
    /// first), the destroy then runs to completion through the gate,
    /// and a second create releases the gate. Exactly one of the two
    /// parked-and-released PUTs lands; if the era-1 writer wins, its
    /// envelope is byte-identical to the destroyed era's — and on
    /// this content-etag counterpart an era-1 token then CASes the
    /// "new" lifetime, the exact ABA batch 7 exists to prevent.
    /// Fail-on-detect canary over bounded attempts; green means the
    /// sampled interleavings never crossed eras.
    #[tokio::test]
    async fn a15_create_destroy_race_cannot_cross_eras() {
        let (store, keyspace, counterpart) = keyspace_fixture("a15").await;
        const ATTEMPTS: usize = 40;
        let payload = Bytes::from_static(b"same-bytes-in-both-eras");
        for attempt in 0..ATTEMPTS {
            let key = format!("cell{attempt}");
            let physical = format!("keyspace/a15/{key}");

            // Era 1: the value exists at incarnation 0, version 0.
            keyspace
                .create(&key, payload.clone())
                .await
                .expect("era-1 create");
            let (_, era1_etag) = keyspace
                .get_with_etag(&key)
                .await
                .expect("era-1 read")
                .expect("era-1 value present");

            // Park the racing writer: arm the two-party conditional-PUT
            // barrier on the value key, then start the create. Its
            // incarnation read (a GET) passes; its If-None-Match PUT
            // becomes barrier arrival #1.
            counterpart.arm_conditional_head_barrier(&physical).await;
            let writer = tokio::spawn({
                let store = Arc::clone(&store);
                let key = key.clone();
                let payload = payload.clone();
                async move {
                    AtomicKeyspace::new(store, "a15")
                        .expect("writer keyspace")
                        .create(&key, payload)
                        .await
                }
            });
            // Deterministic arrival: poll the counterpart until the
            // barrier observed the writer's PUT.
            tokio::time::timeout(std::time::Duration::from_secs(10), {
                let counterpart = &counterpart;
                async {
                    loop {
                        let snapshot = counterpart.snapshot().await;
                        if snapshot.barrier.expect("barrier stays armed").arrivals >= 1 {
                            return;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                }
            })
            .await
            .expect("writer PUT parks at the barrier");

            // Destroy fully while the writer is parked: tombstone,
            // counter bump, value delete. None of those requests hit
            // the barrier key.
            keyspace
                .destroy(&key, "teardown-race", "a15")
                .await
                .expect("destroy under the gate");
            assert_eq!(
                keyspace
                    .incarnation_for_test(&key)
                    .await
                    .expect("counter read"),
                1,
                "the destroy bumped the incarnation while the writer was parked"
            );

            // Release the gate: a fresh create whose incarnation read
            // observes the post-bump counter becomes arrival #2; both
            // parked PUTs proceed and exactly one lands.
            let releaser = tokio::spawn({
                let store = Arc::clone(&store);
                let key = key.clone();
                let payload = payload.clone();
                async move {
                    AtomicKeyspace::new(store, "a15")
                        .expect("releaser keyspace")
                        .create(&key, payload)
                        .await
                }
            });
            let _writer_outcome = writer.await.expect("writer task joins");
            let _releaser_outcome = releaser.await.expect("releaser task joins");

            // The batch-7 promise: an era-1 token is never accepted
            // across the destruction boundary, no matter which writer
            // landed.
            match keyspace
                .compare_exchange(&key, &era1_etag, Bytes::from_static(b"stale-era-writer"))
                .await
            {
                // The only correct outcome: the era-1 token lost.
                Err(crate::KeyspaceError::PreconditionFailed { .. }) => {}
                Ok(new_etag) => panic!(
                    "A15 DEFECT (attempt {attempt}): an era-1 etag was ACCEPTED \
                     across a destroy (counter now 1); the racing create minted \
                     a stale-era envelope, so era-1 bytes recurred on a \
                     content-etag backend and the stale token CASed the new \
                     lifetime (new etag {new_etag})"
                ),
                Err(other) => panic!("A15: unexpected CAS error shape: {other:?}"),
            }
        }
        let _ = counterpart.shutdown().await;
    }

    /// A16 (teardown pass 2026-08-22, batch 7): the v2 value envelope
    /// fails closed on every non-v2 shape — the deliberate 0.x format
    /// break. Raw plants (the only way such bytes can exist; the
    /// module surface cannot write them) must surface as
    /// `ValueEnvelopeMalformed` on every read path — never as payload,
    /// never as absence (law 7).
    #[tokio::test]
    async fn a16_value_envelope_fails_closed_on_non_v2_shapes() {
        let (store, keyspace, counterpart) = keyspace_fixture("a16").await;
        // Batch-4 v1 shape: 8-byte big-endian version, then payload.
        let mut v1 = Vec::new();
        v1.extend_from_slice(&0u64.to_be_bytes());
        v1.extend_from_slice(b"v1-payload");
        // Truncated v2: the 24-byte prefix + incarnation, no version.
        let mut truncated = b"yeetz-keyspace-value/v2\0".to_vec();
        truncated.extend_from_slice(&0u64.to_be_bytes());
        // A counter-shaped object (8 raw bytes) at a value key.
        let counter_shaped = 1u64.to_be_bytes().to_vec();
        let plants: [(&str, Vec<u8>); 4] = [
            ("v1", v1),
            ("truncated", truncated),
            ("counter-shaped", counter_shaped),
            ("garbage", b"not-an-envelope".to_vec()),
        ];
        for (name, bytes) in plants {
            let physical = format!("keyspace/a16/{name}");
            store
                .upload_conditional(&physical, Bytes::from(bytes), None)
                .await
                .expect("plant raw shape")
                .expect("counterpart returns etags");
            assert!(
                matches!(
                    keyspace.get(name).await,
                    Err(crate::KeyspaceError::ValueEnvelopeMalformed(_))
                ),
                "A16: {name} must fail closed on get"
            );
            assert!(
                matches!(
                    keyspace.get_with_etag(name).await,
                    Err(crate::KeyspaceError::ValueEnvelopeMalformed(_))
                ),
                "A16: {name} must fail closed on get_with_etag"
            );
            assert!(
                matches!(
                    keyspace.read_state(name).await,
                    Err(crate::KeyspaceError::ValueEnvelopeMalformed(_))
                ),
                "A16: {name} must fail closed on read_state"
            );
        }
        let _ = counterpart.shutdown().await;
    }
}
