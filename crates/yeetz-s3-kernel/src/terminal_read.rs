use crate::state_kernel::{
    CanonicalRecord, HeadRead, KernelError, RecordDigest, SafeReference, StateKernel,
};
use crate::tombstone::Tombstone;

impl StateKernel {
    /// O(1) terminal read (ADR 0016): the head object plus the
    /// terminal record it names — two GETs, no history walk. Integrity
    /// at O(1) scope: the head must parse, the record must parse, and
    /// the record's digest must equal the digest the head names.
    /// Chain linkage below the terminal is NOT verified — that is the
    /// documented trade; broken chains still surface through
    /// [`Self::fold`]. Equivalence with fold's terminal is asserted
    /// by contract A7.
    pub async fn read_terminal_record(&self) -> Result<TerminalRecordRead, KernelError> {
        let loaded = self.load_head().await?;
        let etag = loaded.etag.ok_or(KernelError::StateUnavailable {
            operation: "terminal read did not return a head ETag",
        })?;
        let record = self.load_record(&loaded.head.record_digest).await?;
        let digest = record.digest()?;
        if digest != loaded.head.record_digest {
            return Err(KernelError::DigestMismatch {
                reference: SafeReference::for_digest(&self.lineage, digest),
            });
        }
        Ok(TerminalRecordRead {
            head: HeadRead {
                head: loaded.head,
                etag,
            },
            record,
        })
    }

    /// The existence taxonomy (ADR 0016, Fugu's amendment; batch 6
    /// extends it): `Absent` when no head and no tombstone exist —
    /// the lineage was never created; `Destroyed` when a tombstone
    /// exists — the lineage existed and was deliberately deleted
    /// (the witness mechanizes the parent-aggregate convention);
    /// `Present` carries the head. Broken-history lineages with an
    /// intact head object remain `Present` here — the incompleteness
    /// surfaces through the record-reading paths as
    /// [`KernelError::StateHistoryIncomplete`], never conflated with
    /// absence. Additive: [`Self::read_head`] semantics are
    /// unchanged. A reborn lineage (fresh genesis after destroy) is
    /// `Present` — the new head's existence supersedes the witness.
    pub async fn read_head_state(&self) -> Result<LineageHeadState, KernelError> {
        match self.load_head().await {
            Ok(loaded) => {
                let etag = loaded.etag.ok_or(KernelError::StateUnavailable {
                    operation: "head state read did not return an ETag",
                })?;
                Ok(LineageHeadState::Present(HeadRead {
                    head: loaded.head,
                    etag,
                }))
            }
            Err(KernelError::StateHistoryIncomplete { .. }) => match self.load_tombstone().await? {
                Some(tombstone) => Ok(LineageHeadState::Destroyed(tombstone)),
                None => Ok(LineageHeadState::Absent),
            },
            Err(err) => Err(err),
        }
    }

    /// Load and parse the lineage tombstone (`None` when absent).
    /// Malformed stored evidence is [`KernelError::TombstoneCorrupt`]
    /// — never absence (law 7).
    async fn load_tombstone(&self) -> Result<Option<Tombstone>, KernelError> {
        match self
            .object_store
            .download(&self.lineage.tombstone_key())
            .await
        {
            Ok(bytes) => Tombstone::decode(bytes.as_ref()).map(Some).map_err(|_| {
                KernelError::TombstoneCorrupt {
                    reference: SafeReference::for_lineage(&self.lineage),
                }
            }),
            Err(yeetz_sdk_s3::ObjectStoreError::NotFound(_)) => Ok(None),
            Err(_) => Err(KernelError::StateUnavailable {
                operation: "tombstone read",
            }),
        }
    }
}

/// The result of an O(1) terminal read: the fenced head plus the
/// terminal record (payload, digest, generation) it names.
#[derive(Debug, Clone)]
pub struct TerminalRecordRead {
    pub head: HeadRead,
    pub record: CanonicalRecord,
}

impl TerminalRecordRead {
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.head.generation()
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        self.record.record_payload()
    }

    #[must_use]
    pub fn digest(&self) -> &RecordDigest {
        self.head.record_digest()
    }
}

/// The existence taxonomy for a lineage's head (ADR 0016; batch 6
/// adds `Destroyed`): distinguishes never-created (`Absent`),
/// deliberately deleted (`Destroyed` with the tombstone witness —
/// mechanizing the parent-aggregate convention), and `Present`.
/// Never conflates destruction with absence (law 7); a
/// broken-history lineage is `Present` here and
/// `StateHistoryIncomplete` through the record paths.
#[derive(Debug, Clone)]
pub enum LineageHeadState {
    Absent,
    Destroyed(Tombstone),
    Present(HeadRead),
}

impl LineageHeadState {
    #[must_use]
    pub fn is_absent(&self) -> bool {
        matches!(self, Self::Absent)
    }
}
