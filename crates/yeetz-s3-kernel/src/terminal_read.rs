use crate::state_kernel::{
    CanonicalRecord, HeadRead, KernelError, RecordDigest, SafeReference, StateKernel,
};

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

    /// The absent/present taxonomy (ADR 0016, Fugu's amendment):
    /// `Absent` when the lineage's head object does not exist — the
    /// lineage was never created (or its head was destroyed, itself an
    /// integrity condition the caller cannot repair from);
    /// `Present` carries the head. Broken-history lineages with an
    /// intact head object remain `Present` here — the incompleteness
    /// surfaces through the record-reading paths as
    /// [`KernelError::StateHistoryIncomplete`], never conflated with
    /// absence. Additive: [`Self::read_head`] semantics are unchanged.
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
            Err(KernelError::StateHistoryIncomplete { .. }) => Ok(LineageHeadState::Absent),
            Err(err) => Err(err),
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

/// Absent vs present for a lineage's head (ADR 0016 taxonomy):
/// distinguishes a never-created lineage from a broken-history one —
/// the latter is `Present` here and `StateHistoryIncomplete` through
/// the record paths, never conflated with absence (law 7).
#[derive(Debug, Clone)]
pub enum LineageHeadState {
    Absent,
    Present(HeadRead),
}

impl LineageHeadState {
    #[must_use]
    pub fn is_absent(&self) -> bool {
        matches!(self, Self::Absent)
    }
}
