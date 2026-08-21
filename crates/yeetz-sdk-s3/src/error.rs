//! Error types for object store operations.

use thiserror::Error;

/// Error during object store operations.
///
/// Note: This enum is `#[non_exhaustive]` — external consumers must include
/// a wildcard arm when matching. All internal callers already use wildcard
/// patterns so no breakage within the workspace.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ObjectStoreError {
    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Upload failed
    #[error("Upload failed: {0}")]
    UploadFailed(String),

    /// Download failed
    #[error("Download failed: {0}")]
    DownloadFailed(String),

    /// List failed
    #[error("List failed: {0}")]
    ListFailed(String),

    /// Delete failed
    #[error("Delete failed: {0}")]
    DeleteFailed(String),

    /// Object not found
    #[error("Object not found: {0}")]
    NotFound(String),

    /// Precondition failed (CAS conflict -- etag mismatch or object already exists)
    #[error("Precondition failed: {0}")]
    PreconditionFailed(String),

    /// Presigning is not supported by this backend/client configuration
    #[error("Presign unsupported: {0}")]
    PresignUnsupported(String),

    /// Signed URL generation failed
    #[error("Signed URL failed: {0}")]
    SignedUrlFailed(String),
}
