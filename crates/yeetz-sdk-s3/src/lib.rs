//! yeetz-sdk-s3: request-scoped S3-compatible object storage mechanics.
//!
//! ## Responsibilities
//! - Connecting to S3-compatible storage backends (AWS, Hetzner, R2, `MinIO`)
//! - Upload and download objects (buffered bytes or streaming chunks)
//! - Streaming upload via S3 multipart upload (constant memory for large objects)
//! - Streaming download via chunked byte stream (constant memory for large objects)
//! - Generating presigned URLs for direct client upload/download
//! - List objects by prefix, with offset/limit support for bounded scans
//! - Delete objects
//! - Check object existence
//! - Building env var maps for forwarding S3 credentials to subprocess invocations
//! - Conditional writes (CAS) via etag for compare-and-swap lease protocols
//! - In-memory test backend behind `test-support` for crate and consumer tests
//!
//! ## Key Types / APIs
//! - [`ObjectStoreClient`] -- Main client for all operations
//! - [`S3Config`] -- Configuration for S3-compatible endpoints
//! - [`S3Config::from_env`] -- Canonical env-based config for runner/app-owned callers
//! - [`S3Config::from_toml_file`] -- Strict file-based config for caller-owned object-store connection TOML
//! - [`s3_env_forwarding`] -- Build env var map for subprocess credential forwarding
//! - [`ObjectStoreError`] -- Error type for all operations
//! - [`DownloadIfChanged`] -- Conditional download result for etag-based version polling
//! - [`DownloadWithMeta`] -- Download result including etag for CAS chains
//! - [`StreamDownload`] -- Streaming download result with byte stream, content metadata, and ETag
//! - [`ObjectStoreClient::download_with_etag`] -- Download with etag metadata
//! - [`ObjectStoreClient::download_if_changed`] -- Conditional GET with If-None-Match for etag polling
//! - [`ObjectStoreClient::upload_conditional`] -- CAS upload (etag match or create-if-absent)
//! - [`ObjectStoreClient::upload_stream`] -- Streaming upload via S3 multipart
//! - [`ObjectStoreClient::download_stream`] -- Streaming download as chunked byte stream
//! - [`ObjectStoreClient::list_prefix_after`] -- Bounded prefix listing after an optional key
//! - [`ObjectStoreClient::signed_upload_url`] -- Presigned PUT URL for direct upload
//! - [`ObjectStoreClient::signed_download_url`] -- Presigned GET URL for direct download
//! - [`ObjectStoreClient::initiate_multipart_upload`] -- Start S3 multipart upload
//! - [`ObjectStoreClient::multipart_part_upload_urls`] -- Presigned part upload URLs
//! - [`ObjectStoreClient::complete_multipart_upload`] -- Finalize multipart upload
//! - [`ObjectStoreClient::abort_multipart_upload`] -- Abort multipart upload
//! - [`ObjectStoreClient::in_memory`] -- In-memory test backend (requires `test-support` feature)
//!
//! ## Invariants
//! - Custom endpoints use path-style requests (not virtual-hosted)
//! - All operations are async
//! - CAS uploads map Precondition and `AlreadyExists` errors to `PreconditionFailed`
//! - `test-support` is test-only and intentionally narrower than production S3 behavior
//! - App-domain policy, public delivery, workflow lifecycle, runner release decisions, and DB ownership stay above this crate
//!
//! ## Usage
//! ```ignore
//! use yeetz_sdk_s3::{ObjectStoreClient, S3Config};
//!
//! let config = S3Config::custom("bucket", "us-east-1", "https://s3.example.com", "key", "secret");
//! let client = ObjectStoreClient::new(&config)?;
//! client.upload("backups/snapshot.tar", data).await?;
//! ```
//!
//! ## Testing
//! Enable the `test-support` feature to get an in-memory backend for tests. This backend is
//! not production-equivalent S3 behavior and must not be used as a rollout fallback:
//! ```toml
//! [dev-dependencies]
//! yeetz-sdk-s3 = { workspace = true, features = ["test-support"] }
//! ```
//! Then construct a client in test code:
//! ```ignore
//! let client = ObjectStoreClient::in_memory("test-bucket");
//! client.upload("key", data).await?;
//! ```

pub mod config;
pub mod error;
pub mod store;

pub use config::S3Config;
pub use config::s3_env_forwarding;
pub use error::ObjectStoreError;
pub use store::CompletedMultipartPart;
pub use store::DeleteObjectsRequestError;
pub use store::DownloadIfChanged;
pub use store::DownloadWithMeta;
pub use store::MultipartPartUploadUrl;
pub use store::ObjectDeleteDiagnostic;
pub use store::ObjectDeleteFailure;
pub use store::ObjectDeleteOutcome;
pub use store::ObjectHead;
pub use store::ObjectStoreClient;
pub use store::SignedUrlMethod;
pub use store::StreamDownload;
