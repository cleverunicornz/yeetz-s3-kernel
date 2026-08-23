//! Object store client for upload, download, list, delete operations.

use aws_sdk_s3::Client as AwsS3Client;
use aws_sdk_s3::config::{Credentials as AwsCredentials, Region};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::delete_objects::DeleteObjectsError;
use aws_sdk_s3::presigning::PresigningConfig;
#[cfg(feature = "test-support")]
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::types::Delete as S3Delete;
use aws_sdk_s3::types::{DeletedObject, Error as S3ErrorEntry, ObjectIdentifier};
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::signer::Signer;
use object_store::{
    Attribute, Attributes, GetOptions, ObjectStore, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, UpdateVersion,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};
use url::Url;

use crate::config::S3Config;
use crate::error::ObjectStoreError;

fn provider_error_text(error: &(impl std::fmt::Display + std::fmt::Debug)) -> String {
    let display = error.to_string();
    let debug = format!("{error:?}");
    if debug == display || debug.contains(&display) {
        debug
    } else {
        format!("{display} | debug={debug}")
    }
}

/// Client for S3-compatible object storage.
pub struct ObjectStoreClient {
    store: Box<dyn ObjectStore>,
    bucket: String,
    signer: Option<Arc<object_store::aws::AmazonS3>>,
    multipart_client: Option<AwsS3Client>,
}

/// HTTP methods supported for presigned URLs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignedUrlMethod {
    /// HTTP GET
    Get,
    /// HTTP PUT
    Put,
    /// HTTP POST
    Post,
    /// HTTP DELETE
    Delete,
    /// HTTP HEAD
    Head,
    /// HTTP PATCH
    Patch,
}

/// Presigned URL information for a multipart upload part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartPartUploadUrl {
    /// 1-based part number.
    pub part_number: i32,
    /// Presigned URL for uploading this part via HTTP PUT.
    pub url: Url,
}

/// Completed multipart part metadata needed to finalize an upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedMultipartPart {
    /// 1-based part number.
    pub part_number: i32,
    /// ETag returned by object storage for this part upload.
    pub e_tag: String,
}

impl SignedUrlMethod {
    fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Put => "PUT",
            Self::Post => "POST",
            Self::Delete => "DELETE",
            Self::Head => "HEAD",
            Self::Patch => "PATCH",
        }
    }
}

/// Result of downloading an object with its S3 metadata.
#[derive(Debug, Clone)]
pub struct DownloadWithMeta {
    /// The object data.
    pub data: Bytes,
    /// The etag (version tag) from S3, if available.
    pub etag: Option<String>,
}
/// Result of a conditional download against a known etag.
#[derive(Debug, Clone)]
pub enum DownloadIfChanged {
    /// The object etag matched the caller's known etag; no body was downloaded.
    NotModified,
    /// The object changed or the caller had no matching version; body and etag are returned.
    Changed(DownloadWithMeta),
}
/// Metadata returned from a HEAD-style object lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectHead {
    /// The object size in bytes.
    pub content_length: usize,
    /// The declared content type, if the store reported one.
    pub content_type: Option<String>,
}

// --- Multi-object delete (ADR 0005) --------------------------------------

/// Combined UTF-8 byte budget for provider `code` + `message` in one
/// multi-object delete diagnostic. Mirrors the kernel's public
/// `DELETE_OBJECTS_MAX_DIAGNOSTIC_BYTES` (the SDK cannot import the
/// kernel without inverting the dependency); the twin values are
/// asserted by the kernel's contract suite (A38).
const DELETE_OBJECTS_DIAGNOSTIC_BUDGET: usize = 512;

/// One member of a [`ObjectStoreClient::delete_objects`] response: the
/// requested path plus its typed per-key result. `Ok(())` is an
/// explicit confirmation from a valid response; `Err` is unresolved
/// remainder, never a presence proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectDeleteOutcome {
    pub path: String,
    pub result: Result<(), ObjectDeleteFailure>,
}

/// A bounded provider diagnostic for multi-object deletion: `code`
/// plus `message` fit the combined budget, truncated at char
/// boundaries when the provider exceeded it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectDeleteDiagnostic {
    pub code: Option<String>,
    pub message: String,
    /// Either provider field was cut to fit the combined budget.
    pub truncated: bool,
}

/// Per-key failure carried in an [`ObjectDeleteOutcome`]: the valid
/// response contained an `<Error>` entry naming this exact path. It
/// is not a proof that the object remains present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectDeleteFailure {
    pub diagnostic: ObjectDeleteDiagnostic,
}

/// Whole-request failure of one [`ObjectStoreClient::delete_objects`]
/// operation. The kernel expands one of these to every affected key
/// instead of collapsing a partial batch into a wholesale error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DeleteObjectsRequestError {
    /// The client lacks the wire capability or the service
    /// definitively refuses DeleteObjects (HTTP 405/501, or service
    /// codes `MethodNotAllowed`/`NotImplemented`). Terminal until
    /// backend qualification changes; no per-key fallback runs.
    #[error("multi-object delete unsupported: {}", diagnostic.message)]
    Unsupported {
        diagnostic: ObjectDeleteDiagnostic,
    },
    /// The request failed without a trustworthy per-key response
    /// (transport failure, timeout, or a non-refusal service error).
    #[error("multi-object delete request failed: {}", diagnostic.message)]
    Request {
        diagnostic: ObjectDeleteDiagnostic,
    },
    /// The response violated the exact bijection: a missing,
    /// duplicate, contradictory, or unexpected member, or a body that
    /// did not decode. No omitted key defaults to success.
    #[error("multi-object delete response invalid: {}", diagnostic.message)]
    InvalidResponse {
        diagnostic: ObjectDeleteDiagnostic,
    },
}

/// Truncate `value` to at most `max` UTF-8 bytes on a char boundary.
fn truncate_utf8(value: &mut String, max: usize) {
    if value.len() <= max {
        return;
    }
    let mut end = max.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
}

/// Bound provider code+message to the combined diagnostic budget:
/// the code is policy-bearing and kept first; the message is truncated
/// to the remainder; a code alone over budget is itself truncated.
fn bounded_delete_diagnostic(
    code: Option<String>,
    message: String,
) -> ObjectDeleteDiagnostic {
    let mut code = code.unwrap_or_default();
    let mut message = message;
    if code.len() + message.len() > DELETE_OBJECTS_DIAGNOSTIC_BUDGET {
        truncate_utf8(&mut code, DELETE_OBJECTS_DIAGNOSTIC_BUDGET);
        let remainder = DELETE_OBJECTS_DIAGNOSTIC_BUDGET - code.len();
        truncate_utf8(&mut message, remainder);
        return ObjectDeleteDiagnostic {
            code: (!code.is_empty()).then_some(code),
            message,
            truncated: true,
        };
    }
    ObjectDeleteDiagnostic {
        code: (!code.is_empty()).then_some(code),
        message,
        truncated: false,
    }
}

/// A bijection-violation report for one DeleteObjects response.
fn delete_objects_invalid_response(reason: String) -> DeleteObjectsRequestError {
    DeleteObjectsRequestError::InvalidResponse {
        diagnostic: bounded_delete_diagnostic(Some("InvalidResponse".to_string()), reason),
    }
}

/// Reconcile a verbose DeleteResult into exactly one outcome per
/// requested path, keyed by exact path regardless of response order.
/// A missing requested member, a duplicate member, a Deleted/Error
/// conflict, or a member naming an unrequested key is
/// [`DeleteObjectsRequestError::InvalidResponse`] — never success,
/// never a panic (the legacy `object_store` path's counterexample).
fn reconcile_delete_objects(
    paths: &[String],
    deleted: &[DeletedObject],
    errors: &[S3ErrorEntry],
) -> Result<Vec<ObjectDeleteOutcome>, DeleteObjectsRequestError> {
    let mut index_of: HashMap<&str, usize> = HashMap::with_capacity(paths.len());
    for (index, path) in paths.iter().enumerate() {
        if index_of.insert(path.as_str(), index).is_some() {
            return Err(delete_objects_invalid_response(format!(
                "request contains a duplicate path: {path}"
            )));
        }
    }
    let mut slots: Vec<Option<Result<(), ObjectDeleteFailure>>> = vec![None; paths.len()];
    for entry in deleted {
        let Some(key) = entry.key() else {
            return Err(delete_objects_invalid_response(
                "Deleted member without a key".to_string(),
            ));
        };
        match index_of.get(key) {
            Some(&index) if slots[index].is_none() => slots[index] = Some(Ok(())),
            _ => {
                return Err(delete_objects_invalid_response(format!(
                    "Deleted member for an unrequested or already-reported key: {key}"
                )));
            }
        }
    }
    for entry in errors {
        let Some(key) = entry.key() else {
            return Err(delete_objects_invalid_response(
                "Error member without a key".to_string(),
            ));
        };
        match index_of.get(key) {
            Some(&index) if slots[index].is_none() => {
                slots[index] = Some(Err(ObjectDeleteFailure {
                    diagnostic: bounded_delete_diagnostic(
                        entry.code().map(str::to_string),
                        entry.message().unwrap_or_default().to_string(),
                    ),
                }));
            }
            _ => {
                return Err(delete_objects_invalid_response(format!(
                    "Error member for an unrequested or already-reported key: {key}"
                )));
            }
        }
    }
    if slots.iter().any(Option::is_none) {
        return Err(delete_objects_invalid_response(
            "response omitted a requested key".to_string(),
        ));
    }
    Ok(paths
        .iter()
        .zip(slots)
        .map(|(path, slot)| ObjectDeleteOutcome {
            path: path.clone(),
            result: slot.expect("every slot filled above"),
        })
        .collect())
}

/// Classify one DeleteObjects request failure. HTTP status precedes
/// body parsing: 405/501 (or the equivalent service codes) is a
/// definitive `Unsupported`; any other service error (403 included)
/// stays `Request` with its provider code so policy can tell it from
/// a transport reset (which carries no code); a response that never
/// decoded is `InvalidResponse`.
fn map_delete_objects_error(
    error: &SdkError<DeleteObjectsError>,
) -> DeleteObjectsRequestError {
    match error {
        SdkError::ServiceError(service) => {
            let diagnostic = bounded_delete_diagnostic(
                service.err().code().map(str::to_string),
                service.err().message().unwrap_or_default().to_string(),
            );
            let status = service.raw().status().as_u16();
            let refused = status == 405
                || status == 501
                || matches!(
                    service.err().code(),
                    Some("MethodNotAllowed") | Some("NotImplemented")
                );
            if refused {
                DeleteObjectsRequestError::Unsupported { diagnostic }
            } else {
                DeleteObjectsRequestError::Request { diagnostic }
            }
        }
        // An HTTP response arrived but never became a typed
        // DeleteObjects output (malformed body): fail closed.
        SdkError::ResponseError(response) => {
            DeleteObjectsRequestError::InvalidResponse {
                diagnostic: bounded_delete_diagnostic(
                    None,
                    format!(
                        "response did not decode ({}): {}",
                        response.raw().status().as_u16(),
                        provider_error_text(error)
                    ),
                ),
            }
        }
        _ => DeleteObjectsRequestError::Request {
            diagnostic: bounded_delete_diagnostic(None, provider_error_text(error)),
        },
    }
}

/// Strip surrounding double-quotes from an S3 etag.
///
/// The `object_store` crate returns etags with HTTP-standard quotes, e.g.
/// `"\"da7f2b...\""`.  Some S3-compatible backends (notably Hetzner / Ceph
/// RADOS Gateway) reject `If-Match` headers that contain quoted etag values.
/// Normalising to the raw hash makes CAS work across all backends.
fn normalize_etag(etag: String) -> String {
    let trimmed = etag.trim_matches('"');
    if trimmed.len() == etag.len() {
        etag
    } else {
        trimmed.to_string()
    }
}

impl ObjectStoreClient {
    /// Create a new client from S3 configuration.
    pub fn new(config: &S3Config) -> Result<Self, ObjectStoreError> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_access_key_id(&config.access_key_id)
            .with_secret_access_key(&config.secret_access_key);

        if let Some(ref endpoint) = config.endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_virtual_hosted_style_request(false);
            if endpoint.starts_with("http://") {
                if config.allow_insecure_http {
                    warn!(
                        "Enabling HTTP (non-TLS) for endpoint: {} — S3 credentials will be transmitted in cleartext",
                        endpoint
                    );
                    builder = builder.with_allow_http(true);
                } else {
                    return Err(ObjectStoreError::Config(format!(
                        "HTTP endpoint '{endpoint}' requires allow_insecure_http=true (or S3_ALLOW_INSECURE_HTTP=true) \
                         — refusing to send credentials over cleartext"
                    )));
                }
            }
        }

        let store = builder
            .build()
            .map_err(|e| ObjectStoreError::Config(e.to_string()))?;
        let signer = Some(Arc::new(store.clone()));

        let aws_credentials = AwsCredentials::new(
            config.access_key_id.clone(),
            config.secret_access_key.clone(),
            None,
            None,
            "yeetz-sdk-s3",
        );
        let mut aws_builder = aws_sdk_s3::Config::builder()
            .region(Region::new(config.region.clone()))
            .credentials_provider(aws_credentials)
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .force_path_style(true);
        if let Some(endpoint) = &config.endpoint {
            aws_builder = aws_builder.endpoint_url(endpoint);
        }
        let aws_conf = aws_builder.build();
        let multipart_client = Some(AwsS3Client::from_conf(aws_conf));

        Ok(Self {
            store: Box::new(store),
            bucket: config.bucket.clone(),
            signer,
            multipart_client,
        })
    }

    /// Create an `ObjectStoreClient` backed by an in-memory store.
    ///
    /// For use in tests only — provides real `object_store` semantics (including
    /// CAS via etags) without requiring an S3 endpoint, Docker, or network access.
    ///
    /// # Arguments
    /// * `bucket` - A bucket name for path construction (any string, e.g. "test-bucket")
    #[cfg(any(feature = "test-support", test))]
    pub fn in_memory(bucket: impl Into<String>) -> Self {
        Self {
            store: Box::new(object_store::memory::InMemory::new()),
            bucket: bucket.into(),
            signer: None,
            multipart_client: None,
        }
    }

    /// Create an `ObjectStoreClient` backed by a shared in-memory store.
    ///
    /// For use in tests that need separate clients to observe the same objects.
    #[cfg(any(feature = "test-support", test))]
    pub fn shared_in_memory(bucket: impl Into<String>) -> Self {
        static STORE: std::sync::OnceLock<object_store::memory::InMemory> =
            std::sync::OnceLock::new();
        let store = STORE
            .get_or_init(object_store::memory::InMemory::new)
            .clone();
        Self {
            store: Box::new(store),
            bucket: bucket.into(),
            signer: None,
            multipart_client: None,
        }
    }

    /// Generate a presigned URL for the given path and method.
    ///
    /// This requires an S3-backed client created via [`Self::new`]. In-memory test
    /// backends do not support presigning.
    pub async fn signed_url(
        &self,
        path: &str,
        method: SignedUrlMethod,
        expires_in: Duration,
    ) -> Result<Url, ObjectStoreError> {
        let signer = self.signer.as_ref().ok_or_else(|| {
            ObjectStoreError::PresignUnsupported(
                "this object store backend does not support presigned URLs".to_string(),
            )
        })?;

        let method = method
            .as_str()
            .parse()
            .map_err(|e| ObjectStoreError::SignedUrlFailed(format!("invalid method: {e}")))?;
        let object_path = ObjectPath::from(path);
        signer
            .signed_url(method, &object_path, expires_in)
            .await
            .map_err(|e| ObjectStoreError::SignedUrlFailed(e.to_string()))
    }

    /// Convenience helper for presigned direct-upload URLs (`PUT`).
    pub async fn signed_upload_url(
        &self,
        path: &str,
        expires_in: Duration,
    ) -> Result<Url, ObjectStoreError> {
        self.signed_url(path, SignedUrlMethod::Put, expires_in)
            .await
    }

    /// Convenience helper for presigned direct-download URLs (`GET`).
    pub async fn signed_download_url(
        &self,
        path: &str,
        expires_in: Duration,
    ) -> Result<Url, ObjectStoreError> {
        self.signed_url(path, SignedUrlMethod::Get, expires_in)
            .await
    }

    /// Initiate a multipart upload and return the provider upload ID.
    pub async fn initiate_multipart_upload(
        &self,
        path: &str,
        content_type: &str,
    ) -> Result<String, ObjectStoreError> {
        let client = self.multipart_client.as_ref().ok_or_else(|| {
            ObjectStoreError::PresignUnsupported(
                "multipart operations are unsupported for this backend".to_string(),
            )
        })?;

        let response = client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(path)
            .content_type(content_type)
            .send()
            .await
            .map_err(|e| {
                ObjectStoreError::UploadFailed(format!("initiate multipart upload failed: {e}"))
            })?;

        response.upload_id().map(str::to_string).ok_or_else(|| {
            ObjectStoreError::UploadFailed(
                "initiate multipart upload returned no upload id".to_string(),
            )
        })
    }

    /// Generate presigned part upload URLs for a multipart upload.
    pub async fn multipart_part_upload_urls(
        &self,
        path: &str,
        upload_id: &str,
        part_count: i32,
        expires_in: Duration,
    ) -> Result<Vec<MultipartPartUploadUrl>, ObjectStoreError> {
        if part_count <= 0 {
            return Err(ObjectStoreError::SignedUrlFailed(
                "part_count must be positive".to_string(),
            ));
        }

        let client = self.multipart_client.as_ref().ok_or_else(|| {
            ObjectStoreError::PresignUnsupported(
                "multipart operations are unsupported for this backend".to_string(),
            )
        })?;
        let presign_config = PresigningConfig::expires_in(expires_in).map_err(|e| {
            ObjectStoreError::SignedUrlFailed(format!("invalid presign expiry: {e}"))
        })?;

        let mut out = Vec::with_capacity(part_count as usize);
        for part_number in 1..=part_count {
            let presigned = client
                .upload_part()
                .bucket(&self.bucket)
                .key(path)
                .upload_id(upload_id)
                .part_number(part_number)
                .presigned(presign_config.clone())
                .await
                .map_err(|e| {
                    ObjectStoreError::SignedUrlFailed(format!(
                        "failed to presign multipart part {part_number}: {e}"
                    ))
                })?;

            out.push(MultipartPartUploadUrl {
                part_number,
                url: presigned.uri().parse().map_err(|e| {
                    ObjectStoreError::SignedUrlFailed(format!(
                        "invalid presigned URL for part {part_number}: {e}"
                    ))
                })?,
            });
        }

        Ok(out)
    }

    /// Complete a multipart upload.
    ///
    /// Returns provider location/URL when available.
    pub async fn complete_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
        parts: Vec<CompletedMultipartPart>,
    ) -> Result<Option<String>, ObjectStoreError> {
        self.complete_multipart_upload_inner(path, upload_id, parts, None)
            .await
    }

    async fn complete_multipart_upload_inner(
        &self,
        path: &str,
        upload_id: &str,
        mut parts: Vec<CompletedMultipartPart>,
        expected_etag: Option<&str>,
    ) -> Result<Option<String>, ObjectStoreError> {
        if parts.is_empty() {
            return Err(ObjectStoreError::UploadFailed(
                "multipart completion requires at least one part".to_string(),
            ));
        }

        let client = self.multipart_client.as_ref().ok_or_else(|| {
            ObjectStoreError::PresignUnsupported(
                "multipart operations are unsupported for this backend".to_string(),
            )
        })?;

        parts.sort_by_key(|part| part.part_number);
        let completed_parts = parts
            .into_iter()
            .map(|part| {
                CompletedPart::builder()
                    .part_number(part.part_number)
                    .e_tag(part.e_tag)
                    .build()
            })
            .collect();
        let completed_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        let mut request = client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(path)
            .upload_id(upload_id)
            .multipart_upload(completed_upload);
        if let Some(etag) = expected_etag {
            request = request.if_match(format!("\"{}\"", etag.trim_matches('"')));
        }
        let response = request.send().await.map_err(|error| {
            match error.as_service_error().and_then(|service| service.code()) {
                Some("PreconditionFailed" | "ConditionalRequestConflict") => {
                    ObjectStoreError::PreconditionFailed(format!(
                        "conditional multipart completion failed for {path}: etag mismatch"
                    ))
                }
                Some("NoSuchUpload") => ObjectStoreError::NotFound(upload_id.to_string()),
                _ => ObjectStoreError::UploadFailed(format!(
                    "complete multipart upload failed: {}",
                    provider_error_text(&error)
                )),
            }
        })?;

        Ok(response.location().map(str::to_string))
    }

    /// Probe-only conditional multipart completion (`CompleteMultipartUpload`
    /// + `If-Match`) against an endpoint backend.
    #[cfg(feature = "test-support")]
    pub async fn complete_multipart_upload_if_match_for_test(
        &self,
        path: &str,
        upload_id: &str,
        parts: Vec<CompletedMultipartPart>,
        expected_etag: &str,
    ) -> Result<(), ObjectStoreError> {
        self.complete_multipart_upload_inner(path, upload_id, parts, Some(expected_etag))
            .await
            .map(|_| ())
    }

    /// Abort a multipart upload.
    pub async fn abort_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
    ) -> Result<(), ObjectStoreError> {
        let client = self.multipart_client.as_ref().ok_or_else(|| {
            ObjectStoreError::PresignUnsupported(
                "multipart operations are unsupported for this backend".to_string(),
            )
        })?;

        client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(path)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|e| {
                ObjectStoreError::UploadFailed(format!("abort multipart upload failed: {e}"))
            })?;

        Ok(())
    }

    /// Probe-only direct part upload through the pinned AWS client.
    #[cfg(feature = "test-support")]
    pub async fn upload_multipart_part_for_test(
        &self,
        path: &str,
        upload_id: &str,
        part_number: i32,
        data: Bytes,
    ) -> Result<CompletedMultipartPart, ObjectStoreError> {
        let client = self.multipart_client.as_ref().ok_or_else(|| {
            ObjectStoreError::PresignUnsupported(
                "multipart operations are unsupported for this backend".to_string(),
            )
        })?;
        let response = client
            .upload_part()
            .bucket(&self.bucket)
            .key(path)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|error| {
                ObjectStoreError::UploadFailed(format!(
                    "upload multipart part {part_number} failed: {}",
                    provider_error_text(&error)
                ))
            })?;
        let e_tag = response.e_tag().map(str::to_string).ok_or_else(|| {
            ObjectStoreError::UploadFailed(format!(
                "upload multipart part {part_number} returned no etag"
            ))
        })?;
        Ok(CompletedMultipartPart { part_number, e_tag })
    }

    /// Probe-only `GetObject?partNumber=N` against a completed multipart
    /// object.
    #[cfg(feature = "test-support")]
    pub async fn download_multipart_part_for_test(
        &self,
        path: &str,
        part_number: i32,
    ) -> Result<Bytes, ObjectStoreError> {
        let client = self.multipart_client.as_ref().ok_or_else(|| {
            ObjectStoreError::PresignUnsupported(
                "multipart operations are unsupported for this backend".to_string(),
            )
        })?;
        let response = client
            .get_object()
            .bucket(&self.bucket)
            .key(path)
            .part_number(part_number)
            .send()
            .await
            .map_err(|error| {
                ObjectStoreError::DownloadFailed(format!(
                    "download multipart part {part_number} failed: {}",
                    provider_error_text(&error)
                ))
            })?;
        response
            .body
            .collect()
            .await
            .map(|body| body.into_bytes())
            .map_err(|error| {
                ObjectStoreError::DownloadFailed(format!(
                    "download multipart part {part_number} body failed: {error}"
                ))
            })
    }

    /// Probe-only listing of in-progress multipart uploads under `prefix`.
    #[cfg(feature = "test-support")]
    pub async fn list_multipart_uploads_for_test(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, String)>, ObjectStoreError> {
        let client = self.multipart_client.as_ref().ok_or_else(|| {
            ObjectStoreError::PresignUnsupported(
                "multipart operations are unsupported for this backend".to_string(),
            )
        })?;
        let response = client
            .list_multipart_uploads()
            .bucket(&self.bucket)
            .prefix(prefix)
            .max_uploads(1000)
            .send()
            .await
            .map_err(|error| {
                ObjectStoreError::ListFailed(format!(
                    "list multipart uploads failed: {}",
                    provider_error_text(&error)
                ))
            })?;
        if response.is_truncated().unwrap_or(false) {
            return Err(ObjectStoreError::ListFailed(format!(
                "multipart upload list for {prefix} was unexpectedly truncated"
            )));
        }
        Ok(response
            .uploads()
            .iter()
            .filter_map(|upload| Some((upload.key()?.to_string(), upload.upload_id()?.to_string())))
            .collect())
    }

    /// Upload bytes to a path.
    pub async fn upload(&self, path: &str, data: Bytes) -> Result<(), ObjectStoreError> {
        self.upload_with_attributes(path, data, Attributes::default())
            .await
    }

    /// Upload bytes to a path with an explicit content type.
    pub async fn upload_with_content_type(
        &self,
        path: &str,
        data: Bytes,
        content_type: &str,
    ) -> Result<(), ObjectStoreError> {
        let mut attributes = Attributes::new();
        attributes.insert(Attribute::ContentType, content_type.to_string().into());
        self.upload_with_attributes(path, data, attributes).await
    }

    async fn upload_with_attributes(
        &self,
        path: &str,
        data: Bytes,
        attributes: Attributes,
    ) -> Result<(), ObjectStoreError> {
        debug!(path = %path, bucket = %self.bucket, "Uploading object");

        // Keep ordinary uploads on the object_store path. The AWS SDK client is
        // still required for explicit multipart flows, but direct put_object
        // regressed large snapshot archive uploads against our S3-compatible
        // backend while this path remained stable.
        let location = ObjectPath::from(path);
        let payload = PutPayload::from_bytes(data);
        let options = PutOptions {
            attributes,
            ..PutOptions::default()
        };

        self.store
            .put_opts(&location, payload, options)
            .await
            .map_err(|e| ObjectStoreError::UploadFailed(provider_error_text(&e)))?;

        info!(path = %path, bucket = %self.bucket, "Upload complete");
        Ok(())
    }

    /// Upload from a local file path (reads entire file into memory first).
    ///
    /// For large files, prefer [`upload_stream`](Self::upload_stream) which uses
    /// S3 multipart upload with constant memory.
    pub async fn upload_file(
        &self,
        key: &str,
        file_path: &std::path::Path,
    ) -> Result<(), ObjectStoreError> {
        let data = tokio::fs::read(file_path)
            .await
            .map_err(|e| ObjectStoreError::UploadFailed(format!("Read file: {e}")))?;

        self.upload(key, Bytes::from(data)).await
    }

    /// Download an object to bytes.
    pub async fn download(&self, path: &str) -> Result<Bytes, ObjectStoreError> {
        let location = ObjectPath::from(path);

        debug!(path = %path, bucket = %self.bucket, "Downloading object");

        let result = self
            .store
            .get_opts(&location, GetOptions::default())
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    ObjectStoreError::NotFound(path.to_string())
                }
                other => ObjectStoreError::DownloadFailed(other.to_string()),
            })?;

        let bytes = result
            .bytes()
            .await
            .map_err(|e| ObjectStoreError::DownloadFailed(e.to_string()))?;

        info!(path = %path, size = bytes.len(), "Download complete");
        Ok(bytes)
    }

    /// Download an object to a local file.
    pub async fn download_file(
        &self,
        key: &str,
        file_path: &std::path::Path,
    ) -> Result<(), ObjectStoreError> {
        let bytes = self.download(key).await?;
        tokio::fs::write(file_path, &bytes)
            .await
            .map_err(|e| ObjectStoreError::DownloadFailed(format!("Write file: {e}")))?;
        Ok(())
    }

    /// List objects with a prefix.
    pub async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let prefix_path = ObjectPath::from(prefix);

        let objects: Vec<_> = self
            .store
            .list(Some(&prefix_path))
            .try_collect()
            .await
            .map_err(|e| ObjectStoreError::ListFailed(e.to_string()))?;

        Ok(objects
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect())
    }

    /// List at most `limit` objects with a prefix after an optional exclusive offset key.
    ///
    /// This is the bounded scan primitive for retained object buckets: callers
    /// can persist the last returned key and continue later without collecting
    /// the entire prefix into memory on every pass.
    pub async fn list_prefix_after(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>, ObjectStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let prefix_path = ObjectPath::from(prefix);
        let stream = match after {
            Some(after) => {
                let offset = ObjectPath::from(after);
                self.store.list_with_offset(Some(&prefix_path), &offset)
            }
            None => self.store.list(Some(&prefix_path)),
        };

        let objects: Vec<_> = stream
            .take(limit)
            .try_collect()
            .await
            .map_err(|e| ObjectStoreError::ListFailed(e.to_string()))?;

        Ok(objects
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect())
    }

    /// List at most `limit` objects with a prefix after an optional
    /// exclusive offset key, returning keys WITH object sizes (the
    /// kernel's chunk metering reads sizes from the LIST itself, not
    /// per-object HEADs).
    pub async fn list_prefix_after_with_sizes(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, ObjectStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let prefix_path = ObjectPath::from(prefix);
        let stream = match after {
            Some(after) => {
                let offset = ObjectPath::from(after);
                self.store.list_with_offset(Some(&prefix_path), &offset)
            }
            None => self.store.list(Some(&prefix_path)),
        };

        let objects: Vec<_> = stream
            .take(limit)
            .try_collect()
            .await
            .map_err(|e| ObjectStoreError::ListFailed(e.to_string()))?;

        Ok(objects
            .into_iter()
            .map(|meta| (meta.location.to_string(), meta.size))
            .collect())
    }

    /// Delete an object.
    pub async fn delete(&self, path: &str) -> Result<(), ObjectStoreError> {
        let location = ObjectPath::from(path);

        // Use delete_stream (the object-safe method on the core ObjectStore trait)
        // with a single-item stream, matching the ObjectStoreExt::delete implementation.
        let stream = futures::stream::once(async { Ok(location) }).boxed();
        let mut result_stream = self.store.delete_stream(stream);

        result_stream
            .try_next()
            .await
            .map_err(|e| ObjectStoreError::DeleteFailed(e.to_string()))?;

        debug!(path = %path, "Object deleted");
        Ok(())
    }

    /// Delete an object only when its current etag matches `expected_etag`
    /// (`DELETE` + `If-Match` — the delete-side dual of
    /// [`upload_conditional`](Self::upload_conditional)'s If-Match CAS).
    ///
    /// S3 conditional-delete wire semantics: `204` deletes; `412
    /// PreconditionFailed` when the stored etag differs; `404 NoSuchKey`
    /// when the object is absent (never-existed and already-deleted are
    /// indistinguishable). The check is single-flight at the store, so the
    /// operation is atomic against concurrent conditional writes and other
    /// deletes: exactly one mutation can win any etag era.
    ///
    /// Requires an endpoint-configured backend (the AWS client shared with
    /// the explicit multipart flows). In-memory test stores do not model
    /// conditional deletes, and a read-then-delete fallback would reopen the
    /// race this primitive exists to close — so they fail closed with
    /// [`ObjectStoreError::DeleteFailed`] rather than degrade to an
    /// unconditional delete.
    pub async fn delete_conditional(
        &self,
        path: &str,
        expected_etag: &str,
    ) -> Result<(), ObjectStoreError> {
        let client = self.multipart_client.as_ref().ok_or_else(|| {
            ObjectStoreError::DeleteFailed(
                "conditional delete requires an S3 endpoint backend; \
                 in-memory test stores do not support it"
                    .to_string(),
            )
        })?;
        // If-Match carries the HTTP-standard quoted etag; inputs here are
        // normalized (quote-free) on every read path, so quote defensively.
        let quoted_etag = format!("\"{}\"", expected_etag.trim_matches('"'));
        debug!(path = %path, etag = %expected_etag, "Conditional delete");

        client
            .delete_object()
            .bucket(&self.bucket)
            .key(path)
            .if_match(quoted_etag)
            .send()
            .await
            .map(|_| ())
            .map_err(|error| match error.as_service_error() {
                Some(service) => match service.code() {
                    Some("PreconditionFailed") => ObjectStoreError::PreconditionFailed(format!(
                        "conditional delete failed for {path}: etag mismatch"
                    )),
                    Some("NoSuchKey") => ObjectStoreError::NotFound(path.to_string()),
                    _ => ObjectStoreError::DeleteFailed(provider_error_text(service)),
                },
                None => ObjectStoreError::DeleteFailed(provider_error_text(&error)),
            })
    }

    /// Exactly one verbose S3 DeleteObjects operation for 1..=1000
    /// unique paths (ADR 0005 — the kernel-closure composition seam;
    /// not a public storage bypass).
    ///
    /// Neither chunks nor retries: the kernel owns chunk sequencing
    /// and bounded policy. Every provider diagnostic is normalized to
    /// the combined 512-byte code/message budget. In-memory test
    /// backends have no DeleteObjects wire and fail closed as
    /// [`DeleteObjectsRequestError::Unsupported`] — there is no
    /// sequential emulation and no per-key fallback.
    ///
    /// A valid response may contain both `<Deleted>` and `<Error>`
    /// entries; both are recorded, one outcome per requested path,
    /// reconciled by exact path regardless of response order. A
    /// response that omits, duplicates, contradicts, or over-reports
    /// a requested path — or fails to decode — is
    /// [`DeleteObjectsRequestError::InvalidResponse`]; an omitted key
    /// never defaults to success.
    pub async fn delete_objects(
        &self,
        paths: &[String],
    ) -> Result<Vec<ObjectDeleteOutcome>, DeleteObjectsRequestError> {
        debug_assert!(
            !paths.is_empty() && paths.len() <= 1_000,
            "delete_objects sends 1..=1000 paths per operation (kernel chunking contract)"
        );
        let Some(client) = self.multipart_client.as_ref() else {
            return Err(DeleteObjectsRequestError::Unsupported {
                diagnostic: ObjectDeleteDiagnostic {
                    code: None,
                    message: "DeleteObjects requires an S3 endpoint backend; \
                              in-memory stores have no multi-object delete wire"
                        .to_string(),
                    truncated: false,
                },
            });
        };

        fn construction_failure(error: impl std::fmt::Display) -> DeleteObjectsRequestError {
            DeleteObjectsRequestError::Request {
                diagnostic: ObjectDeleteDiagnostic {
                    code: None,
                    message: error.to_string(),
                    truncated: false,
                },
            }
        }
        let identifiers: Vec<_> = paths
            .iter()
            .map(|path| ObjectIdentifier::builder().key(path.clone()).build())
            .collect::<Result<_, _>>()
            .map_err(construction_failure)?;
        let delete = S3Delete::builder()
            .set_objects(Some(identifiers))
            // Verbose mode is load-bearing: quiet=false (the default)
            // keeps explicit per-key confirmation on the wire.
            .build()
            .map_err(construction_failure)?;

        debug!(bucket = %self.bucket, count = paths.len(), "DeleteObjects (verbose)");
        match client
            .delete_objects()
            .bucket(&self.bucket)
            .delete(delete)
            // S3 requires a payload checksum on multi-object delete.
            // The loopback ignores headers, so only the real-S3 leg
            // witnesses this construction being accepted (ADR 0005).
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
        {
            Ok(output) => {
                reconcile_delete_objects(paths, output.deleted(), output.errors())
            }
            Err(error) => Err(map_delete_objects_error(&error)),
        }
    }

    /// Check if an object exists.
    pub async fn exists(&self, path: &str) -> Result<bool, ObjectStoreError> {
        let location = ObjectPath::from(path);

        // Use get_opts with head=true to check existence without downloading.
        let options = GetOptions::new().with_head(true);
        match self.store.get_opts(&location, options).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(ObjectStoreError::ListFailed(e.to_string())),
        }
    }

    /// Read object metadata without downloading the body.
    pub async fn head(&self, path: &str) -> Result<ObjectHead, ObjectStoreError> {
        let location = ObjectPath::from(path);
        let result = self
            .store
            .get_opts(&location, GetOptions::new().with_head(true))
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    ObjectStoreError::NotFound(path.to_string())
                }
                other => ObjectStoreError::DownloadFailed(other.to_string()),
            })?;

        let content_type = result
            .attributes
            .get(&Attribute::ContentType)
            .map(|value| value.as_ref().to_string());

        Ok(ObjectHead {
            content_length: result.meta.size as usize,
            content_type,
        })
    }

    /// Download an object along with its etag for CAS operations.
    ///
    /// Use this when you need to later perform a conditional write
    /// via [`upload_conditional`](Self::upload_conditional).
    pub async fn download_with_etag(
        &self,
        path: &str,
    ) -> Result<DownloadWithMeta, ObjectStoreError> {
        let location = ObjectPath::from(path);

        debug!(path = %path, bucket = %self.bucket, "Downloading object with etag");

        let result = self
            .store
            .get_opts(&location, GetOptions::default())
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    ObjectStoreError::NotFound(path.to_string())
                }
                other => ObjectStoreError::DownloadFailed(other.to_string()),
            })?;

        let etag = result.meta.e_tag.clone();
        let bytes = result
            .bytes()
            .await
            .map_err(|e| ObjectStoreError::DownloadFailed(e.to_string()))?;

        let etag = etag.map(normalize_etag);
        info!(path = %path, size = bytes.len(), etag = ?etag, "Download with etag complete");
        Ok(DownloadWithMeta { data: bytes, etag })
    }

    /// Download an object only if its etag differs from `known_etag`.
    ///
    /// This is the read side for cheap version-pointer polling: `NotModified`
    /// means the backend matched `If-None-Match` and no body was pulled.
    pub async fn download_if_changed(
        &self,
        path: &str,
        known_etag: &str,
    ) -> Result<DownloadIfChanged, ObjectStoreError> {
        let location = ObjectPath::from(path);
        let options = GetOptions::new().with_if_none_match(Some(known_etag.to_string()));

        debug!(path = %path, bucket = %self.bucket, known_etag = %known_etag, "Conditionally downloading object by etag");

        let result = match self.store.get_opts(&location, options).await {
            Ok(result) => result,
            Err(object_store::Error::NotModified { .. }) => {
                debug!(path = %path, bucket = %self.bucket, known_etag = %known_etag, "Conditional download not modified");
                return Ok(DownloadIfChanged::NotModified);
            }
            Err(object_store::Error::NotFound { .. }) => {
                return Err(ObjectStoreError::NotFound(path.to_string()));
            }
            Err(other) => return Err(ObjectStoreError::DownloadFailed(other.to_string())),
        };

        let etag = result.meta.e_tag.clone();
        let bytes = result
            .bytes()
            .await
            .map_err(|e| ObjectStoreError::DownloadFailed(e.to_string()))?;
        let etag = etag.map(normalize_etag);
        info!(path = %path, size = bytes.len(), etag = ?etag, "Conditional download returned changed object");
        Ok(DownloadIfChanged::Changed(DownloadWithMeta {
            data: bytes,
            etag,
        }))
    }
    /// Upload bytes conditionally using CAS (compare-and-swap).
    ///
    /// - If `expected_etag` is `Some(tag)`, the write succeeds only if the current
    ///   object's etag matches (`PutMode::Update`). Returns `PreconditionFailed` on mismatch.
    /// - If `expected_etag` is `None`, creates a new object and fails if one already
    ///   exists (`PutMode::Create`).
    ///
    /// Returns the new etag on success (needed for CAS-based rollback chains).
    pub async fn upload_conditional(
        &self,
        path: &str,
        data: Bytes,
        expected_etag: Option<&str>,
    ) -> Result<Option<String>, ObjectStoreError> {
        self.upload_conditional_with_attributes(path, data, expected_etag, Attributes::default())
            .await
    }

    /// Upload bytes conditionally using CAS with an explicit content type.
    pub async fn upload_conditional_with_content_type(
        &self,
        path: &str,
        data: Bytes,
        content_type: &str,
        expected_etag: Option<&str>,
    ) -> Result<Option<String>, ObjectStoreError> {
        let mut attributes = Attributes::new();
        attributes.insert(Attribute::ContentType, content_type.to_string().into());
        self.upload_conditional_with_attributes(path, data, expected_etag, attributes)
            .await
    }

    async fn upload_conditional_with_attributes(
        &self,
        path: &str,
        data: Bytes,
        expected_etag: Option<&str>,
        attributes: Attributes,
    ) -> Result<Option<String>, ObjectStoreError> {
        let location = ObjectPath::from(path);
        let payload = PutPayload::from_bytes(data);

        let mode = match expected_etag {
            Some(etag) => PutMode::Update(UpdateVersion {
                e_tag: Some(etag.to_string()),
                version: None,
            }),
            None => PutMode::Create,
        };

        let options = PutOptions {
            mode,
            attributes,
            ..PutOptions::default()
        };

        debug!(path = %path, etag = ?expected_etag, "Conditional upload");

        let result = self
            .store
            .put_opts(&location, payload, options)
            .await
            .map_err(|e| match e {
                object_store::Error::Precondition { .. } => ObjectStoreError::PreconditionFailed(
                    format!("CAS failed for {path}: etag mismatch"),
                ),
                object_store::Error::AlreadyExists { .. } => ObjectStoreError::PreconditionFailed(
                    format!("CAS failed for {path}: object already exists"),
                ),
                other => ObjectStoreError::UploadFailed(other.to_string()),
            })?;

        let new_etag = result.e_tag.map(normalize_etag);
        info!(path = %path, new_etag = ?new_etag, "Conditional upload complete");
        Ok(new_etag)
    }
}

/// Result of a streaming download, providing both the byte stream and total size.
pub struct StreamDownload {
    /// Stream of byte chunks. Each chunk is typically a few hundred KB.
    pub stream: BoxStream<'static, Result<Bytes, ObjectStoreError>>,
    /// Total size of the object in bytes (from S3 object metadata).
    pub content_length: usize,
    /// Object content type from provider metadata, if present.
    pub content_type: Option<String>,
    /// Object ETag from provider metadata, if present.
    pub etag: Option<String>,
}

impl std::fmt::Debug for StreamDownload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamDownload")
            .field("content_length", &self.content_length)
            .field("content_type", &self.content_type)
            .field("etag", &self.etag)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreClient {
    /// Download an object as a stream of byte chunks.
    ///
    /// Returns the stream along with the total content length from object metadata.
    /// Use this instead of [`download`](Self::download) for large objects to avoid
    /// buffering the entire object in memory.
    pub async fn download_stream(&self, path: &str) -> Result<StreamDownload, ObjectStoreError> {
        let location = ObjectPath::from(path);

        debug!(path = %path, bucket = %self.bucket, "Starting streaming download");

        let result = self
            .store
            .get_opts(&location, GetOptions::default())
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    ObjectStoreError::NotFound(path.to_string())
                }
                other => ObjectStoreError::DownloadFailed(other.to_string()),
            })?;

        let content_length = result.meta.size as usize;
        let content_type = result
            .attributes
            .get(&Attribute::ContentType)
            .map(|value| value.as_ref().to_string());
        let etag = result.meta.e_tag.clone().map(normalize_etag);
        let stream = result
            .into_stream()
            .map(|r| r.map_err(|e| ObjectStoreError::DownloadFailed(e.to_string())))
            .boxed();

        info!(path = %path, content_length, etag = ?etag, "Streaming download started");
        Ok(StreamDownload {
            stream,
            content_length,
            content_type,
            etag,
        })
    }

    /// Upload from a stream of byte chunks using S3 multipart upload.
    ///
    /// Buffers incoming chunks into 64 MB parts (well above the S3 minimum) and
    /// uploads them sequentially. Larger parts reduce request fan-out on large
    /// snapshot artifacts, which materially lowers end-to-end archive latency on
    /// our S3-compatible backends. Aborts the multipart upload on any error — including
    /// `complete()` failure — to prevent orphaned parts on S3.
    ///
    /// Uses the raw `MultipartUpload` trait (not `WriteMultipart`) so that
    /// `abort()` remains callable after `complete()` fails. `WriteMultipart`
    /// consumes `self` on `finish()`, making abort impossible on finalization
    /// failure.
    ///
    /// Use this instead of [`upload`](Self::upload) for large objects to avoid
    /// buffering the entire object in memory.
    pub async fn upload_stream(
        &self,
        path: &str,
        mut stream: std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<Bytes>> + Send>>,
    ) -> Result<(), ObjectStoreError> {
        let location = ObjectPath::from(path);

        debug!(path = %path, bucket = %self.bucket, "Starting streaming upload");

        let mut upload = self
            .store
            .put_multipart_opts(&location, PutMultipartOptions::default())
            .await
            .map_err(|e| ObjectStoreError::UploadFailed(e.to_string()))?;

        /// Use materially larger parts than the 5 MB S3 minimum so large
        /// snapshots do not pay thousands of sequential part uploads.
        const PART_SIZE: usize = 64 * 1024 * 1024;

        let mut buf = bytes::BytesMut::new();
        let mut total_bytes: u64 = 0;
        let mut part_count: u64 = 0;

        // Drive the stream, buffering into PART_SIZE chunks.
        let result: Result<(), ObjectStoreError> = async {
            while let Some(chunk_result) = stream.next().await {
                let chunk = chunk_result.map_err(|e| {
                    ObjectStoreError::UploadFailed(format!("Stream read error: {e}"))
                })?;
                total_bytes += chunk.len() as u64;
                buf.extend_from_slice(&chunk);

                // Flush full parts.
                while buf.len() >= PART_SIZE {
                    let part_data = buf.split_to(PART_SIZE).freeze();
                    upload
                        .put_part(PutPayload::from_bytes(part_data))
                        .await
                        .map_err(|e| ObjectStoreError::UploadFailed(e.to_string()))?;
                    part_count += 1;
                }
            }

            // Flush any remaining bytes as the final part.
            if !buf.is_empty() {
                upload
                    .put_part(PutPayload::from_bytes(buf.split().freeze()))
                    .await
                    .map_err(|e| ObjectStoreError::UploadFailed(e.to_string()))?;
                part_count += 1;
            }

            // Finalize the multipart upload.
            upload
                .complete()
                .await
                .map_err(|e| ObjectStoreError::UploadFailed(e.to_string()))?;

            Ok(())
        }
        .await;

        match result {
            Ok(()) => {
                info!(
                    path = %path,
                    total_bytes,
                    part_count,
                    part_size_bytes = PART_SIZE,
                    "Streaming upload complete"
                );
                Ok(())
            }
            Err(e) => {
                warn!(path = %path, error = %e, "Streaming upload failed, aborting multipart upload");
                let _ = upload.abort().await;
                Err(e)
            }
        }
    }
}

impl std::fmt::Debug for ObjectStoreClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreClient")
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt;

    #[test]
    fn test_config_custom() {
        let config = S3Config::custom(
            "bucket",
            "eu-central-1",
            "https://s3.hetzner.com",
            "key",
            "secret",
        );
        assert_eq!(config.bucket, "bucket");
        assert_eq!(config.region, "eu-central-1");
        assert_eq!(config.endpoint, Some("https://s3.hetzner.com".to_string()));
    }

    #[test]
    fn test_config_aws() {
        let config = S3Config::aws("bucket", "us-east-1", "key", "secret");
        assert_eq!(config.bucket, "bucket");
        assert!(config.endpoint.is_none());
    }

    #[test]
    fn test_client_creation() {
        let mut config = S3Config::custom(
            "test",
            "us-east-1",
            "http://localhost:9000",
            "minioadmin",
            "minioadmin",
        );
        config.allow_insecure_http = true;
        let result = ObjectStoreClient::new(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_download_with_meta_type() {
        // Compile-time check that DownloadWithMeta exists and has correct shape
        fn _assert_type(r: super::DownloadWithMeta) {
            let _bytes: Bytes = r.data;
            let _etag: Option<String> = r.etag;
        }
    }

    #[test]
    fn test_client_debug() {
        let config = S3Config::custom(
            "test",
            "us-east-1",
            "https://s3.example.com",
            "key",
            "secret",
        );
        let client = ObjectStoreClient::new(&config).unwrap();
        let debug = format!("{:?}", client);
        assert!(debug.contains("test"));
        // Verify secrets are not leaked in debug output
        assert!(!debug.contains("secret"));
    }

    /// Construct an `ObjectStoreClient` backed by an in-memory store (for tests).
    fn in_memory_client() -> ObjectStoreClient {
        ObjectStoreClient::in_memory("test-bucket")
    }

    // --- CAS integration tests using in-memory object store ---

    #[tokio::test]
    async fn test_upload_conditional_create_returns_etag() {
        let client = in_memory_client();
        let data = Bytes::from("hello world");
        let result = client
            .upload_conditional("cas/test-create", data, None)
            .await;
        assert!(result.is_ok(), "Create should succeed: {result:?}");
        // InMemory store returns an etag
        // (exact value is implementation-dependent, but should be Some)
    }

    #[tokio::test]
    async fn test_upload_conditional_update_with_correct_etag() {
        let client = in_memory_client();

        // First: create the object
        let data_v1 = Bytes::from("version 1");
        client
            .upload_conditional("cas/test-update", data_v1, None)
            .await
            .expect("Create should succeed");

        // Get the etag from the download
        let downloaded = client.download_with_etag("cas/test-update").await.unwrap();
        let etag = downloaded.etag.expect("InMemory store should return etag");

        // Update with the correct etag
        let data_v2 = Bytes::from("version 2");
        let update_result = client
            .upload_conditional("cas/test-update", data_v2, Some(&etag))
            .await;
        assert!(
            update_result.is_ok(),
            "Update with correct etag should succeed: {update_result:?}"
        );

        // Verify the data was actually updated
        let after = client.download_with_etag("cas/test-update").await.unwrap();
        assert_eq!(after.data, Bytes::from("version 2"));
    }

    #[tokio::test]
    async fn test_upload_conditional_update_with_wrong_etag_fails() {
        let client = in_memory_client();

        // Create the object
        let data = Bytes::from("original");
        client
            .upload_conditional("cas/test-wrong-etag", data, None)
            .await
            .expect("Create should succeed");

        // Try to update with a fake etag
        let data_v2 = Bytes::from("updated");
        let result = client
            .upload_conditional("cas/test-wrong-etag", data_v2, Some("wrong-etag"))
            .await;
        assert!(result.is_err(), "Update with wrong etag should fail");
        match result.unwrap_err() {
            ObjectStoreError::PreconditionFailed(_) => {} // expected
            other => panic!("Expected PreconditionFailed, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_download_with_etag_returns_data_and_etag() {
        let client = in_memory_client();

        // Upload some data
        let data = Bytes::from("test data");
        client
            .upload("cas/test-download", data.clone())
            .await
            .unwrap();

        // Download with etag
        let result = client.download_with_etag("cas/test-download").await;
        assert!(result.is_ok(), "Download should succeed: {result:?}");
        let downloaded = result.unwrap();
        assert_eq!(downloaded.data, data);
        // InMemory store provides etags
    }

    #[tokio::test]
    async fn test_download_if_changed_returns_not_modified_or_changed_body() {
        let client = in_memory_client();
        client
            .upload_conditional("cas/test-if-changed", Bytes::from("version 1"), None)
            .await
            .expect("create should succeed");
        let first = client
            .download_with_etag("cas/test-if-changed")
            .await
            .expect("download with etag");
        let first_etag = first.etag.expect("etag");

        let same = client
            .download_if_changed("cas/test-if-changed", &first_etag)
            .await
            .expect("conditional read");
        assert!(matches!(same, DownloadIfChanged::NotModified));

        client
            .upload_conditional(
                "cas/test-if-changed",
                Bytes::from("version 2"),
                Some(&first_etag),
            )
            .await
            .expect("update should succeed");
        let changed = client
            .download_if_changed("cas/test-if-changed", &first_etag)
            .await
            .expect("conditional read after update");
        match changed {
            DownloadIfChanged::Changed(downloaded) => {
                assert_eq!(downloaded.data, Bytes::from("version 2"));
                assert_ne!(downloaded.etag.as_deref(), Some(first_etag.as_str()));
            }
            DownloadIfChanged::NotModified => panic!("expected changed body after etag move"),
        }
    }

    #[tokio::test]
    async fn test_live_s3_download_if_changed_returns_not_modified_or_changed_body() {
        if std::env::var("S3_ENDPOINT").is_err() || std::env::var("S3_BUCKET").is_err() {
            eprintln!("skipping live S3 conditional GET test; S3_ENDPOINT/S3_BUCKET not set");
            return;
        }

        let config = S3Config::from_env().expect("S3 env should build config");
        let client = ObjectStoreClient::new(&config).expect("live S3 client");
        let prefix =
            std::env::var("S3_TEST_PREFIX").unwrap_or_else(|_| "workflow-config-test".to_string());
        let prefix = prefix.trim_matches('/');
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let key = if prefix.is_empty() {
            format!("sdk-conditional-get/{}-{nanos}.txt", std::process::id())
        } else {
            format!(
                "{prefix}/sdk-conditional-get/{}-{nanos}.txt",
                std::process::id()
            )
        };

        let first_etag = client
            .upload_conditional(&key, Bytes::from("version 1"), None)
            .await
            .expect("live create")
            .expect("live create etag");
        assert!(!first_etag.contains('"'), "etag must be normalized");

        let same = client
            .download_if_changed(&key, &first_etag)
            .await
            .expect("live conditional read unchanged");
        assert!(matches!(same, DownloadIfChanged::NotModified));

        let second_etag = client
            .upload_conditional(&key, Bytes::from("version 2"), Some(&first_etag))
            .await
            .expect("live update")
            .expect("live update etag");
        assert!(!second_etag.contains('"'), "etag must be normalized");

        let changed = client
            .download_if_changed(&key, &first_etag)
            .await
            .expect("live conditional read changed");
        match changed {
            DownloadIfChanged::Changed(downloaded) => {
                assert_eq!(downloaded.data, Bytes::from("version 2"));
                assert_eq!(downloaded.etag.as_deref(), Some(second_etag.as_str()));
            }
            DownloadIfChanged::NotModified => panic!("expected live changed body after etag move"),
        }

        client.delete(&key).await.expect("exact-key cleanup");
    }
    #[tokio::test]
    async fn test_download_with_etag_missing_key_returns_not_found() {
        let client = in_memory_client();

        let result = client.download_with_etag("cas/nonexistent-key").await;
        assert!(result.is_err(), "Download of missing key should fail");
        match result.unwrap_err() {
            ObjectStoreError::NotFound(_) => {} // expected
            other => panic!("Expected NotFound, got: {other:?}"),
        }
    }

    #[test]
    fn test_client_creation_with_http_endpoint_allowed() {
        // Validates the allow_http branch: http:// endpoints with allow_insecure_http=true
        // must set allow_http(true) to avoid TLS errors.
        let mut config = S3Config::custom(
            "test-http",
            "us-east-1",
            "http://localhost:9000",
            "key",
            "secret",
        );
        config.allow_insecure_http = true;
        let client = ObjectStoreClient::new(&config);
        assert!(
            client.is_ok(),
            "HTTP endpoint client creation should succeed with allow_insecure_http"
        );
    }

    #[test]
    fn test_client_creation_with_http_endpoint_rejected() {
        // Validates that http:// endpoints are rejected when allow_insecure_http=false (default).
        let config = S3Config::custom(
            "test-http-rejected",
            "us-east-1",
            "http://localhost:9000",
            "key",
            "secret",
        );
        let result = ObjectStoreClient::new(&config);
        assert!(
            result.is_err(),
            "HTTP endpoint should be rejected without allow_insecure_http"
        );
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("allow_insecure_http"),
            "Error should mention allow_insecure_http, got: {err}"
        );
    }

    #[test]
    fn test_client_creation_with_https_endpoint() {
        // Validates that https:// endpoints do NOT trigger the allow_http branch.
        let config = S3Config::custom(
            "test-https",
            "us-east-1",
            "https://s3.example.com",
            "key",
            "secret",
        );
        let client = ObjectStoreClient::new(&config);
        assert!(
            client.is_ok(),
            "HTTPS endpoint client creation should succeed"
        );
    }

    // === Action #13: upload_conditional create mode when object already exists ===
    #[tokio::test]
    async fn test_upload_conditional_create_already_exists_returns_precondition_failed() {
        let client = in_memory_client();

        // Create the object first
        let data = Bytes::from("initial data");
        client
            .upload_conditional("cas/test-already-exists", data, None)
            .await
            .expect("First create should succeed");

        // Try to create again — should fail with PreconditionFailed
        let data_v2 = Bytes::from("duplicate create");
        let result = client
            .upload_conditional("cas/test-already-exists", data_v2, None)
            .await;
        assert!(result.is_err(), "Create on existing object should fail");
        match result.unwrap_err() {
            ObjectStoreError::PreconditionFailed(_) => {} // expected: AlreadyExists → PreconditionFailed
            other => panic!("Expected PreconditionFailed, got: {other:?}"),
        }
    }

    // === Action #14: download_with_etag round-trip with binary (non-UTF-8) data ===
    #[tokio::test]
    async fn test_download_with_etag_binary_data_roundtrip() {
        let client = in_memory_client();

        // Upload binary data (non-UTF-8)
        let binary_data = Bytes::from(vec![0xFF, 0xFE, 0x00, 0x01, 0x80, 0x7F]);
        client
            .upload("cas/binary-test", binary_data.clone())
            .await
            .unwrap();

        // Download with etag and verify round-trip
        let downloaded = client
            .download_with_etag("cas/binary-test")
            .await
            .expect("Binary data download should succeed");
        assert_eq!(
            downloaded.data, binary_data,
            "Binary data must round-trip correctly through upload/download_with_etag"
        );
    }

    // --- Streaming upload/download tests ---

    #[tokio::test]
    async fn test_upload_stream_and_download_stream_roundtrip() {
        let client = in_memory_client();

        // Build a stream of 3 chunks.
        let chunks: Vec<std::io::Result<Bytes>> = vec![
            Ok(Bytes::from("hello ")),
            Ok(Bytes::from("streaming ")),
            Ok(Bytes::from("world")),
        ];
        let stream = Box::pin(futures::stream::iter(chunks));

        client
            .upload_stream("stream/roundtrip", stream)
            .await
            .expect("Streaming upload should succeed");

        // Verify via buffered download that the data is correct.
        let data = client.download("stream/roundtrip").await.unwrap();
        assert_eq!(data, Bytes::from("hello streaming world"));
    }

    #[tokio::test]
    async fn test_download_stream_returns_correct_metadata() {
        let client = in_memory_client();

        let data = Bytes::from("0123456789");
        client
            .upload_with_content_type("stream/length", data, "text/plain")
            .await
            .unwrap();

        let dl = client
            .download_stream("stream/length")
            .await
            .expect("Streaming download should succeed");

        assert_eq!(dl.content_length, 10);
        assert_eq!(dl.content_type.as_deref(), Some("text/plain"));
        assert!(dl.etag.is_some(), "in-memory backend should return an etag");

        // Collect stream and verify data.
        let collected: Vec<Bytes> = dl
            .stream
            .map(|r| r.expect("chunk should be ok"))
            .collect()
            .await;
        let total: Bytes = collected.into_iter().flatten().collect();
        assert_eq!(total, Bytes::from("0123456789"));
    }

    #[tokio::test]
    async fn test_download_stream_not_found() {
        let client = in_memory_client();

        let result = client.download_stream("stream/nonexistent").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            ObjectStoreError::NotFound(_) => {}
            other => panic!("Expected NotFound, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_upload_stream_source_error_aborts() {
        let client = in_memory_client();

        // Stream that yields one chunk then errors.
        let chunks: Vec<std::io::Result<Bytes>> = vec![
            Ok(Bytes::from("partial")),
            Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "connection lost",
            )),
        ];
        let stream = Box::pin(futures::stream::iter(chunks));

        let result = client.upload_stream("stream/error", stream).await;
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("Stream read error"),
            "Error should mention stream read: {err}"
        );
    }

    #[tokio::test]
    async fn test_upload_stream_empty_stream() {
        let client = in_memory_client();

        let stream = Box::pin(futures::stream::empty::<std::io::Result<Bytes>>());
        client
            .upload_stream("stream/empty", stream)
            .await
            .expect("Empty stream upload should succeed");

        let data = client.download("stream/empty").await.unwrap();
        assert!(data.is_empty());
    }

    #[tokio::test]
    async fn test_stream_download_debug() {
        let dl = super::StreamDownload {
            stream: futures::stream::empty().boxed(),
            content_length: 42,
            content_type: Some("application/octet-stream".to_string()),
            etag: Some("etag".to_string()),
        };
        let debug = format!("{:?}", dl);
        assert!(debug.contains("42"));
        assert!(debug.contains("application/octet-stream"));
        assert!(debug.contains("etag"));
    }

    #[tokio::test]
    async fn test_signed_url_unsupported_for_in_memory_backend() {
        let client = in_memory_client();
        let result = client
            .signed_download_url("test/path.txt", std::time::Duration::from_secs(60))
            .await;
        assert!(matches!(
            result,
            Err(ObjectStoreError::PresignUnsupported(_))
        ));
    }

    #[tokio::test]
    async fn test_multipart_unsupported_for_in_memory_backend() {
        let client = in_memory_client();
        let result = client
            .initiate_multipart_upload("test/path.bin", "application/octet-stream")
            .await;
        assert!(matches!(
            result,
            Err(ObjectStoreError::PresignUnsupported(_))
        ));
    }

    // --- InMemory operation tests (Phase 1.5) ---

    #[tokio::test]
    async fn test_in_memory_upload_download_roundtrip() {
        let client = in_memory_client();
        let data = Bytes::from("hello in-memory");
        client.upload("test/file.txt", data.clone()).await.unwrap();
        let downloaded = client.download("test/file.txt").await.unwrap();
        assert_eq!(downloaded, data);
    }

    #[tokio::test]
    async fn test_in_memory_download_not_found() {
        let client = in_memory_client();
        let result = client.download("nonexistent/path").await;
        assert!(matches!(result, Err(ObjectStoreError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_in_memory_exists() {
        let client = in_memory_client();
        assert!(!client.exists("test/exists.txt").await.unwrap());
        client
            .upload("test/exists.txt", Bytes::from("data"))
            .await
            .unwrap();
        assert!(client.exists("test/exists.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_in_memory_delete() {
        let client = in_memory_client();
        client
            .upload("test/delete-me.txt", Bytes::from("data"))
            .await
            .unwrap();
        assert!(client.exists("test/delete-me.txt").await.unwrap());
        client.delete("test/delete-me.txt").await.unwrap();
        assert!(!client.exists("test/delete-me.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_in_memory_list_prefix() {
        let client = in_memory_client();
        client
            .upload("prefix/a.txt", Bytes::from("a"))
            .await
            .unwrap();
        client
            .upload("prefix/b.txt", Bytes::from("b"))
            .await
            .unwrap();
        client
            .upload("other/c.txt", Bytes::from("c"))
            .await
            .unwrap();

        let listed = client.list_prefix("prefix/").await.unwrap();
        assert_eq!(listed.len(), 2);
        assert!(listed.iter().any(|p| p.contains("a.txt")));
        assert!(listed.iter().any(|p| p.contains("b.txt")));
    }

    #[tokio::test]
    async fn test_in_memory_list_prefix_after_limits_page() {
        let client = in_memory_client();
        for key in ["prefix/a.txt", "prefix/b.txt", "prefix/c.txt"] {
            client.upload(key, Bytes::from_static(b"x")).await.unwrap();
        }

        let first = client.list_prefix_after("prefix/", None, 2).await.unwrap();
        assert_eq!(first, vec!["prefix/a.txt", "prefix/b.txt"]);

        let second = client
            .list_prefix_after("prefix/", Some("prefix/b.txt"), 2)
            .await
            .unwrap();
        assert_eq!(second, vec!["prefix/c.txt"]);
    }

    #[tokio::test]
    async fn test_in_memory_list_prefix_after_seeks_past_large_old_history() {
        let client = in_memory_client();
        for ordinal in 0..192u64 {
            client
                .upload(
                    &format!("history/item-{ordinal:020}"),
                    Bytes::from_static(b"old"),
                )
                .await
                .unwrap();
        }
        client
            .upload(
                "history/item-99999999999999999999",
                Bytes::from_static(b"new"),
            )
            .await
            .unwrap();

        let page = client
            .list_prefix_after("history/", Some("history/item-99999999999999999998"), 2)
            .await
            .unwrap();
        assert_eq!(page, vec!["history/item-99999999999999999999"]);
    }

    /// Integration test: list_prefix on a real S3-compatible endpoint (Hetzner).
    /// Requires S3_* env vars. Run with: cargo test -p yeetz-sdk-s3 -- --ignored test_hetzner_list
    #[tokio::test]
    #[ignore]
    async fn test_hetzner_list_prefix() {
        let config = S3Config::from_env().expect("S3_* env vars required");
        eprintln!("bucket={} endpoint={:?}", config.bucket, config.endpoint);
        let client = ObjectStoreClient::new(&config).expect("client");

        // Test upload
        let t = std::time::Instant::now();
        client
            .upload("_test/ping", Bytes::from("hello"))
            .await
            .expect("upload");
        eprintln!("upload: {:?}", t.elapsed());

        // Test list on empty prefix
        let t = std::time::Instant::now();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            client.list_prefix("durability/yeetz/wal/"),
        )
        .await;
        match result {
            Ok(Ok(keys)) => eprintln!("list empty prefix: {} keys ({:?})", keys.len(), t.elapsed()),
            Ok(Err(e)) => panic!("list failed: {e}"),
            Err(_) => panic!("list_prefix TIMED OUT after 15s — this is the bug"),
        }

        // Test list on prefix with content
        let t = std::time::Instant::now();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            client.list_prefix("_test/"),
        )
        .await;
        match result {
            Ok(Ok(keys)) => eprintln!("list _test/: {} keys ({:?})", keys.len(), t.elapsed()),
            Ok(Err(e)) => panic!("list _test/ failed: {e}"),
            Err(_) => panic!("list_prefix _test/ TIMED OUT after 15s"),
        }

        // Cleanup
        let _ = client.delete("_test/ping").await;
        eprintln!("done");
    }

    fn real_download_probe_env() -> Result<(S3Config, String), String> {
        let config = S3Config::from_env()?;
        let key = std::env::var("YEETZ_OBJECT_STORE_REAL_KEY")
            .map_err(|_| "YEETZ_OBJECT_STORE_REAL_KEY not set".to_string())?;
        Ok((config, key))
    }

    #[tokio::test]
    #[ignore = "env-gated probe for a real staged object-store download"]
    async fn test_real_s3_download_object_store_client() {
        let (config, key) = real_download_probe_env().unwrap();
        let client = ObjectStoreClient::new(&config).expect("real S3 client should initialize");
        client
            .download(&key)
            .await
            .expect("object_store-backed download should succeed");
    }

    #[tokio::test]
    #[ignore = "env-gated probe for a real staged object-store download via aws sdk"]
    async fn test_real_s3_download_aws_sdk() {
        let (config, key) = real_download_probe_env().unwrap();
        let credentials = AwsCredentials::new(
            config.access_key_id.clone(),
            config.secret_access_key.clone(),
            None,
            None,
            "yeetz-sdk-s3-test",
        );
        let mut builder = aws_sdk_s3::Config::builder()
            .region(Region::new(config.region.clone()))
            .credentials_provider(credentials)
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .force_path_style(true);
        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint_url(endpoint);
        }
        let client = AwsS3Client::from_conf(builder.build());
        let output = client
            .get_object()
            .bucket(&config.bucket)
            .key(&key)
            .send()
            .await
            .expect("aws sdk get_object should succeed");
        let aggregated = output
            .body
            .collect()
            .await
            .expect("aws sdk body collect should succeed");
        let _bytes = aggregated.into_bytes();
    }

    #[test]
    fn provider_error_text_preserves_display_and_debug_context() {
        struct MockError;

        impl fmt::Display for MockError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "service error")
            }
        }

        impl fmt::Debug for MockError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "PutObjectError {{ status: 503, request_id: abc123 }}")
            }
        }

        let rendered = provider_error_text(&MockError);
        assert!(rendered.contains("service error"));
        assert!(rendered.contains("status: 503"));
        assert!(rendered.contains("request_id: abc123"));
    }
}
