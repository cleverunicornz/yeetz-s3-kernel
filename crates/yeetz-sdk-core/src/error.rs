//! Error values that preserve terminal request and response evidence.

use bytes::Bytes;
use reqwest::StatusCode;
use reqwest::header::HeaderMap;

/// `yeetz-sdk-core` result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Request-scoped SDK error.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Connection establishment failed.
    #[error("connection error: {0}")]
    Connect(#[source] reqwest::Error),
    /// Request execution failed outside a classified connection or timeout failure.
    #[error("request error: {0}")]
    Request(#[source] reqwest::Error),
    /// Request execution timed out.
    #[error("request timed out: {0}")]
    Timeout(#[source] reqwest::Error),
    /// Response body read failed.
    #[error("response body error: {0}")]
    Body(#[source] reqwest::Error),
    /// Non-success HTTP status with raw terminal response evidence.
    #[error("HTTP error {status}: {raw_response}")]
    HttpStatus {
        /// Terminal HTTP status.
        status: StatusCode,
        /// Raw terminal response body bytes.
        raw_body: Bytes,
        /// Lossy text form of the raw response body.
        raw_response: String,
        /// Terminal response headers.
        headers: Box<HeaderMap>,
        /// Parsed rate-limit metadata from the terminal response.
        rate_limit_info: Option<crate::RateLimitInfo>,
    },
    /// Successful HTTP response whose body failed JSON deserialization.
    #[error("failed to deserialize response (status {status}): {serde_error}")]
    DeserializationFailed {
        /// Raw response text that failed to deserialize.
        raw_response: String,
        /// Serde error message.
        serde_error: String,
        /// HTTP status for the response.
        status: StatusCode,
        /// Response headers for the response.
        headers: Box<HeaderMap>,
    },
    /// Invalid SDK configuration or request metadata.
    #[error("configuration error: {0}")]
    Configuration(String),
    /// Bounded retries were exhausted.
    #[error("max retries exceeded after {attempts} attempts: {last_error}")]
    MaxRetriesExceeded {
        /// Total attempts made, including the initial request.
        attempts: usize,
        /// Last terminal error.
        last_error: Box<Error>,
    },
    /// Request JSON serialization failed.
    #[error("failed to serialize request: {0}")]
    Serialization(String),
    /// URL parsing failed.
    #[error("invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),
}

impl Error {
    /// Creates a configuration error.
    pub fn configuration(message: impl Into<String>) -> Self {
        Self::Configuration(message.into())
    }

    /// Classifies a `reqwest` error for request-scoped retry decisions.
    pub fn from_reqwest(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::Timeout(error)
        } else if error.is_connect() {
            Self::Connect(error)
        } else if error.is_body() || error.is_decode() {
            Self::Body(error)
        } else {
            Self::Request(error)
        }
    }

    /// Returns true when the error is a request-local transient retry candidate.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Connect(_) | Self::Request(_) | Self::Timeout(_) => true,
            Self::HttpStatus { status, .. } => {
                status.is_server_error() || *status == StatusCode::TOO_MANY_REQUESTS
            }
            Self::Body(_)
            | Self::DeserializationFailed { .. }
            | Self::Configuration(_)
            | Self::MaxRetriesExceeded { .. }
            | Self::Serialization(_)
            | Self::InvalidUrl(_) => false,
        }
    }

    /// Returns the HTTP status when the error has one.
    pub fn status(&self) -> Option<StatusCode> {
        match self {
            Self::HttpStatus { status, .. } | Self::DeserializationFailed { status, .. } => {
                Some(*status)
            }
            _ => None,
        }
    }

    /// Returns raw response bytes when retained.
    pub fn raw_body(&self) -> Option<&Bytes> {
        match self {
            Self::HttpStatus { raw_body, .. } => Some(raw_body),
            _ => None,
        }
    }

    /// Returns raw response text when retained.
    pub fn raw_response(&self) -> Option<&str> {
        match self {
            Self::HttpStatus { raw_response, .. }
            | Self::DeserializationFailed { raw_response, .. } => Some(raw_response),
            _ => None,
        }
    }

    /// Returns retained headers when available.
    pub fn headers(&self) -> Option<&HeaderMap> {
        match self {
            Self::HttpStatus { headers, .. } | Self::DeserializationFailed { headers, .. } => {
                Some(headers)
            }
            _ => None,
        }
    }

    /// Returns parsed rate-limit metadata when available.
    pub fn rate_limit_info(&self) -> Option<&crate::RateLimitInfo> {
        match self {
            Self::HttpStatus {
                rate_limit_info, ..
            } => rate_limit_info.as_ref(),
            _ => None,
        }
    }

    /// Returns the capped request-local rate-limit delay when configured.
    pub fn rate_limit_delay(&self, config: &crate::RateLimitConfig) -> Option<std::time::Duration> {
        if !config.enabled {
            return None;
        }
        self.rate_limit_info()?.delay(config)
    }
}
