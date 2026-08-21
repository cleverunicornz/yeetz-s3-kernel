//! Terminal response wrapper with raw-body and metadata retention.

use std::time::Duration;

use bytes::Bytes;
use reqwest::StatusCode;
use reqwest::header::HeaderMap;

/// A successful terminal HTTP response.
#[derive(Debug, Clone)]
pub struct Response<T> {
    /// Typed decoded payload.
    pub data: T,
    /// Raw terminal response body bytes.
    pub raw_body: Bytes,
    /// Terminal HTTP status.
    pub status: StatusCode,
    /// Terminal response headers.
    pub headers: HeaderMap,
    /// Total elapsed time for the call, including retry waits.
    pub latency: Duration,
    /// Number of HTTP attempts made for the call.
    pub attempts: usize,
}

impl<T> Response<T> {
    /// Creates a response wrapper.
    pub fn new(
        data: T,
        raw_body: Bytes,
        status: StatusCode,
        headers: HeaderMap,
        latency: Duration,
        attempts: usize,
    ) -> Self {
        Self {
            data,
            raw_body,
            status,
            headers,
            latency,
            attempts,
        }
    }

    /// Maps the decoded payload while preserving response metadata.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Response<U> {
        Response {
            data: f(self.data),
            raw_body: self.raw_body,
            status: self.status,
            headers: self.headers,
            latency: self.latency,
            attempts: self.attempts,
        }
    }

    /// Returns true when the call needed more than one HTTP attempt.
    pub fn was_retried(&self) -> bool {
        self.attempts > 1
    }

    /// Returns a response header as UTF-8 text.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name)?.to_str().ok()
    }

    /// Returns the raw body as UTF-8 when valid.
    pub fn raw_text(&self) -> Option<&str> {
        std::str::from_utf8(&self.raw_body).ok()
    }
}

impl<T> AsRef<T> for Response<T> {
    fn as_ref(&self) -> &T {
        &self.data
    }
}

impl<T> std::ops::Deref for Response<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
