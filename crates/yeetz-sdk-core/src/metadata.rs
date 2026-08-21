//! Request-scoped provider-neutral metadata.

use std::collections::BTreeMap;

use reqwest::Method;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

/// Provider-neutral authentication input for one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestAuth {
    /// HTTP Basic auth username and password.
    Basic { username: String, password: String },
    /// Bearer token auth value without the `Bearer ` prefix.
    Bearer(String),
}

/// Metadata for one provider-neutral HTTP request.
#[derive(Debug, Clone)]
pub struct RequestMetadata {
    /// HTTP method used for the request.
    pub method: Method,
    /// Path relative to the configured base URL.
    pub path: String,
    /// Request-specific headers.
    pub headers: HeaderMap,
    /// Query parameters appended to the URL.
    pub query_params: BTreeMap<String, String>,
    /// Form fields sent as `application/x-www-form-urlencoded` when no JSON body is provided.
    pub form_data: Option<BTreeMap<String, String>>,
    /// Optional request-specific auth input.
    pub auth: Option<RequestAuth>,
}

impl RequestMetadata {
    /// Creates request metadata with method and path.
    pub fn new(method: Method, path: impl Into<String>) -> Self {
        Self {
            method,
            path: path.into(),
            headers: HeaderMap::new(),
            query_params: BTreeMap::new(),
            form_data: None,
            auth: None,
        }
    }

    /// Adds a request-specific header.
    pub fn with_header(
        mut self,
        name: impl AsRef<str>,
        value: impl AsRef<str>,
    ) -> crate::Result<Self> {
        let name = HeaderName::try_from(name.as_ref()).map_err(|error| {
            crate::Error::configuration(format!("invalid header name: {error}"))
        })?;
        let value = HeaderValue::try_from(value.as_ref()).map_err(|error| {
            crate::Error::configuration(format!("invalid header value: {error}"))
        })?;
        self.headers.insert(name, value);
        Ok(self)
    }

    /// Adds one query parameter.
    pub fn with_query_param(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.query_params.insert(key.into(), value.into());
        self
    }

    /// Adds multiple query parameters.
    pub fn with_query_params(mut self, params: impl IntoIterator<Item = (String, String)>) -> Self {
        self.query_params.extend(params);
        self
    }

    /// Sets form data for requests without a JSON body.
    pub fn with_form_data(mut self, data: BTreeMap<String, String>) -> Self {
        self.form_data = Some(data);
        self
    }

    /// Sets request-specific HTTP Basic auth.
    pub fn with_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.auth = Some(RequestAuth::Basic {
            username: username.into(),
            password: password.into(),
        });
        self
    }

    /// Sets request-specific bearer auth.
    pub fn with_bearer_auth(mut self, token: impl Into<String>) -> Self {
        self.auth = Some(RequestAuth::Bearer(token.into()));
        self
    }
}

impl Default for RequestMetadata {
    fn default() -> Self {
        Self::new(Method::GET, "")
    }
}
