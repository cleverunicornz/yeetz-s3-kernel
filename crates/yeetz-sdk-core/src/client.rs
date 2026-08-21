//! Provider-neutral HTTP client execution with request-local retry behavior.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;
use url::Url;

use crate::metadata::{RequestAuth, RequestMetadata};
use crate::rate_limit::{RateLimitConfig, RateLimitInfo};
use crate::response::Response;
use crate::retry::{RetryOnRetryable, RetryPredicate, RetryStrategy};
use crate::{Error, Result};

/// Successful HTTP response after the provider accepted the request, before the body is consumed.
pub struct StartedResponse {
    response: reqwest::Response,
    /// Terminal HTTP status.
    pub status: StatusCode,
    /// Terminal response headers.
    pub headers: HeaderMap,
    /// Total elapsed time for the call, including retry waits.
    pub latency: Duration,
    /// Number of HTTP attempts made for the call.
    pub attempts: usize,
}

impl StartedResponse {
    /// Returns a response header as UTF-8 text.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name)?.to_str().ok()
    }

    /// Consumes the started response and returns the full terminal body bytes.
    pub async fn into_bytes_response(self) -> Result<Response<Bytes>> {
        let body = self.response.bytes().await.map_err(Error::from_reqwest)?;
        Ok(Response::new(
            body.clone(),
            body,
            self.status,
            self.headers,
            self.latency,
            self.attempts,
        ))
    }
}

/// Reusable provider-neutral HTTP client.
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    http_client: reqwest::Client,
    base_url: Url,
    default_headers: HeaderMap,
    default_auth: Option<RequestAuth>,
    retry_strategy: RetryStrategy,
    retry_predicate: Box<dyn RetryPredicate>,
    timeout: Option<Duration>,
    rate_limit_config: RateLimitConfig,
}

impl Client {
    /// Creates a client builder.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Executes a JSON request and decodes a JSON response.
    pub async fn call_json<Req, Res>(
        &self,
        metadata: RequestMetadata,
        body: Option<&Req>,
    ) -> Result<Response<Res>>
    where
        Req: Serialize,
        Res: DeserializeOwned + Send + 'static,
    {
        let client = self.clone();
        self.retry_loop(&metadata, body, move |response, latency, attempts| {
            let client = client.clone();
            Box::pin(async move {
                client
                    .parse_json_response(response, latency, attempts)
                    .await
            })
        })
        .await
    }

    /// Executes a request and returns the terminal response body bytes.
    pub async fn call_bytes<Req>(
        &self,
        metadata: RequestMetadata,
        body: Option<&Req>,
    ) -> Result<Response<Bytes>>
    where
        Req: Serialize,
    {
        let client = self.clone();
        self.retry_loop(&metadata, body, move |response, latency, attempts| {
            let client = client.clone();
            Box::pin(async move {
                client
                    .parse_bytes_response(response, latency, attempts)
                    .await
            })
        })
        .await
    }

    /// Executes a request and returns after the provider accepted it, before consuming the body.
    pub async fn execute_started<Req>(
        &self,
        metadata: RequestMetadata,
        body: Option<&Req>,
    ) -> Result<StartedResponse>
    where
        Req: Serialize,
    {
        let start = Instant::now();
        let mut attempt = 0usize;
        let mut did_retry = false;

        loop {
            attempt += 1;
            let result = match self.execute_request(&metadata, body).await {
                Ok(response) => {
                    self.accept_started_response(response, start.elapsed(), attempt)
                        .await
                }
                Err(error) => Err(error),
            };

            match result {
                Ok(response) => return Ok(response),
                Err(error) => {
                    if !self.inner.retry_predicate.should_retry(&error, attempt) {
                        return Err(error);
                    }
                    let Some(strategy_delay) = self.inner.retry_strategy.delay_for_attempt(attempt)
                    else {
                        if !did_retry {
                            return Err(error);
                        }
                        return Err(Error::MaxRetriesExceeded {
                            attempts: attempt,
                            last_error: Box::new(error),
                        });
                    };
                    let delay = error
                        .rate_limit_delay(&self.inner.rate_limit_config)
                        .unwrap_or(strategy_delay);
                    tokio::time::sleep(delay).await;
                    did_retry = true;
                }
            }
        }
    }

    /// Executes a GET request and decodes a JSON response.
    pub async fn get<Res>(&self, path: impl Into<String>) -> Result<Response<Res>>
    where
        Res: DeserializeOwned + Send + 'static,
    {
        self.call_json::<(), Res>(RequestMetadata::new(Method::GET, path), None)
            .await
    }

    /// Executes a POST request with a JSON body and decodes a JSON response.
    pub async fn post<Req, Res>(&self, path: impl Into<String>, body: &Req) -> Result<Response<Res>>
    where
        Req: Serialize,
        Res: DeserializeOwned + Send + 'static,
    {
        self.call_json(RequestMetadata::new(Method::POST, path), Some(body))
            .await
    }

    /// Executes a POST request with form data and decodes a JSON response.
    pub async fn post_form<Res>(
        &self,
        path: impl Into<String>,
        form_data: std::collections::BTreeMap<String, String>,
    ) -> Result<Response<Res>>
    where
        Res: DeserializeOwned + Send + 'static,
    {
        self.call_json::<(), Res>(
            RequestMetadata::new(Method::POST, path).with_form_data(form_data),
            None,
        )
        .await
    }

    /// Executes a PUT request with a JSON body and decodes a JSON response.
    pub async fn put<Req, Res>(&self, path: impl Into<String>, body: &Req) -> Result<Response<Res>>
    where
        Req: Serialize,
        Res: DeserializeOwned + Send + 'static,
    {
        self.call_json(RequestMetadata::new(Method::PUT, path), Some(body))
            .await
    }

    /// Executes a PATCH request with a JSON body and decodes a JSON response.
    pub async fn patch<Req, Res>(
        &self,
        path: impl Into<String>,
        body: &Req,
    ) -> Result<Response<Res>>
    where
        Req: Serialize,
        Res: DeserializeOwned + Send + 'static,
    {
        self.call_json(RequestMetadata::new(Method::PATCH, path), Some(body))
            .await
    }

    /// Executes a DELETE request and decodes a JSON response.
    pub async fn delete<Res>(&self, path: impl Into<String>) -> Result<Response<Res>>
    where
        Res: DeserializeOwned + Send + 'static,
    {
        self.call_json::<(), Res>(RequestMetadata::new(Method::DELETE, path), None)
            .await
    }

    /// Executes a GET request and returns response body bytes.
    pub async fn get_bytes(&self, path: impl Into<String>) -> Result<Response<Bytes>> {
        self.call_bytes::<()>(RequestMetadata::new(Method::GET, path), None)
            .await
    }

    /// Executes a POST request with a JSON body and returns response body bytes.
    pub async fn post_bytes<Req>(
        &self,
        path: impl Into<String>,
        body: &Req,
    ) -> Result<Response<Bytes>>
    where
        Req: Serialize,
    {
        self.call_bytes(RequestMetadata::new(Method::POST, path), Some(body))
            .await
    }

    async fn retry_loop<Req, T>(
        &self,
        metadata: &RequestMetadata,
        body: Option<&Req>,
        parser: impl Fn(
            reqwest::Response,
            Duration,
            usize,
        ) -> Pin<Box<dyn Future<Output = Result<Response<T>>> + Send + 'static>>,
    ) -> Result<Response<T>>
    where
        Req: Serialize,
        T: Send + 'static,
    {
        let start = Instant::now();
        let mut attempt = 0usize;
        let mut did_retry = false;

        loop {
            attempt += 1;
            let result = match self.execute_request(metadata, body).await {
                Ok(response) => parser(response, start.elapsed(), attempt).await,
                Err(error) => Err(error),
            };

            match result {
                Ok(response) => return Ok(response),
                Err(error) => {
                    if !self.inner.retry_predicate.should_retry(&error, attempt) {
                        return Err(error);
                    }
                    let Some(strategy_delay) = self.inner.retry_strategy.delay_for_attempt(attempt)
                    else {
                        if !did_retry {
                            return Err(error);
                        }
                        return Err(Error::MaxRetriesExceeded {
                            attempts: attempt,
                            last_error: Box::new(error),
                        });
                    };
                    let delay = error
                        .rate_limit_delay(&self.inner.rate_limit_config)
                        .unwrap_or(strategy_delay);
                    tokio::time::sleep(delay).await;
                    did_retry = true;
                }
            }
        }
    }

    async fn execute_request<Req>(
        &self,
        metadata: &RequestMetadata,
        body: Option<&Req>,
    ) -> Result<reqwest::Response>
    where
        Req: Serialize,
    {
        let mut url = self.inner.base_url.clone();
        url.set_path(&compose_path(url.path(), &metadata.path));
        {
            let mut query = url.query_pairs_mut();
            for (key, value) in &metadata.query_params {
                query.append_pair(key, value);
            }
        }

        let mut request = self.inner.http_client.request(metadata.method.clone(), url);
        for (name, value) in &self.inner.default_headers {
            request = request.header(name, value);
        }
        for (name, value) in &metadata.headers {
            request = request.header(name, value);
        }
        if let Some(timeout) = self.inner.timeout {
            request = request.timeout(timeout);
        }

        let auth = metadata.auth.as_ref().or(self.inner.default_auth.as_ref());
        if let Some(auth) = auth {
            request = apply_auth(request, auth);
        }

        if let Some(body) = body {
            let json = serde_json::to_value(body)
                .map_err(|error| Error::Serialization(error.to_string()))?;
            request = request.json(&json);
        } else if let Some(form_data) = &metadata.form_data {
            let encoded = url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs(form_data.iter())
                .finish();
            request = request
                .header("content-type", "application/x-www-form-urlencoded")
                .body(encoded);
        }

        request.send().await.map_err(Error::from_reqwest)
    }

    async fn parse_json_response<Res>(
        &self,
        response: reqwest::Response,
        latency: Duration,
        attempts: usize,
    ) -> Result<Response<Res>>
    where
        Res: DeserializeOwned,
    {
        let status = response.status();
        let headers = response.headers().clone();

        if !status.is_success() {
            let body = response.bytes().await.unwrap_or_else(|_| Bytes::new());
            return Err(self.http_status_error(status, headers, body));
        }

        let body = response.bytes().await.map_err(Error::from_reqwest)?;
        let raw_response = String::from_utf8_lossy(&body).into_owned();
        let parse_target = if body.is_empty() { "{}" } else { &raw_response };
        let data =
            serde_json::from_str(parse_target).map_err(|error| Error::DeserializationFailed {
                raw_response,
                serde_error: error.to_string(),
                status,
                headers: Box::new(headers.clone()),
            })?;
        Ok(Response::new(
            data, body, status, headers, latency, attempts,
        ))
    }

    async fn parse_bytes_response(
        &self,
        response: reqwest::Response,
        latency: Duration,
        attempts: usize,
    ) -> Result<Response<Bytes>> {
        let status = response.status();
        let headers = response.headers().clone();

        if !status.is_success() {
            let body = response.bytes().await.unwrap_or_else(|_| Bytes::new());
            return Err(self.http_status_error(status, headers, body));
        }

        let body = response.bytes().await.map_err(Error::from_reqwest)?;
        Ok(Response::new(
            body.clone(),
            body,
            status,
            headers,
            latency,
            attempts,
        ))
    }

    async fn accept_started_response(
        &self,
        response: reqwest::Response,
        latency: Duration,
        attempts: usize,
    ) -> Result<StartedResponse> {
        let status = response.status();
        let headers = response.headers().clone();

        if !status.is_success() {
            let body = response.bytes().await.unwrap_or_else(|_| Bytes::new());
            return Err(self.http_status_error(status, headers, body));
        }

        Ok(StartedResponse {
            response,
            status,
            headers,
            latency,
            attempts,
        })
    }

    fn http_status_error(
        &self,
        status: reqwest::StatusCode,
        headers: HeaderMap,
        body: Bytes,
    ) -> Error {
        let rate_limit_info = if self.inner.rate_limit_config.enabled {
            let info = RateLimitInfo::from_headers(&headers);
            (info.is_rate_limited()
                || (status == StatusCode::TOO_MANY_REQUESTS && info.has_wait_hint()))
            .then_some(info)
        } else {
            None
        };
        Error::HttpStatus {
            status,
            raw_response: String::from_utf8_lossy(&body).into_owned(),
            raw_body: body,
            headers: Box::new(headers),
            rate_limit_info,
        }
    }
}

/// Builder for [`Client`].
pub struct ClientBuilder {
    base_url: Option<Url>,
    default_headers: HeaderMap,
    default_auth: Option<RequestAuth>,
    retry_strategy: RetryStrategy,
    retry_predicate: Option<Box<dyn RetryPredicate>>,
    timeout: Option<Duration>,
    rate_limit_config: RateLimitConfig,
}

impl ClientBuilder {
    /// Creates a builder with no retry strategy and default rate-limit parsing enabled.
    pub fn new() -> Self {
        Self {
            base_url: None,
            default_headers: HeaderMap::new(),
            default_auth: None,
            retry_strategy: RetryStrategy::None,
            retry_predicate: None,
            timeout: None,
            rate_limit_config: RateLimitConfig::default(),
        }
    }

    /// Sets the provider-neutral base URL.
    pub fn base_url(mut self, url: impl AsRef<str>) -> Result<Self> {
        self.base_url = Some(Url::parse(url.as_ref())?);
        Ok(self)
    }

    /// Adds a default header.
    pub fn default_header(mut self, name: impl AsRef<str>, value: impl AsRef<str>) -> Result<Self> {
        let name = HeaderName::try_from(name.as_ref())
            .map_err(|error| Error::configuration(format!("invalid header name: {error}")))?;
        let value = HeaderValue::try_from(value.as_ref())
            .map_err(|error| Error::configuration(format!("invalid header value: {error}")))?;
        self.default_headers.insert(name, value);
        Ok(self)
    }

    /// Sets default HTTP Basic auth for requests without request-specific auth.
    pub fn basic_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.default_auth = Some(RequestAuth::Basic {
            username: username.into(),
            password: password.into(),
        });
        self
    }

    /// Sets default bearer auth for requests without request-specific auth.
    pub fn bearer_auth(mut self, token: impl Into<String>) -> Self {
        self.default_auth = Some(RequestAuth::Bearer(token.into()));
        self
    }

    /// Sets the retry strategy.
    pub fn retry_strategy(mut self, strategy: RetryStrategy) -> Self {
        self.retry_strategy = strategy;
        self
    }

    /// Sets the retry predicate.
    pub fn retry_predicate(mut self, predicate: Box<dyn RetryPredicate>) -> Self {
        self.retry_predicate = Some(predicate);
        self
    }

    /// Sets a per-request timeout applied to client requests.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets rate-limit parsing and wait selection configuration.
    pub fn rate_limit_config(mut self, config: RateLimitConfig) -> Self {
        self.rate_limit_config = config;
        self
    }

    /// Builds the client.
    pub fn build(self) -> Result<Client> {
        let base_url = self
            .base_url
            .ok_or_else(|| Error::configuration("base URL is required"))?;
        let http_client = reqwest::Client::builder().build().map_err(|error| {
            Error::configuration(format!("failed to build HTTP client: {error}"))
        })?;
        Ok(Client {
            inner: Arc::new(ClientInner {
                http_client,
                base_url,
                default_headers: self.default_headers,
                default_auth: self.default_auth,
                retry_strategy: self.retry_strategy,
                retry_predicate: self
                    .retry_predicate
                    .unwrap_or_else(|| Box::new(RetryOnRetryable)),
                timeout: self.timeout,
                rate_limit_config: self.rate_limit_config,
            }),
        })
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

fn compose_path(base_path: &str, request_path: &str) -> String {
    let request_path = request_path.trim_start_matches('/');
    if request_path.is_empty() {
        return if base_path.is_empty() {
            "/".to_string()
        } else {
            base_path.to_string()
        };
    }

    let base_path = base_path.trim_end_matches('/');

    match base_path {
        "" | "/" => format!("/{request_path}"),
        base_path => format!("{base_path}/{request_path}"),
    }
}

fn apply_auth(request: reqwest::RequestBuilder, auth: &RequestAuth) -> reqwest::RequestBuilder {
    match auth {
        RequestAuth::Basic { username, password } => {
            let encoded =
                base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
            request.header("authorization", format!("Basic {encoded}"))
        }
        RequestAuth::Bearer(token) => request.bearer_auth(token),
    }
}
