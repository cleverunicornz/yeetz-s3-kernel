use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use httpmock::{
    Method::{DELETE, GET, PATCH, POST, PUT},
    MockServer,
};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;
use yeetz_sdk_core::{
    Client, Error, RateLimitConfig, RequestMetadata, Response, RetryOn5xx, RetryOnConnectionError,
    RetryOnRetryable, RetryOnTimeout, RetryPredicate, RetryStrategy,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TestPayload {
    id: u64,
    name: String,
}

#[derive(Debug)]
struct FailingSerialize;

impl Serialize for FailingSerialize {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom(
            "intentional serialization failure",
        ))
    }
}

#[tokio::test]
async fn json_calls_preserve_response_metadata_and_raw_body() {
    let server = MockServer::start_async().await;
    let mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/items/1");
            then.status(200)
                .header("x-trace-id", "trace-1")
                .json_body(json!({"id": 1, "name": "alpha"}));
        })
        .await;

    let client = Client::builder()
        .base_url(server.url("/"))
        .unwrap()
        .build()
        .unwrap();
    let started = Instant::now();
    let response: Response<TestPayload> = client.get("/items/1").await.unwrap();
    let observed_elapsed = started.elapsed();

    mock.assert_async().await;
    assert_eq!(response.data.name, "alpha");
    assert_eq!(response.status.as_u16(), 200);
    assert_eq!(response.header("x-trace-id"), Some("trace-1"));
    assert_eq!(response.attempts, 1);
    assert!(response.latency <= observed_elapsed);
    assert!(response.raw_text().unwrap().contains("\"alpha\""));
}

#[tokio::test]
async fn supported_methods_form_and_bytes_calls_are_mock_backed() {
    let server = MockServer::start_async().await;
    let get_mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/methods/get");
            then.status(200).json_body(json!({"id": 10, "name": "get"}));
        })
        .await;
    let post_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/methods/post")
                .header("content-type", "application/json")
                .json_body(json!({"id": 11, "name": "post"}));
            then.status(201)
                .json_body(json!({"id": 11, "name": "post"}));
        })
        .await;
    let form_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/methods/form")
                .header("content-type", "application/x-www-form-urlencoded")
                .body("kind=form");
            then.status(200)
                .json_body(json!({"id": 12, "name": "form"}));
        })
        .await;
    let put_mock = server
        .mock_async(|when, then| {
            when.method(PUT).path("/methods/put");
            then.status(200).json_body(json!({"id": 13, "name": "put"}));
        })
        .await;
    let patch_mock = server
        .mock_async(|when, then| {
            when.method(PATCH).path("/methods/patch");
            then.status(200)
                .json_body(json!({"id": 14, "name": "patch"}));
        })
        .await;
    let delete_mock = server
        .mock_async(|when, then| {
            when.method(DELETE).path("/methods/delete");
            then.status(204);
        })
        .await;
    let get_bytes_mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/methods/get-bytes");
            then.status(200).body(vec![1, 3, 5, 7]);
        })
        .await;
    let post_bytes_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/methods/post-bytes")
                .header("content-type", "application/json")
                .json_body(json!({"id": 15, "name": "post-bytes"}));
            then.status(201).body(vec![2, 4, 6, 8]);
        })
        .await;

    let client = Client::builder()
        .base_url(server.url("/"))
        .unwrap()
        .build()
        .unwrap();
    let payload = TestPayload {
        id: 11,
        name: "post".to_string(),
    };
    let mut form = BTreeMap::new();
    form.insert("kind".to_string(), "form".to_string());

    assert_eq!(
        client
            .get::<TestPayload>("/methods/get")
            .await
            .unwrap()
            .data
            .id,
        10
    );
    assert_eq!(
        client
            .post::<_, TestPayload>("/methods/post", &payload)
            .await
            .unwrap()
            .status
            .as_u16(),
        201
    );
    assert_eq!(
        client
            .post_form::<TestPayload>("/methods/form", form)
            .await
            .unwrap()
            .data
            .name,
        "form"
    );
    assert_eq!(
        client
            .put::<_, TestPayload>("/methods/put", &payload)
            .await
            .unwrap()
            .data
            .name,
        "put"
    );
    assert_eq!(
        client
            .patch::<_, TestPayload>("/methods/patch", &payload)
            .await
            .unwrap()
            .data
            .name,
        "patch"
    );
    assert_eq!(
        client
            .delete::<serde_json::Value>("/methods/delete")
            .await
            .unwrap()
            .status
            .as_u16(),
        204
    );
    assert_eq!(
        client.get_bytes("/methods/get-bytes").await.unwrap().data,
        Bytes::from_static(&[1, 3, 5, 7])
    );
    assert_eq!(
        client
            .post_bytes(
                "/methods/post-bytes",
                &TestPayload {
                    id: 15,
                    name: "post-bytes".to_string(),
                }
            )
            .await
            .unwrap()
            .raw_body,
        Bytes::from_static(&[2, 4, 6, 8])
    );

    get_mock.assert_async().await;
    post_mock.assert_async().await;
    form_mock.assert_async().await;
    put_mock.assert_async().await;
    patch_mock.assert_async().await;
    delete_mock.assert_async().await;
    get_bytes_mock.assert_async().await;
    post_bytes_mock.assert_async().await;
}

#[tokio::test]
async fn metadata_builds_headers_query_form_and_auth_inputs() {
    let server = MockServer::start_async().await;
    let form_mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/form")
                .query_param("page", "1")
                .header("x-default", "true")
                .header("x-request", "yes")
                .header("authorization", "Basic dXNlcjpwYXNz")
                .header("content-type", "application/x-www-form-urlencoded")
                .body("field=value");
            then.status(200).json_body(json!({"id": 2, "name": "form"}));
        })
        .await;

    let client = Client::builder()
        .base_url(server.url("/"))
        .unwrap()
        .default_header("x-default", "true")
        .unwrap()
        .basic_auth("user", "pass")
        .build()
        .unwrap();
    let mut form = BTreeMap::new();
    form.insert("field".to_string(), "value".to_string());
    let metadata = RequestMetadata::new(Method::POST, "/form")
        .with_header("x-request", "yes")
        .unwrap()
        .with_query_param("page", "1")
        .with_form_data(form);
    let response: Response<TestPayload> = client.call_json(metadata, None::<&()>).await.unwrap();

    form_mock.assert_async().await;
    assert_eq!(response.data.name, "form");

    let bearer_mock = server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/bearer")
                .header("authorization", "Bearer request-token");
            then.status(200)
                .json_body(json!({"id": 3, "name": "bearer"}));
        })
        .await;
    let metadata = RequestMetadata::new(Method::GET, "/bearer").with_bearer_auth("request-token");
    let response: Response<TestPayload> = client.call_json(metadata, None::<&()>).await.unwrap();
    bearer_mock.assert_async().await;
    assert_eq!(response.data.name, "bearer");
}

#[tokio::test]
async fn retry_strategies_and_predicates_stay_request_local() {
    let server = MockServer::start_async().await;
    let no_retry_mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/retry/none");
            then.status(500).body("no retry");
        })
        .await;
    let exponential_mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/retry/exponential");
            then.status(500).body("exponential retry");
        })
        .await;
    let custom_mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/retry/custom");
            then.status(503).body("custom retry");
        })
        .await;
    let http_date = httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(60));
    let http_date_mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/retry/http-date");
            then.status(429)
                .header("retry-after", http_date.as_str())
                .header("x-ratelimit-remaining", "0")
                .body("date retry");
        })
        .await;

    let no_retry_client = Client::builder()
        .base_url(server.url("/"))
        .unwrap()
        .retry_strategy(RetryStrategy::None)
        .retry_predicate(Box::new(RetryOnRetryable))
        .build()
        .unwrap();
    assert!(matches!(
        no_retry_client
            .get::<TestPayload>("/retry/none")
            .await
            .unwrap_err(),
        Error::HttpStatus { .. }
    ));
    no_retry_mock.assert_calls_async(1).await;

    let exponential_client = Client::builder()
        .base_url(server.url("/"))
        .unwrap()
        .retry_strategy(RetryStrategy::ExponentialBackoff {
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
            max_retries: 2,
            jitter: false,
        })
        .retry_predicate(Box::new(RetryOn5xx))
        .build()
        .unwrap();
    let error = timeout(
        Duration::from_millis(250),
        exponential_client.get::<TestPayload>("/retry/exponential"),
    )
    .await
    .expect("bounded exponential retry")
    .unwrap_err();
    assert!(matches!(
        error,
        Error::MaxRetriesExceeded { attempts: 3, .. }
    ));
    exponential_mock.assert_calls_async(3).await;

    struct RetryOnly503;
    impl RetryPredicate for RetryOnly503 {
        fn should_retry(&self, error: &Error, _attempt: usize) -> bool {
            matches!(error, Error::HttpStatus { status, .. } if status.as_u16() == 503)
        }
    }
    let custom_client = Client::builder()
        .base_url(server.url("/"))
        .unwrap()
        .retry_strategy(RetryStrategy::Custom {
            max_retries: 2,
            delay_fn: |_| Some(Duration::from_millis(1)),
        })
        .retry_predicate(Box::new(RetryOnly503))
        .build()
        .unwrap();
    assert!(matches!(
        custom_client
            .get::<TestPayload>("/retry/custom")
            .await
            .unwrap_err(),
        Error::MaxRetriesExceeded { attempts: 3, .. }
    ));
    custom_mock.assert_calls_async(3).await;

    let http_date_client = Client::builder()
        .base_url(server.url("/"))
        .unwrap()
        .retry_strategy(RetryStrategy::Linear {
            delay: Duration::from_secs(30),
            max_retries: 1,
        })
        .retry_predicate(Box::new(RetryOnRetryable))
        .rate_limit_config(
            RateLimitConfig::builder()
                .max_wait(Duration::from_millis(1))
                .build(),
        )
        .build()
        .unwrap();
    let error = timeout(
        Duration::from_millis(250),
        http_date_client.get::<TestPayload>("/retry/http-date"),
    )
    .await
    .expect("http-date rate-limit delay should be capped")
    .unwrap_err();
    assert!(matches!(
        error,
        Error::MaxRetriesExceeded { attempts: 2, .. }
    ));
    http_date_mock.assert_calls_async(2).await;
}

#[tokio::test]
async fn error_retryability_and_accessors_cover_terminal_variants() {
    let retryable_429 = Error::HttpStatus {
        status: reqwest::StatusCode::TOO_MANY_REQUESTS,
        raw_body: Bytes::from_static(b"limited"),
        raw_response: "limited".to_string(),
        headers: Box::new(reqwest::header::HeaderMap::new()),
        rate_limit_info: None,
    };
    let not_retryable_400 = Error::HttpStatus {
        status: reqwest::StatusCode::BAD_REQUEST,
        raw_body: Bytes::from_static(b"bad"),
        raw_response: "bad".to_string(),
        headers: Box::new(reqwest::header::HeaderMap::new()),
        rate_limit_info: None,
    };
    let deserialization = Error::DeserializationFailed {
        raw_response: "not-json".to_string(),
        serde_error: "expected value".to_string(),
        status: reqwest::StatusCode::OK,
        headers: Box::new(reqwest::header::HeaderMap::new()),
    };
    let configuration = RequestMetadata::new(Method::GET, "/bad")
        .with_header("bad header", "value")
        .unwrap_err();
    let invalid_url = match Client::builder().base_url("not a url") {
        Ok(_) => panic!("expected invalid URL"),
        Err(error) => error,
    };
    let max_retries = Error::MaxRetriesExceeded {
        attempts: 2,
        last_error: Box::new(Error::HttpStatus {
            status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
            raw_body: Bytes::from_static(b"busy"),
            raw_response: "busy".to_string(),
            headers: Box::new(reqwest::header::HeaderMap::new()),
            rate_limit_info: None,
        }),
    };

    assert!(retryable_429.is_retryable());
    assert!(RetryOnRetryable.should_retry(&retryable_429, 1));
    assert_eq!(
        retryable_429.status(),
        Some(reqwest::StatusCode::TOO_MANY_REQUESTS)
    );
    assert_eq!(
        retryable_429.raw_body(),
        Some(&Bytes::from_static(b"limited"))
    );
    assert_eq!(retryable_429.raw_response(), Some("limited"));
    assert!(retryable_429.headers().is_some());
    assert!(!not_retryable_400.is_retryable());
    assert!(!deserialization.is_retryable());
    assert_eq!(deserialization.raw_response(), Some("not-json"));
    assert!(!configuration.is_retryable());
    assert!(!invalid_url.is_retryable());
    assert!(!max_retries.is_retryable());

    let serialization = Client::builder()
        .base_url("http://127.0.0.1/")
        .unwrap()
        .build()
        .unwrap()
        .post::<_, TestPayload>("/serialize", &FailingSerialize)
        .await
        .unwrap_err();
    assert!(matches!(serialization, Error::Serialization(_)));
    assert!(!serialization.is_retryable());

    let body_error = Client::builder()
        .base_url(broken_success_body_url().await)
        .unwrap()
        .build()
        .unwrap()
        .get_bytes("/broken-success")
        .await
        .unwrap_err();
    assert!(matches!(body_error, Error::Body(_)));
    assert!(!body_error.is_retryable());

    let timeout_error = timeout_error().await;
    assert!(matches!(timeout_error, Error::Timeout(_)));
    assert!(RetryOnTimeout.should_retry(&timeout_error, 1));

    let server_error = Error::HttpStatus {
        status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
        raw_body: Bytes::from_static(b"busy"),
        raw_response: "busy".to_string(),
        headers: Box::new(reqwest::header::HeaderMap::new()),
        rate_limit_info: None,
    };
    let and_predicate =
        yeetz_sdk_core::AndPredicate::new(vec![Box::new(RetryOnRetryable), Box::new(RetryOn5xx)]);
    let or_predicate =
        yeetz_sdk_core::OrPredicate::new(vec![Box::new(RetryOnTimeout), Box::new(RetryOn5xx)]);
    assert!(and_predicate.should_retry(&server_error, 1));
    assert!(!and_predicate.should_retry(&retryable_429, 1));
    assert!(or_predicate.should_retry(&server_error, 1));
    assert!(or_predicate.should_retry(&timeout_error, 1));
}

#[tokio::test]
async fn request_paths_are_appended_to_base_url_path_prefixes() {
    let server = MockServer::start_async().await;
    let trailing_slash_mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/api/v1/items");
            then.status(200)
                .json_body(json!({"id": 4, "name": "prefixed"}));
        })
        .await;
    let no_trailing_slash_mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/api/v2/items");
            then.status(200)
                .json_body(json!({"id": 5, "name": "prefixed-no-slash"}));
        })
        .await;

    let trailing_slash_client = Client::builder()
        .base_url(server.url("/api/v1/"))
        .unwrap()
        .build()
        .unwrap();
    let response: Response<TestPayload> = trailing_slash_client.get("/items").await.unwrap();
    trailing_slash_mock.assert_async().await;
    assert_eq!(response.data.name, "prefixed");

    let no_trailing_slash_client = Client::builder()
        .base_url(server.url("/api/v2"))
        .unwrap()
        .build()
        .unwrap();
    let response: Response<TestPayload> = no_trailing_slash_client.get("items").await.unwrap();
    no_trailing_slash_mock.assert_async().await;
    assert_eq!(response.data.name, "prefixed-no-slash");
}

#[tokio::test]
async fn http_and_deserialization_errors_preserve_raw_evidence() {
    let server = MockServer::start_async().await;
    let http_mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/missing");
            then.status(404)
                .header("x-error", "missing")
                .body("not found");
        })
        .await;
    let invalid_json_mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/invalid-json");
            then.status(200)
                .header("x-invalid-json", "retained")
                .body("invalid json");
        })
        .await;
    let client = Client::builder()
        .base_url(server.url("/"))
        .unwrap()
        .build()
        .unwrap();

    let error = client.get::<TestPayload>("/missing").await.unwrap_err();
    http_mock.assert_async().await;
    match error {
        Error::HttpStatus {
            status,
            raw_body,
            raw_response,
            headers,
            ..
        } => {
            assert_eq!(status.as_u16(), 404);
            assert_eq!(raw_body, Bytes::from_static(b"not found"));
            assert_eq!(raw_response, "not found");
            assert_eq!(headers.get("x-error").unwrap(), "missing");
        }
        other => panic!("expected HTTP status error, got {other:?}"),
    }

    let error = client
        .get::<TestPayload>("/invalid-json")
        .await
        .unwrap_err();
    invalid_json_mock.assert_async().await;
    match error {
        Error::DeserializationFailed {
            raw_response,
            serde_error,
            status,
            headers,
        } => {
            assert_eq!(status.as_u16(), 200);
            assert_eq!(raw_response, "invalid json");
            assert!(serde_error.contains("expected"));
            assert_eq!(headers.get("x-invalid-json").unwrap(), "retained");
        }
        other => panic!("expected deserialization error, got {other:?}"),
    }
}

#[tokio::test]
async fn retry_exhaustion_rate_limit_and_bytes_calls_are_request_local() {
    let server = MockServer::start_async().await;
    let retry_mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/rate-limited");
            then.status(429)
                .header("retry-after", "0")
                .header("x-ratelimit-remaining", "0")
                .body("slow down");
        })
        .await;
    let client = Client::builder()
        .base_url(server.url("/"))
        .unwrap()
        .retry_strategy(RetryStrategy::Linear {
            delay: Duration::from_secs(30),
            max_retries: 2,
        })
        .retry_predicate(Box::new(RetryOnRetryable))
        .rate_limit_config(
            RateLimitConfig::builder()
                .enabled(true)
                .max_wait(Duration::from_millis(1))
                .build(),
        )
        .build()
        .unwrap();

    let error = client
        .get::<TestPayload>("/rate-limited")
        .await
        .unwrap_err();
    retry_mock.assert_calls_async(3).await;
    match error {
        Error::MaxRetriesExceeded {
            attempts,
            last_error,
        } => {
            assert_eq!(attempts, 3);
            assert!(matches!(*last_error, Error::HttpStatus { .. }));
        }
        other => panic!("expected retry exhaustion, got {other:?}"),
    }

    let bytes_mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/bytes");
            then.status(200)
                .header("content-type", "application/octet-stream")
                .body(vec![0, 1, 2, 255]);
        })
        .await;
    let response = client.get_bytes("/bytes").await.unwrap();
    bytes_mock.assert_async().await;
    assert_eq!(response.data, Bytes::from_static(&[0, 1, 2, 255]));
    assert_eq!(response.raw_body, Bytes::from_static(&[0, 1, 2, 255]));

    let bytes_retry_mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/bytes-retry");
            then.status(500).body(vec![9, 8, 7]);
        })
        .await;
    let bytes_error = Client::builder()
        .base_url(server.url("/"))
        .unwrap()
        .retry_strategy(RetryStrategy::Linear {
            delay: Duration::from_millis(1),
            max_retries: 1,
        })
        .retry_predicate(Box::new(RetryOnRetryable))
        .build()
        .unwrap()
        .get_bytes("/bytes-retry")
        .await
        .unwrap_err();
    bytes_retry_mock.assert_calls_async(2).await;
    assert!(matches!(
        bytes_error,
        Error::MaxRetriesExceeded { attempts: 2, .. }
    ));

    let disabled_rate_limit_mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/disabled-rate-limit");
            then.status(429)
                .header("retry-after", "10")
                .header("x-ratelimit-remaining", "0")
                .body("limited");
        })
        .await;
    let disabled_error = Client::builder()
        .base_url(server.url("/"))
        .unwrap()
        .rate_limit_config(RateLimitConfig::disabled())
        .build()
        .unwrap()
        .get::<TestPayload>("/disabled-rate-limit")
        .await
        .unwrap_err();
    disabled_rate_limit_mock.assert_async().await;
    match disabled_error {
        Error::HttpStatus {
            rate_limit_info, ..
        } => assert!(rate_limit_info.is_none()),
        other => panic!("expected disabled rate-limit HTTP error, got {other:?}"),
    }
}

#[tokio::test]
async fn reset_only_rate_limit_metadata_drives_request_local_retry_delay() {
    let server = MockServer::start_async().await;
    let reset_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 60;
    let retry_mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/reset-only-rate-limit");
            then.status(429)
                .header("x-ratelimit-reset", reset_at.to_string())
                .body("reset later");
        })
        .await;
    let client = Client::builder()
        .base_url(server.url("/"))
        .unwrap()
        .retry_strategy(RetryStrategy::Linear {
            delay: Duration::from_secs(30),
            max_retries: 1,
        })
        .retry_predicate(Box::new(RetryOnRetryable))
        .rate_limit_config(
            RateLimitConfig::builder()
                .enabled(true)
                .max_wait(Duration::from_millis(1))
                .build(),
        )
        .build()
        .unwrap();

    let error = timeout(
        Duration::from_millis(200),
        client.get::<TestPayload>("/reset-only-rate-limit"),
    )
    .await
    .expect("reset-only rate-limit retry should use capped header delay")
    .unwrap_err();
    retry_mock.assert_calls_async(2).await;
    match error {
        Error::MaxRetriesExceeded { last_error, .. } => match *last_error {
            Error::HttpStatus {
                rate_limit_info, ..
            } => assert!(rate_limit_info.and_then(|info| info.reset_at).is_some()),
            other => panic!("expected terminal HTTP status error, got {other:?}"),
        },
        other => panic!("expected retry exhaustion, got {other:?}"),
    }
}

#[tokio::test]
async fn reset_only_non_rate_limit_status_uses_retry_strategy_delay() {
    let server = MockServer::start_async().await;
    let reset_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 60;
    let retry_mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/server-error-with-reset");
            then.status(503)
                .header("x-ratelimit-reset", reset_at.to_string())
                .body("try again");
        })
        .await;
    let client = Client::builder()
        .base_url(server.url("/"))
        .unwrap()
        .retry_strategy(RetryStrategy::Linear {
            delay: Duration::from_millis(40),
            max_retries: 1,
        })
        .retry_predicate(Box::new(RetryOnRetryable))
        .rate_limit_config(
            RateLimitConfig::builder()
                .enabled(true)
                .max_wait(Duration::from_millis(1))
                .build(),
        )
        .build()
        .unwrap();

    let started = Instant::now();
    let error = timeout(
        Duration::from_secs(1),
        client.get::<TestPayload>("/server-error-with-reset"),
    )
    .await
    .expect("server retry should use configured strategy delay")
    .unwrap_err();

    retry_mock.assert_calls_async(2).await;
    assert!(started.elapsed() >= Duration::from_millis(25));
    match error {
        Error::MaxRetriesExceeded { last_error, .. } => match *last_error {
            Error::HttpStatus {
                rate_limit_info, ..
            } => assert!(rate_limit_info.is_none()),
            other => panic!("expected terminal HTTP status error, got {other:?}"),
        },
        other => panic!("expected retry exhaustion, got {other:?}"),
    }
}

#[tokio::test]
async fn request_and_connect_errors_are_retryable_classifications() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let closed_addr = listener.local_addr().unwrap();
    drop(listener);
    let connect_error = reqwest::Client::builder()
        .timeout(Duration::from_millis(200))
        .build()
        .unwrap()
        .get(format!("http://{closed_addr}/connect"))
        .send()
        .await
        .map_err(Error::from_reqwest)
        .unwrap_err();
    assert!(matches!(connect_error, Error::Connect(_)));
    assert!(connect_error.is_retryable());
    assert!(RetryOnRetryable.should_retry(&connect_error, 1));
    assert!(RetryOnConnectionError.should_retry(&connect_error, 1));

    let server = MockServer::start_async().await;
    let redirect_mock = server
        .mock_async(|when, then| {
            when.method(GET).path("/redirect");
            then.status(302).header("location", "/redirect");
        })
        .await;
    let request_error = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::limited(1))
        .build()
        .unwrap()
        .get(server.url("/redirect"))
        .send()
        .await
        .map_err(Error::from_reqwest)
        .unwrap_err();
    redirect_mock.assert_calls_async(2).await;
    assert!(matches!(request_error, Error::Request(_)));
    assert!(request_error.is_retryable());
    assert!(RetryOnRetryable.should_retry(&request_error, 1));
    assert!(!RetryOnConnectionError.should_retry(&request_error, 1));
}

#[tokio::test]
async fn timeout_errors_are_classified_without_durable_background_state() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buffer = [0u8; 1024];
        let _ = stream.read(&mut buffer).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    let client = Client::builder()
        .base_url(format!("http://{addr}"))
        .unwrap()
        .timeout(Duration::from_millis(20))
        .build()
        .unwrap();
    let error = client.get::<TestPayload>("/timeout").await.unwrap_err();
    if !matches!(error, Error::Timeout(_)) {
        panic!("expected timeout, got {error:?}");
    }
    accept_task.abort();
}

#[tokio::test]
async fn non_success_body_read_failures_preserve_http_status_evidence() {
    let json_url = broken_non_success_body_url(
        "HTTP/1.1 429 Too Many Requests",
        "retry-after: 0\r\nx-ratelimit-remaining: 0\r\nx-error: broken-json\r\n",
    )
    .await;
    let json_client = Client::builder()
        .base_url(json_url)
        .unwrap()
        .build()
        .unwrap();
    let json_error = json_client
        .get::<TestPayload>("/broken-json")
        .await
        .unwrap_err();
    match json_error {
        Error::HttpStatus {
            status,
            raw_body,
            headers,
            rate_limit_info,
            ..
        } => {
            assert_eq!(status.as_u16(), 429);
            assert!(raw_body.is_empty());
            assert_eq!(headers.get("x-error").unwrap(), "broken-json");
            assert!(rate_limit_info.is_some());
        }
        other => panic!("expected status-preserving JSON HTTP error, got {other:?}"),
    }

    let bytes_url = broken_non_success_body_url(
        "HTTP/1.1 503 Service Unavailable",
        "x-error: broken-bytes\r\n",
    )
    .await;
    let bytes_client = Client::builder()
        .base_url(bytes_url)
        .unwrap()
        .build()
        .unwrap();
    let bytes_error = bytes_client.get_bytes("/broken-bytes").await.unwrap_err();
    match bytes_error {
        Error::HttpStatus {
            status,
            raw_body,
            headers,
            rate_limit_info,
            ..
        } => {
            assert_eq!(status.as_u16(), 503);
            assert!(raw_body.is_empty());
            assert_eq!(headers.get("x-error").unwrap(), "broken-bytes");
            assert!(rate_limit_info.is_none());
        }
        other => panic!("expected status-preserving bytes HTTP error, got {other:?}"),
    }
}

async fn timeout_error() -> Error {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buffer = [0u8; 1024];
        let _ = stream.read(&mut buffer).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    let error = Client::builder()
        .base_url(format!("http://{addr}"))
        .unwrap()
        .timeout(Duration::from_millis(20))
        .build()
        .unwrap()
        .get::<TestPayload>("/timeout")
        .await
        .unwrap_err();
    accept_task.abort();
    error
}

async fn broken_success_body_url() -> String {
    broken_body_url(
        "HTTP/1.1 200 OK",
        "content-type: application/octet-stream\r\n",
    )
    .await
}

async fn broken_non_success_body_url(status_line: &'static str, headers: &'static str) -> String {
    broken_body_url(status_line, headers).await
}

async fn broken_body_url(status_line: &'static str, headers: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buffer = [0u8; 1024];
        let _ = stream.read(&mut buffer).await;
        let response = format!(
            "{status_line}\r\n{headers}transfer-encoding: chunked\r\nconnection: close\r\n\r\nZ\r\nbroken\r\n"
        );
        let _ = stream.write_all(response.as_bytes()).await;
    });
    format!("http://{addr}/")
}
