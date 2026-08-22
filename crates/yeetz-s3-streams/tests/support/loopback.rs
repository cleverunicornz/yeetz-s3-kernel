#![allow(dead_code)]
//! A loopback S3 counterpart for the yeetz-s3-streams S-suite — same wire
//! fidelity as the kernel's rig (conditional PUTs with etags,
//! ListObjectsV2 pagination, bulk delete), plus the controls the
//! stream contracts need: LIST freezing (stale under-reporting —
//! staleness never loss), key hiding (contradictory witnesses), and
//! one-shot fault cuts by op/key or by global request index (crash
//! matrices). Test-side rig only.

use axum::Router;
use axum::body::Body;
use axum::body::to_bytes;
use axum::extract::State;
use axum::http::header::{ETAG, IF_MATCH, IF_NONE_MATCH};
use axum::http::request::Parts;
use axum::http::{HeaderValue, Method, StatusCode};
use axum::response::Response;
use axum::routing::{any, get, post};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::oneshot;

pub const BUCKET: &str = "streams-loopback";

#[derive(Debug)]
struct LoopbackObject {
    bytes: Bytes,
    etag: String,
}

/// Which storage operation a fault cut targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageOp {
    Put,
    Get,
    List,
    Delete,
}

/// Fault phases: BeforeEffect refuses (nothing applied); AfterEffect
/// applies and loses the response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FaultPhase {
    Before,
    After,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum FaultMatch {
    /// Cut the next request matching op (and key, when given).
    ByOp { op: StorageOp, key: Option<String> },
    /// Cut the storage request at this global index (0-based).
    ByIndex { index: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArmedFault {
    matches: FaultMatch,
    phase: FaultPhase,
    one_shot: bool,
}

struct Inner {
    objects: BTreeMap<String, LoopbackObject>,
    /// Frozen listing snapshot (stale under-reporting; staleness
    /// never loss).
    frozen_listing: Option<BTreeSet<String>>,
    /// Keys hidden from GET/PUT but still LISTed (contradictory
    /// witnesses for the fail-closed contract).
    hidden: BTreeSet<String>,
    fault: Option<ArmedFault>,
}

struct CounterpartState {
    inner: std::sync::Mutex<Inner>,
    request_index: AtomicU64,
    fault_fired: AtomicU64,
    /// (method, key) per storage request — the S11 wire witness.
    request_log: std::sync::Mutex<Vec<RequestRecord>>,
}

impl CounterpartState {
    fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(Inner {
                objects: BTreeMap::new(),
                frozen_listing: None,
                hidden: BTreeSet::new(),
                fault: None,
            }),
            request_index: AtomicU64::new(0),
            fault_fired: AtomicU64::new(0),
            request_log: std::sync::Mutex::new(Vec::new()),
        }
    }
}

/// A running counterpart: endpoint + control client.
pub struct Loopback {
    pub endpoint: String,
    state: Arc<CounterpartState>,
    control: reqwest::Client,
    shutdown: Option<oneshot::Sender<()>>,
}

/// One recorded storage request (S11 wire witnesses).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestRecord {
    pub method: String,
    pub key: String,
}

impl Loopback {
    pub async fn start() -> Self {
        let state = Arc::new(CounterpartState::new());
        let app = Router::new()
            .route("/__ctl__/freeze-list", post(freeze_list))
            .route("/__ctl__/unfreeze-list", post(unfreeze_list))
            .route("/__ctl__/hide", post(hide_key))
            .route("/__ctl__/unhide", post(unhide_key))
            .route("/__ctl__/arm", post(arm_fault))
            .route("/__ctl__/status", get(status))
            .route("/{bucket}/{*key}", any(s3_request))
            .route("/{bucket}", any(s3_request))
            .with_state(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback counterpart");
        let addr = listener.local_addr().expect("counterpart address");
        let (shutdown, shutdown_receiver) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_receiver.await;
                })
                .await;
        });
        Self {
            endpoint: format!("http://{addr}"),
            state,
            control: reqwest::Client::new(),
            shutdown: Some(shutdown),
        }
    }

    /// An opaque kernel handle pointed at the counterpart.
    pub fn kernel(&self) -> yeetz_s3_kernel::KernelHandle {
        let config = yeetz_s3_kernel::S3Config::custom_with_insecure_http(
            BUCKET,
            "us-east-1",
            &self.endpoint,
            "streams-loopback-key",
            "streams-loopback-secret",
            true,
        );
        yeetz_s3_kernel::KernelHandle::from_s3_config(&config).expect("loopback kernel")
    }

    pub async fn freeze_list(&self) {
        self.control
            .post(format!("{}/__ctl__/freeze-list", self.endpoint))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    pub async fn unfreeze_list(&self) {
        self.control
            .post(format!("{}/__ctl__/unfreeze-list", self.endpoint))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    pub async fn hide_key(&self, key: &str) {
        self.control
            .post(format!("{}/__ctl__/hide", self.endpoint))
            .json(&serde_json::json!({ "key": key }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    pub async fn unhide_key(&self, key: &str) {
        self.control
            .post(format!("{}/__ctl__/unhide", self.endpoint))
            .json(&serde_json::json!({ "key": key }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    /// Arm a one-shot fault cut matching the next request for `op`
    /// (and `key`, when given).
    pub async fn arm_fault(&self, op: StorageOp, key: Option<&str>, phase: FaultPhase) {
        self.control
            .post(format!("{}/__ctl__/arm", self.endpoint))
            .json(&serde_json::json!({
                "matches": { "ByOp": { "op": op, "key": key } },
                "phase": phase,
                "one_shot": true,
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    /// Arm a one-shot fault cut at the global storage-request `index`.
    pub async fn arm_fault_at_index(&self, index: u64, phase: FaultPhase) {
        self.control
            .post(format!("{}/__ctl__/arm", self.endpoint))
            .json(&serde_json::json!({
                "matches": { "ByIndex": { "index": index } },
                "phase": phase,
                "one_shot": true,
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    /// The number of storage requests served so far.
    pub fn request_count(&self) -> u64 {
        self.state.request_index.load(Ordering::SeqCst)
    }

    /// The (method, key) log of every storage request served — the
    /// S11 witness: streams writes must be single PUTs under
    /// `keyspace/` and never touch the chunk root. A poisoned lock
    /// (a panicked handler) still yields the log recorded so far.
    pub fn request_log(&self) -> Vec<RequestRecord> {
        self.state
            .request_log
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Whether the armed fault fired (for crash-matrix completeness).
    pub fn fault_fired(&self) -> bool {
        self.state.fault_fired.load(Ordering::SeqCst) > 0
    }

    pub fn shutdown(mut self) {
        let _ = self.shutdown.take().map(|sender| sender.send(()));
    }
}

// --- control handlers ------------------------------------------------------

async fn freeze_list(State(state): State<Arc<CounterpartState>>) -> StatusCode {
    let mut inner = state.inner.lock().unwrap();
    inner.frozen_listing = Some(inner.objects.keys().cloned().collect());
    StatusCode::OK
}

async fn unfreeze_list(State(state): State<Arc<CounterpartState>>) -> StatusCode {
    state.inner.lock().unwrap().frozen_listing = None;
    StatusCode::OK
}

#[derive(Deserialize)]
struct KeyCommand {
    key: String,
}

async fn hide_key(
    State(state): State<Arc<CounterpartState>>,
    axum::Json(command): axum::Json<KeyCommand>,
) -> StatusCode {
    state.inner.lock().unwrap().hidden.insert(command.key);
    StatusCode::OK
}

async fn unhide_key(
    State(state): State<Arc<CounterpartState>>,
    axum::Json(command): axum::Json<KeyCommand>,
) -> StatusCode {
    state.inner.lock().unwrap().hidden.remove(&command.key);
    StatusCode::OK
}

async fn arm_fault(
    State(state): State<Arc<CounterpartState>>,
    axum::Json(fault): axum::Json<ArmedFault>,
) -> StatusCode {
    state.inner.lock().unwrap().fault = Some(fault);
    state.fault_fired.store(0, Ordering::SeqCst);
    StatusCode::OK
}

async fn status(State(state): State<Arc<CounterpartState>>) -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "requests": state.request_index.load(Ordering::SeqCst),
        "objects": state.inner.lock().unwrap().objects.len(),
    }))
}

// --- S3 wire ---------------------------------------------------------------

fn counterpart_key(path: &str) -> Option<String> {
    path.strip_prefix(&format!("/{BUCKET}/"))
        .filter(|key| !key.is_empty())
        .map(ToOwned::to_owned)
}

fn unquoted_etag(value: &str) -> &str {
    value.trim_matches('"')
}

fn query_param(parts: &Parts, name: &str) -> Option<String> {
    parts.uri.query().and_then(|query| {
        query.split('&').find_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            (key == name).then(|| urldecode(value))
        })
    })
}

fn urldecode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                if let (Some(hex), Ok(byte)) = (
                    std::str::from_utf8(&bytes[index + 1..index + 3]).ok(),
                    u8::from_str_radix(
                        std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or("zz"),
                        16,
                    ),
                ) {
                    let _ = hex;
                    out.push(byte);
                    index += 3;
                } else {
                    out.push(bytes[index]);
                    index += 1;
                }
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(out).unwrap_or_default()
}

/// <Key> values from a bulk-delete body.
fn extract_delete_keys(body: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(body)
        .split("<Key>")
        .skip(1)
        .filter_map(|rest| rest.split_once("</Key>").map(|(key, _)| key.to_string()))
        .collect()
}

fn list_objects_xml(
    prefix: &str,
    entries: &[(String, String, usize)],
    truncated: bool,
    next_token: Option<&str>,
) -> String {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    xml.push_str(&format!(
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>l</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><IsTruncated>{}</IsTruncated>",
        prefix,
        entries.len(),
        truncated
    ));
    if let Some(token) = next_token {
        xml.push_str(&format!(
            "<NextContinuationToken>{token}</NextContinuationToken>"
        ));
    }
    for (key, etag, size) in entries {
        xml.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>&quot;{}&quot;</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            key, etag, size
        ));
    }
    xml.push_str("</ListBucketResult>");
    xml
}

fn op_of(method: &Method, is_list: bool, is_bulk_delete: bool) -> StorageOp {
    if is_list {
        StorageOp::List
    } else if is_bulk_delete || *method == Method::DELETE {
        StorageOp::Delete
    } else if *method == Method::PUT {
        StorageOp::Put
    } else {
        StorageOp::Get
    }
}

async fn s3_request(State(state): State<Arc<CounterpartState>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let method = parts.method.clone();
    let key = counterpart_key(parts.uri.path());
    let list_request =
        parts.uri.query().is_some_and(|q| q.contains("list-type=2")) && method == Method::GET;
    let bulk_delete_request =
        parts.uri.query().is_some_and(|q| q.contains("delete")) && method == Method::POST;
    let if_match = parts
        .headers
        .get(IF_MATCH)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let if_none_match = parts
        .headers
        .get(IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let index = state.request_index.fetch_add(1, Ordering::SeqCst);
    let op = op_of(&method, list_request, bulk_delete_request);
    if let Some(key) = key.as_deref() {
        let record = RequestRecord {
            method: method.to_string(),
            key: key.to_string(),
        };
        let mut log = state
            .request_log
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        log.push(record);
    }

    // One-shot fault cut: Before → refuse (nothing applied); After →
    // apply the effect, lose the response.
    let cut = {
        let armed = state.inner.lock().unwrap().fault.clone();
        match &armed {
            Some(fault) => {
                let matched = match &fault.matches {
                    FaultMatch::ByOp {
                        op: want,
                        key: want_key,
                    } => {
                        *want == op
                            && want_key
                                .as_deref()
                                .is_none_or(|want| Some(want) == key.as_deref())
                    }
                    FaultMatch::ByIndex { index: want } => *want == index,
                };
                if matched {
                    state.fault_fired.fetch_add(1, Ordering::SeqCst);
                    Some(fault.phase)
                } else {
                    None
                }
            }
            None => None,
        }
    };

    let bytes = match to_bytes(body, 64 * 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(_) => return response(StatusCode::PAYLOAD_TOO_LARGE, None, Vec::new()),
    };

    if let Some(phase) = cut
        && phase == FaultPhase::Before
    {
        // Refused: nothing applied.
        return response(StatusCode::BAD_REQUEST, None, Vec::new());
    }

    let mut inner = state.inner.lock().unwrap();
    let (status, etag, body) = if bulk_delete_request {
        let mut deleted = Vec::new();
        let mut errored = Vec::new();
        for key in extract_delete_keys(&bytes) {
            if inner.hidden.contains(&key) {
                errored.push(key);
            } else {
                inner.objects.remove(&key);
                deleted.push(key);
            }
        }
        let mut xml =
            String::from("<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
        for key in &deleted {
            xml.push_str(&format!("<Deleted><Key>{key}</Key></Deleted>"));
        }
        for key in &errored {
            xml.push_str(&format!(
                "<Error><Key>{key}</Key><Code>AccessDenied</Code><Message>hidden</Message></Error>"
            ));
        }
        xml.push_str("</DeleteResult>");
        (StatusCode::OK, None, xml.into_bytes())
    } else if list_request {
        let prefix = query_param(&parts, "prefix").unwrap_or_default();
        let resume_after = query_param(&parts, "continuation-token")
            .or_else(|| query_param(&parts, "start-after"))
            .filter(|token| !token.is_empty());
        let max_keys: usize = query_param(&parts, "max-keys")
            .and_then(|value| value.parse().ok())
            .unwrap_or(1000);
        // The listing may be a frozen (stale, under-reporting) snapshot.
        let snapshot = inner
            .frozen_listing
            .clone()
            .unwrap_or_else(|| inner.objects.keys().cloned().collect());
        let mut matching: Vec<(String, String, usize)> = inner
            .objects
            .iter()
            .filter(|(key, _entry)| {
                snapshot.contains(*key)
                    && key.starts_with(&prefix)
                    && resume_after
                        .as_deref()
                        .is_none_or(|after| key.as_str() > after)
            })
            .map(|(key, entry)| (key.clone(), entry.etag.clone(), entry.bytes.len()))
            .collect();
        matching.sort();
        let truncated = max_keys > 0 && matching.len() > max_keys;
        let next_token = truncated.then(|| matching[max_keys - 1].0.clone());
        let entries: Vec<(String, String, usize)> = matching.into_iter().take(max_keys).collect();
        let xml = list_objects_xml(&prefix, &entries, truncated, next_token.as_deref());
        (StatusCode::OK, None, xml.into_bytes())
    } else {
        match key.as_deref() {
            None => (StatusCode::NOT_FOUND, None, Vec::new()),
            Some(key) if method == Method::DELETE => {
                if inner.hidden.contains(key) {
                    (StatusCode::NOT_FOUND, None, Vec::new())
                } else {
                    inner.objects.remove(key);
                    (StatusCode::NO_CONTENT, None, Vec::new())
                }
            }
            Some(key) if method == Method::PUT => {
                if inner.hidden.contains(key) {
                    (StatusCode::NOT_FOUND, None, Vec::new())
                } else {
                    let existing = inner.objects.get(key);
                    let condition_matches = match (&if_match, &if_none_match) {
                        (_, Some(value)) if value == "*" => existing.is_none(),
                        (Some(expected), _) => {
                            existing.is_some_and(|entry| entry.etag == unquoted_etag(expected))
                        }
                        _ => true,
                    };
                    if !condition_matches {
                        (StatusCode::PRECONDITION_FAILED, None, Vec::new())
                    } else {
                        let etag = format!("s-{index}");
                        inner.objects.insert(
                            key.to_owned(),
                            LoopbackObject {
                                bytes,
                                etag: etag.clone(),
                            },
                        );
                        (StatusCode::OK, Some(etag), Vec::new())
                    }
                }
            }
            Some(key) if method == Method::GET || method == Method::HEAD => {
                if inner.hidden.contains(key) {
                    (StatusCode::NOT_FOUND, None, Vec::new())
                } else {
                    match inner.objects.get(key) {
                        Some(entry) => (
                            StatusCode::OK,
                            Some(entry.etag.clone()),
                            if method == Method::GET {
                                entry.bytes.clone().to_vec()
                            } else {
                                Vec::new()
                            },
                        ),
                        None => (StatusCode::NOT_FOUND, None, Vec::new()),
                    }
                }
            }
            Some(_) => (StatusCode::METHOD_NOT_ALLOWED, None, Vec::new()),
        }
    };

    let status = if cut == Some(FaultPhase::After) {
        // Applied server-side; the response is lost.
        StatusCode::BAD_REQUEST
    } else {
        status
    };
    response(status, etag.as_deref(), body)
}

fn response(status: StatusCode, etag: Option<&str>, body: Vec<u8>) -> Response {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    if let Some(etag) = etag {
        response.headers_mut().insert(
            ETAG,
            HeaderValue::try_from(format!("\"{etag}\"")).expect("loopback ETag header"),
        );
    }
    response
}

type Request = axum::extract::Request;
