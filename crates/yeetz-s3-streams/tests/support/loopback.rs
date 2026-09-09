#![allow(dead_code)]
//! A loopback S3 counterpart for the yeetz-s3-streams S-suite — same wire
//! fidelity as the kernel's rig (conditional PUTs with etags,
//! ListObjectsV2 pagination, bulk delete), plus the controls the
//! stream contracts need: LIST freezing (stale under-reporting —
//! staleness never loss), key hiding (contradictory witnesses),
//! one-shot fault cuts by op/key or by global request index (crash
//! matrices), and deterministic one-shot request pauses (park a matched
//! request immediately before its effect or after it applied but
//! pre-acknowledgement, so retention races against trim/GC are
//! witnessed without timing sleeps). Every storage request is logged —
//! bucket-root requests (ListObjectsV2, bulk delete) with key "" and
//! the raw query string. Test-side rig only.

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
use tokio::sync::{Notify, oneshot};

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

/// The armed form of a one-shot pause: what it matches, where it parks,
/// and the two permit channels shared with the [`RequestPause`] handle.
struct ArmedPause {
    op: StorageOp,
    key: Option<String>,
    phase: FaultPhase,
    reached: Arc<Notify>,
    release: Arc<Notify>,
}

impl ArmedPause {
    /// Same op. `None` key matches any request; `Some("")` matches
    /// bucket-root requests (lists, bulk deletes); `Some(k)` matches
    /// only key `k`.
    fn matches(&self, op: StorageOp, key: Option<&str>) -> bool {
        self.op == op
            && match self.key.as_deref() {
                None => true,
                Some("") => key.is_none(),
                Some(want) => key == Some(want),
            }
    }
}

struct Inner {
    objects: BTreeMap<String, LoopbackObject>,
    /// Frozen listing snapshot (stale under-reporting; staleness
    /// never loss).
    frozen_listing: Option<BTreeSet<String>>,
    /// Keys hidden from GET/PUT but still LISTed (contradictory
    /// witnesses for the fail-closed contract).
    hidden: BTreeSet<String>,
    /// Keys omitted from ListObjectsV2 results only — GET/PUT/DELETE
    /// (and bulk delete) keep full visibility. The mirror fault of
    /// `hidden` (unreadable but LISTed): a present object becomes
    /// unlisted, so a floor witness's listing can only observe
    /// survivors. Test fault, not storage behavior.
    list_omitted: BTreeSet<String>,
    fault: Option<ArmedFault>,
}

struct CounterpartState {
    inner: std::sync::Mutex<Inner>,
    request_index: AtomicU64,
    fault_fired: AtomicU64,
    /// (method, key, query) per storage request — the S11 wire witness.
    request_log: std::sync::Mutex<Vec<RequestRecord>>,
    /// Armed one-shot request pause. Sibling of the fault slot and never
    /// inside `Inner`: arming and pausing never contend the
    /// object-state mutex, and a parked request holds no lock.
    pause: std::sync::Mutex<Option<ArmedPause>>,
}

impl CounterpartState {
    fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(Inner {
                objects: BTreeMap::new(),
                frozen_listing: None,
                hidden: BTreeSet::new(),
                list_omitted: BTreeSet::new(),
                fault: None,
            }),
            request_index: AtomicU64::new(0),
            fault_fired: AtomicU64::new(0),
            request_log: std::sync::Mutex::new(Vec::new()),
            pause: std::sync::Mutex::new(None),
        }
    }

    /// Remove the armed pause identified by its `reached` channel, if
    /// still armed. A pause consumed by a matched request already left
    /// the slot; this clears one whose handle was released or dropped
    /// before any match.
    fn disarm_pause(&self, reached: &Arc<Notify>) {
        let mut slot = self
            .pause
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if slot
            .as_ref()
            .is_some_and(|pause| Arc::ptr_eq(&pause.reached, reached))
        {
            *slot = None;
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

/// One recorded storage request (S11 wire witnesses): method, object
/// key — `""` for bucket-root requests such as ListObjectsV2 and bulk
/// delete — and the raw (undecoded) query string when present, so
/// `list-type=2` trims carrying `start-after`/`continuation-token` are
/// distinguishable from log listing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestRecord {
    pub method: String,
    pub key: String,
    pub query: Option<String>,
}

/// Handle on the next request matched by [`Loopback::pause_next`]:
/// parks that one request at its phase point so a test can drive
/// concurrent storage traffic (trim, GC) and observe the race
/// deterministically, no timing sleeps.
///
/// Signaling rides in-process `Notify` permits, not HTTP:
///
/// - No lost wakeups: `notify_one` stores one permit, so both orderings
///   — notify before `notified().await` is polled, or after — complete
///   the single await each side performs.
/// - No leaked blocked requests: dropping the handle without calling
///   [`release`](Self::release) still notifies `release`, so a parked
///   request resumes, and disarms the pause if no request matched it
///   yet — a failed test path can neither wedge the server nor block
///   the next `pause_next`.
/// - `wait_reached` is single-use: the permit is consumed by the first
///   call; awaiting it twice parks forever.
pub struct RequestPause {
    state: std::sync::Weak<CounterpartState>,
    reached: Arc<Notify>,
    release: Option<Arc<Notify>>,
}

impl RequestPause {
    /// Resolve once the matched request has parked at its pause point.
    pub async fn wait_reached(&self) {
        self.reached.notified().await;
    }

    /// Resume the parked request. Equivalent to dropping the handle;
    /// explicit for test intent.
    pub fn release(mut self) {
        self.release_pause();
    }

    fn release_pause(&mut self) {
        // Disarm first: an unconsumed pause leaves the slot so it can
        // never fire (or block the next pause_next); a consumed pause
        // is already gone and this is a no-op.
        if let Some(state) = self.state.upgrade() {
            state.disarm_pause(&self.reached);
        }
        if let Some(release) = self.release.take() {
            release.notify_one();
        }
    }
}

impl Drop for RequestPause {
    fn drop(&mut self) {
        self.release_pause();
    }
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

    /// Test fault: omit `key` from ListObjectsV2 results — live and
    /// frozen listings alike — while GET/PUT/DELETE (and bulk delete)
    /// keep full visibility; the object stays present and readable.
    /// The mirror image of `hide_key` (hidden keys stay LISTed;
    /// omitted keys stay readable). The floor-regression witness
    /// (O7): pause an expected append after a floor-2 observation,
    /// omit only the higher trim-certificate key, and the next listing
    /// can only report the surviving floor. Pagination is computed
    /// after the omission, so listed counts and continuation cursors
    /// stay coherent.
    pub fn omit_from_list(&self, key: &str) {
        self.state
            .inner
            .lock()
            .unwrap()
            .list_omitted
            .insert(key.to_owned());
    }

    /// Reverse [`omit_from_list`](Self::omit_from_list): `key`
    /// reappears in ListObjectsV2 results; its object never left.
    pub fn restore_to_list(&self, key: &str) {
        self.state.inner.lock().unwrap().list_omitted.remove(key);
    }

    /// Test-only backend corruption control: replace the stored body
    /// of an EXISTING physical object with `bytes`, leaving its ETag,
    /// visibility, LIST membership, and every instrument (request log,
    /// faults, pauses) untouched. Intended for corrupting an outer
    /// kernel v2 storage envelope — not inner stream JSON — so a typed
    /// read must classify the object as `Corrupt` (never
    /// `InvalidArgument`), and a post-attempt readback can witness
    /// `Corrupt` + `PossiblyCommitted` together. The ETag is kept: the
    /// object's identity is unchanged, only its body is now
    /// intentionally malformed, so a conditional re-read with a
    /// previously observed ETag still matches and the digest mismatch
    /// is the corruption signal.
    pub fn replace_stored_bytes(&self, key: &str, bytes: &[u8]) {
        let mut inner = self.state.inner.lock().unwrap();
        let object = inner.objects.get_mut(key).unwrap_or_else(|| {
            panic!("replace_stored_bytes: no stored object at {key:?} to corrupt")
        });
        object.bytes = Bytes::copy_from_slice(bytes);
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

    /// Arm a deterministic one-shot pause on the next request matching
    /// `op` and `key`, parking it at `phase`:
    ///
    /// - [`FaultPhase::Before`] — immediately before the request's
    ///   effect is applied (its body is already read);
    /// - [`FaultPhase::After`] — immediately after the effect is
    ///   applied and the object-state lock released, before the
    ///   response is sent: the writer is durable but unacknowledged.
    ///
    /// `key` matching: `None` matches any request; `Some("")` matches
    /// bucket-root requests (ListObjectsV2, bulk delete); `Some(k)`
    /// matches only key `k`. Only the matched request parks, holding no
    /// lock, so concurrent trim/GC requests proceed.
    ///
    /// At most one pause may be armed at a time: arming while another
    /// is still armed panics immediately, because silently replacing it
    /// would strand the old handle's `wait_reached`. Re-arming is valid
    /// once the prior handle is released or dropped — both disarm an
    /// unconsumed pause.
    ///
    /// Interaction with armed faults: the fault match and one-shot
    /// consumption happen before the body read and both pause points;
    /// a `Before` fault cut refuses a request before the pause point,
    /// so the pause stays armed for the next match. An `After` fault
    /// cut and an `After` pause can both take the same request —
    /// effect applied, parked pre-acknowledgement, and on release the
    /// response is cut. Instrument order per request: log record →
    /// fault match/consume → body read → Before cut → Before pause →
    /// effect → After pause → After cut → response.
    pub fn pause_next(&self, op: StorageOp, key: Option<&str>, phase: FaultPhase) -> RequestPause {
        let reached = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mut slot = self
            .state
            .pause
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            slot.is_none(),
            "pause_next while a pause is still armed: release or drop the prior handle first"
        );
        *slot = Some(ArmedPause {
            op,
            key: key.map(ToOwned::to_owned),
            phase,
            reached: Arc::clone(&reached),
            release: Arc::clone(&release),
        });
        RequestPause {
            state: Arc::downgrade(&self.state),
            reached,
            release: Some(release),
        }
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

/// Consume the armed pause if it matches this request and phase point.
/// Locks only the pause slot — never the object-state mutex — so a
/// concurrent request in the effect section is never blocked by arming.
fn take_pause(
    state: &CounterpartState,
    op: StorageOp,
    key: Option<&str>,
    phase: FaultPhase,
) -> Option<ArmedPause> {
    let mut slot = state
        .pause
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let matched = slot
        .as_ref()
        .is_some_and(|pause| pause.phase == phase && pause.matches(op, key));
    if matched { slot.take() } else { None }
}

/// Park the request matched at this phase point until its handle
/// releases it. Suspends holding no lock, so unmatched concurrent
/// requests (trim reads, GC deletes) proceed while the target parks.
async fn park_if_matched(
    state: &Arc<CounterpartState>,
    op: StorageOp,
    key: Option<&str>,
    phase: FaultPhase,
) {
    if let Some(pause) = take_pause(state, op, key, phase) {
        // Permit semantics: the awaiting side completes whether this
        // notify lands before or after `notified().await` is polled.
        pause.reached.notify_one();
        pause.release.notified().await;
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
    // Wire witness: every request is recorded, bucket-root requests
    // (list-type=2, bulk delete) included with key "" and the raw query,
    // so trim reads carrying start-after/continuation-token are
    // distinguishable from log listing by query.
    {
        let record = RequestRecord {
            method: method.to_string(),
            key: key.clone().unwrap_or_default(),
            query: parts.uri.query().map(ToOwned::to_owned),
        };
        let mut log = state
            .request_log
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        log.push(record);
    }

    // One-shot fault cut: Before → refuse (nothing applied); After →
    // apply the effect, lose the response. A matched one-shot fault is
    // consumed here — atomically under the mutex, before any await —
    // so exactly one request is cut per arm; `one_shot: false` (never
    // requested by the public arm APIs) stays armed and cuts every
    // matching request.
    let cut = {
        let mut inner = state.inner.lock().unwrap();
        match inner.fault.clone() {
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
                    if fault.one_shot {
                        inner.fault = None;
                    }
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

    // Deterministic pause point (Before): the matched request parks
    // immediately before its effect, holding no lock.
    park_if_matched(&state, op, key.as_deref(), FaultPhase::Before).await;

    // The effect runs under the object-state mutex inside a lexical
    // block that returns the response triple: the guard is out of
    // scope — not merely explicitly dropped — before the After pause
    // point, so no await is ever reached while the lock is held.
    let (status, etag, body) = {
        let mut inner = state.inner.lock().unwrap();
        if bulk_delete_request {
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
                        && !inner.list_omitted.contains(*key)
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
            let entries: Vec<(String, String, usize)> =
                matching.into_iter().take(max_keys).collect();
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
        }
    };

    // Deterministic pause point (After): the effect is applied and the
    // object-state lock released; the writer is not yet acknowledged.
    park_if_matched(&state, op, key.as_deref(), FaultPhase::After).await;

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
