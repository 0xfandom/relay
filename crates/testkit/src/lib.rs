//! A customer endpoint that misbehaves on command.
//!
//! Every milestone after this one has to prove behaviour that only appears when a
//! receiver fails in a specific way: hanging for thirty seconds so a worker can be
//! killed mid-send, failing five times then recovering so backoff is visible,
//! rate-limiting us so deferral can be observed, staying dead long enough to trip
//! a breaker. Real servers will not do any of that on request.
//!
//! So this is built now rather than at the end. It is the laboratory.
//!
//! `/verify` is also an *independent* implementation of signature checking, which
//! is what makes issue #1 meaningful — the signer is checked by something other
//! than itself.

use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::Bytes,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;

/// How far out of date a timestamp may be before the request is treated as a
/// replay. Five minutes is the conventional window.
const TOLERANCE_SECS: i64 = 300;

#[derive(Clone)]
pub struct Receiver {
    inner: Arc<Inner>,
}

struct Inner {
    secret: String,
    /// Delivery ids seen, in order. Duplicates are kept deliberately: proving that
    /// retries reuse one id requires seeing the repeats.
    received: Mutex<Vec<String>>,
    /// Bodies seen, for assertions about byte fidelity.
    bodies: Mutex<Vec<Vec<u8>>>,
    hits: AtomicU64,
}

impl Receiver {
    pub fn new(secret: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Inner {
                secret: secret.into(),
                received: Mutex::new(Vec::new()),
                bodies: Mutex::new(Vec::new()),
                hits: AtomicU64::new(0),
            }),
        }
    }

    pub fn received_ids(&self) -> Vec<String> {
        self.inner.received.lock().unwrap().clone()
    }

    pub fn bodies(&self) -> Vec<Vec<u8>> {
        self.inner.bodies.lock().unwrap().clone()
    }

    pub fn hits(&self) -> u64 {
        self.inner.hits.load(Ordering::Relaxed)
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/verify", post(verify))
            .route("/always500", post(always_500))
            .route("/slow", post(slow))
            .route("/flaky", post(flaky))
            .route("/429", post(too_many))
            .route("/received", get(received))
            .with_state(self.clone())
    }

    /// Bind an ephemeral port and serve in the background. Returns the address to
    /// point Relay at.
    pub async fn spawn(&self) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = self.router();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }
}

fn record(state: &Receiver, headers: &HeaderMap, body: &Bytes) {
    state.inner.hits.fetch_add(1, Ordering::Relaxed);
    if let Some(id) = headers
        .get("relay-delivery-id")
        .and_then(|v| v.to_str().ok())
    {
        state.inner.received.lock().unwrap().push(id.to_string());
    }
    state.inner.bodies.lock().unwrap().push(body.to_vec());
}

/// The honest receiver: checks freshness, then the signature, then accepts.
async fn verify(State(state): State<Receiver>, headers: HeaderMap, body: Bytes) -> Response {
    record(&state, &headers, &body);

    let Some(ts) = headers
        .get("relay-timestamp")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<i64>().ok())
    else {
        return (StatusCode::UNAUTHORIZED, "missing or invalid timestamp").into_response();
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Without this check a captured request could be replayed forever. The
    // timestamp is inside the signed string, so it cannot be edited to suit.
    if (now - ts).abs() > TOLERANCE_SECS {
        return (StatusCode::UNAUTHORIZED, "timestamp outside tolerance").into_response();
    }

    let Some(header) = headers
        .get("relay-signature")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
    else {
        return (StatusCode::UNAUTHORIZED, "missing signature").into_response();
    };

    // The header carries a comma-separated list so a secret can be rotated without
    // a window of failed deliveries: accept if any entry matches.
    let ok = header
        .split(',')
        .filter_map(|p| p.trim().strip_prefix("v1="))
        .any(|candidate| {
            relay_domain::signature::verify(state.inner.secret.as_bytes(), ts, &body, candidate)
        });

    if ok {
        (StatusCode::OK, "ok").into_response()
    } else {
        (StatusCode::UNAUTHORIZED, "bad signature").into_response()
    }
}

async fn always_500(State(state): State<Receiver>, headers: HeaderMap, body: Bytes) -> Response {
    record(&state, &headers, &body);
    (StatusCode::INTERNAL_SERVER_ERROR, "nope").into_response()
}

#[derive(Deserialize)]
pub struct SlowParams {
    #[serde(default = "default_slow_ms")]
    pub ms: u64,
}
fn default_slow_ms() -> u64 {
    30_000
}

async fn slow(
    State(state): State<Receiver>,
    Query(p): Query<SlowParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    record(&state, &headers, &body);
    tokio::time::sleep(Duration::from_millis(p.ms)).await;
    (StatusCode::OK, "slow ok").into_response()
}

#[derive(Deserialize)]
pub struct FlakyParams {
    #[serde(default = "default_pct")]
    pub pct: u64,
}
fn default_pct() -> u64 {
    50
}

/// Deterministic rather than random: failure is decided by request count, so a
/// test that expects "fails twice then succeeds" gets exactly that every run.
async fn flaky(
    State(state): State<Receiver>,
    Query(p): Query<FlakyParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    record(&state, &headers, &body);
    let n = state.inner.hits.load(Ordering::Relaxed);
    if (n % 100) < p.pct {
        (StatusCode::INTERNAL_SERVER_ERROR, "flaked").into_response()
    } else {
        (StatusCode::OK, "ok").into_response()
    }
}

#[derive(Deserialize)]
pub struct RetryAfterParams {
    #[serde(default = "default_retry_after")]
    pub retry_after: u64,
}
fn default_retry_after() -> u64 {
    10
}

async fn too_many(
    State(state): State<Receiver>,
    Query(p): Query<RetryAfterParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    record(&state, &headers, &body);
    (
        StatusCode::TOO_MANY_REQUESTS,
        [("retry-after", p.retry_after.to_string())],
        "slow down",
    )
        .into_response()
}

async fn received(State(state): State<Receiver>) -> Response {
    axum::Json(state.received_ids()).into_response()
}
