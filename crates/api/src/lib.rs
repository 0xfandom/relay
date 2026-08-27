//! Relay's ingest and admin HTTP surface.
//!
//! The contract of this crate is: accept fast, never block on delivery. A request
//! to `POST /v1/events` performs one transaction and returns `202`. It does not
//! wait for any customer endpoint, because a customer endpoint may take thirty
//! seconds or hang forever, and the caller must not inherit that latency.

use std::time::{Duration, Instant};

use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::{HeaderMap, HeaderName, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use rand::RngExt;
use relay_domain::{idempotency, rate_limit::Rate, transport, url_guard::Policy};
use relay_store::{Cursor, DeadLetterFilter, DeadReason, DeliveryStatus, Store};
use serde::{Deserialize, Serialize};

pub mod extract;
pub mod readiness;

use extract::RawBody;
use readiness::{Facts, Thresholds};

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    /// What registration will accept.
    ///
    /// Read from the same place the dispatcher reads it, because the two disagreeing
    /// is the failure this guards against: a URL accepted here and refused at send
    /// time is a delivery that dies in the dead letter queue for a reason the caller
    /// was never told at the point they could have fixed it.
    pub policy: Policy,
    /// Largest body `POST /v1/events` will accept.
    ///
    /// Should match the dispatcher's `max_payload_bytes`. If ingest accepts more than
    /// delivery will send, the difference is a set of events that are stored, fanned
    /// out, and then permanently fail — accepted with a `202` that was a lie.
    pub max_body_bytes: usize,
    /// How long both signatures are sent after a rotation.
    ///
    /// The customer's whole deployment has to fit inside it, so it is measured in
    /// hours rather than minutes: a window shorter than the time it takes to notice
    /// the rotation, change a config and roll a fleet is a window that expires
    /// mid-migration, which is the outage the overlap exists to prevent.
    pub secret_overlap: Duration,
    /// The transports registration will accept, and where their APIs live.
    ///
    /// The same registry the dispatcher builds, for the same reason the URL policy is
    /// shared: an address accepted here and unbuildable at send time is a delivery
    /// that dies for a reason the caller was never told.
    pub transports: transport::Registry,
    /// How patient `/readyz` is before it reports a stall.
    pub readiness: Thresholds,
}

/// Long enough for a customer to notice, change a value and deploy.
pub const DEFAULT_SECRET_OVERLAP: Duration = Duration::from_secs(24 * 60 * 60);

impl AppState {
    /// The strict policy. Tests that are not about the guard use this; the binary
    /// reads the environment.
    pub fn new(store: Store) -> Self {
        Self {
            store,
            policy: Policy::default(),
            max_body_bytes: extract::MAX_BODY_BYTES,
            secret_overlap: DEFAULT_SECRET_OVERLAP,
            transports: transport::Registry::default(),
            readiness: Thresholds::default(),
        }
    }

    /// For local development and tests with receivers on loopback.
    pub fn permissive(store: Store) -> Self {
        Self {
            store,
            policy: Policy::permissive(),
            max_body_bytes: extract::MAX_BODY_BYTES,
            secret_overlap: DEFAULT_SECRET_OVERLAP,
            transports: transport::Registry::default(),
            readiness: Thresholds::default(),
        }
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/endpoints", post(create_endpoint))
        .route("/v1/endpoints/{id}/rotate-secret", post(rotate_secret))
        .route(
            "/v1/events",
            // Timed from outside the handler rather than inside it, so the
            // measurement includes the work axum does on the way in and out —
            // reading the body, running extractors, rendering the error for a
            // request that never reached the handler at all. A caller's stopwatch
            // includes all of that, and a latency metric that disagrees with the
            // caller is not measuring the thing anyone cares about.
            post(ingest_event).layer(middleware::from_fn(measure_ingest)),
        )
        .route("/v1/deliveries/{id}", get(get_delivery))
        .route("/v1/endpoints/{id}/deliveries", get(list_deliveries))
        .route("/v1/dlq", get(list_dead_letters))
        .route("/v1/dlq/replay", post(replay_many))
        .route("/v1/deliveries/{id}/replay", post(replay_one))
        .with_state(state)
}

/// Serve `/metrics` alongside the ingest API.
///
/// Separate from [`router`] because installing the recorder is a process-global
/// action that can only happen once, and the tests that build a router are not
/// entitled to assume they are the only one in the process.
pub fn router_with_metrics(state: AppState, exporter: relay_metrics::Exporter) -> Router {
    router(state).merge(exporter.router())
}

/// Record how long an ingest took and how it ended.
async fn measure_ingest(req: Request, next: Next) -> Response {
    let started = Instant::now();
    let response = next.run(req).await;
    relay_metrics::ingest(ingest_outcome(&response), started.elapsed());
    response
}

/// Read the outcome off the response rather than being told it.
///
/// The handler has several exits — a rejection, a fresh event, a replay — and
/// threading a metric through each of them means a new exit added later silently
/// stops being counted. The response already carries the answer.
fn ingest_outcome(response: &Response) -> relay_metrics::Ingest {
    let status = response.status();
    if status.is_server_error() {
        // Kept apart from a rejection on purpose. A spike in rejections is a
        // customer shipping a change; a spike in errors is us.
        return relay_metrics::Ingest::Error;
    }
    if !status.is_success() {
        return relay_metrics::Ingest::Rejected;
    }
    let replayed = response
        .headers()
        .get("relay-idempotent-replay")
        .and_then(|v| v.to_str().ok())
        == Some("true");
    if replayed {
        relay_metrics::Ingest::Replayed
    } else {
        relay_metrics::Ingest::Accepted
    }
}

/// Is this process alive?
///
/// Nothing more. An orchestrator restarts what fails liveness, so checking a shared
/// dependency here would mean that one database blip restarts every replica at once
/// — turning a recoverable outage into a total one. The database *is* checked, but
/// in `/readyz`, where failing means "send traffic elsewhere" rather than "kill it".
async fn healthz() -> Response {
    (StatusCode::OK, "ok").into_response()
}

/// Should this instance be sent traffic?
///
/// Gathers the three facts and hands them to [`readiness::evaluate`], which owns
/// every judgement. The split is deliberate: this function can only be tested
/// against a live stack, and the rules deciding when to pull a node out of rotation
/// deserve tests that run in microseconds.
async fn readyz(State(state): State<AppState>) -> Response {
    let facts = match state.store.ping().await {
        Err(e) => Facts::DatabaseDown(e.to_string()),
        Ok(()) => {
            // Two queries rather than one join. They are cheap, independent, and
            // reporting "the database is up but readiness could not be computed"
            // would be a worse answer than either of them failing on its own.
            let heartbeat_age_secs = state
                .store
                .heartbeat_age(readiness::DISPATCHER)
                .await
                .unwrap_or(None);
            let lateness_secs = state
                .store
                .queue_stats()
                .await
                .ok()
                .and_then(|s| s.oldest_pending_age_secs);
            Facts::Live {
                heartbeat_age_secs,
                lateness_secs,
            }
        }
    };

    let report = readiness::evaluate(&facts, state.readiness);
    // 503 rather than 500: this is a state the instance is expected to recover from,
    // and it is what every load balancer and orchestrator reads as "not now".
    let code = if report.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    // The body says which check failed. A bare 503 sends whoever is paged to read
    // the source to find out what was even being measured.
    (code, Json(report)).into_response()
}

// ------------------------------------------------------------------ endpoints

#[derive(Deserialize)]
pub struct CreateEndpoint {
    /// The destination. A URL for `http`; `telegram://<chat_id>` or
    /// `discord://<webhook_id>` for the chat transports, whose credential goes in
    /// `secret` rather than into the address.
    pub url: String,
    /// `http` (the default), `telegram` or `discord`.
    pub transport: Option<String>,
    /// The bot or webhook token, for a chat transport.
    ///
    /// Absent for `http`, where Relay generates the signing secret itself — there is
    /// nothing for the caller to supply, and letting them supply one would make a
    /// weak secret their decision to get wrong.
    pub secret: Option<String>,
    /// Empty or absent means "every event type".
    #[serde(default)]
    pub event_types: Vec<String>,
    /// Sustained deliveries per second. Absent means the conservative default.
    pub rate_per_second: Option<f64>,
    /// The most that may leave at once after an idle period. Defaults with the rate.
    pub burst: Option<f64>,
}

#[derive(Serialize)]
pub struct CreatedEndpoint {
    pub id: uuid::Uuid,
    pub url: String,
    pub transport: String,
    /// Returned exactly once, at creation. Relay stores it but never shows it again.
    ///
    /// `None` for a chat transport: the caller supplied the credential, so handing it
    /// back would echo their own token into a response body for no gain.
    pub secret: Option<String>,
    /// Echoed back so the caller can see what they got when they configured nothing.
    pub rate_per_second: f64,
    pub burst: f64,
}

async fn create_endpoint(
    State(state): State<AppState>,
    Json(req): Json<CreateEndpoint>,
) -> Result<Response, ApiError> {
    // Fast feedback only. The gate that matters is in the dispatcher, at send time:
    // a domain that is public today can be repointed at an internal address
    // tomorrow, and only the address resolved at the moment of connecting is worth
    // trusting. Rejecting an obviously bad URL here saves the caller a round trip
    // and a delivery that was always going to be refused.
    let kind = match req.transport.as_deref() {
        None => transport::Kind::Http,
        Some(t) => transport::Kind::parse(t).ok_or_else(|| {
            ApiError::BadRequest(format!(
                "unknown transport {t:?}, expected http, telegram or discord"
            ))
        })?,
    };

    // Every transport checks its own address form. A chat address is not a URL and
    // would fail the checks below for entirely the wrong reason.
    state
        .transports
        .validate(kind, &req.url)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    let secret = match (kind, req.secret.as_deref()) {
        // Relay generates the signing key. Letting the caller choose one would make a
        // weak secret their decision to get wrong.
        (transport::Kind::Http, _) => generate_secret(),
        (_, Some(token)) if !token.trim().is_empty() => token.to_string(),
        (_, _) => {
            return Err(ApiError::BadRequest(format!(
                "{} endpoints need a secret: the bot or webhook token",
                kind.as_str()
            )));
        }
    };

    // The URL that will actually be connected to, which for a chat transport is not
    // the address that was stored. Checking the stored one would check the wrong
    // string; building it here means registration and the send path apply the same
    // policy to the same URL.
    let destination = state
        .transports
        .build(
            kind,
            &transport::Context {
                address: &req.url,
                credential: &secret,
                previous_credential: None,
                event_type: "relay.registration_check",
                delivery_id: "00000000-0000-0000-0000-000000000000",
                payload: b"{}",
                timestamp: 0,
            },
        )
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .url;

    let parsed = reqwest_url(&destination)?;
    let bad = |e: relay_domain::url_guard::Refused| ApiError::BadRequest(e.to_string());
    state.policy.check_scheme(parsed.scheme()).map_err(bad)?;
    // An empty host as well as a missing one. Nothing that can be connected to, and
    // cheaper to refuse by name than to let it fail as "resolved to nothing" in a
    // different process an hour later.
    if parsed.host_str().is_none_or(str::is_empty) {
        return Err(ApiError::BadRequest("url has no host".into()));
    }
    // The port is a property of the URL, so it can be settled now. The *address* is
    // not — that is a DNS answer, and the only one worth trusting is the one
    // resolved at the moment of connecting.
    state
        .policy
        .check_port(parsed.port_or_known_default().unwrap_or(80))
        .map_err(bad)?;

    let rate = requested_rate(&req)?;

    let ep = state
        .store
        .create_endpoint_with(&req.url, &secret, &req.event_types, kind)
        .await?;

    // Applied after creation rather than passed into it, so that the overwhelmingly
    // common case — no rate specified — needs no configuration and touches nothing.
    if let Some(rate) = rate {
        state.store.set_endpoint_rate(ep.id, rate).await?;
    }
    let rate = rate.unwrap_or_default();

    Ok((
        StatusCode::CREATED,
        Json(CreatedEndpoint {
            id: ep.id,
            url: ep.url,
            transport: ep.transport,
            secret: (kind == transport::Kind::Http).then_some(secret),
            rate_per_second: rate.per_second,
            burst: rate.burst,
        }),
    )
        .into_response())
}

/// The rate the caller asked for, validated, or `None` to keep the default.
///
/// Rejected here rather than left to the database's CHECK constraint, because a
/// constraint violation reaches the caller as an opaque `500`. A rate of zero is not
/// "unlimited" — it is "never", and it would park every delivery to this endpoint
/// forever while looking like configuration.
fn requested_rate(req: &CreateEndpoint) -> Result<Option<Rate>, ApiError> {
    if req.rate_per_second.is_none() && req.burst.is_none() {
        return Ok(None);
    }
    let default = Rate::default();
    let per_second = req.rate_per_second.unwrap_or(default.per_second);
    let burst = req.burst.unwrap_or(default.burst);

    if !per_second.is_finite() || per_second <= 0.0 {
        return Err(ApiError::BadRequest(format!(
            "rate_per_second must be a positive number, got {per_second}"
        )));
    }
    // A bucket that cannot hold one whole token can never spend one.
    if !burst.is_finite() || burst < 1.0 {
        return Err(ApiError::BadRequest(format!(
            "burst must be at least 1, got {burst}"
        )));
    }
    Ok(Some(Rate::new(per_second, burst)))
}

fn reqwest_url(url: &str) -> Result<url::Url, ApiError> {
    url::Url::parse(url).map_err(|e| ApiError::BadRequest(format!("invalid url: {e}")))
}

/// 32 bytes of randomness, hex encoded, with a prefix that makes an accidentally
/// leaked secret greppable in logs and repositories.
fn generate_secret() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill(&mut bytes);
    format!("whsec_{}", hex::encode(bytes))
}

#[derive(Serialize)]
pub struct RotatedSecret {
    pub id: uuid::Uuid,
    /// Returned exactly once, like the secret at creation.
    pub secret: String,
    /// When the old secret stops being sent. Until then both signatures go out and
    /// the receiver may switch at any moment.
    pub previous_secret_expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// `POST /v1/endpoints/{id}/rotate-secret`
///
/// The old secret keeps signing alongside the new one until the window closes, so
/// there is no instant at which the receiver's choice is wrong. Without that, a
/// rotation is a cutover: whichever side changes first is rejected by the other
/// until it catches up, and the customer cannot fix it by deploying faster.
///
/// Rotating twice inside one window discards the secret from the first rotation.
/// That is deliberate — the number of keys that can sign as you is exactly the
/// number a rotation exists to keep at one.
async fn rotate_secret(
    State(state): State<AppState>,
    Path(id): Path<uuid::Uuid>,
) -> Result<Response, ApiError> {
    let secret = generate_secret();
    state
        .store
        .rotate_secret(id, &secret, state.secret_overlap)
        .await?;
    let rotation = state.store.secret_rotation(id).await?;

    Ok((
        StatusCode::OK,
        Json(RotatedSecret {
            id,
            secret,
            previous_secret_expires_at: rotation.expires_at,
        }),
    )
        .into_response())
}

// --------------------------------------------------------------------- events

/// `POST /v1/events`
///
/// The body is taken as raw bytes and stored verbatim. The event type is read
/// from the `Relay-Event-Type` header, or failing that from a `type` field in the
/// JSON body.
///
/// Note the asymmetry: parsing the body to *route* on is fine, because that
/// parsed value is thrown away. What must never happen is parsing it, storing the
/// parsed form, and re-serialising it later — that changes the bytes the
/// signature covers.
///
/// An `Idempotency-Key` header makes the request safe to retry: a second request
/// carrying the same key creates nothing and is answered with the first one's
/// response, byte for byte. Without the header every request creates an event,
/// because two identical bodies a second apart may be a retry or may be two real
/// events, and only the producer knows which.
async fn ingest_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawBody(body): RawBody,
) -> Result<Response, ApiError> {
    let event_type = event_type_from(&headers, &body)
        .ok_or_else(|| ApiError::BadRequest("missing event type".into()))?;

    let Some(key) = idempotency_key(&headers)? else {
        let accepted = state
            .store
            .insert_event_and_fan_out(&event_type, &body)
            .await?;
        return Ok((StatusCode::ACCEPTED, Json(accepted)).into_response());
    };

    // Covers the type as well as the body, because fanning out to a different set
    // of endpoints is a different request even when the payload is identical.
    let digest = idempotency::digest(&event_type, &body);
    let ingested = state
        .store
        .insert_event_idempotent(&event_type, &body, &key, &digest)
        .await?;

    // The stored bytes, returned untouched. Re-rendering them from a parsed form
    // would risk a different key order or a different delivery id order, and a
    // caller comparing two responses would see a difference that is not there.
    Ok((
        StatusCode::ACCEPTED,
        [
            (header::CONTENT_TYPE, "application/json"),
            (
                HeaderName::from_static("relay-idempotent-replay"),
                if ingested.replayed { "true" } else { "false" },
            ),
        ],
        ingested.response,
    )
        .into_response())
}

/// The `Idempotency-Key` header, validated.
///
/// `Ok(None)` means the header was absent, which is allowed. A present but unusable
/// key is refused rather than ignored: silently dropping it would turn a request the
/// caller believes is deduplicated into one that is not, and they would find out by
/// billing someone twice.
fn idempotency_key(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    let Some(raw) = headers.get("idempotency-key") else {
        return Ok(None);
    };
    let key = raw
        .to_str()
        .map_err(|_| ApiError::BadRequest(idempotency::BadKey::Unprintable.to_string()))?;
    idempotency::check_key(key).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    Ok(Some(key.to_string()))
}

fn event_type_from(headers: &HeaderMap, body: &[u8]) -> Option<String> {
    if let Some(v) = headers
        .get("relay-event-type")
        .and_then(|v| v.to_str().ok())
        && !v.is_empty()
    {
        return Some(v.to_string());
    }
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()?
        .get("type")?
        .as_str()
        .map(str::to_string)
}

// ------------------------------------------------------------------- history

/// `GET /v1/deliveries/{id}`
///
/// The delivery and every attempt made on it. This is the whole point of the
/// append-only attempt log: "what happened to my event" should be a query, not an
/// investigation across three log aggregators.
async fn get_delivery(
    State(state): State<AppState>,
    Path(id): Path<uuid::Uuid>,
) -> Result<Response, ApiError> {
    let Some(delivery) = state.store.delivery_summary(id).await? else {
        return Err(ApiError::NotFound("no delivery with that id".into()));
    };
    // Ordered by (generation, attempt_no), so a replayed delivery reads as two runs
    // rather than as two attempt zeroes in an ambiguous order.
    let attempts = state.store.attempt_history(id).await?;
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "delivery": delivery, "attempts": attempts })),
    )
        .into_response())
}

#[derive(Deserialize)]
pub struct HistoryQuery {
    /// `pending`, `inflight`, `succeeded` or `dead`.
    pub status: Option<String>,
    pub limit: Option<i64>,
    /// Opaque. It is the previous page's `next_cursor`, and nothing else.
    pub cursor: Option<String>,
}

/// `GET /v1/endpoints/{id}/deliveries`
///
/// Newest first, paged by position rather than by offset. Scoped to the endpoint in
/// the store's `WHERE` clause rather than filtered here — the scope belongs beside
/// the query, where a route added later cannot forget it.
async fn list_deliveries(
    State(state): State<AppState>,
    Path(endpoint_id): Path<uuid::Uuid>,
    Query(q): Query<HistoryQuery>,
) -> Result<Response, ApiError> {
    // An unknown endpoint is a `404`, not an empty page. "This endpoint has had no
    // failures" is the most reassuring answer there is, and it is the wrong thing to
    // tell someone who has pasted the wrong id.
    if !state.store.endpoint_exists(endpoint_id).await? {
        return Err(ApiError::NotFound("no endpoint with that id".into()));
    }

    let status = match q.status.as_deref() {
        None => None,
        Some(s) => Some(DeliveryStatus::parse(s).ok_or_else(|| {
            ApiError::BadRequest(format!(
                "unknown status {s:?}, expected pending, inflight, succeeded or dead"
            ))
        })?),
    };
    let limit = q.limit.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE);
    let after = q.cursor.as_deref().map(decode_cursor).transpose()?;

    let items = state
        .store
        .deliveries_for_endpoint(endpoint_id, status, after, limit)
        .await?;

    // Offered only on a full page. A short page cannot have more behind it, and
    // handing back a cursor that leads to nothing invites a client to loop.
    let next = (items.len() as i64 == limit)
        .then(|| items.last())
        .flatten()
        .map(|d| {
            encode_cursor(Cursor {
                created_at: d.created_at,
                id: d.id,
            })
        });

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "count": items.len(),
            "items": items,
            "next_cursor": next,
        })),
    )
        .into_response())
}

/// A page position, as an opaque string.
///
/// Opaque on purpose. A cursor that reads as `created_at=...&id=...` is a cursor
/// callers will construct by hand, and then the pair of columns it names can never
/// change without breaking them. Hex is not encryption and is not meant to be — it
/// is a sign that says "this is ours".
fn encode_cursor(c: Cursor) -> String {
    // Microseconds, because that is `timestamptz`'s own resolution. Anything finer
    // would round on the way back and land the next page one row off.
    hex::encode(format!("{}:{}", c.created_at.timestamp_micros(), c.id))
}

fn decode_cursor(s: &str) -> Result<Cursor, ApiError> {
    let bad = || ApiError::BadRequest("invalid cursor".into());
    let raw = hex::decode(s).map_err(|_| bad())?;
    let text = String::from_utf8(raw).map_err(|_| bad())?;
    let (micros, id) = text.split_once(':').ok_or_else(bad)?;
    Ok(Cursor {
        created_at: chrono::DateTime::from_timestamp_micros(micros.parse().map_err(|_| bad())?)
            .ok_or_else(bad)?,
        id: id.parse().map_err(|_| bad())?,
    })
}

// --------------------------------------------------------------- dead letters

/// Cap on how many dead letters one request may list or replay.
///
/// Not a suggestion. Replaying an unbounded set would schedule every parked
/// delivery at once, aimed at an endpoint that has only just recovered — the exact
/// flood the jittered backoff exists to prevent, delivered on purpose.
const MAX_PAGE: i64 = 500;
const DEFAULT_PAGE: i64 = 100;

#[derive(Deserialize)]
pub struct DlqQuery {
    pub endpoint_id: Option<uuid::Uuid>,
    /// `permanent_failure` or `attempts_exhausted`.
    pub reason: Option<String>,
    pub event_type: Option<String>,
    pub limit: Option<i64>,
}

impl DlqQuery {
    fn into_filter(self) -> Result<(DeadLetterFilter, i64), ApiError> {
        let reason = match self.reason.as_deref() {
            None => None,
            Some(r) => Some(DeadReason::parse(r).ok_or_else(|| {
                ApiError::BadRequest(format!(
                    "unknown reason {r:?}, expected permanent_failure or attempts_exhausted"
                ))
            })?),
        };
        let limit = self.limit.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE);
        Ok((
            DeadLetterFilter {
                endpoint_id: self.endpoint_id,
                reason,
                event_type: self.event_type,
            },
            limit,
        ))
    }
}

/// `GET /v1/dlq`
async fn list_dead_letters(
    State(state): State<AppState>,
    Query(q): Query<DlqQuery>,
) -> Result<Response, ApiError> {
    let (filter, limit) = q.into_filter()?;
    let items = state.store.dead_letters(&filter, limit).await?;
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "count": items.len(), "items": items })),
    )
        .into_response())
}

/// `POST /v1/deliveries/{id}/replay`
async fn replay_one(
    State(state): State<AppState>,
    Path(id): Path<uuid::Uuid>,
) -> Result<Response, ApiError> {
    // Only dead deliveries are replayable. Replaying one that is merely slow would
    // hand a second worker a delivery the first is still sending.
    if state.store.replay(id).await? {
        Ok((
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "replayed": 1 })),
        )
            .into_response())
    } else {
        Err(ApiError::NotFound(
            "no dead delivery with that id".to_string(),
        ))
    }
}

/// `POST /v1/dlq/replay`
async fn replay_many(
    State(state): State<AppState>,
    Query(q): Query<DlqQuery>,
) -> Result<Response, ApiError> {
    let (filter, limit) = q.into_filter()?;
    let replayed = state.store.replay_many(&filter, limit).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "replayed": replayed, "limit": limit })),
    )
        .into_response())
}

// ---------------------------------------------------------------------- error

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error(transparent)]
    Store(#[from] relay_store::StoreError),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, msg) = match &self {
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m.clone()),
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m.clone()),
            // Rotating an endpoint that does not exist is the caller's mistake, not
            // ours, and a `500` would have them retry it forever.
            ApiError::Store(relay_store::StoreError::EndpointNotFound) => (
                StatusCode::NOT_FOUND,
                "no endpoint with that id".to_string(),
            ),
            // The caller reused one key for two different requests. Their bug, and
            // one they can only fix if we say so — answering with the first
            // request's result instead would drop the second event while looking
            // like a success.
            ApiError::Store(relay_store::StoreError::IdempotencyKeyReused) => (
                StatusCode::CONFLICT,
                "idempotency key was already used for a different request".to_string(),
            ),
            ApiError::Store(e) => {
                // Never leak database internals to a caller; log them instead.
                tracing::error!(error = %e, "store error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_string(),
                )
            }
        };
        (status, Json(serde_json::json!({ "error": msg }))).into_response()
    }
}
