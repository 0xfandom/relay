//! Relay's ingest and admin HTTP surface.
//!
//! The contract of this crate is: accept fast, never block on delivery. A request
//! to `POST /v1/events` performs one transaction and returns `202`. It does not
//! wait for any customer endpoint, because a customer endpoint may take thirty
//! seconds or hang forever, and the caller must not inherit that latency.

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use rand::RngExt;
use relay_store::{DeadLetterFilter, DeadReason, Store};
use serde::{Deserialize, Serialize};

pub mod extract;

use extract::RawBody;

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/endpoints", post(create_endpoint))
        .route("/v1/events", post(ingest_event))
        .route("/v1/dlq", get(list_dead_letters))
        .route("/v1/dlq/replay", post(replay_many))
        .route("/v1/deliveries/{id}/replay", post(replay_one))
        .with_state(state)
}

async fn healthz(State(state): State<AppState>) -> Response {
    match state.store.ping().await {
        Ok(()) => (StatusCode::OK, "ok").into_response(),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
    }
}

// ------------------------------------------------------------------ endpoints

#[derive(Deserialize)]
pub struct CreateEndpoint {
    pub url: String,
    /// Empty or absent means "every event type".
    #[serde(default)]
    pub event_types: Vec<String>,
}

#[derive(Serialize)]
pub struct CreatedEndpoint {
    pub id: uuid::Uuid,
    pub url: String,
    /// Returned exactly once, at creation. Relay stores it but never shows it again.
    pub secret: String,
}

async fn create_endpoint(
    State(state): State<AppState>,
    Json(req): Json<CreateEndpoint>,
) -> Result<Response, ApiError> {
    let secret = generate_secret();
    let ep = state
        .store
        .create_endpoint(&req.url, &secret, &req.event_types)
        .await?;

    Ok((
        StatusCode::CREATED,
        Json(CreatedEndpoint {
            id: ep.id,
            url: ep.url,
            secret,
        }),
    )
        .into_response())
}

/// 32 bytes of randomness, hex encoded, with a prefix that makes an accidentally
/// leaked secret greppable in logs and repositories.
fn generate_secret() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill(&mut bytes);
    format!("whsec_{}", hex::encode(bytes))
}

// --------------------------------------------------------------------- events

#[derive(Serialize)]
pub struct Accepted {
    pub event_id: uuid::Uuid,
    pub delivery_ids: Vec<uuid::Uuid>,
}

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
async fn ingest_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawBody(body): RawBody,
) -> Result<Response, ApiError> {
    let event_type = event_type_from(&headers, &body)
        .ok_or_else(|| ApiError::BadRequest("missing event type".into()))?;

    let accepted = state
        .store
        .insert_event_and_fan_out(&event_type, &body)
        .await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(Accepted {
            event_id: accepted.event_id,
            delivery_ids: accepted.delivery_ids,
        }),
    )
        .into_response())
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
