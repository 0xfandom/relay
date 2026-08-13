//! Row types.
//!
//! `FromRow` is derived rather than hand-written: sqlx maps columns to fields by
//! name, which is why the queries in `lib.rs` alias columns explicitly.

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct Endpoint {
    pub id: Uuid,
    pub url: String,
    /// Never serialise this to a client. It is returned exactly once, at creation.
    #[serde(skip_serializing)]
    pub secret: String,
    pub event_types: Vec<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct Delivery {
    pub id: Uuid,
    pub event_id: Uuid,
    pub endpoint_id: Uuid,
    pub status: String,
    pub attempt: i32,
}

/// One row of the append-only attempt log.
///
/// Everything needed to reconstruct what happened on a single try: what the endpoint
/// said, how long it took, which process asked, and what was decided next.
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct Attempt {
    pub delivery_id: Uuid,
    pub attempt_no: i32,
    pub http_status: Option<i32>,
    pub latency_ms: i32,
    /// `success`, `deferred`, `retryable` or `permanent`.
    pub outcome_class: String,
    pub error: Option<String>,
    pub response_snippet: Option<String>,
    /// Which sender made the attempt. `None` only for rows predating the column.
    pub worker_id: Option<String>,
    /// When the retry was scheduled for, or `None` if this attempt was terminal.
    /// Without it, a `retryable` attempt that happened to be the last one looks
    /// identical to one that was rescheduled.
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub at: DateTime<Utc>,
}

/// A delivery joined with everything the sender needs to build one request, so
/// the send path performs a single query rather than three.
#[derive(Debug, Clone, FromRow)]
pub struct PendingDelivery {
    pub delivery_id: Uuid,
    pub attempt: i32,
    pub event_type: String,
    /// The exact bytes that arrived. Signing anything else breaks verification.
    pub raw_payload: Vec<u8>,
    pub endpoint_id: Uuid,
    pub url: String,
    pub secret: String,
}
