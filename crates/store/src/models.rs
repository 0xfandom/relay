//! Row types.
//!
//! `FromRow` is derived rather than hand-written: sqlx maps columns to fields by
//! name, which is why the queries in `lib.rs` alias columns explicitly.

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
