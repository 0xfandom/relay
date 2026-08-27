//! Row types.
//!
//! `FromRow` is derived rather than hand-written: sqlx maps columns to fields by
//! name, which is why the queries in `lib.rs` alias columns explicitly.

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::FromRow;
use uuid::Uuid;

/// A signing secret.
///
/// A newtype rather than a `String`, and the whole value of it is what it refuses to
/// do: it has no `Display`, and its `Debug` prints `<redacted>`. Reading the actual
/// bytes takes [`Secret::expose`], which is deliberately awkward to type and trivial
/// to grep for at review time.
///
/// The alternative is a rule people have to remember, and the failure mode of that
/// rule is one `{secret}` in a log line written during an incident, after which
/// every customer has to be told to change their verification key.
#[derive(Clone, PartialEq, Eq, sqlx::Type)]
#[sqlx(transparent)]
pub struct Secret(String);

impl Secret {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// The bytes to sign with. Named so that using it is a visible act.
    pub fn expose(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// The secret as a string, for the one place it is legitimately handed out: the
    /// response to creating or rotating an endpoint.
    pub fn reveal(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

#[derive(Clone, FromRow, Serialize)]
pub struct Endpoint {
    pub id: Uuid,
    pub url: String,
    /// Never serialise this to a client. It is returned exactly once, at creation.
    #[serde(skip_serializing)]
    pub secret: Secret,
    pub event_types: Vec<String>,
    pub enabled: bool,
    /// `http`, `telegram` or `discord`. Decides how the request is built and nothing
    /// else — every retry, backoff, breaker and rate-limit rule is shared.
    pub transport: String,
}

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct Delivery {
    pub id: Uuid,
    pub event_id: Uuid,
    pub endpoint_id: Uuid,
    pub status: String,
    pub attempt: i32,
    /// Set only while `status` is `dead`, and always set then.
    pub dead_reason: Option<String>,
    pub generation: i32,
}

/// A parked delivery, joined with enough context to triage it.
///
/// The URL and event type are included because the first question about a dead
/// letter is always "which endpoint, and what was it?" — and answering that from a
/// bare delivery id means two more queries per row.
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct DeadLetter {
    pub delivery_id: Uuid,
    pub endpoint_id: Uuid,
    pub event_id: Uuid,
    pub event_type: String,
    pub url: String,
    /// Attempts used in the current generation.
    pub attempt: i32,
    /// How many times this delivery has already been replayed.
    pub generation: i32,
    /// `permanent_failure` or `attempts_exhausted`.
    pub dead_reason: Option<String>,
    pub created_at: DateTime<Utc>,
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
    /// Which replay run this attempt belonged to. Without it a replayed delivery
    /// has two attempt 0s and the log cannot be read in order.
    pub generation: i32,
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

/// One row of an endpoint's delivery history.
///
/// The event type and the timestamps are joined in because the first question about
/// a listed delivery is always "what was it, and when" — and answering that from a
/// bare id means a query per row, which is how a page of a hundred becomes a
/// hundred and one round trips.
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct DeliverySummary {
    pub id: Uuid,
    pub event_id: Uuid,
    pub endpoint_id: Uuid,
    pub event_type: String,
    pub status: String,
    pub attempt: i32,
    pub generation: i32,
    pub dead_reason: Option<String>,
    /// When this delivery will next be tried. In the past for a delivery that is
    /// due, and meaningless once it has succeeded or died.
    pub next_attempt_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

/// A delivery joined with everything the sender needs to build one request, so
/// the send path performs a single query rather than three.
#[derive(Clone, FromRow)]
pub struct PendingDelivery {
    pub delivery_id: Uuid,
    pub attempt: i32,
    pub event_type: String,
    /// The exact bytes that arrived. Signing anything else breaks verification.
    pub raw_payload: Vec<u8>,
    pub endpoint_id: Uuid,
    pub url: String,
    pub secret: Secret,
    /// The secret being rotated away from, while its overlap window is open.
    ///
    /// Both signatures go out together during the window, so there is no instant at
    /// which the receiver's choice of secret is wrong. `None` once the window has
    /// closed — decided by the query rather than by a sweeper, so a pruner that
    /// stopped running cannot quietly keep an old secret alive.
    pub previous_secret: Option<Secret>,
    /// The endpoint's configured rate, carried on the claim so the sender needs no
    /// second query to decide whether it is allowed to send yet.
    pub rate_per_second: f64,
    pub burst: f64,
    /// The endpoint's breaker as it stood when this row was claimed.
    ///
    /// Read here rather than queried at send time so the gate costs nothing. It can
    /// be stale by a few milliseconds — another worker may have tripped the breaker
    /// in between — and that is acceptable: the cost is a handful of extra requests
    /// to an endpoint that is already failing, against one query per delivery
    /// forever.
    pub breaker_state: String,
    pub breaker_probe_at: Option<DateTime<Utc>>,
    /// How to turn `url` and `secret` into a request. Carried on the claim like
    /// everything else the send path needs, so building one costs no extra query.
    pub transport: String,
}

/// An endpoint's circuit breaker, as stored.
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct BreakerRow {
    /// `closed`, `open` or `half_open`.
    pub breaker_state: String,
    pub consecutive_failures: i32,
    pub breaker_trips: i32,
    /// When a probe may next be issued. `None` only while closed.
    pub breaker_probe_at: Option<DateTime<Utc>>,
    pub breaker_opened_at: Option<DateTime<Utc>>,
}

/// `Debug` is written out rather than derived, and the secret is redacted.
///
/// A structural guarantee instead of a rule people have to remember. The signing
/// secret rides along on every claimed row because the sender needs it, which means
/// one `?pending` in a log line — added during an incident, by someone in a hurry —
/// would write every customer's secret into a log aggregator that is backed up,
/// searchable and shared with people who should never see it. Rotating after that
/// means telling every customer to change their verification key.
///
/// The derive cannot be trusted to stay safe: it prints whatever fields exist, so a
/// field added later is exposed by default. This is the opposite default.
impl std::fmt::Debug for PendingDelivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingDelivery")
            .field("delivery_id", &self.delivery_id)
            .field("attempt", &self.attempt)
            .field("event_type", &self.event_type)
            // Length, not content. A payload is customer data and can be enormous;
            // its size is the part that is ever useful in a log.
            .field("raw_payload_bytes", &self.raw_payload.len())
            .field("endpoint_id", &self.endpoint_id)
            .field("url", &self.url)
            .field("secret", &self.secret)
            .field("previous_secret", &self.previous_secret)
            .field("rate_per_second", &self.rate_per_second)
            .field("burst", &self.burst)
            .field("breaker_state", &self.breaker_state)
            .field("breaker_probe_at", &self.breaker_probe_at)
            .field("transport", &self.transport)
            .finish()
    }
}

/// Same reasoning as [`PendingDelivery`]: the secret never reaches a log by
/// accident. `Serialize` already skips it, so this closes the other door.
impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Endpoint")
            .field("id", &self.id)
            .field("url", &self.url)
            .field("secret", &self.secret)
            .field("event_types", &self.event_types)
            .field("enabled", &self.enabled)
            .field("transport", &self.transport)
            .finish()
    }
}

impl BreakerRow {
    /// The stored row as the domain's value type.
    ///
    /// An unrecognised state reads as closed. That is the safe direction: the
    /// alternative is refusing to deliver anything to an endpoint because of a typo
    /// in a column, and the `CHECK` constraint already makes it unreachable.
    pub fn breaker(&self) -> relay_domain::breaker::Breaker {
        relay_domain::breaker::Breaker {
            state: relay_domain::breaker::State::parse(&self.breaker_state)
                .unwrap_or(relay_domain::breaker::State::Closed),
            consecutive_failures: self.consecutive_failures.max(0) as u32,
            trips: self.breaker_trips.max(0) as u32,
        }
    }
}
