//! The sender.
//!
//! M1 keeps this deliberately dumb: one delivery at a time, no retries, no
//! concurrency. Everything clever — the `SKIP LOCKED` claim, the worker pool, the
//! reaper, backoff, breakers, rate limits — arrives in later milestones and is
//! layered onto this shape.
//!
//! Building the boring version first is not laziness. Once retries exist, a bug in
//! signing looks exactly like a bug in retrying, and the only way to tell them
//! apart is to have made the single-shot path boringly reliable beforehand.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use relay_store::{PendingDelivery, Store};
use uuid::Uuid;

/// Customer error pages can be enormous. Store enough to debug with, no more.
const SNIPPET_BYTES: usize = 2048;

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error(transparent)]
    Store(#[from] relay_store::StoreError),
    #[error("http client: {0}")]
    Client(#[from] reqwest::Error),
}

/// What happened to one attempt.
///
/// M1 only distinguishes success from failure. M3 splits `Failed` into retryable
/// and permanent, which is the point at which this becomes a real classifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Succeeded { status: u16 },
    Failed { status: Option<u16>, error: String },
}

pub struct Sender {
    store: Store,
    client: reqwest::Client,
}

impl Sender {
    pub fn new(store: Store) -> Self {
        let client = reqwest::Client::builder()
            // Three separate limits. A missing *total* timeout is the classic way a
            // worker pool dies: a per-read timeout resets on every byte, so a slow
            // trickle can hold a connection open indefinitely.
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("reqwest client builds with static configuration");
        Self { store, client }
    }

    /// Take the next due delivery, if any, and attempt it.
    pub async fn deliver_next(&self) -> Result<Option<Outcome>, SendError> {
        let Some(pending) = self.store.next_pending_delivery().await? else {
            return Ok(None);
        };
        Ok(Some(self.deliver(pending).await?))
    }

    /// Attempt one named delivery.
    pub async fn deliver_by_id(&self, delivery_id: Uuid) -> Result<Option<Outcome>, SendError> {
        let Some(pending) = self.store.pending_delivery_by_id(delivery_id).await? else {
            return Ok(None);
        };
        Ok(Some(self.deliver(pending).await?))
    }

    async fn deliver(&self, p: PendingDelivery) -> Result<Outcome, SendError> {
        let timestamp = unix_now();

        // Sign the stored bytes, and send those same bytes. Nothing in between may
        // parse and re-encode the payload: JSON key order is not defined, and the
        // signature covers bytes rather than meaning.
        let signature =
            relay_domain::signature::sign(p.secret.as_bytes(), timestamp, &p.raw_payload);

        let started = Instant::now();
        let result = self
            .client
            .post(&p.url)
            .header("content-type", "application/json")
            .header("relay-timestamp", timestamp.to_string())
            // A list, not a single value, so a secret can be rotated with an overlap
            // window instead of a cutover. M8 fills in the second entry.
            .header("relay-signature", format!("v1={signature}"))
            // Stable across every attempt of this delivery. If this changed per
            // attempt, receivers could not deduplicate retries.
            .header("relay-delivery-id", p.delivery_id.to_string())
            .header("relay-event-type", &p.event_type)
            .body(p.raw_payload.clone())
            .send()
            .await;

        let latency_ms = started.elapsed().as_millis() as i32;

        let (outcome, http_status, error, snippet) = match result {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                let snippet = truncate(&body, SNIPPET_BYTES);
                if status.is_success() {
                    (
                        Outcome::Succeeded {
                            status: status.as_u16(),
                        },
                        Some(status.as_u16() as i32),
                        None,
                        Some(snippet),
                    )
                } else {
                    (
                        Outcome::Failed {
                            status: Some(status.as_u16()),
                            error: format!("HTTP {status}"),
                        },
                        Some(status.as_u16() as i32),
                        Some(format!("HTTP {status}")),
                        Some(snippet),
                    )
                }
            }
            Err(e) => (
                Outcome::Failed {
                    status: None,
                    error: e.to_string(),
                },
                None,
                Some(e.to_string()),
                None,
            ),
        };

        let outcome_class = match &outcome {
            Outcome::Succeeded { .. } => "success",
            Outcome::Failed { .. } => "failed",
        };

        // The attempt row is written before the status change, so a crash between
        // the two leaves evidence rather than a silent gap.
        self.store
            .record_attempt(
                p.delivery_id,
                p.attempt,
                http_status,
                latency_ms,
                outcome_class,
                error.as_deref(),
                snippet.as_deref(),
            )
            .await?;

        match &outcome {
            Outcome::Succeeded { .. } => self.store.mark_succeeded(p.delivery_id).await?,
            // No retry policy yet, so every failure is terminal. M3 changes this.
            Outcome::Failed { .. } => self.store.mark_dead(p.delivery_id).await?,
        }

        Ok(outcome)
    }
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after 1970")
        .as_secs() as i64
}

/// Truncate on a character boundary so the result is always valid UTF-8.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}
