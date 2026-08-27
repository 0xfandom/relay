//! Where a worker finds out which deliveries to attempt.
//!
//! Relay has two answers and they differ only in *discovery*. Polling asks Postgres
//! "what is due?" on a timer. The broker is told "delivery X is ready" by a publisher
//! and pushed the answer. Everything downstream — claiming the row, gating, sending,
//! recording the outcome — is identical, and that is the point of this trait: the
//! delivery path should not be able to tell which mode it is running in.
//!
//! # The lease does not move
//!
//! Whichever source is in use, a worker takes the database lease before it sends. A
//! broker message means "this delivery is worth trying", never "you exclusively own
//! it": Redis redelivers, and reclaim deliberately hands the same message to a second
//! consumer. Trusting the broker's delivery guarantee and dropping the lease is how
//! at-least-once delivery turns into at-least-twice.
//!
//! So both implementations end at the same place — a row that this worker has
//! claimed — and a claim that loses simply produces nothing.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use relay_broker::Broker;

use crate::SendError;
use relay_store::{PendingDelivery, Store};

/// What a source needs handed back once a delivery has been resolved.
///
/// Opaque, and deliberately so. Polling has nothing to settle; the broker has a
/// message to acknowledge. Making the delivery path aware of the difference would
/// undo the abstraction at the one place it matters most.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Receipt(Option<String>);

impl Receipt {
    pub const NONE: Self = Self(None);

    pub fn new(token: impl Into<String>) -> Self {
        Self(Some(token.into()))
    }

    pub fn token(&self) -> Option<&str> {
        self.0.as_deref()
    }
}

/// A delivery this worker has claimed, and whatever the source needs back afterwards.
pub struct Claimed {
    pub pending: PendingDelivery,
    pub receipt: Receipt,
}

#[async_trait]
pub trait Source: Send + Sync {
    /// Claim up to `want` deliveries for `worker`.
    ///
    /// May return fewer, including none. Returning fewer is not an error and not
    /// necessarily an empty queue: the broker source discards messages naming rows
    /// somebody else already claimed.
    async fn claim(&self, want: usize, worker: &str) -> Result<Vec<Claimed>, SendError>;

    /// Report that a claimed delivery has been resolved.
    ///
    /// Called whatever the outcome, success or failure, because the question it
    /// answers is "has this worker finished with the message", not "did the webhook
    /// arrive". The delivery's own fate is already recorded in Postgres.
    async fn settle(&self, receipt: &Receipt) -> Result<(), SendError>;

    /// For logs and metrics.
    fn mode(&self) -> &'static str;
}

/// Ask the database what is due.
///
/// The default, and a complete Relay on its own. One `UPDATE ... RETURNING` with
/// `SKIP LOCKED` both finds and claims the work, so there is no window between
/// discovering a row and owning it.
pub struct Polling {
    store: Store,
}

impl Polling {
    pub fn new(store: Store) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Source for Polling {
    async fn claim(&self, want: usize, worker: &str) -> Result<Vec<Claimed>, SendError> {
        let batch = self.store.claim_batch(want as i64, worker).await?;
        Ok(batch
            .into_iter()
            .map(|pending| Claimed {
                pending,
                receipt: Receipt::NONE,
            })
            .collect())
    }

    /// Nothing to do. The claim and the outcome are the same two writes they always
    /// were, and there is no third party holding a copy of the work.
    async fn settle(&self, _receipt: &Receipt) -> Result<(), SendError> {
        Ok(())
    }

    fn mode(&self) -> &'static str {
        "polling"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_receipt_with_nothing_in_it_is_the_polling_case() {
        assert_eq!(Receipt::NONE.token(), None);
        assert_eq!(Receipt::default(), Receipt::NONE);
    }

    #[test]
    fn a_receipt_carries_whatever_the_source_needs_back() {
        assert_eq!(Receipt::new("1700000000-0").token(), Some("1700000000-0"));
    }
}

/// How a consumer reads from the broker.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// How long a read waits for a message before giving up and returning nothing.
    ///
    /// Blocking in Redis rather than sleeping here. A poll interval is a choice
    /// between wasted queries and added latency; a blocking read has neither, and the
    /// only reason not to block forever is that shutdown has to be noticed.
    pub block: Duration,
    /// How long a message may sit unacknowledged before another consumer may take it.
    ///
    /// Wants to be comfortably longer than a slow delivery. Reclaim cannot tell a
    /// dead consumer from a busy one, so a threshold shorter than the request timeout
    /// hands work to a second consumer while the first is still mid-request — which
    /// the database lease then refuses, wasting the round trip and doing nothing else.
    pub reclaim_idle: Duration,
    /// How often to look for messages a dead consumer left behind.
    pub reclaim_every: Duration,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            block: Duration::from_millis(500),
            // Longer than the sender's default total request timeout of 10s.
            reclaim_idle: Duration::from_secs(60),
            reclaim_every: Duration::from_secs(15),
        }
    }
}

/// Take work from a broker, and still take the database lease before sending.
///
/// The message says *which* delivery. The lease decides *whether this worker gets
/// it*. Both are needed, and the second is the one that makes the first safe: Redis
/// delivers at-least-once and reclaim deliberately hands the same message to a second
/// consumer, so a source that trusted the message alone would send some webhooks
/// twice.
///
/// A message naming a row somebody else already claimed is not an error. It is the
/// ordinary cost of at-least-once, and the response is to acknowledge it and carry
/// on — leaving it unacknowledged would have it reclaimed forever.
pub struct BrokerSource {
    store: Store,
    broker: Arc<dyn Broker>,
    config: ConsumerConfig,
    /// This process's name within the consumer group.
    ///
    /// Must be stable for the life of the process and distinct between processes:
    /// Redis tracks unacknowledged messages per consumer name, so two processes
    /// sharing one name would each be able to reclaim the other's in-flight work
    /// instantly, and a name that changed on every read would leak a consumer entry
    /// per call.
    consumer: String,
    last_reclaim: Mutex<Instant>,
}

impl BrokerSource {
    pub fn new(
        store: Store,
        broker: Arc<dyn Broker>,
        consumer: impl Into<String>,
        config: ConsumerConfig,
    ) -> Self {
        Self {
            store,
            broker,
            config,
            consumer: consumer.into(),
            last_reclaim: Mutex::new(Instant::now()),
        }
    }

    pub fn consumer(&self) -> &str {
        &self.consumer
    }

    /// True once per `reclaim_every`, and it takes the turn when it says yes.
    fn reclaim_due(&self) -> bool {
        let mut last = self.last_reclaim.lock().expect("not poisoned");
        if last.elapsed() >= self.config.reclaim_every {
            *last = Instant::now();
            return true;
        }
        false
    }

    /// Turn messages into claims, acknowledging the ones that lead nowhere.
    async fn claim_each(
        &self,
        messages: Vec<relay_broker::Received>,
        worker: &str,
    ) -> Result<Vec<Claimed>, SendError> {
        let mut claimed = Vec::with_capacity(messages.len());
        let mut stale = Vec::new();

        for m in messages {
            match self.store.claim_one(m.delivery_id, worker).await? {
                Some(pending) => claimed.push(Claimed {
                    pending,
                    receipt: Receipt::new(m.receipt),
                }),
                // Somebody else has it, or it is no longer due. Acknowledged here
                // rather than returned: there is nothing to deliver, and an
                // unacknowledged message would come back on every reclaim forever.
                None => {
                    relay_metrics::stale_message();
                    stale.push(m.receipt);
                }
            }
        }

        if !stale.is_empty() {
            tracing::debug!(
                count = stale.len(),
                "messages named deliveries we could not claim"
            );
            // Best effort. A failure here means they are reclaimed again later, which
            // is noise rather than harm.
            if let Err(e) = self.broker.ack(&stale).await {
                tracing::debug!(error = %e, "could not acknowledge stale messages");
            }
        }

        Ok(claimed)
    }
}

#[async_trait]
impl Source for BrokerSource {
    async fn claim(&self, want: usize, worker: &str) -> Result<Vec<Claimed>, SendError> {
        // Reclaim before reading anything new. Work a dead consumer was holding is
        // older than anything in the stream, and leaving it behind a fresh backlog is
        // how a message ends up waiting for the reconciliation sweep to rescue it.
        if self.reclaim_due() {
            match self
                .broker
                .reclaim(&self.consumer, self.config.reclaim_idle, want)
                .await
            {
                Ok(taken) if !taken.is_empty() => {
                    relay_metrics::reclaimed(taken.len() as u64);
                    tracing::warn!(
                        count = taken.len(),
                        idle = ?self.config.reclaim_idle,
                        "took over messages a consumer stopped reporting on"
                    );
                    return self.claim_each(taken, worker).await;
                }
                Ok(_) => {}
                // Never fatal. The next pass tries again, and the reconciliation
                // sweep is the backstop that does not depend on the broker at all.
                Err(e) => tracing::warn!(error = %e, "reclaim failed"),
            }
        }

        let messages = self
            .broker
            .consume(&self.consumer, want, self.config.block)
            .await
            .map_err(|e| SendError::Broker(e.to_string()))?;

        if messages.is_empty() {
            return Ok(Vec::new());
        }
        relay_metrics::consumed(messages.len() as u64);
        self.claim_each(messages, worker).await
    }

    /// Acknowledge the message, whatever became of the delivery.
    ///
    /// The question this answers is "is this consumer finished with the message", not
    /// "did the webhook arrive". A failed delivery has already been recorded in
    /// Postgres and rescheduled there; leaving its message unacknowledged would have
    /// the broker redeliver it to attempt a row that is no longer due, which the lease
    /// would refuse anyway.
    async fn settle(&self, receipt: &Receipt) -> Result<(), SendError> {
        let Some(token) = receipt.token() else {
            return Ok(());
        };
        self.broker
            .ack(std::slice::from_ref(&token.to_string()))
            .await
            .map_err(|e| SendError::Broker(e.to_string()))?;
        Ok(())
    }

    fn mode(&self) -> &'static str {
        "broker"
    }
}
