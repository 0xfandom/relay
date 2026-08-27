//! Announcing committed rows to the broker.
//!
//! # The problem this solves
//!
//! The obvious way to run a queue over a broker is to write the event to Postgres and
//! publish it to Redis, one after the other. That is the dual-write problem, and it
//! has no safe ordering:
//!
//! - Publish first and the broker can carry a message for a row that never committed.
//!   A consumer looks the delivery up, finds nothing, and cannot tell a lost write
//!   from a message that arrived early.
//! - Commit first and a crash before the publish leaves an event that exists, is
//!   perfectly valid, and that nothing will ever deliver. Neither system can detect
//!   the gap, because neither knows the other was supposed to hear about it.
//!
//! The outbox removes the choice. Postgres is the only place a delivery is created,
//! in one transaction, exactly as it already was. This loop runs *afterwards* and
//! entirely separately, reading committed rows and announcing them. There is no
//! window in which the two disagree about whether a delivery exists — only about
//! whether it has been announced yet, and that is a question Postgres itself answers.
//!
//! # What a crash costs
//!
//! The mark is written before the publish, so a crash between them leaves a row
//! marked as announced with no message anywhere. That is a gap, and it is deliberate:
//! the other ordering announces a row repeatedly until the mark lands, and a
//! publisher restarting into a large backlog would flood the broker.
//!
//! A gap is affordable because it is recoverable and bounded — the reconciliation
//! sweep finds rows marked long ago that never progressed and un-marks them. A
//! publish that merely *fails* does not even wait for that: the mark is rolled back
//! immediately.

use std::{sync::Arc, time::Duration};

use relay_broker::Broker;
use relay_store::Store;
use tokio_util::sync::CancellationToken;

use crate::SendError;

#[derive(Debug, Clone)]
pub struct PublisherConfig {
    /// Rows announced per pass.
    ///
    /// This is the bounded in-flight window. Everything in a batch is published in
    /// one pipeline, so the batch is also the most that can be lost to a crash
    /// between the mark and the publish.
    pub batch: i64,
    /// How long to wait after finding nothing.
    ///
    /// Only applies to an empty pass. A full batch means there is more waiting, and
    /// the loop goes straight round again — otherwise a backlog would drain at
    /// `batch` per interval regardless of how far behind it is.
    pub idle: Duration,
    /// How long a row may sit announced before the sweep assumes the message is gone.
    ///
    /// Must be comfortably longer than the worst honest time between announcing a
    /// delivery and a consumer claiming it, or the sweep republishes work that is
    /// merely queued and doubles the broker's traffic for no reason.
    pub stale_after: Duration,
    /// How often the reconciliation sweep runs.
    pub sweep_every: Duration,
}

impl Default for PublisherConfig {
    fn default() -> Self {
        Self {
            batch: 256,
            idle: Duration::from_millis(100),
            stale_after: Duration::from_secs(60),
            sweep_every: Duration::from_secs(30),
        }
    }
}

/// Reads committed rows and tells the broker about them.
pub struct Publisher {
    store: Store,
    broker: Arc<dyn Broker>,
    config: PublisherConfig,
}

impl Publisher {
    pub fn new(store: Store, broker: Arc<dyn Broker>, config: PublisherConfig) -> Self {
        Self {
            store,
            broker,
            config,
        }
    }

    /// Announce one batch. Returns how many were published.
    pub async fn publish_once(&self) -> Result<u64, SendError> {
        let ids = self.store.mark_queued(self.config.batch).await?;
        if ids.is_empty() {
            return Ok(0);
        }

        match self.broker.publish(&ids).await {
            Ok(n) => {
                relay_metrics::published(n);
                tracing::debug!(published = n, "announced deliveries");
                Ok(n)
            }
            Err(e) => {
                // Put them straight back rather than leaving the sweep to find them.
                // The sweep's threshold is measured in tens of seconds and exists for
                // a crash; a Redis blip should cost one interval, not one threshold.
                let restored = self.store.unmark_queued(&ids).await.unwrap_or(0);
                tracing::warn!(
                    error = %e,
                    marked = ids.len(),
                    restored,
                    "could not announce a batch; marks rolled back"
                );
                Err(SendError::Broker(e.to_string()))
            }
        }
    }

    /// Put back anything announced long ago that never moved.
    ///
    /// This is what makes losing the broker survivable, and it is the proof of the
    /// claim that Postgres is the only record. Everything Redis holds can be
    /// reconstructed from the `deliveries` table, and this is the code that does the
    /// reconstructing — without it the outbox pattern is only half implemented.
    ///
    /// During normal operation it should find nothing at all.
    pub async fn sweep_once(&self) -> Result<u64, SendError> {
        let n = self
            .store
            .requeue_stale(self.config.stale_after, self.config.batch)
            .await?;
        if n > 0 {
            relay_metrics::requeued(n);
            // Warn, not info. Zero is the normal value, so any of this is a report
            // that messages are going missing between here and the consumers.
            tracing::warn!(
                requeued = n,
                stale_after = ?self.config.stale_after,
                "announced deliveries had not progressed; announcing them again"
            );
        }
        Ok(n)
    }

    /// Report what the broker is holding, for the dashboard.
    async fn report_lag(&self) {
        match self.broker.lag().await {
            Ok(lag) => relay_metrics::broker_lag(lag.unread, lag.unacked),
            // Logged and carried on. A publisher that stopped publishing because it
            // could not read a gauge would be a worse outage than a missing panel.
            Err(e) => tracing::debug!(error = %e, "could not read broker lag"),
        }
    }

    pub async fn run(&self, cancel: CancellationToken) {
        let mut since_sweep = Duration::ZERO;

        loop {
            let published = match self.publish_once().await {
                Ok(n) => n,
                Err(e) => {
                    tracing::error!(error = %e, "publisher pass failed");
                    0
                }
            };

            // A full batch means there is more behind it. Sleeping here would cap the
            // drain rate at one batch per interval no matter how far behind the
            // outbox is.
            let backlog_likely = published as i64 >= self.config.batch;
            let wait = if backlog_likely {
                Duration::ZERO
            } else {
                self.config.idle
            };

            if since_sweep >= self.config.sweep_every {
                since_sweep = Duration::ZERO;
                if let Err(e) = self.sweep_once().await {
                    tracing::error!(error = %e, "reconciliation sweep failed");
                }
                self.report_lag().await;
            }
            since_sweep += wait.max(Duration::from_millis(1));

            if wait.is_zero() {
                // Still a yield point, so cancellation is noticed while draining a
                // backlog rather than only between batches.
                if cancel.is_cancelled() {
                    break;
                }
                tokio::task::yield_now().await;
                continue;
            }

            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = cancel.cancelled() => break,
            }
        }

        tracing::info!("publisher stopped");
    }
}
