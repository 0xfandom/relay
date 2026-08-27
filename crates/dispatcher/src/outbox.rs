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
    /// How many unread entries the broker may hold before the sweep stands down.
    ///
    /// Above this, the system is behind rather than broken, and re-announcing would
    /// make the backlog it is reacting to longer. Small but not zero: a healthy busy
    /// broker always has a few entries in flight, and a threshold of zero would
    /// disable recovery on any system doing work.
    pub sweep_below_unread: u64,
}

impl Default for PublisherConfig {
    fn default() -> Self {
        Self {
            batch: 256,
            idle: Duration::from_millis(100),
            stale_after: Duration::from_secs(60),
            sweep_every: Duration::from_secs(30),
            // One batch. Enough that ordinary in-flight work does not block recovery,
            // small enough that a real backlog does.
            sweep_below_unread: 256,
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

    /// Report what the broker is holding, and say whether it is backed up.
    ///
    /// `None` when the broker could not be asked. Logged and carried on rather than
    /// fatal — a publisher that stopped publishing because it could not read a gauge
    /// would be a worse outage than a missing panel.
    async fn report_lag(&self) -> Option<relay_broker::Lag> {
        match self.broker.lag().await {
            Ok(lag) => {
                relay_metrics::broker_lag(lag.unread, lag.unacked);
                Some(lag)
            }
            Err(e) => {
                tracing::debug!(error = %e, "could not read broker lag");
                None
            }
        }
    }

    /// Whether it is safe to sweep right now.
    ///
    /// The sweep cannot tell a message that was *lost* from one that is merely
    /// waiting behind a long backlog. Both look identical from Postgres: a row that
    /// is marked as announced and has not moved.
    ///
    /// Getting that wrong is not a harmless false positive, it is a feedback loop.
    /// Re-announcing a delivery that is already sitting in the stream appends another
    /// entry to the very backlog that made it look stalled, which makes the next
    /// sweep find more rows, which appends more entries. Measured on a chaos run:
    /// 30,000 deliveries produced 119,000 published messages and a stream of 70,000
    /// entries, and the consumers spent their time acknowledging duplicates of work
    /// that had already succeeded.
    ///
    /// So when the broker still holds entries no consumer has reached, the honest
    /// reading is "behind", not "broken", and the sweep stands down. Nothing is lost
    /// by waiting: the entries are there, and a consumer will get to them.
    /// Whether a sweep would run right now, asking the broker for the answer.
    ///
    /// Exposed so the rule can be tested without driving the whole loop and timing
    /// it, which is the kind of test that passes locally and fails on a shared runner.
    pub async fn would_sweep(&self) -> bool {
        let lag = self.broker.lag().await.ok();
        self.safe_to_sweep(lag)
    }

    fn safe_to_sweep(&self, lag: Option<relay_broker::Lag>) -> bool {
        match lag {
            // Unread entries mean the stream still has work nobody has looked at.
            // A row marked as announced is most likely one of them.
            Some(lag) => lag.unread <= self.config.sweep_below_unread,
            // Could not ask. Sweeping is the safer default: a broker that cannot
            // answer is more likely to have lost something than to be busy.
            None => true,
        }
    }

    pub async fn run(&self, cancel: CancellationToken) {
        let mut since_sweep = Duration::ZERO;
        // Set when a sweep comes back full, which means there is more behind it.
        //
        // This exists because of what a total broker loss looks like. Every row is
        // marked as announced and every message is gone, so the publisher — which
        // only reads *unannounced* rows — finds nothing at all, and the entire
        // recovery has to come through the sweep. One batch per interval then caps
        // recovery at `batch / sweep_every`: with the defaults, around eight
        // deliveries a second, so thirty thousand of them would take an hour while
        // the system looks idle. Measured, not guessed — a chaos run flushing Redis
        // mid-flight recovered at exactly that rate before this was here.
        let mut sweep_again = false;

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

            if sweep_again || since_sweep >= self.config.sweep_every {
                since_sweep = Duration::ZERO;
                let was_chasing = sweep_again;
                sweep_again = false;
                let lag = self.report_lag().await;
                if !self.safe_to_sweep(lag) {
                    tracing::debug!(
                        unread = lag.map(|l| l.unread),
                        "broker is behind; standing down rather than adding to its backlog"
                    );
                    // Not a failure and not a reason to keep chasing.
                    let _ = was_chasing;
                    continue;
                }
                match self.sweep_once().await {
                    // Straight round again rather than waiting for the interval.
                    // Alternating one sweep with one publish is what makes recovery
                    // run at the database's speed instead of a timer's: the sweep
                    // un-marks a batch and the publisher announces it on the very
                    // next pass.
                    Ok(n) if n as i64 >= self.config.batch => sweep_again = true,
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = %e, "reconciliation sweep failed"),
                }
            }

            let wait = if backlog_likely || sweep_again {
                Duration::ZERO
            } else {
                self.config.idle
            };
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
