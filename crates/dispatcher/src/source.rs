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

use async_trait::async_trait;

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
