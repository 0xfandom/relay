//! A transport for "delivery X is ready", and nothing more.
//!
//! # What the broker is not
//!
//! It is not the record. Postgres is. Every message here is a `Uuid` naming a row
//! that is *already committed*, and everything the broker holds can be rebuilt by
//! reading the `deliveries` table. That is a deliberate constraint, and two things
//! fall out of it:
//!
//! **No customer data crosses it.** The payload, the headers and the signing secret
//! stay in Postgres. Losing Redis, or someone reading it, exposes a list of row ids.
//!
//! **Losing it entirely is survivable.** A reconciliation sweep can republish
//! anything the broker dropped, because the broker was never the only copy.
//!
//! # Why a message is not ownership
//!
//! Receiving a message means "this delivery is worth trying", never "you exclusively
//! own it". Redis redelivers, and reclaim deliberately hands the same message to a
//! second consumer when the first goes quiet. So the thing that actually prevents two
//! sends is the database lease that was already there — a consumer claims the row
//! before it sends, and a claim that loses simply acknowledges the message and moves
//! on.
//!
//! Building it the other way round — trusting the broker's delivery guarantee and
//! dropping the lease — is the mistake this design exists to avoid. Redis Streams
//! offers at-least-once, and at-least-once plus "send immediately" is at-least-twice.
//!
//! # Why Redis Streams and not Kafka
//!
//! Consumer groups, acknowledgements and idle-message reclaim, in one container that
//! starts in a second. Kafka's log semantics — replay from an offset, many
//! independent consumer groups over one retained history — buy nothing until several
//! separate systems want the same stream, and the operational footprint is
//! disproportionate long before then. The trait below is what keeps that reversible.

use std::time::Duration;

use async_trait::async_trait;
use uuid::Uuid;

pub mod redis_streams;

pub use redis_streams::RedisStreams;

/// The field a message's delivery id is stored under.
///
/// One character because it is repeated in every entry Redis holds, and a stream of
/// several million entries is somewhere this actually shows up.
pub const FIELD: &str = "d";

/// A message taken from the broker but not yet acknowledged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Received {
    /// The broker's own id for this message, needed to acknowledge it.
    ///
    /// Deliberately a `String` rather than anything structured. It is Redis's
    /// `<millis>-<seq>` today, and the whole point of the trait is that the next
    /// implementation gets to choose its own.
    pub receipt: String,
    /// The delivery this message is about.
    pub delivery_id: Uuid,
}

/// How far behind the broker is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Lag {
    /// Entries in the stream that no consumer group member has read yet.
    pub unread: u64,
    /// Read but not yet acknowledged. A number that climbs and does not come back
    /// down means consumers are taking work and dying with it.
    pub unacked: u64,
}

#[async_trait]
pub trait Broker: Send + Sync {
    /// Announce that these deliveries are ready.
    ///
    /// Takes a slice rather than one id because the publisher reads rows in batches
    /// and a round trip per row is most of the cost at any real rate.
    async fn publish(&self, delivery_ids: &[Uuid]) -> Result<u64, Error>;

    /// Take up to `max` messages for this consumer, waiting up to `block` for one.
    ///
    /// Blocking in the broker rather than sleeping in the caller: a poll interval is
    /// a choice between wasted queries and added latency, and a blocking read has
    /// neither.
    async fn consume(
        &self,
        consumer: &str,
        max: usize,
        block: Duration,
    ) -> Result<Vec<Received>, Error>;

    /// Acknowledge messages, so they stop being redelivered.
    async fn ack(&self, receipts: &[String]) -> Result<u64, Error>;

    /// Take over messages that some consumer read and never acknowledged.
    ///
    /// This is what makes a dead consumer survivable within the broker. It is not
    /// what makes a dead consumer *safe* — that is still the database lease, because
    /// reclaim cannot tell "the consumer died" from "the consumer is slow", and hands
    /// the message over either way.
    async fn reclaim(
        &self,
        consumer: &str,
        idle: Duration,
        max: usize,
    ) -> Result<Vec<Received>, Error>;

    /// Read the backlog, for metrics.
    async fn lag(&self) -> Result<Lag, Error>;

    /// Ensure the stream and consumer group exist. Safe to call repeatedly.
    async fn ensure(&self) -> Result<(), Error>;

    /// Forget everything. Only for tests that prove losing the broker loses nothing.
    async fn purge(&self) -> Result<(), Error>;
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("broker transport: {0}")]
    Transport(String),
    /// A message whose payload could not be read as a delivery id.
    ///
    /// Its own variant because the response is different: a transport error is worth
    /// retrying and this one never will be, so it has to be acknowledged and dropped
    /// or it is redelivered forever.
    #[error("unreadable message {receipt}: {reason}")]
    Unreadable { receipt: String, reason: String },
}

/// Where the broker lives, and how it is addressed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub url: String,
    /// The stream key.
    pub stream: String,
    /// The consumer group. Every dispatcher joins the same one, which is what makes
    /// them split the work instead of each receiving everything.
    pub group: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379".into(),
            stream: "relay:deliveries".into(),
            group: "relay-dispatchers".into(),
        }
    }
}

impl Config {
    /// Read from the environment, or `None` when no broker is configured.
    ///
    /// `None` is the ordinary case and must stay that way: a single node polling
    /// Postgres is a complete, working Relay, and the broker is an option for when
    /// one node is not enough. A deployment that silently required Redis to start
    /// would have made the smallest deployment strictly worse.
    pub fn from_env() -> Option<Self> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Option<Self> {
        let url = get("RELAY_BROKER_URL")?;
        if url.trim().is_empty() {
            return None;
        }
        let d = Self::default();
        Some(Self {
            url,
            stream: get("RELAY_BROKER_STREAM").unwrap_or(d.stream),
            group: get("RELAY_BROKER_GROUP").unwrap_or(d.group),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_url_means_no_broker() {
        assert!(Config::from_lookup(|_| None).is_none());
    }

    #[test]
    fn an_empty_url_means_no_broker_rather_than_a_broker_at_nowhere() {
        // `RELAY_BROKER_URL=` in a compose file is how somebody turns the broker off
        // without deleting the line. Reading that as "connect to the empty string"
        // would fail at startup with a confusing error.
        let c = Config::from_lookup(|k| (k == "RELAY_BROKER_URL").then(|| "  ".to_string()));
        assert!(c.is_none());
    }

    #[test]
    fn a_url_is_enough_and_the_rest_have_defaults() {
        let c = Config::from_lookup(|k| {
            (k == "RELAY_BROKER_URL").then(|| "redis://elsewhere:6379".to_string())
        })
        .expect("configured");
        assert_eq!(c.url, "redis://elsewhere:6379");
        assert_eq!(c.stream, Config::default().stream);
        assert_eq!(c.group, Config::default().group);
    }

    #[test]
    fn the_group_can_be_overridden_to_run_two_independent_fleets() {
        let c = Config::from_lookup(|k| match k {
            "RELAY_BROKER_URL" => Some("redis://x".into()),
            "RELAY_BROKER_GROUP" => Some("canary".into()),
            _ => None,
        })
        .expect("configured");
        assert_eq!(c.group, "canary");
    }
}
