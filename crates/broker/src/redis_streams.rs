//! [`Broker`] over Redis Streams.
//!
//! The whole implementation is four commands. `XADD` publishes, `XREADGROUP` takes
//! work for one consumer in a group, `XACK` finishes it, and `XAUTOCLAIM` takes over
//! what a dead consumer left behind. Everything else here is turning Redis's replies
//! into something the delivery path can use without knowing any of that.

use std::time::Duration;

use async_trait::async_trait;
use redis::{
    AsyncCommands,
    aio::ConnectionManager,
    streams::{StreamAutoClaimOptions, StreamAutoClaimReply, StreamReadOptions, StreamReadReply},
};
use uuid::Uuid;

use crate::{Broker, Config, Error, FIELD, Lag, Received};

pub struct RedisStreams {
    conn: ConnectionManager,
    config: Config,
}

impl RedisStreams {
    /// Connect and make sure the stream and group exist.
    pub async fn connect(config: Config) -> Result<Self, Error> {
        let client = redis::Client::open(config.url.as_str()).map_err(transport)?;
        // The manager, not a bare connection: it reconnects on its own, so a Redis
        // restart is a pause rather than a dispatcher that has to be restarted too.
        let conn = ConnectionManager::new(client).await.map_err(transport)?;
        let broker = Self { conn, config };
        broker.ensure().await?;
        Ok(broker)
    }

    /// The stream key this broker publishes to. For tests that need to write a raw
    /// entry Redis would accept but this code should refuse.
    pub fn stream(&self) -> &str {
        &self.config.stream
    }

    fn conn(&self) -> ConnectionManager {
        self.conn.clone()
    }

    /// Turn Redis entries into messages, dropping any that cannot be read.
    ///
    /// An unreadable entry is acknowledged rather than returned, because there is
    /// nothing a caller could do with it: the id is the entire content, and an entry
    /// whose id will not parse can never become a delivery. Leaving it unacknowledged
    /// would make it reappear on every reclaim, forever.
    async fn interpret(&self, ids: Vec<redis::streams::StreamId>) -> Result<Vec<Received>, Error> {
        let mut out = Vec::with_capacity(ids.len());
        let mut poison = Vec::new();
        for id in ids {
            match id.get::<String>(FIELD).map(|v| Uuid::parse_str(&v)) {
                Some(Ok(delivery_id)) => out.push(Received {
                    receipt: id.id,
                    delivery_id,
                }),
                other => {
                    tracing_warn(&id.id, other.is_none());
                    poison.push(id.id);
                }
            }
        }
        if !poison.is_empty() {
            // Best effort. If this fails the entries are merely reclaimed again later,
            // which is noisy rather than harmful.
            let _ = self.ack(&poison).await;
        }
        Ok(out)
    }
}

fn tracing_warn(receipt: &str, missing_field: bool) {
    if missing_field {
        tracing::warn!(%receipt, field = FIELD, "stream entry has no delivery id; acknowledged and dropped");
    } else {
        tracing::warn!(%receipt, "stream entry's delivery id will not parse; acknowledged and dropped");
    }
}

fn transport(e: redis::RedisError) -> Error {
    Error::Transport(e.to_string())
}

/// Whether Redis is telling us the consumer group is gone.
///
/// It goes when the stream goes: deleting the key deletes the group with it, and
/// every subsequent read fails rather than returning an empty list. A consumer that
/// treated this as an ordinary transport error would retry forever against a group
/// that will never come back on its own — the whole fleet silently stops delivering
/// while Postgres looks perfectly healthy, which is the worst shape a failure can
/// take here.
fn is_missing_group(e: &redis::RedisError) -> bool {
    e.code() == Some("NOGROUP")
}

#[async_trait]
impl Broker for RedisStreams {
    async fn ensure(&self) -> Result<(), Error> {
        let mut conn = self.conn();
        // `MKSTREAM` so the group can be created before anything has been published.
        // Without it the first dispatcher to start fails, and which one that is
        // depends on compose's ordering.
        let created: Result<String, _> = conn
            .xgroup_create_mkstream(&self.config.stream, &self.config.group, "$")
            .await;
        match created {
            Ok(_) => Ok(()),
            // Every process calls this at startup, so all but the first find the group
            // already there. That is the expected path, not an error.
            Err(e) if e.code() == Some("BUSYGROUP") => Ok(()),
            Err(e) => Err(transport(e)),
        }
    }

    async fn publish(&self, delivery_ids: &[Uuid]) -> Result<u64, Error> {
        if delivery_ids.is_empty() {
            return Ok(0);
        }
        let mut conn = self.conn();
        // One pipeline rather than one round trip each. At the rates the load test
        // measured, the round trip is the cost.
        let mut pipe = redis::pipe();
        for id in delivery_ids {
            pipe.xadd(
                &self.config.stream,
                "*",
                &[(FIELD, id.to_string().as_str())],
            )
            .ignore();
        }
        pipe.query_async::<()>(&mut conn).await.map_err(transport)?;
        Ok(delivery_ids.len() as u64)
    }

    async fn consume(
        &self,
        consumer: &str,
        max: usize,
        block: Duration,
    ) -> Result<Vec<Received>, Error> {
        let mut conn = self.conn();
        let opts = StreamReadOptions::default()
            .group(&self.config.group, consumer)
            .count(max)
            .block(block.as_millis() as usize);
        // `>` means "entries never delivered to any consumer in this group". Reading
        // from `0` instead would return this consumer's own unacknowledged backlog,
        // which is what `reclaim` is for and would otherwise be re-sent here on every
        // single call.
        let read: Result<Option<StreamReadReply>, _> = conn
            .xread_options(&[&self.config.stream], &[">"], &opts)
            .await;

        let reply = match read {
            Ok(reply) => reply,
            Err(e) if is_missing_group(&e) => {
                // Recreate it and return empty rather than retrying the read here.
                // Anything the group would have carried went with it, and the
                // reconciliation sweep is what brings those deliveries back — it
                // reads Postgres, which still has all of them. Retrying the read
                // would find nothing and only delay noticing.
                tracing::warn!(
                    stream = %self.config.stream,
                    group = %self.config.group,
                    "consumer group had gone; recreating it"
                );
                self.ensure().await?;
                return Ok(Vec::new());
            }
            Err(e) => return Err(transport(e)),
        };

        let Some(reply) = reply else {
            // Nil, which is how Redis says the block expired with nothing to give.
            return Ok(Vec::new());
        };
        let ids = reply.keys.into_iter().flat_map(|k| k.ids).collect();
        self.interpret(ids).await
    }

    async fn ack(&self, receipts: &[String]) -> Result<u64, Error> {
        if receipts.is_empty() {
            return Ok(0);
        }
        let mut conn = self.conn();
        let acked: u64 = conn
            .xack(&self.config.stream, &self.config.group, receipts)
            .await
            .map_err(transport)?;
        Ok(acked)
    }

    async fn reclaim(
        &self,
        consumer: &str,
        idle: Duration,
        max: usize,
    ) -> Result<Vec<Received>, Error> {
        let mut conn = self.conn();
        let claimed: Result<StreamAutoClaimReply, _> = conn
            .xautoclaim_options(
                &self.config.stream,
                &self.config.group,
                consumer,
                idle.as_millis() as usize,
                // From the beginning of the pending list every time. The cursor exists
                // to page through a large backlog; starting over is correct here
                // because anything still pending on the next call is still the oldest
                // thing that needs attention.
                "0-0",
                StreamAutoClaimOptions::default().count(max),
            )
            .await;

        let reply = match claimed {
            Ok(reply) => reply,
            // Same as a read: the pending list went with the group, and Postgres is
            // where those deliveries are recovered from.
            Err(e) if is_missing_group(&e) => {
                self.ensure().await?;
                return Ok(Vec::new());
            }
            Err(e) => return Err(transport(e)),
        };
        self.interpret(reply.claimed).await
    }

    async fn lag(&self) -> Result<Lag, Error> {
        let mut conn = self.conn();
        // A missing stream or group is reported as no backlog rather than as an
        // error. It is a true statement — there is nothing waiting — and a metrics
        // read should never be the thing that takes a process down.
        if self.ensure().await.is_err() {
            return Ok(Lag::default());
        }
        // `XPENDING` in its summary form: one number, no scan of the pending list.
        let unacked: u64 = redis::cmd("XPENDING")
            .arg(&self.config.stream)
            .arg(&self.config.group)
            .query_async::<redis::Value>(&mut conn)
            .await
            .map_err(transport)
            .map(|v| match v {
                redis::Value::Array(items) => items
                    .first()
                    .and_then(|n| redis::from_redis_value_ref::<u64>(n).ok())
                    .unwrap_or(0),
                _ => 0,
            })?;

        // Entries added but not yet handed to any consumer. Redis reports this per
        // group as `lag`, and it can be nil after certain trims — nil is reported as
        // zero rather than as an error, because a missing backlog number should not
        // take out the metrics endpoint.
        let unread: u64 = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(&self.config.stream)
            .query_async::<redis::Value>(&mut conn)
            .await
            .map_err(transport)
            .map(|v| group_lag(&v, &self.config.group))?;

        Ok(Lag { unread, unacked })
    }

    async fn purge(&self) -> Result<(), Error> {
        let mut conn = self.conn();
        let _: Result<i64, _> = conn.del(&self.config.stream).await;
        self.ensure().await
    }
}

/// Pull one group's `lag` out of an `XINFO GROUPS` reply.
///
/// Written by hand because the reply is a list of maps whose keys differ between
/// Redis versions, and a typed deserialisation would fail on the version that adds a
/// field rather than ignoring it.
fn group_lag(value: &redis::Value, group: &str) -> u64 {
    let redis::Value::Array(groups) = value else {
        return 0;
    };
    for g in groups {
        let fields = match g {
            redis::Value::Array(f) => f.clone(),
            redis::Value::Map(m) => m.iter().flat_map(|(k, v)| [k.clone(), v.clone()]).collect(),
            _ => continue,
        };
        let mut name = None;
        let mut lag = 0u64;
        for pair in fields.chunks(2) {
            let [k, v] = pair else { continue };
            let Ok(key) = redis::from_redis_value_ref::<String>(k) else {
                continue;
            };
            match key.as_str() {
                "name" => name = redis::from_redis_value_ref::<String>(v).ok(),
                "lag" => lag = redis::from_redis_value_ref::<u64>(v).unwrap_or(0),
                _ => {}
            }
        }
        if name.as_deref() == Some(group) {
            return lag;
        }
    }
    0
}
