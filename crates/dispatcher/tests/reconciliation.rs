//! Losing the broker, and losing nothing.
//!
//! This is the file that decides whether the outbox is actually implemented or only
//! half implemented. Everything else in M2b assumes Postgres is the record and Redis
//! is a transport; these tests are the proof, because they take the transport away
//! and check that no delivery goes missing.
//!
//! Three ways to lose a message, in increasing severity:
//!
//! 1. A crash between marking a row announced and publishing it.
//! 2. A message that vanished from the stream — trimmed, evicted, or lost.
//! 3. The entire broker wiped: stream, consumer group, pending list, everything.
//!
//! All three look identical from Postgres: a row that is `pending`, marked as
//! announced, and not moving. One sweep covers all three, which is what makes the
//! claim "the broker is never the record" something more than a slogan.
//!
//! Requires Postgres and Redis: `docker compose up -d postgres redis`.

use std::{sync::Arc, time::Duration};

use relay_broker::{Broker, Config, RedisStreams};
use relay_dispatcher::{BrokerSource, ConsumerConfig, Publisher, PublisherConfig, Source};
use relay_store::Store;
use sqlx::PgPool;
use uuid::Uuid;

fn broker_config(name: &str) -> Config {
    Config {
        url: std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into()),
        stream: format!("test:recon:{name}:{}", Uuid::new_v4()),
        group: "test-group".into(),
    }
}

async fn broker(config: Config) -> Arc<RedisStreams> {
    Arc::new(RedisStreams::connect(config).await.expect("redis"))
}

/// A staleness threshold of zero, so "long enough that the message is gone" needs no
/// sleeping to express.
fn eager() -> PublisherConfig {
    PublisherConfig {
        batch: 500,
        idle: Duration::from_millis(10),
        stale_after: Duration::ZERO,
        sweep_every: Duration::from_secs(30),
        sweep_below_unread: 256,
    }
}

fn patient() -> PublisherConfig {
    PublisherConfig {
        stale_after: Duration::from_secs(3600),
        ..eager()
    }
}

fn consumer_config() -> ConsumerConfig {
    ConsumerConfig {
        block: Duration::from_millis(50),
        reclaim_idle: Duration::ZERO,
        reclaim_every: Duration::from_secs(3600),
    }
}

async fn seed(store: &Store, n: usize) -> Vec<Uuid> {
    store
        .create_endpoint("https://example.com/hook", "whsec_recon", &[])
        .await
        .expect("endpoint");
    let mut ids = Vec::new();
    for _ in 0..n {
        let accepted = store
            .insert_event_and_fan_out("thing.happened", br#"{"a":1}"#)
            .await
            .expect("event");
        ids.extend(accepted.delivery_ids);
    }
    ids
}

#[sqlx::test(migrations = "../store/migrations")]
async fn deleting_the_entire_broker_state_loses_nothing(pool: PgPool) {
    // The headline claim. Everything Redis holds is thrown away mid-run — stream,
    // consumer group, pending list — and every delivery still arrives.
    //
    // Note what is *not* asserted: that the messages survive. They do not. What
    // survives is the deliveries, because they were never in Redis to begin with.
    let store = Store::from_pool(pool);
    let seeded = seed(&store, 25).await;
    let cfg = broker_config("wipe");
    let b = broker(cfg.clone()).await;
    let publisher = Publisher::new(store.clone(), b.clone(), eager());

    assert_eq!(publisher.publish_once().await.unwrap(), 25);

    // Everything the broker knows, gone.
    b.purge().await.expect("purge");
    assert_eq!(b.lag().await.unwrap().unread, 0, "the stream is empty");

    // Postgres still believes all 25 were announced, so without the sweep they would
    // sit pending forever with nothing to deliver them and nothing to report it.
    assert_eq!(store.outbox_backlog().await.unwrap(), 0, "all still marked");

    assert_eq!(
        publisher.sweep_once().await.unwrap(),
        25,
        "the sweep found them"
    );
    assert_eq!(
        publisher.publish_once().await.unwrap(),
        25,
        "and re-announced them"
    );

    let source = BrokerSource::new(store.clone(), b, "c1", consumer_config());
    let claimed = source.claim(100, "worker-1").await.unwrap();
    assert_eq!(claimed.len(), 25, "every delivery came back");

    let recovered: Vec<Uuid> = claimed.iter().map(|c| c.pending.delivery_id).collect();
    for id in &seeded {
        assert!(recovered.contains(id), "delivery {id} was lost");
    }
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_consumer_survives_the_group_being_deleted_underneath_it(pool: PgPool) {
    // The failure mode a wipe actually produces. Deleting the stream deletes the
    // consumer group with it, and Redis answers the next read with `NOGROUP` rather
    // than an empty list. A consumer that treated that as an ordinary error would
    // log and retry forever against a group that will never come back on its own,
    // and the fleet would go silent while Postgres looked perfectly healthy.
    let store = Store::from_pool(pool);
    seed(&store, 3).await;
    let cfg = broker_config("nogroup");
    let b = broker(cfg.clone()).await;
    let publisher = Publisher::new(store.clone(), b.clone(), eager());
    publisher.publish_once().await.unwrap();

    // Delete the stream out from under the consumer, taking the group with it.
    let mut conn = redis::Client::open(cfg.url.clone())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: i64 = redis::cmd("DEL")
        .arg(&cfg.stream)
        .query_async(&mut conn)
        .await
        .unwrap();

    let source = BrokerSource::new(store.clone(), b.clone(), "c1", consumer_config());
    // Must not error. Nothing to deliver, because the messages went with the stream —
    // but the consumer has to still be working afterwards.
    let first = source
        .claim(10, "worker-1")
        .await
        .expect("NOGROUP is recovered from");
    assert!(first.is_empty());

    // And the recovered group works: the sweep re-announces, the consumer reads.
    assert_eq!(publisher.sweep_once().await.unwrap(), 3);
    assert_eq!(publisher.publish_once().await.unwrap(), 3);
    assert_eq!(source.claim(10, "worker-1").await.unwrap().len(), 3);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_single_dropped_message_comes_back(pool: PgPool) {
    // Less dramatic than a wipe and far more likely: one entry trimmed or evicted
    // while the rest of the stream is fine. Nothing about the row says anything is
    // wrong — it is pending, it is marked, and it is simply never going to move.
    let store = Store::from_pool(pool);
    let ids = seed(&store, 3).await;
    let cfg = broker_config("dropped");
    let b = broker(cfg.clone()).await;
    let publisher = Publisher::new(store.clone(), b.clone(), eager());
    publisher.publish_once().await.unwrap();

    // Remove exactly one entry, leaving the others alone.
    let mut conn = redis::Client::open(cfg.url.clone())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let entries: Vec<(String, Vec<String>)> = redis::cmd("XRANGE")
        .arg(&cfg.stream)
        .arg("-")
        .arg("+")
        .query_async(&mut conn)
        .await
        .unwrap();
    let victim = entries.first().expect("at least one entry").0.clone();
    let _: i64 = redis::cmd("XDEL")
        .arg(&cfg.stream)
        .arg(&victim)
        .query_async(&mut conn)
        .await
        .unwrap();

    let source = BrokerSource::new(store.clone(), b.clone(), "c1", consumer_config());
    assert_eq!(
        source.claim(10, "worker-1").await.unwrap().len(),
        2,
        "one of the three is gone"
    );

    // The sweep notices the one that never moved, and only that one.
    assert_eq!(publisher.sweep_once().await.unwrap(), 1);
    assert_eq!(publisher.publish_once().await.unwrap(), 1);

    let recovered = source.claim(10, "worker-1").await.unwrap();
    assert_eq!(recovered.len(), 1);
    assert!(ids.contains(&recovered[0].pending.delivery_id));
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_sweep_leaves_alone_a_delivery_a_consumer_is_working_on(pool: PgPool) {
    // The false positive that would make the sweep harmful. A delivery being worked
    // on right now is `inflight`, not `pending`, so it is outside the sweep's
    // predicate entirely — but this is worth pinning down, because widening that
    // predicate later would double every in-flight send.
    let store = Store::from_pool(pool);
    seed(&store, 1).await;
    let b = broker(broker_config("inflight")).await;
    let publisher = Publisher::new(store.clone(), b.clone(), eager());
    publisher.publish_once().await.unwrap();

    let source = BrokerSource::new(store.clone(), b, "c1", consumer_config());
    assert_eq!(source.claim(10, "worker-1").await.unwrap().len(), 1);

    assert_eq!(
        publisher.sweep_once().await.unwrap(),
        0,
        "a delivery in flight is not a delivery that went missing"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_sweep_waits_for_the_staleness_threshold(pool: PgPool) {
    // The threshold is what separates "the message is gone" from "the message was
    // announced a moment ago and nobody has got to it yet". Without it the sweep
    // re-announces everything on every pass, and the broker carries each delivery
    // many times over.
    let store = Store::from_pool(pool);
    seed(&store, 5).await;
    let b = broker(broker_config("threshold")).await;
    let publisher = Publisher::new(store.clone(), b.clone(), patient());
    publisher.publish_once().await.unwrap();
    b.purge().await.unwrap();

    assert_eq!(
        publisher.sweep_once().await.unwrap(),
        0,
        "gone, but not yet gone long enough to be sure"
    );

    // Once it has been long enough, the same five are recovered.
    sqlx::query("UPDATE deliveries SET queued_at = now() - interval '2 hours'")
        .execute(store.pool())
        .await
        .unwrap();
    assert_eq!(publisher.sweep_once().await.unwrap(), 5);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_sweep_does_not_resurrect_a_finished_delivery(pool: PgPool) {
    // A delivery that succeeded is not pending, so it is outside the predicate. If it
    // were not, the sweep would re-announce completed work and the endpoint would
    // receive the webhook a second time — turning the safety net into the exact
    // problem it exists to prevent.
    let store = Store::from_pool(pool);
    seed(&store, 2).await;
    let b = broker(broker_config("finished")).await;
    let publisher = Publisher::new(store.clone(), b.clone(), eager());
    publisher.publish_once().await.unwrap();

    sqlx::query("UPDATE deliveries SET status = 'succeeded'")
        .execute(store.pool())
        .await
        .unwrap();
    b.purge().await.unwrap();

    assert_eq!(publisher.sweep_once().await.unwrap(), 0);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_dead_delivery_is_not_swept_back_into_the_queue(pool: PgPool) {
    // Same reasoning, other terminal state. A dead letter is replayed deliberately
    // through the DLQ, never by a background sweep noticing it has not moved.
    let store = Store::from_pool(pool);
    seed(&store, 2).await;
    let b = broker(broker_config("dead")).await;
    let publisher = Publisher::new(store.clone(), b.clone(), eager());
    publisher.publish_once().await.unwrap();

    sqlx::query("UPDATE deliveries SET status = 'dead', dead_reason = 'permanent_failure'")
        .execute(store.pool())
        .await
        .unwrap();
    b.purge().await.unwrap();

    assert_eq!(publisher.sweep_once().await.unwrap(), 0);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_sweep_stands_down_while_the_broker_is_merely_behind(pool: PgPool) {
    // The feedback loop, pinned.
    //
    // A row that is marked as announced and has not moved looks the same whether its
    // message was lost or is simply queued behind a long backlog. Sweeping the second
    // case appends another entry to the very backlog that made it look stalled, which
    // makes the next sweep find more rows, which appends more entries.
    //
    // Measured before this guard existed: a chaos run of 30,000 deliveries produced
    // 119,000 published messages and a stream of 70,000 entries, and the consumers
    // spent their time acknowledging duplicates of work that had already succeeded.
    //
    // So the rule is: unread entries mean "behind", not "broken", and the sweep waits.
    let store = Store::from_pool(pool);
    seed(&store, 10).await;
    let b = broker(broker_config("backed-up")).await;

    // Everything announced and nothing consumed: exactly a consumer that is behind.
    let publisher = Publisher::new(
        store.clone(),
        b.clone(),
        PublisherConfig {
            // One entry of tolerance, so ten unread is unambiguously a backlog.
            sweep_below_unread: 1,
            ..eager()
        },
    );
    assert_eq!(publisher.publish_once().await.unwrap(), 10);
    assert_eq!(b.lag().await.unwrap().unread, 10);

    // The rows are stale by the eager threshold, so only the backlog check stops it.
    assert!(
        !publisher.would_sweep().await,
        "ten unread entries is a consumer that is behind, not ten lost messages"
    );

    // Drain the broker, and the same publisher is willing again.
    let source = BrokerSource::new(store.clone(), b.clone(), "c1", consumer_config());
    let claimed = source.claim(100, "worker-1").await.unwrap();
    assert_eq!(claimed.len(), 10);
    for c in &claimed {
        source.settle(&c.receipt).await.unwrap();
    }
    assert_eq!(b.lag().await.unwrap().unread, 0);
    assert!(
        publisher.would_sweep().await,
        "with the backlog gone, a stalled row is a lost message again"
    );
}
