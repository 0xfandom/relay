//! The transactional outbox: announcing committed rows, and never anything else.
//!
//! The property under test is narrow and worth stating exactly. It is *not* "the
//! broker receives every delivery" — the broker is allowed to lose messages, and the
//! reconciliation sweep exists because it will. It is that **the broker never carries
//! a message for a row that does not exist**, and that anything the broker loses is
//! still recoverable from Postgres.
//!
//! That asymmetry is the whole design. Getting it backwards — trusting the broker and
//! treating Postgres as a cache — is the dual-write problem with extra steps.
//!
//! Requires Postgres and Redis: `docker compose up -d postgres redis`.

use std::{sync::Arc, time::Duration};

use relay_broker::{Broker, Config, RedisStreams};
use relay_dispatcher::{Publisher, PublisherConfig};
use relay_store::Store;
use sqlx::PgPool;
use uuid::Uuid;

async fn broker(name: &str) -> Arc<RedisStreams> {
    let config = Config {
        url: std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into()),
        stream: format!("test:outbox:{name}:{}", Uuid::new_v4()),
        group: "test-group".into(),
    };
    Arc::new(RedisStreams::connect(config).await.expect("redis"))
}

fn config() -> PublisherConfig {
    PublisherConfig {
        batch: 100,
        idle: Duration::from_millis(10),
        stale_after: Duration::from_secs(60),
        sweep_every: Duration::from_secs(30),
    }
}

/// An endpoint and `n` events fanned out to it.
async fn seed(store: &Store, n: usize) -> Vec<Uuid> {
    store
        .create_endpoint("https://example.com/hook", "whsec_outbox", &[])
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
async fn every_published_message_names_a_committed_row(pool: PgPool) {
    // The criterion the pattern exists for. The publisher reads rows that are already
    // committed, so there is no ordering in which the broker can carry a message for
    // a delivery that does not exist.
    let store = Store::from_pool(pool);
    let seeded = seed(&store, 5).await;
    let b = broker("committed").await;

    let publisher = Publisher::new(store.clone(), b.clone(), config());
    assert_eq!(publisher.publish_once().await.unwrap(), 5);

    let messages = b
        .consume("c1", 100, Duration::from_millis(50))
        .await
        .unwrap();
    assert_eq!(messages.len(), 5);
    for m in &messages {
        assert!(
            seeded.contains(&m.delivery_id),
            "the broker carried an id nobody created: {}",
            m.delivery_id
        );
        assert!(
            store.get_delivery(m.delivery_id).await.unwrap().is_some(),
            "and it names a row that is really there"
        );
    }
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_row_is_announced_once_rather_than_on_every_pass(pool: PgPool) {
    // Without the mark, the publisher re-reads the same due rows forever and the
    // broker fills with duplicates of work nobody has got to yet.
    let store = Store::from_pool(pool);
    seed(&store, 3).await;
    let publisher = Publisher::new(store.clone(), broker("once").await, config());

    assert_eq!(publisher.publish_once().await.unwrap(), 3);
    assert_eq!(publisher.publish_once().await.unwrap(), 0, "nothing new");
    assert_eq!(publisher.publish_once().await.unwrap(), 0);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_delivery_that_is_not_due_yet_is_not_announced(pool: PgPool) {
    // A retry scheduled for later is not work; announcing it would put a message in
    // the broker that every consumer picks up, fails to claim, and acknowledges.
    let store = Store::from_pool(pool);
    let ids = seed(&store, 1).await;
    sqlx::query("UPDATE deliveries SET next_attempt_at = now() + interval '1 hour' WHERE id = $1")
        .bind(ids[0])
        .execute(store.pool())
        .await
        .unwrap();

    let publisher = Publisher::new(store.clone(), broker("notdue").await, config());
    assert_eq!(publisher.publish_once().await.unwrap(), 0);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_retry_is_announced_again(pool: PgPool) {
    // THE bug this design is most likely to have. A row is announced once and marked;
    // it fails; it goes back to `pending` for another attempt. If the mark survived
    // that, the delivery would sit pending forever with no message anywhere and
    // nothing to notice — the retry would silently never happen.
    //
    // The mark is cleared by a database trigger rather than by each of the five code
    // paths that return a row to `pending`, so a path added later cannot forget.
    let store = Store::from_pool(pool);
    let ids = seed(&store, 1).await;
    let publisher = Publisher::new(store.clone(), broker("retry").await, config());

    assert_eq!(publisher.publish_once().await.unwrap(), 1);
    assert!(store.queued_at(ids[0]).await.unwrap().is_some(), "marked");

    // Take it and fail it, exactly as a worker would.
    assert!(store.claim(ids[0], "w1").await.unwrap());
    store
        .finish_attempt(
            ids[0],
            0,
            relay_store::AttemptResult::Retry {
                delay: Duration::ZERO,
            },
            Some(500),
            1,
            "retryable",
            Some("boom"),
            None,
            "w1",
        )
        .await
        .expect("finish");

    assert!(
        store.queued_at(ids[0]).await.unwrap().is_none(),
        "the mark was cleared when the row went back to pending"
    );
    assert_eq!(
        publisher.publish_once().await.unwrap(),
        1,
        "so the retry is announced again"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_deferral_is_announced_again(pool: PgPool) {
    // The same hazard by a different route. A deferral spends no attempt and returns
    // the row to `pending`, and it is a different query from the one above — which is
    // exactly why the mark is cleared by a trigger and not by either of them.
    let store = Store::from_pool(pool);
    let ids = seed(&store, 1).await;
    let publisher = Publisher::new(store.clone(), broker("defer").await, config());

    assert_eq!(publisher.publish_once().await.unwrap(), 1);
    assert!(store.claim(ids[0], "w1").await.unwrap());
    store
        .defer_delivery(ids[0], 0, Duration::ZERO, "rate limit", "w1")
        .await
        .expect("defer");

    assert!(store.queued_at(ids[0]).await.unwrap().is_none());
    assert_eq!(publisher.publish_once().await.unwrap(), 1);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_reaped_delivery_is_announced_again(pool: PgPool) {
    // And a third: a worker died holding it, and the reaper put it back. Three
    // separate queries, one invariant, enforced in one place.
    let store = Store::from_pool(pool);
    let ids = seed(&store, 1).await;
    let publisher = Publisher::new(store.clone(), broker("reap").await, config());

    assert_eq!(publisher.publish_once().await.unwrap(), 1);
    assert!(store.claim(ids[0], "doomed").await.unwrap());
    assert_eq!(store.reap_expired_leases(Duration::ZERO).await.unwrap(), 1);

    assert!(store.queued_at(ids[0]).await.unwrap().is_none());
    assert_eq!(publisher.publish_once().await.unwrap(), 1);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_batch_bounds_how_much_one_pass_takes(pool: PgPool) {
    // The bounded in-flight window. It is also the most that can be lost to a crash
    // between marking and publishing, which is what makes it worth being small.
    let store = Store::from_pool(pool);
    seed(&store, 10).await;
    let publisher = Publisher::new(
        store.clone(),
        broker("batch").await,
        PublisherConfig {
            batch: 4,
            ..config()
        },
    );

    assert_eq!(publisher.publish_once().await.unwrap(), 4);
    assert_eq!(publisher.publish_once().await.unwrap(), 4);
    assert_eq!(publisher.publish_once().await.unwrap(), 2);
    assert_eq!(publisher.publish_once().await.unwrap(), 0);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_failed_publish_puts_the_marks_straight_back(pool: PgPool) {
    // Redis is unreachable. The rows must not be left marked, or they wait for the
    // sweep's threshold — tens of seconds — to recover from what was a blip.
    let store = Store::from_pool(pool);
    seed(&store, 3).await;

    // A real unreachable Redis would fail at `connect`, before a publisher could be
    // built at all. What needs testing is the publish itself failing, so the broker
    // here is one that connects fine and refuses every call.
    let broker: Arc<dyn Broker> = Arc::new(AlwaysFails);

    let publisher = Publisher::new(store.clone(), broker, config());
    assert!(publisher.publish_once().await.is_err());
    assert_eq!(
        store.outbox_backlog().await.unwrap(),
        3,
        "all three are unannounced again, ready for the next pass"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_sweep_finds_nothing_during_normal_operation(pool: PgPool) {
    // An acceptance criterion in its own right. A sweep that republishes during
    // ordinary running would double the broker's traffic and make the metric that is
    // supposed to mean "messages are going missing" mean nothing.
    let store = Store::from_pool(pool);
    seed(&store, 5).await;
    let publisher = Publisher::new(store.clone(), broker("quiet").await, config());
    publisher.publish_once().await.unwrap();

    assert_eq!(publisher.sweep_once().await.unwrap(), 0);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_sweep_recovers_a_row_whose_message_vanished(pool: PgPool) {
    // The gap the chosen ordering deliberately accepts: marked, then the process died
    // before the message was published. Nothing else in the system would ever notice
    // — the row is pending, not due for anything, and no message names it.
    let store = Store::from_pool(pool);
    let ids = seed(&store, 1).await;
    let publisher = Publisher::new(store.clone(), broker("gap").await, config());

    // Mark without publishing, which is exactly what that crash leaves behind.
    assert_eq!(store.mark_queued(10).await.unwrap().len(), 1);
    assert_eq!(publisher.sweep_once().await.unwrap(), 0, "not stale yet");

    sqlx::query("UPDATE deliveries SET queued_at = now() - interval '1 hour' WHERE id = $1")
        .bind(ids[0])
        .execute(store.pool())
        .await
        .unwrap();

    assert_eq!(publisher.sweep_once().await.unwrap(), 1, "recovered");
    assert_eq!(
        publisher.publish_once().await.unwrap(),
        1,
        "and announced on the next pass"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn two_publishers_do_not_announce_the_same_row(pool: PgPool) {
    // `SKIP LOCKED`, same as the delivery claim. Two publishers is not a supported
    // deployment, but it is an accident somebody will have, and the cost of it should
    // be nothing rather than every delivery announced twice.
    let store = Store::from_pool(pool);
    seed(&store, 20).await;
    let a = Publisher::new(store.clone(), broker("two-a").await, config());
    let b = Publisher::new(store.clone(), broker("two-b").await, config());

    let (x, y) = tokio::join!(a.publish_once(), b.publish_once());
    assert_eq!(x.unwrap() + y.unwrap(), 20, "between them, each row once");
    assert_eq!(store.outbox_backlog().await.unwrap(), 0);
}

/// A broker that is always unreachable.
struct AlwaysFails;

#[async_trait::async_trait]
impl Broker for AlwaysFails {
    async fn publish(&self, _: &[Uuid]) -> Result<u64, relay_broker::Error> {
        Err(relay_broker::Error::Transport("unreachable".into()))
    }
    async fn consume(
        &self,
        _: &str,
        _: usize,
        _: Duration,
    ) -> Result<Vec<relay_broker::Received>, relay_broker::Error> {
        Err(relay_broker::Error::Transport("unreachable".into()))
    }
    async fn ack(&self, _: &[String]) -> Result<u64, relay_broker::Error> {
        Err(relay_broker::Error::Transport("unreachable".into()))
    }
    async fn reclaim(
        &self,
        _: &str,
        _: Duration,
        _: usize,
    ) -> Result<Vec<relay_broker::Received>, relay_broker::Error> {
        Err(relay_broker::Error::Transport("unreachable".into()))
    }
    async fn lag(&self) -> Result<relay_broker::Lag, relay_broker::Error> {
        Err(relay_broker::Error::Transport("unreachable".into()))
    }
    async fn ensure(&self) -> Result<(), relay_broker::Error> {
        Ok(())
    }
    async fn purge(&self) -> Result<(), relay_broker::Error> {
        Ok(())
    }
}
