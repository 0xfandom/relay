//! Consuming from the broker, and the lease that makes it safe.
//!
//! The claim these tests exist to check is narrow: **a broker message says which
//! delivery is ready, never that this consumer exclusively owns it.** Redis delivers
//! at-least-once, and reclaim deliberately hands the same message to a second
//! consumer, so a design that trusted the message alone would send some webhooks
//! twice. The database lease from M2 is what actually prevents that, and it is still
//! there, unchanged, on both paths.
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
        stream: format!("test:consumer:{name}:{}", Uuid::new_v4()),
        group: "test-group".into(),
    }
}

async fn broker(config: Config) -> Arc<RedisStreams> {
    Arc::new(RedisStreams::connect(config).await.expect("redis"))
}

fn consumer_config() -> ConsumerConfig {
    ConsumerConfig {
        block: Duration::from_millis(50),
        // Zero, so "a consumer that is not coming back" needs no sleeping to express.
        reclaim_idle: Duration::ZERO,
        reclaim_every: Duration::from_secs(3600),
    }
}

fn publisher_config() -> PublisherConfig {
    PublisherConfig {
        batch: 500,
        idle: Duration::from_millis(10),
        stale_after: Duration::from_secs(60),
        sweep_every: Duration::from_secs(30),
        sweep_below_unread: 256,
    }
}

async fn seed(store: &Store, n: usize) -> Vec<Uuid> {
    store
        .create_endpoint("https://example.com/hook", "whsec_consumer", &[])
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
async fn a_published_delivery_comes_back_ready_to_send(pool: PgPool) {
    let store = Store::from_pool(pool);
    let ids = seed(&store, 1).await;
    let b = broker(broker_config("basic")).await;
    Publisher::new(store.clone(), b.clone(), publisher_config())
        .publish_once()
        .await
        .unwrap();

    let source = BrokerSource::new(store.clone(), b, "c1", consumer_config());
    let claimed = source.claim(10, "worker-1").await.unwrap();

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].pending.delivery_id, ids[0]);
    assert!(
        claimed[0].receipt.token().is_some(),
        "and carries the message to acknowledge"
    );
    assert_eq!(
        store.get_delivery(ids[0]).await.unwrap().unwrap().status,
        "inflight",
        "the lease was taken, not just the message"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_message_delivered_twice_still_produces_one_send(pool: PgPool) {
    // The acceptance criterion the whole design turns on. Redis is at-least-once; the
    // second claim finds the row already `inflight` and yields nothing, so the
    // duplicate message costs one query rather than one webhook.
    let store = Store::from_pool(pool);
    let ids = seed(&store, 1).await;
    let cfg = broker_config("dupe");
    let b = broker(cfg.clone()).await;
    Publisher::new(store.clone(), b.clone(), publisher_config())
        .publish_once()
        .await
        .unwrap();

    let source = BrokerSource::new(store.clone(), b.clone(), "c1", consumer_config());
    assert_eq!(source.claim(10, "worker-1").await.unwrap().len(), 1);

    // The same delivery announced again, exactly as a redelivery would look.
    b.publish(&ids).await.unwrap();
    let second = BrokerSource::new(store.clone(), b, "c2", consumer_config());
    assert!(
        second.claim(10, "worker-2").await.unwrap().is_empty(),
        "the lease refused the duplicate"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn two_consumers_split_the_work_without_duplicating_a_send(pool: PgPool) {
    // Adding consumers adds throughput. If they duplicated instead of splitting,
    // every webhook would go out twice — so the test asserts both halves: all the
    // work is taken, and no delivery is taken by both.
    let store = Store::from_pool(pool);
    let ids = seed(&store, 20).await;
    let cfg = broker_config("split");
    let b = broker(cfg.clone()).await;
    Publisher::new(store.clone(), b.clone(), publisher_config())
        .publish_once()
        .await
        .unwrap();

    let a = BrokerSource::new(
        store.clone(),
        broker(cfg.clone()).await,
        "c-a",
        consumer_config(),
    );
    let z = BrokerSource::new(store.clone(), broker(cfg).await, "c-b", consumer_config());

    let first = a.claim(10, "worker-a").await.unwrap();
    let second = z.claim(10, "worker-b").await.unwrap();

    assert_eq!(first.len() + second.len(), 20, "all of it was taken");
    let taken: Vec<Uuid> = first
        .iter()
        .chain(second.iter())
        .map(|c| c.pending.delivery_id)
        .collect();
    let mut unique = taken.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), taken.len(), "and nothing was taken twice");
    for id in &ids {
        assert!(taken.contains(id));
    }
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_message_for_a_delivery_that_is_no_longer_due_is_dropped(pool: PgPool) {
    // A message that sat in the broker while its delivery was rescheduled. Claiming
    // it would send early — before a backoff had elapsed, which is the whole point of
    // the backoff. The single-row claim carries the same `next_attempt_at` predicate
    // the batch claim does, which is what refuses it.
    let store = Store::from_pool(pool);
    let ids = seed(&store, 1).await;
    let b = broker(broker_config("notdue")).await;
    b.publish(&ids).await.unwrap();

    sqlx::query("UPDATE deliveries SET next_attempt_at = now() + interval '1 hour' WHERE id = $1")
        .bind(ids[0])
        .execute(store.pool())
        .await
        .unwrap();

    let source = BrokerSource::new(store.clone(), b.clone(), "c1", consumer_config());
    assert!(source.claim(10, "worker-1").await.unwrap().is_empty());
    assert_eq!(
        b.lag().await.unwrap().unacked,
        0,
        "and the useless message was acknowledged rather than left to be reclaimed forever"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn killing_a_consumer_mid_delivery_lets_another_finish_the_work(pool: PgPool) {
    // The acceptance criterion for crash recovery, and it takes *both* mechanisms.
    //
    // The broker's reclaim moves the message to a live consumer. That alone is not
    // enough: the row is still `inflight` under the dead worker's lease, so the new
    // consumer cannot claim it. The lease reaper is what releases it. Neither piece
    // recovers this on its own, which is why both are still here.
    let store = Store::from_pool(pool);
    let ids = seed(&store, 1).await;
    let cfg = broker_config("kill");
    let b = broker(cfg.clone()).await;
    Publisher::new(store.clone(), b.clone(), publisher_config())
        .publish_once()
        .await
        .unwrap();

    // A consumer takes the message and the lease, then dies without acknowledging
    // either. Dropping the source is exactly what a killed process leaves behind.
    let doomed = BrokerSource::new(store.clone(), b.clone(), "doomed", consumer_config());
    assert_eq!(doomed.claim(10, "worker-doomed").await.unwrap().len(), 1);
    drop(doomed);

    // The lease expires and the reaper returns the row to the queue.
    assert_eq!(store.reap_expired_leases(Duration::ZERO).await.unwrap(), 1);

    // And the message is taken over by somebody still alive.
    let survivor = BrokerSource::new(
        store.clone(),
        broker(cfg).await,
        "survivor",
        ConsumerConfig {
            reclaim_every: Duration::ZERO,
            ..consumer_config()
        },
    );
    let recovered = survivor.claim(10, "worker-survivor").await.unwrap();
    assert_eq!(
        recovered.len(),
        1,
        "the work was picked up by another consumer"
    );
    assert_eq!(recovered[0].pending.delivery_id, ids[0]);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn reclaim_does_not_take_work_from_a_consumer_that_is_merely_slow(pool: PgPool) {
    // The false positive that would undo the point. Reclaim cannot tell a dead
    // consumer from a busy one, so the idle threshold is the only thing keeping it
    // from taking work out from under a delivery that is still in flight. The lease
    // would refuse the second claim anyway — but the round trip, and the reclaim
    // metric that is supposed to mean "a consumer died", would both be noise.
    let store = Store::from_pool(pool);
    seed(&store, 1).await;
    let cfg = broker_config("slow");
    let b = broker(cfg.clone()).await;
    Publisher::new(store.clone(), b.clone(), publisher_config())
        .publish_once()
        .await
        .unwrap();

    let busy = BrokerSource::new(store.clone(), b, "busy", consumer_config());
    assert_eq!(busy.claim(10, "worker-busy").await.unwrap().len(), 1);

    let impatient = BrokerSource::new(
        store.clone(),
        broker(cfg).await,
        "impatient",
        ConsumerConfig {
            reclaim_idle: Duration::from_secs(3600),
            reclaim_every: Duration::ZERO,
            ..consumer_config()
        },
    );
    assert!(
        impatient
            .claim(10, "worker-impatient")
            .await
            .unwrap()
            .is_empty()
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn settling_finishes_the_message(pool: PgPool) {
    // Called on every path, success or failure, because it answers "is this consumer
    // finished with the message" rather than "did the webhook arrive". A message left
    // unacknowledged after a failed send would be redelivered to attempt a row that is
    // no longer due — which the lease refuses, so the only effect is wasted work.
    let store = Store::from_pool(pool);
    seed(&store, 1).await;
    let b = broker(broker_config("settle")).await;
    Publisher::new(store.clone(), b.clone(), publisher_config())
        .publish_once()
        .await
        .unwrap();

    let source = BrokerSource::new(store.clone(), b.clone(), "c1", consumer_config());
    let claimed = source.claim(10, "worker-1").await.unwrap();
    assert_eq!(b.lag().await.unwrap().unacked, 1);

    source.settle(&claimed[0].receipt).await.unwrap();
    assert_eq!(b.lag().await.unwrap().unacked, 0);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn an_empty_broker_yields_nothing_rather_than_failing(pool: PgPool) {
    // An idle fleet. A blocking read that times out must not read as an error, or
    // every consumer logs a failure a few times a second forever.
    let store = Store::from_pool(pool);
    let b = broker(broker_config("empty")).await;
    let source = BrokerSource::new(store, b, "c1", consumer_config());
    assert!(source.claim(10, "worker-1").await.unwrap().is_empty());
}
