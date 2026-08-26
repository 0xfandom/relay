//! Relay refuses to overwhelm a destination.
//!
//! Every other protection in this codebase reacts to a failure. This one prevents
//! the failure, and the failure it prevents is one Relay causes: a customer
//! subscribes to a high-volume event, one burst fans out into ten thousand
//! deliveries, and their server falls over. Every one of those then fails, retries,
//! and arrives again as a wave.
//!
//! The load-bearing property is not "slower". It is that a deferral **does not spend
//! an attempt**. If rate limiting incremented the attempt counter, a busy endpoint's
//! deliveries would reach the dead letter queue having never had a single request
//! made to them — a retry budget consumed entirely by our own throttle.
//!
//! Requires Postgres: `docker compose up -d`.

use std::time::{Duration, Instant};

use relay_dispatcher::{Limits, Outcome, Pool, PoolConfig, RequestLimits, Sender, SenderConfig};
use relay_domain::{backoff::Backoff, rate_limit::Rate, url_guard::Policy};
use relay_store::Store;
use relay_testkit::Receiver;
use sqlx::PgPool;
use uuid::Uuid;

/// Rate limiting on, loopback allowed, retries fast enough to watch.
fn limited() -> SenderConfig {
    SenderConfig {
        backoff: Backoff {
            base: Duration::from_millis(5),
            cap: Duration::from_millis(20),
            max_attempts: 12,
            retry_after_cap: Duration::from_secs(300),
        },
        policy: Policy::permissive(),
        request: RequestLimits::default(),
        rate_limit: true,
        // Breaker off: several of these tests fail one endpoint repeatedly on
        // purpose, and tripping it would replace the behaviour under test with a
        // deferral.
        breaker: None,
        // Wide, so the bucket is the only thing holding anything back. The
        // concurrency caps are tested in `bulkhead.rs`; here they would only muddy
        // which limit a deferral came from.
        limits: Limits {
            max_in_flight: 1024,
            per_endpoint: 1024,
        },
    }
}

fn pool_config() -> PoolConfig {
    PoolConfig {
        workers: 16,
        batch_size: 16,
        idle_poll: Duration::from_millis(2),
        shutdown_deadline: Duration::from_secs(5),
    }
}

/// Register one endpoint at `rate` and queue `n` deliveries to it.
async fn seed(store: &Store, receiver: &Receiver, rate: Rate, n: usize) -> Vec<Uuid> {
    let addr = receiver.spawn().await;
    let event_type = format!("rl.{}", Uuid::new_v4());
    let ep = store
        .create_endpoint(
            &format!("http://{addr}/verify"),
            "whsec_rl_test",
            std::slice::from_ref(&event_type),
        )
        .await
        .expect("endpoint");
    store
        .set_endpoint_rate(ep.id, rate)
        .await
        .expect("set rate");

    let mut ids = Vec::new();
    for _ in 0..n {
        ids.extend(
            store
                .insert_event_and_fan_out(&event_type, br#"{"hello":"world"}"#)
                .await
                .expect("insert")
                .delivery_ids,
        );
    }
    ids
}

async fn attempt_classes(store: &Store, id: Uuid) -> Vec<String> {
    store
        .attempt_history(id)
        .await
        .expect("history")
        .into_iter()
        .map(|a| a.outcome_class)
        .collect()
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_burst_is_capped_at_the_configured_burst(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_rl_test");

    // Five tokens, and a rate slow enough that no meaningful sixth arrives during
    // the test. Twenty deliveries ready at once.
    let rate = Rate::new(0.5, 5.0);
    seed(&store, &receiver, rate, 20).await;

    let sender = Pool::with_config(store.clone(), pool_config(), limited());
    for _ in 0..5 {
        sender.run_once().await.expect("run");
    }

    // The endpoint saw the burst and nothing beyond it. Without a limiter it would
    // have seen all twenty.
    assert_eq!(
        receiver.hits(),
        5,
        "the endpoint received {} requests for a burst of 5",
        receiver.hits()
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn deferrals_do_not_spend_an_attempt(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_rl_test");

    // One token. The first delivery takes it; everything after is deferred.
    let ids = seed(&store, &receiver, Rate::new(0.5, 1.0), 4).await;

    let sender = Pool::with_config(store.clone(), pool_config(), limited());
    for _ in 0..4 {
        sender.run_once().await.expect("run");
    }

    // Find one that was held back rather than sent.
    let deferred = {
        let mut found = None;
        for id in &ids {
            let d = store.get_delivery(*id).await.unwrap().unwrap();
            if d.status == "pending" {
                found = Some(*id);
                break;
            }
        }
        found.expect("some delivery must have been deferred")
    };

    let d = store.get_delivery(deferred).await.unwrap().unwrap();
    // The whole issue in one assertion. Deferrals here would burn through a twelve
    // attempt budget in seconds and park a perfectly healthy delivery in the dead
    // letter queue without ever having contacted the endpoint.
    assert_eq!(
        d.attempt, 0,
        "a deferral must not count as an attempt (attempt = {})",
        d.attempt
    );
    assert_eq!(d.status, "pending", "a deferral is not a failure");

    // But it is recorded, because "held back for 300ms" is what someone asking why
    // a webhook was late needs to see.
    let classes = attempt_classes(&store, deferred).await;
    assert!(
        classes.iter().all(|c| c == "deferred"),
        "expected only deferrals, got {classes:?}"
    );
    assert!(!classes.is_empty(), "the deferral was never recorded");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_deferred_delivery_is_scheduled_for_when_a_token_arrives(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_rl_test");

    // Two per second: a token every 500ms.
    let ids = seed(&store, &receiver, Rate::new(2.0, 1.0), 2).await;
    let sender = Sender::with_config(store.clone(), limited());

    // First takes the only token.
    let first = sender
        .deliver_by_id(ids[0])
        .await
        .expect("deliver")
        .expect("attempted");
    assert!(matches!(first, Outcome::Succeeded { .. }), "{first:?}");

    let second = sender
        .deliver_by_id(ids[1])
        .await
        .expect("deliver")
        .expect("attempted");
    let Outcome::Deferred { after } = second else {
        panic!("expected a deferral, got {second:?}");
    };

    // Bounded by one token's worth of time, never more. The bucket has been
    // refilling throughout the first delivery, so the exact figure depends on how
    // long that took — what must hold is that the wait is real and no longer than
    // the interval between tokens.
    assert!(
        after > Duration::ZERO && after <= Duration::from_millis(500),
        "expected at most one token interval, got {after:?}"
    );

    // Deferred, not delivered.
    assert_eq!(receiver.hits(), 1);
    assert_eq!(
        store.get_delivery(ids[1]).await.unwrap().unwrap().status,
        "pending"
    );

    // The property that matters: coming back when told works. A delay that landed
    // even slightly short would make a throttled endpoint's deliveries bounce twice
    // for every one that goes out.
    tokio::time::sleep(after).await;
    let third = sender
        .deliver_by_id(ids[1])
        .await
        .expect("deliver")
        .expect("attempted");
    assert!(
        matches!(third, Outcome::Succeeded { .. }),
        "the delay was too short: {third:?}"
    );
    assert_eq!(receiver.hits(), 2);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_sustained_rate_is_held_over_time(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_rl_test");

    // Twenty per second, burst of two, twenty deliveries queued. In roughly half a
    // second no more than about a dozen may go out.
    seed(&store, &receiver, Rate::new(20.0, 2.0), 20).await;

    let sender = Pool::with_config(store.clone(), pool_config(), limited());
    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(500) {
        sender.run_once().await.expect("run");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // burst (2) + rate * elapsed. Generous headroom on the upper bound so a slow
    // machine does not fail the test, but far below the 20 an unlimited sender
    // would have managed.
    let elapsed = started.elapsed().as_secs_f64();
    let ceiling = (2.0 + 20.0 * elapsed).ceil() as u64 + 1;
    assert!(
        receiver.hits() <= ceiling,
        "sent {} in {elapsed:.3}s, ceiling is {ceiling}",
        receiver.hits()
    );
    assert!(receiver.hits() >= 2, "nothing got through at all");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn one_endpoint_being_throttled_does_not_throttle_another(pool: PgPool) {
    let store = Store::from_pool(pool);

    // The bucket is per endpoint, so a customer who configured a crawl must not slow
    // down a customer who did not.
    let slow = Receiver::new("whsec_rl_test");
    seed(&store, &slow, Rate::new(0.5, 1.0), 10).await;

    let fast = Receiver::new("whsec_rl_test");
    let fast_ids = seed(&store, &fast, Rate::new(1000.0, 1000.0), 10).await;

    let sender = Pool::with_config(store.clone(), pool_config(), limited());
    for _ in 0..6 {
        sender.run_once().await.expect("run");
    }

    assert_eq!(fast.hits(), 10, "the unthrottled endpoint was held back");
    assert_eq!(slow.hits(), 1, "the throttled endpoint got its one token");

    for id in fast_ids {
        assert_eq!(
            store.get_delivery(id).await.unwrap().unwrap().status,
            "succeeded"
        );
    }
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_throttled_delivery_still_arrives_eventually(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_rl_test");

    // The other half. A limiter that delays forever is an outage with a nicer name.
    let ids = seed(&store, &receiver, Rate::new(40.0, 1.0), 4).await;

    let sender = Pool::with_config(store.clone(), pool_config(), limited());
    for _ in 0..300 {
        sender.run_once().await.expect("run");
        let done = {
            let mut all = true;
            for id in &ids {
                all &= store.get_delivery(*id).await.unwrap().unwrap().status == "succeeded";
            }
            all
        };
        if done {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    for id in &ids {
        let d = store.get_delivery(*id).await.unwrap().unwrap();
        assert_eq!(d.status, "succeeded", "a throttled delivery never arrived");
        // And it arrived with its retry budget intact, however many times it was
        // held back on the way.
        assert_eq!(d.attempt, 1, "deferrals ate into the retry budget");
    }
    assert_eq!(receiver.hits(), 4);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn switching_the_limiter_off_removes_the_ceiling(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_rl_test");
    seed(&store, &receiver, Rate::new(0.5, 1.0), 10).await;

    // The escape hatch, tested so that the tests which rely on it are relying on
    // something real. Off is a deliberate load-test setting, never a default.
    let unlimited = SenderConfig {
        request: RequestLimits::default(),
        rate_limit: false,
        // Breaker off: several of these tests fail one endpoint repeatedly on
        // purpose, and tripping it would replace the behaviour under test with a
        // deferral.
        breaker: None,
        ..limited()
    };
    let sender = Pool::with_config(store.clone(), pool_config(), unlimited);
    for _ in 0..3 {
        sender.run_once().await.expect("run");
    }

    assert_eq!(receiver.hits(), 10);
}
