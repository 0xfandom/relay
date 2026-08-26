//! One endpoint cannot take the whole pool with it.
//!
//! Two caps, and they protect different people.
//!
//! The **per-endpoint** cap is the bulkhead. An endpoint that accepts connections
//! and then never replies is the worst kind of failure: nothing errors, nothing
//! retries, the workers simply stop coming back for a full request timeout. With
//! thirty-two workers and one such endpoint holding a deep backlog, every other
//! customer's webhooks wait behind a server that is not even answering.
//!
//! The **global** cap protects Relay itself — sockets, file descriptors, and the
//! memory of every response being buffered at once — and is deliberately independent
//! of the worker count, because a worker spends most of its life waiting and the two
//! numbers are not the same question.
//!
//! Requires Postgres: `docker compose up -d`.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use relay_dispatcher::{Bulkhead, Limits, Pool, PoolConfig, RequestLimits, SenderConfig};
use relay_domain::{backoff::Backoff, rate_limit::Rate, url_guard::Policy};
use relay_store::Store;
use relay_testkit::Receiver;
use sqlx::PgPool;
use uuid::Uuid;

fn config(limits: Limits) -> SenderConfig {
    SenderConfig {
        backoff: Backoff {
            base: Duration::from_millis(5),
            cap: Duration::from_millis(20),
            max_attempts: 12,
            retry_after_cap: Duration::from_secs(300),
        },
        policy: Policy::permissive(),
        // Off: this file is about concurrency, and a token shortage would look
        // exactly like a bulkhead deferral in the results.
        request: RequestLimits::default(),
        rate_limit: false,
        // Breaker off: several of these tests fail one endpoint repeatedly on
        // purpose, and tripping it would replace the behaviour under test with a
        // deferral.
        breaker: None,
        limits,
    }
}

fn pool_config() -> PoolConfig {
    PoolConfig {
        workers: 32,
        batch_size: 32,
        idle_poll: Duration::from_millis(2),
        shutdown_deadline: Duration::from_secs(10),
    }
}

/// Register an endpoint at `path` on `receiver` and queue `n` deliveries to it.
async fn seed(store: &Store, receiver: &Receiver, path: &str, n: usize) -> Vec<Uuid> {
    let addr = receiver.spawn().await;
    let event_type = format!("bh.{}", Uuid::new_v4());
    store
        .create_endpoint(
            &format!("http://{addr}{path}"),
            "whsec_bh_test",
            std::slice::from_ref(&event_type),
        )
        .await
        .expect("endpoint");

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

#[sqlx::test(migrations = "../store/migrations")]
async fn an_endpoint_may_not_exceed_its_own_concurrency_cap(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_bh_test");
    // Twenty deliveries, thirty-two workers, and a receiver that holds every request
    // open for a while. Without a cap the endpoint would see twenty at once.
    seed(&store, &receiver, "/slow?ms=400", 20).await;

    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        config(Limits {
            max_in_flight: 64,
            per_endpoint: 3,
        }),
    );

    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(900) {
        sender.run_once().await.expect("run");
    }

    assert!(
        receiver.max_in_flight() <= 3,
        "endpoint saw {} concurrent requests, cap is 3",
        receiver.max_in_flight()
    );
    assert!(receiver.hits() >= 1, "nothing got through at all");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_hanging_endpoint_does_not_delay_a_healthy_one(pool: PgPool) {
    let store = Store::from_pool(pool);

    // The acceptance criterion, and the reason the per-endpoint cap is checked with
    // `try_acquire` rather than by waiting. A blocking reservation would park a
    // worker on the hanging endpoint and the healthy one would queue behind it.
    let hanging = Receiver::new("whsec_bh_test");
    seed(&store, &hanging, "/slow?ms=9000", 40).await;

    let healthy = Receiver::new("whsec_bh_test");
    let healthy_ids = seed(&store, &healthy, "/verify", 5).await;

    // `Pool::run`, not `run_once`. `run_once` claims a batch and waits for all of it
    // to finish, which is what makes it deterministic for other tests — and exactly
    // the coupling this test is about, applied by the test harness instead of by the
    // sender. Production uses `run`, which never waits for a batch to drain.
    let sender = Arc::new(Pool::with_config(
        store.clone(),
        pool_config(),
        config(Limits {
            max_in_flight: 16,
            per_endpoint: 4,
        }),
    ));

    let cancel = tokio_util::sync::CancellationToken::new();
    let running = {
        let sender = sender.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move { sender.run(cancel).await })
    };

    let started = Instant::now();
    let mut healthy_done = None;
    for _ in 0..400 {
        let mut all = true;
        for id in &healthy_ids {
            all &= store.get_delivery(*id).await.unwrap().unwrap().status == "succeeded";
        }
        if all {
            healthy_done = Some(started.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    cancel.cancel();
    // The hanging deliveries are still in flight and will not finish, so the drain
    // deadline is what ends this rather than the work completing.
    let _ = tokio::time::timeout(Duration::from_secs(15), running).await;

    let elapsed = healthy_done.expect("the healthy endpoint never finished");
    // The hanging endpoint holds its four slots for nine seconds. If the healthy
    // endpoint's deliveries were queued behind it, this would be measured in seconds.
    assert!(
        elapsed < Duration::from_secs(3),
        "healthy deliveries took {elapsed:?} behind a hanging endpoint"
    );
    assert_eq!(healthy.hits(), 5);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn total_in_flight_never_exceeds_the_global_cap(pool: PgPool) {
    let store = Store::from_pool(pool);

    // Six endpoints pointing at one receiver, so the receiver's own high-water mark
    // measures *global* concurrency rather than any one endpoint's. Each endpoint may
    // have 4 in flight, so the per-endpoint caps alone would permit 24.
    let receiver = Receiver::new("whsec_bh_test");
    let addr = receiver.spawn().await;
    for _ in 0..6 {
        let event_type = format!("bh.{}", Uuid::new_v4());
        store
            .create_endpoint(
                &format!("http://{addr}/slow?ms=300"),
                "whsec_bh_test",
                std::slice::from_ref(&event_type),
            )
            .await
            .expect("endpoint");
        for _ in 0..6 {
            store
                .insert_event_and_fan_out(&event_type, br#"{"hello":"world"}"#)
                .await
                .expect("insert");
        }
    }

    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        config(Limits {
            max_in_flight: 5,
            per_endpoint: 4,
        }),
    );

    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(1200) {
        sender.run_once().await.expect("run");
    }

    assert!(
        receiver.max_in_flight() <= 5,
        "{} requests were in flight at once, the global cap is 5",
        receiver.max_in_flight()
    );
    // And it is a bound, not a stop.
    assert!(receiver.hits() >= 5, "only {} got through", receiver.hits());
}

#[sqlx::test(migrations = "../store/migrations")]
async fn hitting_the_cap_defers_rather_than_failing(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_bh_test");
    let ids = seed(&store, &receiver, "/slow?ms=300", 12).await;

    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        config(Limits {
            max_in_flight: 64,
            per_endpoint: 2,
        }),
    );

    // One pass: two get slots, the rest find the endpoint full.
    sender.run_once().await.expect("run");

    let mut deferred = 0;
    for id in &ids {
        let d = store.get_delivery(*id).await.unwrap().unwrap();
        if d.status == "pending" && d.attempt == 0 {
            let classes: Vec<String> = store
                .attempt_history(*id)
                .await
                .unwrap()
                .into_iter()
                .map(|a| a.outcome_class)
                .collect();
            if classes.iter().any(|c| c == "deferred") {
                // The same rule as the rate limiter. Nothing was sent, so nothing was
                // learned about whether the endpoint works — charging an attempt
                // would let a busy endpoint's deliveries die without one request
                // ever having been made.
                assert_eq!(d.attempt, 0, "a bulkhead deferral spent an attempt");
                deferred += 1;
            }
        }
    }
    assert!(deferred > 0, "nothing was deferred at a cap of 2");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_delivery_stopped_by_the_bulkhead_does_not_spend_a_token(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_bh_test");

    // Exactly three tokens, and a refill rate slow enough that no fourth arrives
    // during the test. Ten deliveries and a cap of one in flight, so nearly all of
    // them meet the bulkhead.
    //
    // The assertion is a count, not a rate. Three tokens must buy three requests: no
    // more, because the bucket says so, and — the point of this test — no fewer,
    // because a delivery turned away by the bulkhead must not have spent one on the
    // way. Taking a token and *then* finding no slot would leak it on a request that
    // was never made, and the endpoint would quietly receive less than its
    // configured rate: a limiter leaking capacity through a limiter.
    const TOKENS: u64 = 3;
    let ids = seed(&store, &receiver, "/verify", 10).await;
    let endpoint = endpoint_of(&store, ids[0]).await;
    store
        .set_endpoint_rate(endpoint, Rate::new(0.01, TOKENS as f64))
        .await
        .expect("set rate");

    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            // On, deliberately: the point is what happens when both gates are live.
            request: RequestLimits::default(),
            rate_limit: true,
            // Breaker off: several of these tests fail one endpoint repeatedly on
            // purpose, and tripping it would replace the behaviour under test with a
            // deferral.
            breaker: None,
            ..config(Limits {
                max_in_flight: 64,
                per_endpoint: 1,
            })
        },
    );

    // Driven until the tokens are spent rather than for a fixed stretch of time. A
    // cap of one serialises the sends and every bulkhead deferral waits out a
    // jittered delay, so how long this takes depends on the machine — but how many
    // get through does not.
    let deadline = Instant::now() + Duration::from_secs(20);
    while receiver.hits() < TOKENS && Instant::now() < deadline {
        sender.run_once().await.expect("run");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(
        receiver.hits(),
        TOKENS,
        "three tokens must buy exactly three requests"
    );

    // A few more passes to prove the fourth never comes: the bucket is empty and
    // nothing about being deferred by the bulkhead refills it.
    for _ in 0..20 {
        sender.run_once().await.expect("run");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        receiver.hits(),
        TOKENS,
        "the bucket handed out a fourth token"
    );

    // And nothing failed. Every outcome here is a send or a deferral.
    for id in &ids {
        let classes: Vec<String> = store
            .attempt_history(*id)
            .await
            .unwrap()
            .into_iter()
            .map(|a| a.outcome_class)
            .collect();
        assert!(
            classes.iter().all(|c| c == "deferred" || c == "success"),
            "unexpected outcome for {id}: {classes:?}"
        );
    }
}

/// The endpoint a delivery belongs to.
async fn endpoint_of(store: &Store, delivery_id: Uuid) -> Uuid {
    store
        .get_delivery(delivery_id)
        .await
        .unwrap()
        .unwrap()
        .endpoint_id
}

#[tokio::test]
async fn a_panicking_task_does_not_leak_a_permit() {
    // Permits are released by `Drop`, and `Drop` runs while unwinding. Worth an
    // explicit test because the failure mode is invisible until the process has
    // slowly leaked its whole allowance and stops sending anything at all.
    let bulkhead = Arc::new(Bulkhead::new(Limits {
        max_in_flight: 2,
        per_endpoint: 2,
    }));
    let endpoint = Uuid::new_v4();

    for _ in 0..5 {
        let doomed = bulkhead.clone();
        let handle = tokio::spawn(async move {
            let reserved = doomed.try_reserve(endpoint).expect("a slot is free");
            let _slot = doomed.enter(reserved).await;
            panic!("worker died mid-request");
        });
        assert!(handle.await.is_err(), "the task should have panicked");
        assert_eq!(bulkhead.in_flight(), 0, "a permit leaked on panic");
    }

    // And the bulkhead still works afterwards.
    let reserved = bulkhead.try_reserve(endpoint).expect("a slot is free");
    let slot = bulkhead.enter(reserved).await;
    assert_eq!(bulkhead.in_flight(), 1);
    drop(slot);
    assert_eq!(bulkhead.in_flight(), 0);
}

#[tokio::test]
async fn the_per_endpoint_cap_is_per_endpoint() {
    let bulkhead = Bulkhead::new(Limits {
        max_in_flight: 64,
        per_endpoint: 1,
    });
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();

    let _held = bulkhead.try_reserve(a).expect("a's only slot");
    assert!(
        bulkhead.try_reserve(a).is_none(),
        "a second slot on a saturated endpoint"
    );
    // The whole point: b is unaffected by a being full.
    assert!(bulkhead.try_reserve(b).is_some(), "b was blocked by a");
    assert_eq!(bulkhead.tracked(), 2);
}

#[tokio::test]
async fn a_reservation_is_released_when_dropped() {
    let bulkhead = Bulkhead::new(Limits {
        max_in_flight: 64,
        per_endpoint: 1,
    });
    let id = Uuid::new_v4();

    let reserved = bulkhead.try_reserve(id).expect("free");
    assert!(bulkhead.try_reserve(id).is_none());
    // A deferral drops the reservation without ever entering the global pool. If it
    // were not released, one deferral would retire that endpoint's slot forever.
    drop(reserved);
    assert!(
        bulkhead.try_reserve(id).is_some(),
        "a deferred reservation was never returned"
    );
}
