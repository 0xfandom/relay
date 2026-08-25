//! Deliveries stop when an endpoint is dead, and start again when it is not.
//!
//! Retries and rate limits both assume the endpoint is worth talking to. This is the
//! case where it is not. A server that has been down for an hour will fail every
//! delivery, and each one costs a worker the full request timeout to learn what the
//! last thousand already established — so with a deep backlog, one dead endpoint
//! quietly consumes the pool while producing nothing.
//!
//! The state lives on the endpoint row rather than in process memory, and that is
//! the point worth testing hardest. In-process state looks correct with one worker
//! and silently fails with several: each sees a fraction of the failures, none
//! reaches the threshold, and every worker independently concludes the endpoint is
//! merely unlucky.
//!
//! Requires Postgres: `docker compose up -d`.

use std::time::Duration;

use relay_dispatcher::{Limits, Outcome, Pool, PoolConfig, Sender, SenderConfig};
use relay_domain::{backoff::Backoff, breaker, url_guard::Policy};
use relay_store::Store;
use relay_testkit::Receiver;
use sqlx::PgPool;
use uuid::Uuid;

/// Trips after three failures, with a cooldown short enough to wait through.
fn policy() -> breaker::Policy {
    breaker::Policy {
        threshold: 3,
        cooldown: Duration::from_millis(300),
        max_cooldown: Duration::from_millis(1200),
    }
}

fn config(breaker: Option<breaker::Policy>) -> SenderConfig {
    SenderConfig {
        backoff: Backoff {
            base: Duration::from_millis(5),
            cap: Duration::from_millis(20),
            max_attempts: 50,
            retry_after_cap: Duration::from_secs(300),
        },
        policy: Policy::permissive(),
        // Off: a token shortage would look exactly like a breaker deferral here.
        rate_limit: false,
        limits: Limits {
            max_in_flight: 1024,
            per_endpoint: 1024,
        },
        breaker,
    }
}

fn pool_config() -> PoolConfig {
    PoolConfig {
        workers: 8,
        batch_size: 8,
        idle_poll: Duration::from_millis(5),
        shutdown_deadline: Duration::from_secs(5),
    }
}

/// Register an endpoint at `path` and queue `n` deliveries. Returns (endpoint, ids).
async fn seed(store: &Store, receiver: &Receiver, path: &str, n: usize) -> (Uuid, Vec<Uuid>) {
    let addr = receiver.spawn().await;
    let event_type = format!("br.{}", Uuid::new_v4());
    let ep = store
        .create_endpoint(
            &format!("http://{addr}{path}"),
            "whsec_breaker_test",
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
    (ep.id, ids)
}

async fn state(store: &Store, endpoint: Uuid) -> String {
    store
        .endpoint_breaker(endpoint)
        .await
        .expect("breaker")
        .breaker_state
}

#[sqlx::test(migrations = "../store/migrations")]
async fn consecutive_failures_trip_the_breaker(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_breaker_test");
    let (endpoint, ids) = seed(&store, &receiver, "/always500", 3).await;

    let sender = Sender::with_config(store.clone(), config(Some(policy())));
    for (i, id) in ids.iter().enumerate() {
        sender.deliver_by_id(*id).await.expect("deliver");
        let b = store.endpoint_breaker(endpoint).await.unwrap();
        if i < 2 {
            assert_eq!(b.breaker_state, "closed", "tripped early at {i}");
            assert_eq!(b.consecutive_failures, i as i32 + 1);
        }
    }

    let b = store.endpoint_breaker(endpoint).await.unwrap();
    assert_eq!(
        b.breaker_state, "open",
        "three failures should have tripped it"
    );
    assert_eq!(b.breaker_trips, 1);
    assert!(
        b.breaker_probe_at.is_some(),
        "an open breaker must be probeable"
    );
    assert!(b.breaker_opened_at.is_some());
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_404_does_not_trip_the_breaker(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_breaker_test");
    // A misconfigured URL, not a dead server. The endpoint answers every time — it
    // just answers "no". Tripping here would cut off a destination that is working
    // fine while hiding a problem that needs a person to fix the URL.
    let (endpoint, _) = seed(&store, &receiver, "/no-such-route", 10).await;

    let sender = Pool::with_config(store.clone(), pool_config(), config(Some(policy())));
    for _ in 0..5 {
        sender.run_once().await.expect("run");
    }

    let b = store.endpoint_breaker(endpoint).await.unwrap();
    assert_eq!(b.breaker_state, "closed");
    assert_eq!(
        b.consecutive_failures, 0,
        "a 404 is not evidence of ill health"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn one_success_resets_the_streak(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_breaker_test");
    // Fails twice then succeeds, repeatedly. Two below a threshold of three, so a
    // breaker counting cumulative failures would trip and a correct one never does.
    let (endpoint, _) = seed(&store, &receiver, "/flaky?pct=3", 9).await;

    let sender = Pool::with_config(store.clone(), pool_config(), config(Some(policy())));
    for _ in 0..30 {
        sender.run_once().await.expect("run");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    assert_eq!(
        state(&store, endpoint).await,
        "closed",
        "an endpoint that recovers between failures is not dead"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn an_open_breaker_defers_rather_than_failing(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_breaker_test");
    let (endpoint, ids) = seed(&store, &receiver, "/always500", 12).await;

    let sender = Sender::with_config(store.clone(), config(Some(policy())));
    // Trip it.
    for id in ids.iter().take(3) {
        sender.deliver_by_id(*id).await.expect("deliver");
    }
    assert_eq!(state(&store, endpoint).await, "open");

    let hits_when_tripped = receiver.hits();
    let target = ids[5];
    let before = store.get_delivery(target).await.unwrap().unwrap();

    let outcome = sender
        .deliver_by_id(target)
        .await
        .expect("deliver")
        .expect("attempted");
    assert!(matches!(outcome, Outcome::Deferred { .. }), "{outcome:?}");

    // Nothing left the process.
    assert_eq!(receiver.hits(), hits_when_tripped);

    let after = store.get_delivery(target).await.unwrap().unwrap();
    assert_eq!(after.status, "pending", "a deferral is not a failure");
    // The one that matters most of the three deferrals. Charging attempts for the
    // time an endpoint is cut off would empty every pending delivery's retry budget
    // during the outage, and they would all be dead by the time it came back.
    assert_eq!(
        after.attempt, before.attempt,
        "an open breaker spent an attempt"
    );

    let classes: Vec<String> = store
        .attempt_history(target)
        .await
        .unwrap()
        .into_iter()
        .map(|a| a.outcome_class)
        .collect();
    assert_eq!(classes, vec!["deferred"], "the deferral was not recorded");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_deep_backlog_against_a_dead_endpoint_stops_hammering_it(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_breaker_test");
    // Forty deliveries to a server that answers 500 to everything. Without a breaker
    // every pass would send every one of them, forever.
    let (endpoint, _) = seed(&store, &receiver, "/always500", 40).await;

    let sender = Pool::with_config(store.clone(), pool_config(), config(Some(policy())));
    for _ in 0..15 {
        sender.run_once().await.expect("run");
    }

    assert_eq!(state(&store, endpoint).await, "open");
    // Three to trip it, plus whatever was already in flight in that first batch. The
    // ceiling is what matters: nowhere near forty, let alone forty per pass.
    assert!(
        receiver.hits() <= 12,
        "the endpoint was hit {} times after tripping at 3",
        receiver.hits()
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_recovered_endpoint_is_delivered_to_again(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_breaker_test");
    // The other half, and the reason half-open exists. A breaker that opens and never
    // closes is not a protection, it is a permanent outage with extra steps.
    let (endpoint, ids) = seed(&store, &receiver, "/toggle", 6).await;

    let sender = Pool::with_config(store.clone(), pool_config(), config(Some(policy())));
    for _ in 0..6 {
        sender.run_once().await.expect("run");
    }
    assert_eq!(state(&store, endpoint).await, "open");

    // The endpoint comes back.
    receiver.set_failing(false);
    tokio::time::sleep(Duration::from_millis(400)).await;

    for _ in 0..40 {
        sender.run_once().await.expect("run");
        if state(&store, endpoint).await == "closed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        state(&store, endpoint).await,
        "closed",
        "the breaker never reclosed after the endpoint recovered"
    );

    for _ in 0..60 {
        sender.run_once().await.expect("run");
        let all = {
            let mut done = true;
            for id in &ids {
                done &= store.get_delivery(*id).await.unwrap().unwrap().status == "succeeded";
            }
            done
        };
        if all {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for id in &ids {
        assert_eq!(
            store.get_delivery(*id).await.unwrap().unwrap().status,
            "succeeded",
            "a delivery held during the outage was never sent afterwards"
        );
    }
}

#[sqlx::test(migrations = "../store/migrations")]
async fn every_worker_sees_the_same_breaker(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_breaker_test");
    let (endpoint, ids) = seed(&store, &receiver, "/always500", 6).await;

    // Three independent senders, standing in for three dispatcher processes. Held in
    // process memory each would see one failure, none would reach a threshold of
    // three, and the breaker would never trip while all three concluded the endpoint
    // was merely unlucky.
    let senders: Vec<Sender> = (0..3)
        .map(|_| Sender::with_config(store.clone(), config(Some(policy()))))
        .collect();

    for (i, id) in ids.iter().take(3).enumerate() {
        senders[i].deliver_by_id(*id).await.expect("deliver");
    }

    assert_eq!(
        state(&store, endpoint).await,
        "open",
        "three processes, one failure each, one shared threshold"
    );

    // And every one of them acts on it, including the two that never saw a trip.
    for sender in &senders {
        let outcome = sender
            .deliver_by_id(ids[4])
            .await
            .expect("deliver")
            .map(|o| matches!(o, Outcome::Deferred { .. }));
        if let Some(deferred) = outcome {
            assert!(deferred, "a sender ignored another's breaker");
            break;
        }
    }
}

#[sqlx::test(migrations = "../store/migrations")]
async fn concurrent_failures_are_all_counted(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_breaker_test");
    let (endpoint, _) = seed(&store, &receiver, "/always500", 3).await;

    // Three deliveries reported at once. Read-then-write without a row lock would
    // have all three read zero, all three write one, and the breaker would record
    // one failure where three happened — at a threshold of three, the difference
    // between tripping and not.
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..3 {
        let store = store.clone();
        tasks.spawn(async move {
            store
                .record_health(endpoint, breaker::Health::Failing, &policy())
                .await
        });
    }
    while let Some(joined) = tasks.join_next().await {
        joined.expect("task did not panic").expect("record");
    }

    assert_eq!(state(&store, endpoint).await, "open");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_breaker_can_be_switched_off(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_breaker_test");
    let (endpoint, _) = seed(&store, &receiver, "/always500", 8).await;

    // The escape hatch, tested so the tests relying on it rely on something real.
    let sender = Pool::with_config(store.clone(), pool_config(), config(None));
    for _ in 0..3 {
        sender.run_once().await.expect("run");
    }

    assert_eq!(state(&store, endpoint).await, "closed");
    assert!(receiver.hits() >= 8, "only {} went out", receiver.hits());
}

#[sqlx::test(migrations = "../store/migrations")]
async fn an_operator_can_reset_a_breaker(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_breaker_test");
    let (endpoint, ids) = seed(&store, &receiver, "/always500", 4).await;

    let sender = Sender::with_config(store.clone(), config(Some(policy())));
    for id in ids.iter().take(3) {
        sender.deliver_by_id(*id).await.expect("deliver");
    }
    assert_eq!(state(&store, endpoint).await, "open");

    // Somebody who knows the endpoint is fine should not have to wait out a cooldown
    // that was calculated from evidence they know is stale.
    store.reset_breaker(endpoint).await.expect("reset");

    let b = store.endpoint_breaker(endpoint).await.unwrap();
    assert_eq!(b.breaker_state, "closed");
    assert_eq!(b.consecutive_failures, 0);
    assert_eq!(b.breaker_trips, 0);
    assert!(b.breaker_probe_at.is_none());
}
