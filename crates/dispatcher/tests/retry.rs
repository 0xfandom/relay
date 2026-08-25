//! Retries, end to end.
//!
//! `crates/domain` tests the schedule as arithmetic. What it cannot test is whether
//! a failed delivery actually comes back — that it returns to `pending` instead of
//! `dead`, that its next attempt is scheduled rather than immediate, that a
//! permanent failure stops at once, and that a delivery eventually runs out of
//! attempts instead of retrying forever.
//!
//! Requires Postgres: `docker compose up -d`.

use std::time::Duration;

use relay_dispatcher::{Pool, PoolConfig, SenderConfig};
use relay_domain::{backoff::Backoff, url_guard::Policy};
use relay_store::Store;
use relay_testkit::Receiver;
use sqlx::PgPool;
use uuid::Uuid;

/// Delays short enough that a test can wait through several of them.
fn fast_backoff(max_attempts: u32) -> Backoff {
    Backoff {
        base: Duration::from_millis(10),
        cap: Duration::from_millis(50),
        max_attempts,
        retry_after_cap: Duration::from_secs(300),
    }
}

fn pool_config() -> PoolConfig {
    PoolConfig {
        workers: 4,
        batch_size: 4,
        idle_poll: Duration::from_millis(10),
        shutdown_deadline: Duration::from_secs(5),
    }
}

async fn endpoint(store: &Store, receiver: &Receiver, path: &str) -> Uuid {
    let addr = receiver.spawn().await;
    let event_type = format!("retry.{}", Uuid::new_v4());
    store
        .create_endpoint(
            &format!("http://{addr}{path}"),
            "whsec_retry_test",
            std::slice::from_ref(&event_type),
        )
        .await
        .expect("endpoint");
    store
        .insert_event_and_fan_out(&event_type, br#"{"hello":"world"}"#)
        .await
        .expect("insert")
        .delivery_ids[0]
}

/// Drive the pool until the delivery reaches a terminal state, or give up.
async fn run_until_settled(pool: &Pool, store: &Store, id: Uuid) -> String {
    for _ in 0..200 {
        pool.run_once().await.expect("run");
        let status = store.get_delivery(id).await.unwrap().unwrap().status;
        if status == "succeeded" || status == "dead" {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    panic!("delivery never settled");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_retryable_failure_goes_back_to_pending_rather_than_dead(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_retry_test");
    let id = endpoint(&store, &receiver, "/always500").await;

    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(5),
            ..local()
        },
    );
    assert_eq!(sender.run_once().await.expect("run"), 1);

    let d = store.get_delivery(id).await.unwrap().unwrap();
    assert_eq!(
        d.status, "pending",
        "a 500 must be rescheduled, not given up on — this is the whole of M3"
    );
    assert_eq!(d.attempt, 1, "the attempt counter must advance");
    assert_eq!(receiver.hits(), 1);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_rescheduled_delivery_is_not_claimable_until_its_delay_has_passed(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_retry_test");
    endpoint(&store, &receiver, "/always500").await;

    // A whole second of backoff, so the delay is unmistakable.
    let slow = Backoff {
        base: Duration::from_secs(1),
        cap: Duration::from_secs(1),
        max_attempts: 5,
        retry_after_cap: Duration::from_secs(300),
    };
    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: slow,
            ..local()
        },
    );
    assert_eq!(sender.run_once().await.expect("run"), 1);

    // `pending` alone is not enough: without `next_attempt_at` moving into the
    // future the row would be claimed again immediately, and the retry would be a
    // hot loop rather than a backoff.
    assert_eq!(
        sender.run_once().await.expect("run"),
        0,
        "a delivery still inside its backoff window was claimed"
    );
    assert_eq!(
        receiver.hits(),
        1,
        "the endpoint was hit during the backoff"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_permanent_failure_dies_on_the_first_attempt(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_retry_test");
    // No route at this path, so the receiver answers 404.
    let id = endpoint(&store, &receiver, "/no-such-route").await;

    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(12),
            ..local()
        },
    );
    assert_eq!(sender.run_once().await.expect("run"), 1);

    let d = store.get_delivery(id).await.unwrap().unwrap();
    assert_eq!(
        d.status, "dead",
        "a 404 will be a 404 next time too; retrying it wastes the budget and looks \
         like an attack from the endpoint's side"
    );
    assert_eq!(d.attempt, 1);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_delivery_runs_out_of_attempts_and_dies(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_retry_test");
    let id = endpoint(&store, &receiver, "/always500").await;

    let max_attempts = 4;
    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(max_attempts),
            ..local()
        },
    );

    assert_eq!(run_until_settled(&sender, &store, id).await, "dead");

    let d = store.get_delivery(id).await.unwrap().unwrap();
    assert_eq!(
        d.attempt, max_attempts as i32,
        "a delivery must stop at max_attempts rather than retrying forever"
    );
    assert_eq!(receiver.hits(), max_attempts as u64);
    assert_eq!(
        store.attempts_for(id).await.unwrap(),
        max_attempts as i64,
        "every attempt should have left a row behind"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_delivery_that_recovers_succeeds_without_using_every_attempt(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_retry_test");
    // The receiver fails while its request count is below `pct`, so `pct=3` fails
    // the first two requests and succeeds on the third. Deterministic by count
    // rather than random, so this is the same run every time.
    let id = endpoint(&store, &receiver, "/flaky?pct=3").await;

    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(12),
            ..local()
        },
    );
    assert_eq!(run_until_settled(&sender, &store, id).await, "succeeded");

    let d = store.get_delivery(id).await.unwrap().unwrap();
    assert_eq!(
        d.attempt, 3,
        "two failures then a success is three attempts, and the delivery must stop there"
    );
    assert_eq!(receiver.hits(), 3);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn retry_after_overrides_the_computed_backoff(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_retry_test");
    // 429 with `Retry-After: 2`.
    endpoint(&store, &receiver, "/429?retry_after=2").await;

    // A backoff so short that without honouring the header the delivery would be
    // claimable again almost immediately.
    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(12),
            ..local()
        },
    );
    assert_eq!(sender.run_once().await.expect("run"), 1);

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        sender.run_once().await.expect("run"),
        0,
        "the endpoint asked for two seconds and knows when its rate limit window \
         resets; our own schedule does not"
    );
    assert_eq!(receiver.hits(), 1);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn every_attempt_of_a_retried_delivery_reuses_one_delivery_id(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_retry_test");
    let id = endpoint(&store, &receiver, "/always500").await;

    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(3),
            ..local()
        },
    );
    assert_eq!(run_until_settled(&sender, &store, id).await, "dead");

    let seen = receiver.received_ids();
    assert_eq!(seen.len(), 3);
    assert!(
        seen.iter().all(|s| *s == id.to_string()),
        "retries must reuse the delivery id, or the receiver cannot tell a retry \
         from a new event and deduplication becomes impossible: {seen:?}"
    );
}

/// Every receiver in these tests runs on loopback, which the strict policy refuses.
///
/// Opted into explicitly rather than making permissive the default. A default that
/// allows internal addresses is a vulnerability that ships whenever somebody forgets
/// to configure it, and the tests are exactly where that forgetting would hide.
fn local() -> SenderConfig {
    SenderConfig {
        policy: Policy::permissive(),
        // Rate limiting off: these tests are about something else, and a deferral
        // would add attempt rows for requests that were never made.
        rate_limit: false,
        // Breaker off: several of these tests fail one endpoint repeatedly on
        // purpose, and tripping it would replace the behaviour under test with a
        // deferral.
        breaker: None,
        ..Default::default()
    }
}
