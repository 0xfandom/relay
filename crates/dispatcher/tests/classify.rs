//! The classifier, exercised through a real delivery.
//!
//! `crates/domain` already tests the rule itself against every possible status.
//! What it cannot test is whether the dispatcher asks the rule at all, hands it the
//! right input, and records what it said. A classifier that is never consulted is
//! indistinguishable from one that is wrong, and the retry logic in #11 will read
//! this value to decide whether a delivery gets another attempt.
//!
//! Requires Postgres: `docker compose up -d`.

use std::time::Duration;

use relay_dispatcher::{Outcome, Pool, PoolConfig};
use relay_domain::outcome::Class;
use relay_store::Store;
use relay_testkit::Receiver;
use sqlx::PgPool;
use uuid::Uuid;

async fn deliver_to(store: &Store, path: &str) -> Uuid {
    let receiver = Receiver::new("whsec_classify_test");
    let addr = receiver.spawn().await;
    let event_type = format!("classify.{}", Uuid::new_v4());
    store
        .create_endpoint(
            &format!("http://{addr}{path}"),
            "whsec_classify_test",
            std::slice::from_ref(&event_type),
        )
        .await
        .expect("endpoint");

    let accepted = store
        .insert_event_and_fan_out(&event_type, br#"{"hello":"world"}"#)
        .await
        .expect("insert");

    let pool = Pool::new(
        store.clone(),
        PoolConfig {
            workers: 1,
            batch_size: 1,
            idle_poll: Duration::from_millis(10),
            shutdown_deadline: Duration::from_secs(5),
        },
    );
    assert_eq!(pool.run_once().await.expect("run"), 1);

    accepted.delivery_ids[0]
}

async fn recorded_class(store: &Store, delivery_id: Uuid) -> String {
    sqlx::query_scalar("SELECT outcome_class FROM delivery_attempts WHERE delivery_id = $1")
        .bind(delivery_id)
        .fetch_one(store.pool())
        .await
        .expect("attempt row")
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_server_error_is_recorded_as_retryable(pool: PgPool) {
    let store = Store::from_pool(pool);
    let id = deliver_to(&store, "/always500").await;

    assert_eq!(
        recorded_class(&store, id).await,
        "retryable",
        "a 500 is the endpoint being broken, not the request being wrong"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_missing_endpoint_is_recorded_as_permanent(pool: PgPool) {
    let store = Store::from_pool(pool);
    // No route is registered at this path, so the receiver answers 404.
    let id = deliver_to(&store, "/no-such-route").await;

    assert_eq!(
        recorded_class(&store, id).await,
        "permanent",
        "a 404 means the URL is wrong, and retrying it for hours would be waste"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_success_is_recorded_as_success(pool: PgPool) {
    let store = Store::from_pool(pool);
    let id = deliver_to(&store, "/verify").await;

    assert_eq!(recorded_class(&store, id).await, "success");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn an_unreachable_host_is_retryable(pool: PgPool) {
    let store = Store::from_pool(pool);
    let event_type = format!("classify.{}", Uuid::new_v4());
    // Port 1 on loopback: nothing listens, so the connection is refused rather than
    // answered. That is a transport failure, not a status code, which is the other
    // half of the classifier.
    store
        .create_endpoint(
            "http://127.0.0.1:1/unreachable",
            "whsec_classify_test",
            std::slice::from_ref(&event_type),
        )
        .await
        .expect("endpoint");
    let accepted = store
        .insert_event_and_fan_out(&event_type, br#"{}"#)
        .await
        .expect("insert");

    let pool = Pool::new(
        store.clone(),
        PoolConfig {
            workers: 1,
            batch_size: 1,
            idle_poll: Duration::from_millis(10),
            shutdown_deadline: Duration::from_secs(5),
        },
    );
    assert_eq!(pool.run_once().await.expect("run"), 1);

    assert_eq!(
        recorded_class(&store, accepted.delivery_ids[0]).await,
        "retryable",
        "a refused connection is usually a restart, and giving up on it loses a delivery"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_outcome_carries_the_class_to_the_caller(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_classify_test");
    let addr = receiver.spawn().await;
    let event_type = format!("classify.{}", Uuid::new_v4());
    store
        .create_endpoint(
            &format!("http://{addr}/always500"),
            "whsec_classify_test",
            std::slice::from_ref(&event_type),
        )
        .await
        .expect("endpoint");
    let accepted = store
        .insert_event_and_fan_out(&event_type, br#"{}"#)
        .await
        .expect("insert");

    let sender = relay_dispatcher::Sender::new(store.clone());
    let outcome = sender
        .deliver_by_id(accepted.delivery_ids[0])
        .await
        .expect("deliver")
        .expect("attempted");

    // #11 reads this to decide whether to reschedule, so it has to be visible on the
    // returned value and not only in the database.
    match outcome {
        Outcome::Failed { class, status, .. } => {
            assert_eq!(class, Class::Retryable);
            assert_eq!(status, Some(500));
        }
        other => panic!("expected a failure, got {other:?}"),
    }
}
