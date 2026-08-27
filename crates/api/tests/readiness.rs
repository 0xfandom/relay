//! `/readyz` against a real database, a real heartbeat and a real queue.
//!
//! The judgement itself is unit-tested in `relay_api::readiness`, where it costs
//! microseconds. What cannot be tested there is whether the three facts are read
//! correctly — whether the heartbeat the dispatcher writes is the row the API looks
//! for, and whether a delivery sitting past its due date actually shows up as
//! lateness. Those are the two joins between processes, and a mistake in either one
//! produces an endpoint that answers confidently and means nothing.
//!
//! Requires Postgres: `docker compose up -d`.

use std::{net::SocketAddr, time::Duration};

use relay_api::{AppState, readiness::Thresholds, router};
use relay_store::Store;
use serde_json::Value;
use sqlx::PgPool;

async fn serve(store: Store, readiness: Thresholds) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(AppState {
        readiness,
        ..AppState::permissive(store)
    });
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

async fn get(addr: SocketAddr, path: &str) -> (u16, Value) {
    let resp = reqwest::get(format!("http://{addr}{path}"))
        .await
        .expect("get");
    let status = resp.status().as_u16();
    let body = resp.json::<Value>().await.unwrap_or(Value::Null);
    (status, body)
}

/// A delivery already past its due time by `late`.
///
/// Written through SQL with `now()` rather than a timestamp computed here, because
/// the whole measurement happens inside Postgres. A due time stamped from this
/// process's clock would make the test depend on the host and the container agreeing,
/// which they do not have to.
async fn overdue_delivery(store: &Store, late: Duration) {
    store
        .create_endpoint("https://example.com/hook", "whsec_readiness", &[])
        .await
        .expect("endpoint");
    let accepted = store
        .insert_event_and_fan_out("thing.happened", br#"{"a":1}"#)
        .await
        .expect("event");
    let delivery = accepted.delivery_ids[0];

    sqlx::query(
        "UPDATE deliveries SET next_attempt_at = now() - make_interval(secs => $1) WHERE id = $2",
    )
    .bind(late.as_secs_f64())
    .bind(delivery)
    .execute(store.pool())
    .await
    .expect("backdate");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn liveness_does_not_depend_on_anything_shared(pool: PgPool) {
    // The distinction that keeps a database blip from restarting every replica at
    // once: `/healthz` answers for this process and nothing else.
    let addr = serve(Store::from_pool(pool), Thresholds::default()).await;
    let resp = reqwest::get(format!("http://{addr}/healthz"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_dispatcher_that_never_started_is_not_ready(pool: PgPool) {
    // The gap a queue-depth check cannot see. The queue is empty, the database is
    // fine, and nothing in the system is capable of delivering a webhook.
    let addr = serve(Store::from_pool(pool), Thresholds::default()).await;
    let (status, body) = get(addr, "/readyz").await;
    assert_eq!(status, 503);
    assert_eq!(body["ready"], false);
    assert_eq!(body["database"]["status"], "pass");
    assert_eq!(body["dispatcher"]["status"], "fail");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_fresh_heartbeat_and_an_empty_queue_is_ready(pool: PgPool) {
    let store = Store::from_pool(pool);
    store
        .heartbeat(relay_store::HEARTBEAT_DISPATCHER)
        .await
        .unwrap();

    let addr = serve(store, Thresholds::default()).await;
    let (status, body) = get(addr, "/readyz").await;
    assert_eq!(status, 200);
    assert_eq!(body["ready"], true);
    assert_eq!(body["queue"]["status"], "pass");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_beating_dispatcher_with_a_stalled_queue_is_not_ready(pool: PgPool) {
    // The case the milestone singles out. The process is alive and looping; the queue
    // is not moving. Liveness alone reports this as healthy.
    let store = Store::from_pool(pool);
    store
        .heartbeat(relay_store::HEARTBEAT_DISPATCHER)
        .await
        .unwrap();
    overdue_delivery(&store, Duration::from_secs(600)).await;

    let addr = serve(store, Thresholds::default()).await;
    let (status, body) = get(addr, "/readyz").await;
    assert_eq!(status, 503);
    assert_eq!(body["dispatcher"]["status"], "pass", "the process is fine");
    assert_eq!(body["queue"]["status"], "fail", "the work is not");
    assert!(
        body["queue"]["detail"]
            .as_str()
            .unwrap()
            .contains("past due"),
        "the body says what is wrong: {body}"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_delivery_waiting_for_a_backoff_is_not_a_stall(pool: PgPool) {
    // The false positive that would make this endpoint useless. A retry scheduled for
    // an hour from now is a large, old, entirely healthy queue — and every deliberate
    // wait in Relay looks exactly like this one.
    let store = Store::from_pool(pool);
    store
        .heartbeat(relay_store::HEARTBEAT_DISPATCHER)
        .await
        .unwrap();

    store
        .create_endpoint("https://example.com/hook", "whsec_backoff", &[])
        .await
        .expect("endpoint");
    let accepted = store
        .insert_event_and_fan_out("thing.happened", br#"{"a":1}"#)
        .await
        .expect("event");

    sqlx::query("UPDATE deliveries SET next_attempt_at = now() + interval '1 hour' WHERE id = $1")
        .bind(accepted.delivery_ids[0])
        .execute(store.pool())
        .await
        .unwrap();

    let addr = serve(store, Thresholds::default()).await;
    let (status, body) = get(addr, "/readyz").await;
    assert_eq!(status, 200, "a scheduled retry is not a stall: {body}");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn lateness_within_the_threshold_is_tolerated(pool: PgPool) {
    // A burst being worked through. Late, because the workers are behind, but behind
    // is not stalled — and an endpoint that fails here would remove capacity at the
    // exact moment capacity is short.
    let store = Store::from_pool(pool);
    store
        .heartbeat(relay_store::HEARTBEAT_DISPATCHER)
        .await
        .unwrap();
    overdue_delivery(&store, Duration::from_secs(5)).await;

    let addr = serve(store, Thresholds::default()).await;
    let (status, _) = get(addr, "/readyz").await;
    assert_eq!(status, 200);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn readiness_recovers_once_the_queue_drains(pool: PgPool) {
    // Not a one-way door. A `503` an orchestrator never withdraws is a node that
    // stays out of rotation until somebody restarts it by hand.
    let store = Store::from_pool(pool);
    store
        .heartbeat(relay_store::HEARTBEAT_DISPATCHER)
        .await
        .unwrap();
    overdue_delivery(&store, Duration::from_secs(600)).await;

    let addr = serve(store.clone(), Thresholds::default()).await;
    assert_eq!(get(addr, "/readyz").await.0, 503);

    sqlx::query("UPDATE deliveries SET status = 'succeeded'")
        .execute(store.pool())
        .await
        .unwrap();

    assert_eq!(get(addr, "/readyz").await.0, 200);
}
