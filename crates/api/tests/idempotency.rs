//! `Idempotency-Key`, over real HTTP.
//!
//! `crates/store` tests the race against the database. What it cannot test is the
//! part the caller actually sees: the status code, the headers, and whether two
//! responses are byte-identical after passing through the whole HTTP stack.
//!
//! The acceptance criterion that matters is negative — *no request returns a `5xx`
//! as a result of losing the race*. A caller that gets a `500` will retry, hit the
//! same race, and get another `500`, so a losing request that reports itself
//! honestly is worse than useless.
//!
//! Requires Postgres: `docker compose up -d`.

use std::net::SocketAddr;

use relay_api::{AppState, router};
use relay_store::Store;
use sqlx::PgPool;

/// Serve the real router on an ephemeral port.
async fn serve(store: Store) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(AppState { store });
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

/// An endpoint subscribed to everything, so each event fans out to one delivery.
async fn endpoint(store: &Store) {
    store
        .create_endpoint("https://example.com/hook", "whsec_api_idem", &[])
        .await
        .expect("endpoint");
}

async fn event_count(store: &Store) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM events")
        .fetch_one(store.pool())
        .await
        .expect("count")
}

fn post(addr: SocketAddr) -> reqwest::RequestBuilder {
    reqwest::Client::new()
        .post(format!("http://{addr}/v1/events"))
        .header("content-type", "application/json")
        .header("relay-event-type", "order.paid")
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_hundred_concurrent_duplicates_all_succeed_identically(pool: PgPool) {
    let store = Store::from_pool(pool);
    endpoint(&store).await;
    let addr = serve(store.clone()).await;

    let body = r#"{"order":123,"paid":true}"#;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..100 {
        tasks.spawn(async move {
            let resp = post(addr)
                .header("idempotency-key", "order-123-paid")
                .body(body)
                .send()
                .await
                .expect("request");
            let status = resp.status();
            let replayed = resp
                .headers()
                .get("relay-idempotent-replay")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            (status, replayed, resp.bytes().await.expect("body"))
        });
    }

    let mut fresh = 0;
    let mut bodies = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        let (status, replayed, body) = joined.expect("task did not panic");
        assert_eq!(
            status,
            reqwest::StatusCode::ACCEPTED,
            "losing the race must never surface as an error: {}",
            String::from_utf8_lossy(&body)
        );
        match replayed.as_deref() {
            Some("false") => fresh += 1,
            Some("true") => {}
            other => panic!("missing replay header: {other:?}"),
        }
        bodies.push(body);
    }

    assert_eq!(fresh, 1, "exactly one request created the event");
    assert_eq!(event_count(&store).await, 1);

    let first = &bodies[0];
    assert!(
        bodies.iter().all(|b| b == first),
        "every caller must get the same bytes back"
    );
    // And the body is the ordinary 202 shape, not a special duplicate response.
    let parsed: serde_json::Value = serde_json::from_slice(first).expect("json");
    assert!(parsed["event_id"].is_string());
    assert_eq!(parsed["delivery_ids"].as_array().unwrap().len(), 1);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_retry_is_answered_with_the_original(pool: PgPool) {
    let store = Store::from_pool(pool);
    endpoint(&store).await;
    let addr = serve(store.clone()).await;

    let send = async |body: &'static str| {
        let resp = post(addr)
            .header("idempotency-key", "k1")
            .body(body)
            .send()
            .await
            .expect("request");
        let replayed = resp
            .headers()
            .get("relay-idempotent-replay")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        (replayed, resp.bytes().await.unwrap())
    };

    let (first_replayed, first) = send(r#"{"order":7}"#).await;
    let (second_replayed, second) = send(r#"{"order":7}"#).await;

    assert_eq!(first_replayed, "false");
    assert_eq!(second_replayed, "true");
    assert_eq!(first, second);
    assert_eq!(event_count(&store).await, 1);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn no_key_means_no_deduplication(pool: PgPool) {
    let store = Store::from_pool(pool);
    endpoint(&store).await;
    let addr = serve(store.clone()).await;

    // Opt-in, and it has to be: two identical bodies a second apart may be a retry
    // or may be two real orders, and only the producer can tell them apart.
    for _ in 0..2 {
        let resp = post(addr).body(r#"{"order":7}"#).send().await.unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::ACCEPTED);
        assert!(
            resp.headers().get("relay-idempotent-replay").is_none(),
            "the header only applies to keyed requests"
        );
    }
    assert_eq!(event_count(&store).await, 2);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn reusing_a_key_for_a_different_body_is_a_conflict(pool: PgPool) {
    let store = Store::from_pool(pool);
    endpoint(&store).await;
    let addr = serve(store.clone()).await;

    let resp = post(addr)
        .header("idempotency-key", "k1")
        .body(r#"{"amount":100}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::ACCEPTED);

    // 409, not 202. Answering with the first event's id would drop this one while
    // reporting success — a lost event that no metric would ever show.
    let resp = post(addr)
        .header("idempotency-key", "k1")
        .body(r#"{"amount":999}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);

    assert_eq!(event_count(&store).await, 1);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn an_unusable_key_is_refused_rather_than_ignored(pool: PgPool) {
    let store = Store::from_pool(pool);
    endpoint(&store).await;
    let addr = serve(store.clone()).await;

    // An empty key is what an unset template variable looks like. Ignoring it would
    // leave the caller believing their request is deduplicated when it is not.
    for key in ["", &"x".repeat(256)] {
        let resp = post(addr)
            .header("idempotency-key", key)
            .body(r#"{"order":7}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "key {key:?} should be refused"
        );
    }

    assert_eq!(event_count(&store).await, 0, "nothing was ingested");
}
