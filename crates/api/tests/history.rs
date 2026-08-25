//! "What happened to my event" as a query rather than an investigation.
//!
//! The interesting half of this is the paging. `OFFSET` is the obvious way to do it
//! and it is wrong twice over: it costs more with every page, on the largest and
//! fastest-growing table in the system, and it silently corrupts its own results
//! when rows are being inserted at the same time — which, for a delivery history,
//! is always. Both failures are tested here rather than argued about.
//!
//! Requires Postgres: `docker compose up -d`.

use std::{net::SocketAddr, time::Duration};

use relay_api::{AppState, router};
use relay_store::{AttemptResult, DeadReason, Store};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

async fn serve(store: Store) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(AppState { store });
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

/// An endpoint subscribed to one event type, so nothing else fans out to it.
async fn endpoint(store: &Store, event_type: &str) -> Uuid {
    store
        .create_endpoint(
            "https://example.com/hook",
            "whsec_history_test",
            std::slice::from_ref(&event_type.to_string()),
        )
        .await
        .expect("endpoint")
        .id
}

/// Queue `n` deliveries to whichever endpoints subscribe to `event_type`.
async fn queue(store: &Store, event_type: &str, n: usize) -> Vec<Uuid> {
    let mut ids = Vec::new();
    for i in 0..n {
        ids.extend(
            store
                .insert_event_and_fan_out(event_type, format!(r#"{{"n":{i}}}"#).as_bytes())
                .await
                .expect("insert")
                .delivery_ids,
        );
    }
    ids
}

fn ids_of(page: &Value) -> Vec<String> {
    page["items"]
        .as_array()
        .expect("items")
        .iter()
        .map(|d| d["id"].as_str().expect("id").to_string())
        .collect()
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_deliverys_whole_attempt_history_is_returned(pool: PgPool) {
    let store = Store::from_pool(pool);
    let event_type = format!("h.{}", Uuid::new_v4());
    endpoint(&store, &event_type).await;
    let ids = queue(&store, &event_type, 1).await;

    // Two attempts: one that failed and was rescheduled, one that gave up. This is
    // the shape the attempt log exists to preserve — the deliveries row holds only
    // the current state, so without the log the first attempt is simply gone.
    store
        .finish_attempt(
            ids[0],
            0,
            AttemptResult::Retry {
                delay: Duration::from_secs(30),
            },
            Some(503),
            412,
            "retryable",
            Some("HTTP 503"),
            Some("upstream unavailable"),
            "worker-a",
        )
        .await
        .expect("first attempt");
    store
        .finish_attempt(
            ids[0],
            1,
            AttemptResult::Dead {
                reason: DeadReason::AttemptsExhausted,
            },
            Some(503),
            377,
            "retryable",
            Some("HTTP 503"),
            None,
            "worker-b",
        )
        .await
        .expect("second attempt");

    let addr = serve(store).await;
    let (status, body) = get(addr, &format!("/v1/deliveries/{}", ids[0])).await;

    assert_eq!(status, 200);
    assert_eq!(body["delivery"]["status"], "dead");
    assert_eq!(body["delivery"]["dead_reason"], "attempts_exhausted");
    // Joined in, so triaging a delivery does not need a second request to find out
    // what it even was.
    assert_eq!(body["delivery"]["event_type"], event_type);

    let attempts = body["attempts"].as_array().expect("attempts");
    assert_eq!(attempts.len(), 2, "both attempts, not just the last one");
    // Everything needed to reconstruct the try: what the endpoint said, how long it
    // took, what went wrong and which process asked.
    assert_eq!(attempts[0]["attempt_no"], 0);
    assert_eq!(attempts[0]["http_status"], 503);
    assert_eq!(attempts[0]["latency_ms"], 412);
    assert_eq!(attempts[0]["error"], "HTTP 503");
    assert_eq!(attempts[0]["worker_id"], "worker-a");
    // The retry that was actually scheduled, which the deliveries row has since
    // overwritten. Without it, an attempt that was rescheduled is indistinguishable
    // from one that was the last.
    assert!(attempts[0]["next_attempt_at"].is_string());
    assert_eq!(attempts[1]["attempt_no"], 1);
    assert!(attempts[1]["next_attempt_at"].is_null());
}

#[sqlx::test(migrations = "../store/migrations")]
async fn an_unknown_delivery_is_a_404(pool: PgPool) {
    let addr = serve(Store::from_pool(pool)).await;
    let (status, _) = get(addr, &format!("/v1/deliveries/{}", Uuid::new_v4())).await;
    assert_eq!(status, 404);
}

// ------------------------------------------------------------------- the paging

#[sqlx::test(migrations = "../store/migrations")]
async fn a_page_is_resumed_from_its_own_last_row(pool: PgPool) {
    let store = Store::from_pool(pool);
    let event_type = format!("h.{}", Uuid::new_v4());
    let ep = endpoint(&store, &event_type).await;
    queue(&store, &event_type, 5).await;

    let addr = serve(store).await;
    let mut seen: Vec<String> = Vec::new();
    let mut url = format!("/v1/endpoints/{ep}/deliveries?limit=2");

    loop {
        let (status, page) = get(addr, &url).await;
        assert_eq!(status, 200);
        seen.extend(ids_of(&page));
        match page["next_cursor"].as_str() {
            Some(c) => url = format!("/v1/endpoints/{ep}/deliveries?limit=2&cursor={c}"),
            None => break,
        }
    }

    assert_eq!(seen.len(), 5);
    let mut unique = seen.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 5, "a row was returned on two different pages");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_concurrent_insert_does_not_shift_the_next_page(pool: PgPool) {
    let store = Store::from_pool(pool);
    let event_type = format!("h.{}", Uuid::new_v4());
    let ep = endpoint(&store, &event_type).await;
    queue(&store, &event_type, 4).await;

    let addr = serve(store.clone()).await;
    let (_, first) = get(addr, &format!("/v1/endpoints/{ep}/deliveries?limit=2")).await;
    let first_ids = ids_of(&first);
    let cursor = first["next_cursor"].as_str().expect("a cursor").to_string();

    // Somebody sends an event between the two requests. With `OFFSET 2` this shifts
    // every remaining row down by one, and the second page repeats the last row of
    // the first while the oldest delivery is never seen at all. Paging from a
    // position cannot be moved by an insert somewhere else in the ordering.
    queue(&store, &event_type, 1).await;

    let (_, second) = get(
        addr,
        &format!("/v1/endpoints/{ep}/deliveries?limit=2&cursor={cursor}"),
    )
    .await;
    let second_ids = ids_of(&second);

    assert_eq!(second_ids.len(), 2);
    for id in &second_ids {
        assert!(
            !first_ids.contains(id),
            "the insert pushed a row from page one onto page two"
        );
    }
}

#[sqlx::test(migrations = "../store/migrations")]
async fn rows_sharing_a_timestamp_are_each_returned_once(pool: PgPool) {
    let store = Store::from_pool(pool);
    let event_type = format!("h.{}", Uuid::new_v4());
    let ep = endpoint(&store, &event_type).await;
    let ids = queue(&store, &event_type, 6).await;

    // Not contrived: a fan-out writes every delivery for one event in a single
    // transaction, so a busy endpoint routinely has rows to the microsecond. Forced
    // here because six separate inserts would each get their own.
    sqlx::query("UPDATE deliveries SET created_at = now() WHERE endpoint_id = $1")
        .bind(ep)
        .execute(store.pool())
        .await
        .expect("collapse the timestamps");

    let addr = serve(store).await;
    let mut seen: Vec<String> = Vec::new();
    // One at a time, so every page boundary falls inside the tied group.
    let mut url = format!("/v1/endpoints/{ep}/deliveries?limit=1");
    for _ in 0..10 {
        let (_, page) = get(addr, &url).await;
        seen.extend(ids_of(&page));
        match page["next_cursor"].as_str() {
            Some(c) => url = format!("/v1/endpoints/{ep}/deliveries?limit=1&cursor={c}"),
            None => break,
        }
    }

    // Without the `id` tiebreak the cursor cannot say *which* of the tied rows it
    // stopped at, so the page either repeats the whole group forever or skips it.
    let mut unique = seen.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(seen.len(), 6, "a tied row was returned twice");
    assert_eq!(unique.len(), 6, "a tied row was skipped");
    for id in &ids {
        assert!(unique.contains(&id.to_string()), "{id} was never returned");
    }
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_broken_cursor_is_rejected_rather_than_ignored(pool: PgPool) {
    let store = Store::from_pool(pool);
    let event_type = format!("h.{}", Uuid::new_v4());
    let ep = endpoint(&store, &event_type).await;
    queue(&store, &event_type, 2).await;
    let addr = serve(store).await;

    // Ignoring it would silently hand back page one, and a client paging in a loop
    // would never terminate while looking like it was making progress.
    let (status, _) = get(
        addr,
        &format!("/v1/endpoints/{ep}/deliveries?cursor=not-a-cursor"),
    )
    .await;
    assert_eq!(status, 400);
}

// ---------------------------------------------------------------- the filtering

#[sqlx::test(migrations = "../store/migrations")]
async fn the_status_filter_narrows_the_page(pool: PgPool) {
    let store = Store::from_pool(pool);
    let event_type = format!("h.{}", Uuid::new_v4());
    let ep = endpoint(&store, &event_type).await;
    let ids = queue(&store, &event_type, 3).await;

    store
        .finish_attempt(
            ids[0],
            0,
            AttemptResult::Dead {
                reason: DeadReason::PermanentFailure,
            },
            Some(404),
            5,
            "permanent",
            Some("HTTP 404"),
            None,
            "worker-a",
        )
        .await
        .expect("kill one");

    let addr = serve(store).await;
    let (status, page) = get(addr, &format!("/v1/endpoints/{ep}/deliveries?status=dead")).await;
    assert_eq!(status, 200);
    assert_eq!(ids_of(&page), vec![ids[0].to_string()]);

    let (_, page) = get(
        addr,
        &format!("/v1/endpoints/{ep}/deliveries?status=pending"),
    )
    .await;
    assert_eq!(page["count"], 2);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn an_unknown_status_is_rejected_rather_than_answered_with_nothing(pool: PgPool) {
    let store = Store::from_pool(pool);
    let event_type = format!("h.{}", Uuid::new_v4());
    let ep = endpoint(&store, &event_type).await;
    let addr = serve(store).await;

    // An empty page is the most reassuring possible answer, and it is the wrong one
    // to give somebody who has just typed `failed` instead of `dead`.
    let (status, body) = get(
        addr,
        &format!("/v1/endpoints/{ep}/deliveries?status=failed"),
    )
    .await;
    assert_eq!(status, 400);
    assert!(
        body["error"].as_str().expect("an error").contains("dead"),
        "the rejection should name the values that would work"
    );
}

// ----------------------------------------------------------------- the scoping

#[sqlx::test(migrations = "../store/migrations")]
async fn one_endpoints_history_never_contains_anothers(pool: PgPool) {
    let store = Store::from_pool(pool);
    let mine = format!("h.{}", Uuid::new_v4());
    let theirs = format!("h.{}", Uuid::new_v4());
    let ep_mine = endpoint(&store, &mine).await;
    let ep_theirs = endpoint(&store, &theirs).await;
    let my_ids = queue(&store, &mine, 2).await;
    let their_ids = queue(&store, &theirs, 3).await;

    let addr = serve(store).await;

    // The scope lives in the store's `WHERE` clause rather than in the handler, so a
    // route added later cannot forget to apply it. The endpoint is Relay's ownership
    // boundary today; when tenants land the tenant predicate joins it there.
    let (_, page) = get(addr, &format!("/v1/endpoints/{ep_mine}/deliveries")).await;
    let seen = ids_of(&page);
    assert_eq!(seen.len(), 2);
    for id in &their_ids {
        assert!(
            !seen.contains(&id.to_string()),
            "{id} leaked across endpoints"
        );
    }

    let (_, page) = get(addr, &format!("/v1/endpoints/{ep_theirs}/deliveries")).await;
    let seen = ids_of(&page);
    assert_eq!(seen.len(), 3);
    for id in &my_ids {
        assert!(
            !seen.contains(&id.to_string()),
            "{id} leaked across endpoints"
        );
    }
}

#[sqlx::test(migrations = "../store/migrations")]
async fn an_unknown_endpoint_is_a_404_not_an_empty_page(pool: PgPool) {
    let addr = serve(Store::from_pool(pool)).await;
    let (status, _) = get(
        addr,
        &format!("/v1/endpoints/{}/deliveries", Uuid::new_v4()),
    )
    .await;
    assert_eq!(status, 404, "an empty page would read as 'no failures'");
}
