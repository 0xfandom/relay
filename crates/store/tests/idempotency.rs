//! Duplicate ingests resolve to one event.
//!
//! The substance is the race. A producer whose request timed out retries, and the
//! retry can arrive while the original is still being processed — so the two
//! requests are concurrent, both find no key, and both try to claim it. Exactly one
//! may win, and the loser must not learn about it through a `5xx`: it would retry,
//! hit the same race, and fail the same way forever.
//!
//! Requires Postgres: `docker compose up -d`.

use std::time::Duration;

use relay_domain::idempotency::digest;
use relay_store::{Store, StoreError};
use sqlx::PgPool;
use tokio::task::JoinSet;
use uuid::Uuid;

/// An endpoint subscribed to everything, so every event fans out to one delivery.
async fn endpoint(store: &Store) -> Uuid {
    store
        .create_endpoint("https://example.com/hook", "whsec_idem_test", &[])
        .await
        .expect("endpoint")
        .id
}

async fn deliveries_for(store: &Store, event_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE event_id = $1")
        .bind(event_id)
        .fetch_one(store.pool())
        .await
        .expect("count")
}

async fn event_count(store: &Store) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM events")
        .fetch_one(store.pool())
        .await
        .expect("count")
}

#[sqlx::test(migrations = "./migrations")]
async fn a_hundred_concurrent_duplicates_create_one_event(pool: PgPool) {
    let store = Store::from_pool(pool);
    endpoint(&store).await;

    let body = br#"{"order":123,"paid":true}"#;
    let key = "order-123-paid";
    let d = digest("order.paid", body);

    // All hundred fired without waiting for each other, which is the only way to
    // exercise the window where two transactions are both mid-flight.
    let mut tasks = JoinSet::new();
    for _ in 0..100 {
        let store = store.clone();
        tasks.spawn(async move {
            store
                .insert_event_idempotent("order.paid", body, key, &d)
                .await
        });
    }

    let mut created = 0;
    let mut responses = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        // Not `expect`: a task that panicked would otherwise be reported as a
        // failure of whatever assertion happens to run next.
        let result = joined.expect("task did not panic");
        let ingested = result.expect("no request may fail — a 5xx here is the bug");
        if !ingested.replayed {
            created += 1;
        }
        responses.push(ingested.response);
    }

    assert_eq!(created, 1, "exactly one request may create the event");
    assert_eq!(event_count(&store).await, 1, "one event row");

    // Byte-identical, not merely equivalent. A caller that stores the returned
    // event id and compares it on retry has to get the same answer.
    let first = &responses[0];
    assert!(
        responses.iter().all(|r| r == first),
        "every response must be identical"
    );

    // And the losers rolled back their own work rather than leaving it behind.
    let deliveries: i64 = sqlx::query_scalar("SELECT count(*) FROM deliveries")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(
        deliveries, 1,
        "the losing transactions left no delivery rows"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn a_duplicate_long_after_the_original_returns_the_original(pool: PgPool) {
    let store = Store::from_pool(pool);
    endpoint(&store).await;

    let body = br#"{"order":7}"#;
    let d = digest("order.paid", body);

    let first = store
        .insert_event_idempotent("order.paid", body, "k1", &d)
        .await
        .expect("first");
    assert!(!first.replayed);

    let second = store
        .insert_event_idempotent("order.paid", body, "k1", &d)
        .await
        .expect("second");

    assert!(second.replayed, "the second request created nothing");
    assert_eq!(second.event_id, first.event_id);
    assert_eq!(second.response, first.response);
    assert_eq!(event_count(&store).await, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn different_keys_create_different_events(pool: PgPool) {
    let store = Store::from_pool(pool);
    endpoint(&store).await;

    // The other half. Deduplicating everything is not idempotency, it is data loss:
    // two genuinely different events must survive as two.
    let body = br#"{"order":7}"#;
    let d = digest("order.paid", body);

    let a = store
        .insert_event_idempotent("order.paid", body, "k1", &d)
        .await
        .expect("a");
    let b = store
        .insert_event_idempotent("order.paid", body, "k2", &d)
        .await
        .expect("b");

    assert_ne!(a.event_id, b.event_id);
    assert_eq!(event_count(&store).await, 2);
}

#[sqlx::test(migrations = "./migrations")]
async fn reusing_a_key_for_a_different_request_is_refused(pool: PgPool) {
    let store = Store::from_pool(pool);
    endpoint(&store).await;

    let first = br#"{"amount":100}"#;
    store
        .insert_event_idempotent("order.paid", first, "k1", &digest("order.paid", first))
        .await
        .expect("first");

    // Same key, different body. Without the fingerprint this would be answered with
    // the first event's id and the second event would vanish — a silent loss that
    // looks like a success to the caller.
    let second = br#"{"amount":999}"#;
    let err = store
        .insert_event_idempotent("order.paid", second, "k1", &digest("order.paid", second))
        .await
        .expect_err("must not be treated as a duplicate");
    assert!(matches!(err, StoreError::IdempotencyKeyReused), "{err}");

    // The same key with a different event type is equally a different request.
    let err = store
        .insert_event_idempotent(
            "order.refunded",
            first,
            "k1",
            &digest("order.refunded", first),
        )
        .await
        .expect_err("must not be treated as a duplicate");
    assert!(matches!(err, StoreError::IdempotencyKeyReused), "{err}");

    assert_eq!(event_count(&store).await, 1, "nothing extra was written");
}

#[sqlx::test(migrations = "./migrations")]
async fn an_ingest_without_a_key_is_never_deduplicated(pool: PgPool) {
    let store = Store::from_pool(pool);
    endpoint(&store).await;

    // The key is opt-in. A producer that sends the same body twice deliberately —
    // two identical orders a second apart — must get two events, because we cannot
    // tell that apart from a retry and only the producer can.
    let body = br#"{"order":7}"#;
    store
        .insert_event_and_fan_out("order.paid", body)
        .await
        .unwrap();
    store
        .insert_event_and_fan_out("order.paid", body)
        .await
        .unwrap();

    assert_eq!(event_count(&store).await, 2);
}

#[sqlx::test(migrations = "./migrations")]
async fn pruning_removes_expired_keys_and_leaves_fresh_ones(pool: PgPool) {
    let store = Store::from_pool(pool);
    endpoint(&store).await;

    let body = br#"{"order":7}"#;
    let d = digest("order.paid", body);
    let old = store
        .insert_event_idempotent("order.paid", body, "old", &d)
        .await
        .expect("old");
    store
        .insert_event_idempotent("order.paid", body, "fresh", &d)
        .await
        .expect("fresh");

    // Backdated rather than slept through: the window is a day.
    sqlx::query(
        "UPDATE idempotency_keys SET created_at = now() - interval '25 hours' WHERE key = 'old'",
    )
    .execute(store.pool())
    .await
    .unwrap();

    let pruned = store
        .prune_idempotency_keys(Duration::from_secs(24 * 60 * 60))
        .await
        .expect("prune");
    assert_eq!(pruned, 1);

    // The event itself survives. Pruning a key forgets that a request was made, not
    // what it produced — the deliveries it created are still owed.
    assert_eq!(event_count(&store).await, 2);
    assert!(
        deliveries_for(&store, old.event_id).await > 0,
        "the pruned key's deliveries are untouched"
    );

    // And the fresh key still deduplicates.
    let again = store
        .insert_event_idempotent("order.paid", body, "fresh", &d)
        .await
        .expect("fresh again");
    assert!(again.replayed);
}

#[sqlx::test(migrations = "./migrations")]
async fn a_duplicate_after_the_window_creates_a_second_event(pool: PgPool) {
    let store = Store::from_pool(pool);
    endpoint(&store).await;

    // The cost of the retention window, asserted rather than merely documented. A
    // producer retrying a day later gets a second event, and that is the deliberate
    // trade for not keeping every key forever.
    let body = br#"{"order":7}"#;
    let d = digest("order.paid", body);
    let first = store
        .insert_event_idempotent("order.paid", body, "k1", &d)
        .await
        .expect("first");

    sqlx::query("UPDATE idempotency_keys SET created_at = now() - interval '25 hours'")
        .execute(store.pool())
        .await
        .unwrap();
    store
        .prune_idempotency_keys(Duration::from_secs(24 * 60 * 60))
        .await
        .expect("prune");

    let second = store
        .insert_event_idempotent("order.paid", body, "k1", &d)
        .await
        .expect("second");
    assert!(!second.replayed);
    assert_ne!(second.event_id, first.event_id);
    assert_eq!(event_count(&store).await, 2);
}
