//! Expired idempotency keys are swept.
//!
//! Nothing here is load-bearing for correctness: a key that is never pruned still
//! deduplicates perfectly. What the pruner prevents is a table that grows as fast
//! as the event table and never shrinks. So the tests worth having are the ones
//! that check it deletes *only* what it should — a pruner that is too eager
//! silently reopens the duplicate window it exists to support.
//!
//! Requires Postgres: `docker compose up -d`.

use std::time::Duration;

use relay_dispatcher::{Pruner, PrunerConfig};
use relay_domain::idempotency::digest;
use relay_store::Store;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

const DAY: Duration = Duration::from_secs(24 * 60 * 60);

async fn ingest(store: &Store, key: &str) {
    let body = br#"{"order":7}"#;
    store
        .insert_event_idempotent("order.paid", body, key, &digest("order.paid", body))
        .await
        .expect("ingest");
}

async fn key_count(store: &Store) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM idempotency_keys")
        .fetch_one(store.pool())
        .await
        .expect("count")
}

async fn backdate(store: &Store, key: &str, hours: i64) {
    sqlx::query("UPDATE idempotency_keys SET created_at = now() - make_interval(hours => $2) WHERE key = $1")
        .bind(key)
        .bind(hours as i32)
        .execute(store.pool())
        .await
        .expect("backdate");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn only_keys_past_the_window_are_deleted(pool: PgPool) {
    let store = Store::from_pool(pool);
    ingest(&store, "expired").await;
    ingest(&store, "fresh").await;
    // Just inside the window. The boundary is the whole point: a pruner that is off
    // by an hour deletes keys a producer may still retry against.
    ingest(&store, "nearly").await;

    backdate(&store, "expired", 25).await;
    backdate(&store, "nearly", 23).await;

    let pruner = Pruner::new(
        store.clone(),
        PrunerConfig {
            idempotency: DAY,
            interval: Duration::from_secs(3600),
            ..PrunerConfig::default()
        },
    );

    assert_eq!(pruner.prune_once().await.expect("prune").keys_pruned, 1);
    assert_eq!(pruner.pruned(), 1);
    assert_eq!(key_count(&store).await, 2);

    // A second sweep with nothing to do is not an error and does not double-count.
    assert_eq!(pruner.prune_once().await.expect("prune").keys_pruned, 0);
    assert_eq!(pruner.pruned(), 1);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn pruning_a_key_leaves_its_event_and_deliveries(pool: PgPool) {
    let store = Store::from_pool(pool);
    store
        .create_endpoint("https://example.com/hook", "whsec_prune", &[])
        .await
        .expect("endpoint");
    ingest(&store, "expired").await;
    backdate(&store, "expired", 25).await;

    let pruner = Pruner::new(store.clone(), PrunerConfig::default());
    assert_eq!(pruner.prune_once().await.expect("prune").keys_pruned, 1);

    // Forgetting that a request was made is not the same as undoing it. The
    // deliveries it created are still owed to the endpoint.
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM events")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let deliveries: i64 = sqlx::query_scalar("SELECT count(*) FROM deliveries")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(events, 1);
    assert_eq!(deliveries, 1);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_loop_stops_when_cancelled(pool: PgPool) {
    let store = Store::from_pool(pool);
    ingest(&store, "expired").await;
    backdate(&store, "expired", 25).await;

    // A long interval, so the test only passes if cancellation interrupts the sleep
    // rather than the loop happening to come round again.
    let pruner = Pruner::new(
        store.clone(),
        PrunerConfig {
            idempotency: DAY,
            interval: Duration::from_secs(3600),
            ..PrunerConfig::default()
        },
    );

    let cancel = CancellationToken::new();
    let handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            pruner.run(cancel).await;
            pruner.pruned()
        })
    };

    tokio::time::sleep(Duration::from_millis(100)).await;
    cancel.cancel();

    let pruned = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("the loop must not outlive its cancellation")
        .expect("task did not panic");

    // It did its first pass before parking on the sleep.
    assert_eq!(pruned, 1);
    assert_eq!(key_count(&store).await, 0);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_default_window_is_a_day(pool: PgPool) {
    // Asserted rather than assumed, because it is a documented part of the contract:
    // a duplicate arriving after this window creates a second event.
    let _ = Store::from_pool(pool);
    assert_eq!(PrunerConfig::default().idempotency, DAY);
}
