//! The dispatcher saying it is still here.
//!
//! Small, and worth testing anyway, because the failure mode is silent in a
//! particular way: nothing in the dispatcher reads this row. A heartbeat that beat
//! under the wrong name, or that beat once and stopped, or that only beat after its
//! first sleep, would look completely healthy from this side. The API is the only
//! thing that would notice, and by then it is reporting a false outage.
//!
//! Requires Postgres: `docker compose up -d`.

use std::time::Duration;

use relay_dispatcher::Heartbeat;
use relay_store::Store;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

#[sqlx::test(migrations = "../store/migrations")]
async fn nothing_has_beaten_before_the_dispatcher_starts(pool: PgPool) {
    // The state a cold start is in, and the reason "no row" must read as stale rather
    // than as fine: this is exactly what a dispatcher that has never once managed to
    // write looks like.
    let store = Store::from_pool(pool);
    assert_eq!(
        store
            .heartbeat_age(relay_store::HEARTBEAT_DISPATCHER)
            .await
            .unwrap(),
        None
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_first_beat_lands_before_the_first_sleep(pool: PgPool) {
    // Sleeping first would add a full interval of self-inflicted unreadiness to every
    // deploy. Proven by cancelling before the loop ever runs: a beat still lands.
    let store = Store::from_pool(pool);
    let cancel = CancellationToken::new();
    cancel.cancel();

    Heartbeat::new(store.clone(), Duration::from_secs(3600))
        .run(cancel)
        .await;

    let age = store
        .heartbeat_age(relay_store::HEARTBEAT_DISPATCHER)
        .await
        .unwrap()
        .expect("one beat, despite being cancelled before the first sleep");
    assert!(age >= 0.0, "stamped by the database, not from the future");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn beating_again_refreshes_rather_than_duplicates(pool: PgPool) {
    // One row per component. An insert-only heartbeat would grow forever and make
    // "how old is the newest" a scan instead of a lookup.
    let store = Store::from_pool(pool);
    for _ in 0..3 {
        store
            .heartbeat(relay_store::HEARTBEAT_DISPATCHER)
            .await
            .unwrap();
    }

    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM relay_heartbeat")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(rows, 1);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_later_beat_makes_the_age_smaller(pool: PgPool) {
    // What "fresh" actually depends on. If the upsert wrote the row but left `at`
    // alone, the age would climb forever and the API would declare a perfectly
    // healthy dispatcher dead.
    let store = Store::from_pool(pool);
    sqlx::query(
        "INSERT INTO relay_heartbeat (component, at)
         VALUES ($1, now() - interval '1 hour')",
    )
    .bind(relay_store::HEARTBEAT_DISPATCHER)
    .execute(store.pool())
    .await
    .unwrap();

    let stale = store
        .heartbeat_age(relay_store::HEARTBEAT_DISPATCHER)
        .await
        .unwrap()
        .unwrap();
    assert!(stale > 3000.0, "an hour old: {stale}");

    store
        .heartbeat(relay_store::HEARTBEAT_DISPATCHER)
        .await
        .unwrap();

    let fresh = store
        .heartbeat_age(relay_store::HEARTBEAT_DISPATCHER)
        .await
        .unwrap()
        .unwrap();
    assert!(fresh < 60.0, "refreshed, not merely present: {fresh}");
}
