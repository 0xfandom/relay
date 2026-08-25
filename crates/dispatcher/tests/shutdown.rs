//! Tests for graceful shutdown.
//!
//! Without this, every deploy is a small outage. The process is killed holding
//! claimed work, those rows sit `inflight` until their lease expires, and the reaper
//! rescues them half a minute later — so a routine restart delays some customers by
//! the whole lease TTL, and repeats on the next deploy.
//!
//! Two things have to be true at once. Deliveries already sent must be allowed to
//! finish, because a request that has gone out may well have arrived and dropping it
//! only throws away the answer. And shutdown must still end, even when an endpoint
//! never replies.
//!
//! Requires Postgres: `docker compose up -d`.

use std::{sync::Arc, time::Duration};

use relay_dispatcher::{Pool, PoolConfig, Reaper, ReaperConfig, SenderConfig};
use relay_domain::url_guard::Policy;
use relay_store::Store;
use relay_testkit::Receiver;
use sqlx::PgPool;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

fn config(shutdown_deadline: Duration) -> PoolConfig {
    PoolConfig {
        workers: 8,
        batch_size: 8,
        idle_poll: Duration::from_millis(10),
        shutdown_deadline,
    }
}

async fn seed(store: &Store, receiver: &Receiver, path: &str, n: usize) {
    let addr = receiver.spawn().await;
    let event_type = format!("shutdown.{path}");
    store
        .create_endpoint(
            &format!("http://{addr}{path}"),
            "whsec_shutdown_test",
            std::slice::from_ref(&event_type),
        )
        .await
        .expect("endpoint");

    for i in 0..n {
        store
            .insert_event_and_fan_out(&event_type, format!(r#"{{"n":{i}}}"#).as_bytes())
            .await
            .expect("insert");
    }
}

async fn count_with_status(store: &Store, status: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE status = $1")
        .bind(status)
        .fetch_one(store.pool())
        .await
        .expect("count")
}

async fn wait_until(within: Duration, label: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out after {within:?} waiting for: {label}");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_clean_shutdown_leaves_nothing_inflight(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_shutdown_test");
    seed(&store, &receiver, "/verify?ms=0", 8).await;

    let pool = Arc::new(Pool::with_config(
        store.clone(),
        config(Duration::from_secs(10)),
        local(),
    ));
    let cancel = CancellationToken::new();
    let handle = tokio::spawn({
        let (pool, cancel) = (pool.clone(), cancel.clone());
        async move { pool.run(cancel).await }
    });

    wait_until(Duration::from_secs(10), "all deliveries", || {
        receiver.hits() == 8
    })
    .await;

    cancel.cancel();
    handle.await.expect("pool loop");

    // The point of the whole issue. Anything left `inflight` is work the reaper has
    // to rescue, which is a delay measured in lease TTLs on every single deploy.
    assert_eq!(
        count_with_status(&store, "inflight").await,
        0,
        "a clean shutdown left rows inflight"
    );
    assert_eq!(count_with_status(&store, "succeeded").await, 8);
    assert_eq!(count_with_status(&store, "pending").await, 0);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn in_flight_deliveries_finish_rather_than_being_cut_off(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_shutdown_test");
    // Slow enough that cancellation lands squarely in the middle of every request.
    seed(&store, &receiver, "/slow?ms=600", 4).await;

    let pool = Arc::new(Pool::with_config(
        store.clone(),
        config(Duration::from_secs(10)),
        local(),
    ));
    let cancel = CancellationToken::new();
    let handle = tokio::spawn({
        let (pool, cancel) = (pool.clone(), cancel.clone());
        async move { pool.run(cancel).await }
    });

    // All four have been sent and none can have answered yet.
    wait_until(Duration::from_secs(5), "all four requests to start", || {
        receiver.hits() == 4
    })
    .await;
    assert_eq!(count_with_status(&store, "inflight").await, 4);

    let started = Instant::now();
    cancel.cancel();
    handle.await.expect("pool loop");

    // Shutdown waited for them instead of dropping them. Cutting them off would
    // leave four rows to be reaped and re-sent to an endpoint that already had them.
    assert!(
        started.elapsed() >= Duration::from_millis(400),
        "shutdown returned in {:?}, too fast to have waited for in-flight requests",
        started.elapsed()
    );
    assert_eq!(count_with_status(&store, "succeeded").await, 4);
    assert_eq!(count_with_status(&store, "inflight").await, 0);
    assert_eq!(receiver.hits(), 4, "nothing was re-sent");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn shutdown_completes_within_the_deadline_when_an_endpoint_never_answers(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_shutdown_test");
    seed(&store, &receiver, "/slow?ms=30000", 4).await;

    let deadline = Duration::from_millis(300);
    let pool = Arc::new(Pool::with_config(store.clone(), config(deadline), local()));
    let cancel = CancellationToken::new();
    let handle = tokio::spawn({
        let (pool, cancel) = (pool.clone(), cancel.clone());
        async move { pool.run(cancel).await }
    });

    wait_until(Duration::from_secs(5), "requests to start", || {
        receiver.hits() == 4
    })
    .await;

    let started = Instant::now();
    cancel.cancel();
    handle.await.expect("pool loop");
    let elapsed = started.elapsed();

    // Bounded, not indefinite. Without a deadline the process would sit here until
    // the request timeout, and an orchestrator would eventually SIGKILL it — the
    // ungraceful shutdown this exists to avoid.
    assert!(
        elapsed < deadline * 10,
        "shutdown took {elapsed:?} against a deadline of {deadline:?}"
    );

    // These are abandoned rather than resolved, which is correct: whether they
    // arrived is unknown. They stay `inflight` for the reaper.
    assert_eq!(count_with_status(&store, "inflight").await, 4);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_delivery_claimed_but_not_yet_started_is_released(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_shutdown_test");
    seed(&store, &receiver, "/verify", 3).await;

    // Cancelled before the loop ever runs, so the first claim happens with shutdown
    // already in progress and every row it takes has to be handed straight back.
    let pool = Pool::with_config(store.clone(), config(Duration::from_secs(5)), local());
    let cancel = CancellationToken::new();
    cancel.cancel();
    pool.run(cancel).await;

    assert_eq!(
        count_with_status(&store, "inflight").await,
        0,
        "rows claimed during shutdown must be released, not left for the reaper"
    );
    assert_eq!(receiver.hits(), 0, "nothing should have been sent");

    // Released, not consumed: still claimable, and no attempt was charged.
    let again = store.claim_batch(10, "worker-b").await.unwrap();
    assert_eq!(again.len(), 3);
    assert!(again.iter().all(|d| d.attempt == 0));
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_reaper_stops_when_cancelled(pool: PgPool) {
    let store = Store::from_pool(pool);
    let reaper = Arc::new(
        Reaper::new(
            store,
            ReaperConfig {
                lease_ttl: Duration::from_secs(30),
                // Far longer than the test will wait, so returning promptly can only
                // be cancellation and not the interval elapsing.
                interval: Duration::from_secs(3600),
            },
        )
        .expect("config"),
    );

    let cancel = CancellationToken::new();
    let handle = tokio::spawn({
        let (reaper, cancel) = (reaper.clone(), cancel.clone());
        async move { reaper.run(cancel).await }
    });

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("reaper ignored cancellation and slept out its interval")
        .expect("reaper loop");
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
