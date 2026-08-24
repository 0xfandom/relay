//! Tests for the worker pool.
//!
//! Three properties, and they pull against each other. The pool must be genuinely
//! concurrent, or there was no point building it. It must never exceed its
//! configured bound, or "concurrency limit" means nothing. And a slow endpoint must
//! not delay a healthy one, which is the property the whole design exists for and
//! the only one a naive implementation gets wrong.
//!
//! Each test runs against its own database. `claim_batch` takes whatever is pending
//! in the entire table, so pool tests sharing a database would claim each other's
//! rows regardless of how carefully their event types were kept apart.
//!
//! Requires Postgres: `docker compose up -d`.

use std::{sync::Arc, time::Duration};

use relay_dispatcher::{Limits, Pool, PoolConfig, SenderConfig};
use relay_domain::url_guard::Policy;
use relay_store::Store;
use relay_testkit::Receiver;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// How long a `/slow` receiver takes to answer. Long enough that serial execution
/// is unmistakably slower than concurrent, short enough to keep the suite quick.
const SLOW_MS: u64 = 200;

/// Register one endpoint and queue `n` deliveries to it.
async fn seed(store: &Store, receiver: &Receiver, path: &str, n: usize) {
    let addr = receiver.spawn().await;
    let event_type = format!("pool.{path}");
    store
        .create_endpoint(
            &format!("http://{addr}{path}"),
            "whsec_pool_test",
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

/// Poll until `f` holds, or fail after `within`.
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
async fn the_pool_delivers_a_batch_concurrently(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_pool_test");
    let n = 20;
    seed(&store, &receiver, &format!("/slow?ms={SLOW_MS}"), n).await;

    let pool = Pool::with_config(
        store,
        PoolConfig {
            workers: n,
            batch_size: n,
            idle_poll: Duration::from_millis(10),
            shutdown_deadline: Duration::from_secs(10),
        },
        local(),
    );

    let started = Instant::now();
    assert_eq!(pool.run_once().await.expect("run"), n);
    let elapsed = started.elapsed();

    assert_eq!(receiver.hits(), n as u64, "every delivery should be sent");

    // Serial would be n × SLOW_MS = 4s. Concurrent is one SLOW_MS plus overhead.
    // The bound is deliberately loose: this is asserting "not serial", not a
    // latency budget, and a tight bound here would fail on a loaded CI runner.
    let serial = Duration::from_millis(SLOW_MS * n as u64);
    assert!(
        elapsed < serial / 4,
        "expected concurrent delivery, took {elapsed:?} against a serial time of {serial:?}"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn in_flight_requests_never_exceed_the_worker_count(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_pool_test");
    let workers = 4;
    seed(&store, &receiver, &format!("/slow?ms={SLOW_MS}"), 20).await;

    let pool = Arc::new(Pool::with_config(
        store,
        PoolConfig {
            workers,
            // Larger than the worker count on purpose: the bound must come from the
            // permits, not from the batch size happening to match.
            batch_size: 50,
            idle_poll: Duration::from_millis(10),
            shutdown_deadline: Duration::from_secs(10),
        },
        local(),
    ));

    // The continuous loop rather than `run_once`, because it claims again the moment
    // a permit frees. That is when a bound is most likely to be exceeded — a pool
    // that only respects its limit within a single batch fails here.
    let cancel = CancellationToken::new();
    let running = pool.clone();
    let handle = tokio::spawn({
        let cancel = cancel.clone();
        async move { running.run(cancel).await }
    });

    wait_until(Duration::from_secs(15), "all 20 deliveries", || {
        receiver.hits() == 20
    })
    .await;
    cancel.cancel();
    handle.await.expect("pool loop");

    assert!(
        receiver.max_in_flight() <= workers as u64,
        "pool exceeded its bound: {} requests in flight with workers={workers}",
        receiver.max_in_flight()
    );
    // Guards against passing for the wrong reason: a pool that ran everything one
    // at a time would also never exceed 4.
    assert!(
        receiver.max_in_flight() > 1,
        "pool never ran anything concurrently"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_hanging_endpoint_does_not_delay_healthy_ones(pool: PgPool) {
    let store = Store::from_pool(pool);

    // The hanging deliveries are queued first, so they are claimed first. Anything
    // that waits for a batch to drain before claiming the next will block here.
    let hanging = Receiver::new("whsec_pool_test");
    seed(&store, &hanging, "/slow?ms=30000", 2).await;

    let healthy = Receiver::new("whsec_pool_test");
    seed(&store, &healthy, "/verify", 5).await;

    let pool = Arc::new(Pool::with_config(
        store,
        PoolConfig {
            workers: 8,
            // Small batches so the first claim takes only the hanging pair. The
            // healthy rows can then only be reached by claiming again while those
            // two are still outstanding.
            batch_size: 2,
            idle_poll: Duration::from_millis(10),
            // Short, because the two hanging deliveries will still be outstanding at
            // shutdown and waiting the full request timeout for them would only slow
            // the test down. Abandoning them is what the reaper is for.
            shutdown_deadline: Duration::from_millis(100),
        },
        local(),
    ));

    let cancel = CancellationToken::new();
    let running = pool.clone();
    let handle = tokio::spawn({
        let cancel = cancel.clone();
        async move { running.run(cancel).await }
    });

    wait_until(Duration::from_secs(5), "all healthy deliveries", || {
        healthy.hits() == 5
    })
    .await;

    // Still hanging — otherwise this proved nothing about overtaking them.
    assert_eq!(
        hanging.hits(),
        2,
        "the hanging endpoint should have been contacted and still be unfinished"
    );

    cancel.cancel();
    handle.await.expect("pool loop");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn no_database_connection_is_held_during_a_request(
    opts: PgPoolOptions,
    conn: PgConnectOptions,
) {
    // One connection for the whole pool. If the claim's transaction were held open
    // across the outbound request, the first delivery would take the only connection
    // and nothing could ever record an outcome — the test would hang rather than
    // fail. That is the real-world failure too: a few dead endpoints exhaust the
    // connection pool and ingest stops as well.
    let db = opts
        .max_connections(1)
        .connect_with(conn)
        .await
        .expect("db");
    let store = Store::from_pool(db);
    store.migrate().await.expect("migrate");

    let receiver = Receiver::new("whsec_pool_test");
    seed(&store, &receiver, &format!("/slow?ms={SLOW_MS}"), 4).await;

    let pool = Pool::with_config(
        store,
        PoolConfig {
            workers: 4,
            batch_size: 4,
            idle_poll: Duration::from_millis(10),
            shutdown_deadline: Duration::from_secs(10),
        },
        local(),
    );

    let ran = tokio::time::timeout(Duration::from_secs(10), pool.run_once())
        .await
        .expect("a connection was held across the request: the pool deadlocked")
        .expect("run");

    assert_eq!(ran, 4);
    assert_eq!(receiver.hits(), 4);
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
        // Same reasoning for the in-flight caps. This file asserts the *worker pool*
        // is the bound on concurrency, so the bulkhead has to be wide enough not to
        // become the bound instead — `crates/dispatcher/tests/bulkhead.rs` is where
        // that one is tested.
        limits: Limits {
            max_in_flight: 1024,
            per_endpoint: 1024,
        },
        ..Default::default()
    }
}
