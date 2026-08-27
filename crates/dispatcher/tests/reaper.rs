//! Tests for the lease reaper.
//!
//! The reaper exists for one situation that is otherwise invisible: a worker dies
//! holding claimed work. The rows are `inflight`, nobody owns them, and the claim
//! query steps over them because they are not `pending`. Nothing errors. Nothing
//! retries. The deliveries are simply never sent.
//!
//! So the tests here are mostly about the boundary — rescuing the abandoned rows
//! without touching the ones that are still legitimately being worked on. A reaper
//! that is too eager is worse than no reaper at all, because it sends duplicates
//! instead of merely being slow.
//!
//! Requires Postgres: `docker compose up -d`.

use std::time::Duration;

use relay_dispatcher::{
    Pool, PoolConfig, REQUEST_TIMEOUT, Reaper, ReaperConfig, RequestLimits, SenderConfig,
};
use relay_domain::url_guard::Policy;
use relay_store::Store;
use relay_testkit::Receiver;
use sqlx::PgPool;
use uuid::Uuid;

/// Long enough to be unambiguous, short enough not to slow the suite.
const LEASE_TTL: Duration = Duration::from_secs(30);

fn config() -> ReaperConfig {
    ReaperConfig {
        lease_ttl: LEASE_TTL,
        interval: Duration::from_millis(50),
    }
}

async fn seed(store: &Store, receiver: &Receiver, path: &str, n: usize) {
    let addr = receiver.spawn().await;
    let event_type = format!("reap.{path}");
    store
        .create_endpoint(
            &format!("http://{addr}{path}"),
            "whsec_reaper_test",
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

/// Age a lease so it looks like it was taken `secs` ago.
///
/// The alternative is sleeping for the real TTL, which would mean a thirty-second
/// test. Moving the clock backwards on the row tests the same predicate.
async fn backdate_lease(store: &Store, delivery_id: Uuid, secs: i64) {
    sqlx::query(
        "UPDATE deliveries SET locked_at = now() - make_interval(secs => $2) WHERE id = $1",
    )
    .bind(delivery_id)
    .bind(secs as f64)
    .execute(store.pool())
    .await
    .expect("backdate");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn an_expired_lease_returns_to_the_queue(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_reaper_test");
    seed(&store, &receiver, "/verify", 1).await;

    // A worker takes it, then dies. Nothing marks it finished.
    let claimed = store.claim_batch(1, "worker-that-dies").await.unwrap();
    let id = claimed[0].delivery_id;
    assert_eq!(
        store.get_delivery(id).await.unwrap().unwrap().status,
        "inflight"
    );

    // Nothing else can see it while the lease holds.
    assert!(store.claim_batch(10, "worker-b").await.unwrap().is_empty());

    backdate_lease(&store, id, LEASE_TTL.as_secs() as i64 + 1).await;

    let reaper = Reaper::new(store.clone(), config()).expect("config");
    assert_eq!(reaper.reap_once().await.unwrap(), 1);
    assert_eq!(reaper.rescued(), 1);

    let d = store.get_delivery(id).await.unwrap().unwrap();
    assert_eq!(
        d.status, "pending",
        "an expired lease must return to pending"
    );
    assert_eq!(
        d.attempt, 0,
        "the attempt counter must not move: whether the request reached the endpoint \
         is unknown, so charging a retry for it spends the budget on a guess"
    );

    assert_eq!(
        store.claim_batch(10, "worker-b").await.unwrap().len(),
        1,
        "a rescued delivery must be claimable again"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_live_lease_is_left_alone(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_reaper_test");
    seed(&store, &receiver, "/verify", 3).await;

    let claimed = store.claim_batch(3, "worker-a").await.unwrap();
    assert_eq!(claimed.len(), 3);

    let reaper = Reaper::new(store.clone(), config()).expect("config");

    // This is the dangerous direction. Rescuing a delivery that is still being sent
    // means a second worker sends it too, and the endpoint sees it twice.
    assert_eq!(
        reaper.reap_once().await.unwrap(),
        0,
        "the reaper rescued a delivery whose lease had not expired"
    );
    assert_eq!(
        reaper.rescued(),
        0,
        "rescues must be zero in normal operation"
    );

    for c in claimed {
        assert_eq!(
            store
                .get_delivery(c.delivery_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "inflight"
        );
    }
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_finished_delivery_is_never_reaped(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_reaper_test");
    seed(&store, &receiver, "/verify", 1).await;

    // Deliver it for real, so the row ends up `succeeded` by the normal path.
    let pool_cfg = PoolConfig {
        workers: 1,
        batch_size: 1,
        idle_poll: Duration::from_millis(10),
        shutdown_deadline: Duration::from_secs(5),
    };
    assert_eq!(
        Pool::with_config(store.clone(), pool_cfg, local())
            .run_once()
            .await
            .unwrap(),
        1
    );

    let id = store.claim_batch(10, "x").await.unwrap();
    assert!(
        id.is_empty(),
        "a succeeded delivery should not be claimable"
    );

    // Even with a lease timestamp older than any TTL. A reaper keyed on age alone
    // rather than on `status = 'inflight'` would resurrect finished work and send
    // every delivery a second time.
    sqlx::query("UPDATE deliveries SET locked_at = now() - make_interval(secs => 86400)")
        .execute(store.pool())
        .await
        .unwrap();

    let reaper = Reaper::new(store.clone(), config()).expect("config");
    assert_eq!(
        reaper.reap_once().await.unwrap(),
        0,
        "the reaper resurrected a delivery that had already succeeded"
    );
    assert_eq!(receiver.hits(), 1, "the delivery must not be sent twice");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_delivery_stranded_by_a_dead_worker_is_eventually_delivered(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_reaper_test");
    seed(&store, &receiver, "/verify", 1).await;

    // Simulate the crash: claim the work, then never finish it. This is exactly the
    // state `kill -9` on a worker leaves behind — the row is claimed, the process
    // that claimed it is gone, and no outcome was ever written.
    let claimed = store.claim_batch(1, "worker-killed").await.unwrap();
    let id = claimed[0].delivery_id;
    drop(claimed);

    // Without the reaper the delivery is lost: the pool cannot see it at all.
    let pool_cfg = PoolConfig {
        workers: 4,
        batch_size: 4,
        idle_poll: Duration::from_millis(10),
        shutdown_deadline: Duration::from_secs(5),
    };
    let sender = Pool::with_config(store.clone(), pool_cfg, local());
    assert_eq!(sender.run_once().await.unwrap(), 0);
    assert_eq!(receiver.hits(), 0);

    backdate_lease(&store, id, LEASE_TTL.as_secs() as i64 + 1).await;

    let reaper = Reaper::new(store.clone(), config()).expect("config");
    assert_eq!(reaper.reap_once().await.unwrap(), 1);

    // Now the normal path picks it up with no special handling.
    assert_eq!(sender.run_once().await.unwrap(), 1);
    assert_eq!(
        receiver.hits(),
        1,
        "the rescued delivery should have been sent"
    );
    assert_eq!(
        store.get_delivery(id).await.unwrap().unwrap().status,
        "succeeded"
    );
}

// Async only because the lazy pool wants a Tokio context to exist; nothing here
// touches the database.

#[tokio::test]
async fn a_lease_shorter_than_the_request_timeout_is_rejected() {
    // Not a warning. A lease that can expire mid-request makes the reaper a source
    // of duplicate deliveries rather than a safety net, and nothing downstream would
    // report it — the endpoint just quietly receives everything twice.
    let too_short = ReaperConfig {
        lease_ttl: REQUEST_TIMEOUT,
        interval: Duration::from_secs(1),
    };
    let Err(err) = Reaper::new(dummy_store(), too_short) else {
        panic!("a lease equal to the request timeout was accepted");
    };
    assert_eq!(err.lease_ttl, REQUEST_TIMEOUT);

    assert!(
        ReaperConfig::default().lease_ttl > REQUEST_TIMEOUT,
        "the default configuration must itself be valid"
    );
}

/// A store that is never queried — the config check happens before any I/O.
fn dummy_store() -> Store {
    Store::from_pool(PgPool::connect_lazy("postgres://unused/unused").expect("lazy pool"))
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
        request: RequestLimits::default(),
        transports: Default::default(),
        rate_limit: false,
        // Breaker off: several of these tests fail one endpoint repeatedly on
        // purpose, and tripping it would replace the behaviour under test with a
        // deferral.
        breaker: None,
        ..Default::default()
    }
}

#[tokio::test]
async fn a_lease_shorter_than_a_raised_request_timeout_is_rejected() {
    // The two are configured independently, and raising one without the other is a
    // silent source of duplicate deliveries: the lease expires while a request is
    // still in flight, the reaper hands the row to a second worker, and the endpoint
    // receives the same webhook twice with nothing anywhere reporting why.
    //
    // A 30-second lease is perfectly safe against the default 10-second timeout and
    // unsafe against a 60-second one, so checking against a constant would have
    // approved this.
    let store = Store::from_pool(sqlx::PgPool::connect_lazy("postgres://unused").unwrap());
    let config = ReaperConfig {
        lease_ttl: Duration::from_secs(30),
        interval: Duration::from_secs(10),
    };

    assert!(
        Reaper::with_request_timeout(store.clone(), config.clone(), Duration::from_secs(10))
            .is_ok()
    );
    let Err(err) = Reaper::with_request_timeout(store, config, Duration::from_secs(60)) else {
        panic!("a lease inside the request timeout must be refused");
    };
    assert_eq!(err.lease_ttl, Duration::from_secs(30));
    assert_eq!(err.request_timeout, Duration::from_secs(60));
}
