//! What Relay says about itself, checked against what it actually did.
//!
//! Five mechanisms now hold deliveries back — the backoff, two rate limits, two
//! bulkheads and the breaker — and from outside the process none of them is
//! visible. That is the gap this closes, and it is worth testing hard for a reason
//! that is easy to miss: a metric that is wrong is worse than a metric that is
//! missing. A missing panel gets investigated; a panel reading zero gets believed.
//!
//! Every assertion here is a delta rather than an absolute, because the recorder is
//! process-global and these tests share one. The lock is what makes the deltas
//! meaningful — without it two tests interleave and each sees the other's counts.
//!
//! Requires Postgres: `docker compose up -d`.

use std::{sync::OnceLock, time::Duration};

use relay_dispatcher::{Limits, Reaper, ReaperConfig, RequestLimits, Sender, SenderConfig};
use relay_domain::{backoff::Backoff, breaker, rate_limit::Rate, url_guard::Policy};
use relay_metrics::Exporter;
use relay_store::Store;
use relay_testkit::{
    Receiver,
    metrics::{counter, is_described, sample},
};
use sqlx::PgPool;
use uuid::Uuid;

/// The one recorder this process gets. `install` is global and fails on a second
/// call, so it happens once and every test renders through the same registry.
static RECORDER: OnceLock<Exporter> = OnceLock::new();

/// Serialises the tests in this file.
///
/// Not fussiness. Counters are shared, so two tests running at once make each
/// other's before-and-after deltas meaningless; and `render` refreshes the gauges
/// from a store before rendering, so a concurrent render would overwrite this
/// test's numbers with another test's database between the two halves.
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn exporter(store: &Store) -> Exporter {
    RECORDER
        .get_or_init(|| Exporter::install().expect("the recorder installs exactly once"))
        .clone()
        .with_queue_gauges(store.clone())
}

fn config(breaker: Option<breaker::Policy>, rate_limit: bool) -> SenderConfig {
    SenderConfig {
        backoff: Backoff {
            base: Duration::from_millis(5),
            cap: Duration::from_millis(20),
            max_attempts: 50,
            retry_after_cap: Duration::from_secs(300),
        },
        policy: Policy::permissive(),
        request: RequestLimits::default(),
        rate_limit,
        limits: Limits {
            max_in_flight: 1024,
            per_endpoint: 1024,
        },
        breaker,
    }
}

/// Register an endpoint at `path` and queue `n` deliveries.
async fn seed(store: &Store, receiver: &Receiver, path: &str, n: usize) -> (Uuid, Vec<Uuid>) {
    let addr = receiver.spawn().await;
    let event_type = format!("mx.{}", Uuid::new_v4());
    let ep = store
        .create_endpoint(
            &format!("http://{addr}{path}"),
            "whsec_metrics_test",
            std::slice::from_ref(&event_type),
        )
        .await
        .expect("endpoint");

    let mut ids = Vec::new();
    for _ in 0..n {
        ids.extend(
            store
                .insert_event_and_fan_out(&event_type, br#"{"hello":"world"}"#)
                .await
                .expect("insert")
                .delivery_ids,
        );
    }
    (ep.id, ids)
}

const ATTEMPTS: &str = "relay_delivery_attempts_total";
const DEFERRALS: &str = "relay_deliveries_deferred_total";
const DEAD: &str = "relay_deliveries_dead_total";
const LATENCY_COUNT: &str = "relay_delivery_duration_seconds_count";
const DEPTH: &str = "relay_queue_depth";
const OLDEST: &str = "relay_queue_oldest_pending_age_seconds";

// ------------------------------------------------------------------ the gauges

#[sqlx::test(migrations = "../store/migrations")]
async fn the_queue_gauges_count_what_is_actually_open(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_metrics_test");
    seed(&store, &receiver, "/verify", 3).await;

    let rendered = exporter(&store).render().await;

    assert_eq!(
        sample(&rendered, DEPTH, &[("status", "pending")]),
        Some(3.0)
    );
    assert_eq!(
        sample(&rendered, DEPTH, &[("status", "inflight")]),
        Some(0.0)
    );
    assert_eq!(sample(&rendered, DEPTH, &[("status", "dead")]), Some(0.0));
    assert_eq!(sample(&rendered, "relay_queue_due", &[]), Some(3.0));
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_delivery_waiting_on_a_backoff_is_pending_but_not_due(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_metrics_test");
    let (_, ids) = seed(&store, &receiver, "/verify", 2).await;

    // The distinction the pair of gauges exists to make. A queue of two with
    // nothing due is a system waiting on purpose; a queue of two with both due is a
    // system that is behind, and depth alone cannot tell them apart.
    sqlx::query("UPDATE deliveries SET next_attempt_at = now() + interval '1 hour' WHERE id = $1")
        .bind(ids[0])
        .execute(store.pool())
        .await
        .expect("delay one");

    let rendered = exporter(&store).render().await;

    assert_eq!(
        sample(&rendered, DEPTH, &[("status", "pending")]),
        Some(2.0)
    );
    assert_eq!(sample(&rendered, "relay_queue_due", &[]), Some(1.0));
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_oldest_pending_age_reports_a_stalled_delivery(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_metrics_test");
    let (_, ids) = seed(&store, &receiver, "/verify", 2).await;

    // Stalled an hour ago. Backdated rather than waited for: the property under
    // test is that the gauge reads the row's own due time, and a test that slept to
    // produce a real age would be measuring the test runner instead.
    sqlx::query("UPDATE deliveries SET next_attempt_at = now() - interval '1 hour' WHERE id = $1")
        .bind(ids[0])
        .execute(store.pool())
        .await
        .expect("backdate");

    let rendered = exporter(&store).render().await;

    let age = sample(&rendered, OLDEST, &[]).expect("an age is reported");
    assert!(
        age >= 3600.0,
        "the stalled delivery should dominate the age, got {age}"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn an_empty_queue_reports_no_age_rather_than_zero(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let store = Store::from_pool(pool);

    let rendered = exporter(&store).render().await;

    // The dangerous alternative. Zero seconds and "nothing is queued" are both
    // healthy states, so a zero here would be indistinguishable from the real
    // thing — and the moment the queue *did* stall, nobody would trust the panel
    // that had been flat at zero all week.
    let age = sample(&rendered, OLDEST, &[]).expect("the gauge is reported");
    assert!(age.is_nan(), "an empty queue has no oldest item, got {age}");
    assert_eq!(
        sample(&rendered, DEPTH, &[("status", "pending")]),
        Some(0.0)
    );
}

// ---------------------------------------------------------------- the counters

#[sqlx::test(migrations = "../store/migrations")]
async fn a_successful_delivery_is_counted_and_timed(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_metrics_test");
    seed(&store, &receiver, "/verify", 1).await;

    let before = exporter(&store).render().await;
    let sent_before = counter(&before, ATTEMPTS, &[("outcome", "success")]);
    let timed_before = counter(&before, LATENCY_COUNT, &[]);

    Sender::with_config(store.clone(), config(None, false))
        .deliver_next()
        .await
        .expect("deliver");

    let after = exporter(&store).render().await;
    assert_eq!(
        counter(&after, ATTEMPTS, &[("outcome", "success")]) - sent_before,
        1.0
    );
    assert_eq!(counter(&after, LATENCY_COUNT, &[]) - timed_before, 1.0);
    // The delivery left the queue, so it is no longer any kind of open row.
    assert_eq!(sample(&after, DEPTH, &[("status", "pending")]), Some(0.0));
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_failed_delivery_is_counted_as_retryable_and_still_timed(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_metrics_test");
    seed(&store, &receiver, "/always500", 1).await;

    let before = exporter(&store).render().await;
    let failed_before = counter(&before, ATTEMPTS, &[("outcome", "retryable")]);
    let timed_before = counter(&before, LATENCY_COUNT, &[]);

    Sender::with_config(store.clone(), config(None, false))
        .deliver_next()
        .await
        .expect("deliver");

    let after = exporter(&store).render().await;
    assert_eq!(
        counter(&after, ATTEMPTS, &[("outcome", "retryable")]) - failed_before,
        1.0
    );
    // Timed as well as counted. A request that failed still occupied a worker for
    // as long as it took, and leaving failures out of the histogram would make an
    // endpoint look fastest at the moment it stopped working.
    assert_eq!(counter(&after, LATENCY_COUNT, &[]) - timed_before, 1.0);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_deferral_is_counted_by_the_gate_that_caused_it_and_never_timed(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_metrics_test");
    let (endpoint, _) = seed(&store, &receiver, "/verify", 2).await;

    // One token and a refill slow enough that no second one arrives during the
    // test. The first delivery spends it; the second must be deferred.
    store
        .set_endpoint_rate(endpoint, Rate::new(0.01, 1.0))
        .await
        .expect("rate");

    let before = exporter(&store).render().await;
    let deferred_before = counter(&before, ATTEMPTS, &[("outcome", "deferred")]);
    let rate_before = counter(&before, DEFERRALS, &[("reason", "rate_limit")]);
    let timed_before = counter(&before, LATENCY_COUNT, &[]);

    let sender = Sender::with_config(store.clone(), config(None, true));
    sender.deliver_next().await.expect("first");
    sender.deliver_next().await.expect("second");

    let after = exporter(&store).render().await;
    assert_eq!(
        counter(&after, ATTEMPTS, &[("outcome", "deferred")]) - deferred_before,
        1.0
    );
    assert_eq!(
        counter(&after, DEFERRALS, &[("reason", "rate_limit")]) - rate_before,
        1.0,
        "the deferral should be attributed to the limiter that caused it"
    );
    // Only the delivery that was actually sent is in the histogram. A deferral
    // takes no time at all, and folding a zero in for it would drag every
    // percentile down towards requests that never happened.
    assert_eq!(counter(&after, LATENCY_COUNT, &[]) - timed_before, 1.0);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_dead_delivery_is_counted_by_reason_and_shows_in_the_queue(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_metrics_test");
    // No such route. A 404 is a permanent failure: retrying cannot make a path
    // exist.
    seed(&store, &receiver, "/nope", 1).await;

    let before = exporter(&store).render().await;
    let dead_before = counter(&before, DEAD, &[("reason", "permanent_failure")]);

    Sender::with_config(store.clone(), config(None, false))
        .deliver_next()
        .await
        .expect("deliver");

    let after = exporter(&store).render().await;
    assert_eq!(
        counter(&after, DEAD, &[("reason", "permanent_failure")]) - dead_before,
        1.0
    );
    assert_eq!(sample(&after, DEPTH, &[("status", "dead")]), Some(1.0));
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_tripped_breaker_is_visible_as_a_count_and_a_state(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_metrics_test");
    // Three failures trip it, and the cooldown outlasts the test so no probe fires
    // and changes the count under us.
    let policy = breaker::Policy {
        threshold: 3,
        cooldown: Duration::from_secs(60),
        max_cooldown: Duration::from_secs(60),
    };
    seed(&store, &receiver, "/always500", 3).await;

    let before = exporter(&store).render().await;
    let trips_before = counter(&before, "relay_breaker_trips_total", &[]);

    let sender = Sender::with_config(store.clone(), config(Some(policy), false));
    for _ in 0..3 {
        sender.deliver_next().await.expect("deliver");
    }

    let after = exporter(&store).render().await;
    assert_eq!(
        counter(&after, "relay_breaker_trips_total", &[]) - trips_before,
        1.0,
        "three failures at a threshold of three is one trip, not three"
    );
    // The counter and the gauge answer different questions: how many times has this
    // happened, and is it happening right now.
    assert_eq!(
        sample(&after, "relay_endpoint_breakers", &[("state", "open")]),
        Some(1.0)
    );
    assert_eq!(
        sample(&after, "relay_endpoint_breakers", &[("state", "closed")]),
        Some(0.0)
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_rescued_delivery_is_counted(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_metrics_test");
    let (_, ids) = seed(&store, &receiver, "/verify", 1).await;

    // A worker that claimed the row and died. Backdated past the lease rather than
    // waited out, for the same reason as the age gauge above.
    store
        .claim(ids[0], "worker-that-died")
        .await
        .expect("claim");
    sqlx::query("UPDATE deliveries SET locked_at = now() - interval '1 hour' WHERE id = $1")
        .bind(ids[0])
        .execute(store.pool())
        .await
        .expect("strand");

    let before = exporter(&store).render().await;
    let rescued_before = counter(&before, "relay_reaper_rescued_total", &[]);
    // The row is claimed, so it is inflight with nobody sending it — the state the
    // reaper exists to find.
    assert_eq!(sample(&before, DEPTH, &[("status", "inflight")]), Some(1.0));

    let reaper = Reaper::new(
        store.clone(),
        ReaperConfig {
            lease_ttl: Duration::from_secs(30),
            interval: Duration::from_secs(60),
        },
    )
    .expect("lease outlasts the request timeout");
    assert_eq!(reaper.reap_once().await.expect("reap"), 1);

    let after = exporter(&store).render().await;
    assert_eq!(
        counter(&after, "relay_reaper_rescued_total", &[]) - rescued_before,
        1.0
    );
    assert_eq!(sample(&after, DEPTH, &[("status", "pending")]), Some(1.0));
}

// ------------------------------------------------------------------ the format

#[sqlx::test(migrations = "../store/migrations")]
async fn every_metric_is_described_before_anything_reports_into_it(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let store = Store::from_pool(pool);

    let rendered = exporter(&store).render().await;

    // The reason this matters: an undescribed counter that has never been
    // incremented does not appear at all, so a dashboard panel reading "no data"
    // cannot be told apart from one reading zero. Describing every metric up front
    // means absence always means something is broken.
    for name in [
        "relay_delivery_attempts_total",
        "relay_deliveries_deferred_total",
        "relay_deliveries_dead_total",
        "relay_deliveries_refused_total",
        "relay_delivery_duration_seconds",
        "relay_breaker_trips_total",
        "relay_breaker_probes_total",
        "relay_breaker_probes_recovered_total",
        "relay_reaper_rescued_total",
        "relay_idempotency_keys_pruned_total",
        "relay_queue_depth",
        "relay_queue_due",
        "relay_queue_oldest_pending_age_seconds",
        "relay_endpoint_breakers",
    ] {
        assert!(is_described(&rendered, name), "{name} has no HELP line");
    }
}
