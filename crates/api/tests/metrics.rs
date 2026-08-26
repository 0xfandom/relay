//! What the ingest path reports about itself.
//!
//! One acceptance criterion here is a negative, and it is the whole reason ingest
//! is timed separately from delivery: accepting an event must not get slower
//! because customer endpoints are failing. The two paths share a database and
//! nothing else, and this is the number that would show it if that ever stopped
//! being true.
//!
//! Requires Postgres: `docker compose up -d`.

use std::{net::SocketAddr, sync::OnceLock};

use relay_api::{AppState, router_with_metrics};
use relay_metrics::Exporter;
use relay_store::Store;
use relay_testkit::metrics::{counter, is_described, sample};
use sqlx::PgPool;

/// One recorder per process; `install` is global and fails on a second call.
static RECORDER: OnceLock<Exporter> = OnceLock::new();

/// Counters are shared across the tests in this binary, so the before-and-after
/// deltas each test measures are only meaningful one at a time.
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn exporter() -> Exporter {
    RECORDER
        .get_or_init(|| Exporter::install().expect("the recorder installs exactly once"))
        .clone()
}

/// Serve the real router, `/metrics` included, on an ephemeral port.
async fn serve(store: Store) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router_with_metrics(AppState::permissive(store), exporter());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

async fn endpoint(store: &Store) {
    store
        .create_endpoint("https://example.com/hook", "whsec_api_metrics", &[])
        .await
        .expect("endpoint");
}

/// Scrape over HTTP rather than calling `render` directly, so the route, the
/// content type and the wiring are all exercised too.
async fn scrape(addr: SocketAddr) -> String {
    let resp = reqwest::get(format!("http://{addr}/metrics"))
        .await
        .expect("scrape");
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .starts_with("text/plain"),
        "Prometheus will not parse a scrape it is served as JSON"
    );
    resp.text().await.expect("body")
}

fn post(addr: SocketAddr) -> reqwest::RequestBuilder {
    reqwest::Client::new()
        .post(format!("http://{addr}/v1/events"))
        .header("content-type", "application/json")
        .header("relay-event-type", "order.paid")
}

const INGEST: &str = "relay_ingest_total";
const INGEST_COUNT: &str = "relay_ingest_duration_seconds_count";

#[sqlx::test(migrations = "../store/migrations")]
async fn an_accepted_event_is_counted_and_timed(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let store = Store::from_pool(pool);
    endpoint(&store).await;
    let addr = serve(store).await;

    let before = scrape(addr).await;
    let accepted_before = counter(&before, INGEST, &[("outcome", "accepted")]);
    let timed_before = counter(&before, INGEST_COUNT, &[]);

    let resp = post(addr).body(r#"{"order":1}"#).send().await.unwrap();
    assert_eq!(resp.status(), 202);

    let after = scrape(addr).await;
    assert_eq!(
        counter(&after, INGEST, &[("outcome", "accepted")]) - accepted_before,
        1.0
    );
    assert_eq!(counter(&after, INGEST_COUNT, &[]) - timed_before, 1.0);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_replayed_request_is_counted_apart_from_a_fresh_one(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let store = Store::from_pool(pool);
    endpoint(&store).await;
    let addr = serve(store).await;

    let before = scrape(addr).await;
    let accepted_before = counter(&before, INGEST, &[("outcome", "accepted")]);
    let replayed_before = counter(&before, INGEST, &[("outcome", "replayed")]);

    for _ in 0..3 {
        let resp = post(addr)
            .header("idempotency-key", "k-metrics-1")
            .body(r#"{"order":2}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 202);
    }

    let after = scrape(addr).await;
    // Three requests, one event. Both numbers are true and they are not the same
    // number, which is exactly why they are separate labels: a producer whose
    // retries suddenly all count as `accepted` has lost its idempotency key, and
    // the only place that is visible is here.
    assert_eq!(
        counter(&after, INGEST, &[("outcome", "accepted")]) - accepted_before,
        1.0
    );
    assert_eq!(
        counter(&after, INGEST, &[("outcome", "replayed")]) - replayed_before,
        2.0
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_bad_request_is_counted_as_rejected_not_as_an_error(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let store = Store::from_pool(pool);
    endpoint(&store).await;
    let addr = serve(store).await;

    let before = scrape(addr).await;
    let rejected_before = counter(&before, INGEST, &[("outcome", "rejected")]);
    let errors_before = counter(&before, INGEST, &[("outcome", "error")]);

    // No event type anywhere: not in the header, not in the body.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/events"))
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    let after = scrape(addr).await;
    assert_eq!(
        counter(&after, INGEST, &[("outcome", "rejected")]) - rejected_before,
        1.0
    );
    // The distinction worth paging on. A spike in rejections is a customer
    // deploying a change; a spike in errors is us.
    assert_eq!(
        counter(&after, INGEST, &[("outcome", "error")]) - errors_before,
        0.0
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn ingest_is_timed_even_when_every_endpoint_is_unreachable(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let store = Store::from_pool(pool);
    // A black hole: the address is reserved for documentation and answers nothing.
    // Every delivery fanned out to it will eventually time out.
    store
        .create_endpoint("http://192.0.2.1:9/hook", "whsec_api_metrics", &[])
        .await
        .expect("endpoint");
    let addr = serve(store.clone()).await;

    let before = scrape(addr).await;
    let timed_before = counter(&before, INGEST_COUNT, &[]);

    for i in 0..5 {
        let resp = post(addr)
            .body(format!(r#"{{"order":{i}}}"#))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 202);
    }

    let after = scrape(addr).await;
    assert_eq!(counter(&after, INGEST_COUNT, &[]) - timed_before, 5.0);

    // The structural claim behind "ingest latency stays flat while deliveries
    // fail", asserted as a property rather than as a stopwatch reading: the API
    // never sent anything, so five deliveries are sitting in the queue unattempted
    // and none of that time was spent inside a request. A timing assertion here
    // would be measuring the CI runner instead.
    let queued: i64 =
        sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE status = 'pending'")
            .fetch_one(store.pool())
            .await
            .expect("count");
    assert_eq!(queued, 5);
    let attempted: i64 = sqlx::query_scalar("SELECT count(*) FROM delivery_attempts")
        .fetch_one(store.pool())
        .await
        .expect("count");
    assert_eq!(
        attempted, 0,
        "ingest must not attempt a delivery of its own"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_api_does_not_report_the_queue(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let store = Store::from_pool(pool);
    endpoint(&store).await;
    let addr = serve(store).await;

    let rendered = scrape(addr).await;

    assert!(is_described(&rendered, INGEST));
    // Deliberately absent. These describe rows in a database both processes share,
    // and the dispatcher is the one that reports them — two reporters would show up
    // as two series under different `instance` labels, and any dashboard summing
    // across instances would double the queue.
    assert_eq!(sample(&rendered, "relay_queue_depth", &[]), None);
    assert_eq!(
        sample(&rendered, "relay_queue_oldest_pending_age_seconds", &[]),
        None
    );
}
