//! Bounds on what one endpoint can make Relay spend.
//!
//! Two failure modes, and the second is the one that actually kills worker pools.
//!
//! An endpoint that accepts a connection and then says nothing is easy: a read
//! timeout ends it in seconds. An endpoint that answers, slowly, forever — one byte
//! every fifty milliseconds — satisfies a read timeout indefinitely, because the
//! timeout resets on every byte. Only a total timeout ends that one, and a service
//! with two of the three timeouts configured looks correct right up until somebody
//! points a trickling endpoint at it and every worker is gone.
//!
//! Requires Postgres: `docker compose up -d`.

use std::time::{Duration, Instant};

use relay_dispatcher::{Limits, Outcome, PoolConfig, RequestLimits, Sender, SenderConfig};
use relay_domain::{backoff::Backoff, url_guard::Policy};
use relay_store::Store;
use relay_testkit::Receiver;
use sqlx::PgPool;
use uuid::Uuid;

fn config(request: RequestLimits) -> SenderConfig {
    SenderConfig {
        backoff: Backoff {
            base: Duration::from_millis(5),
            cap: Duration::from_millis(20),
            max_attempts: 12,
            retry_after_cap: Duration::from_secs(300),
        },
        policy: Policy::permissive(),
        rate_limit: false,
        limits: Limits {
            max_in_flight: 1024,
            per_endpoint: 1024,
        },
        request,
        transports: Default::default(),
        // Off: these tests fail the same endpoint repeatedly and a trip would
        // replace the behaviour under test with a deferral.
        breaker: None,
    }
}

#[allow(dead_code)]
fn pool_config() -> PoolConfig {
    PoolConfig {
        workers: 4,
        batch_size: 4,
        idle_poll: Duration::from_millis(5),
        shutdown_deadline: Duration::from_secs(5),
    }
}

async fn queue(store: &Store, url: &str, payload: &[u8]) -> Uuid {
    let event_type = format!("lim.{}", Uuid::new_v4());
    store
        .create_endpoint(url, "whsec_limits_test", std::slice::from_ref(&event_type))
        .await
        .expect("endpoint");
    store
        .insert_event_and_fan_out(&event_type, payload)
        .await
        .expect("insert")
        .delivery_ids[0]
}

// ------------------------------------------------------------------- timeouts

#[sqlx::test(migrations = "../store/migrations")]
async fn an_endpoint_that_trickles_forever_is_abandoned_at_the_total_timeout(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_limits_test");
    let addr = receiver.spawn().await;
    // Answers `200` immediately, then sends the body one byte every 50ms, for as
    // long as anyone will listen.
    let id = queue(&store, &format!("http://{addr}/trickle?ms=50"), b"{}").await;

    // The read timeout is four times the gap between bytes, so it never fires —
    // which is the whole point. A service configured with only a connect timeout and
    // a read timeout looks correct and would hold this worker until the process was
    // restarted. Filling a 2048-byte cap at this rate takes over a minute and a half.
    let sender = Sender::with_config(
        store.clone(),
        config(RequestLimits {
            connect: Duration::from_secs(2),
            read: Duration::from_millis(200),
            total: Duration::from_millis(600),
            max_response_bytes: 2048,
            ..RequestLimits::default()
        }),
    );

    let started = Instant::now();
    let outcome = sender.deliver_by_id(id).await.expect("deliver");
    let took = started.elapsed();

    // The claim is not "this was fast". It is that it ended at all, nowhere near the
    // forever the endpoint was offering. The margin is wide enough that a busy
    // runner cannot fail it.
    assert!(
        took < Duration::from_secs(5),
        "the trickling endpoint held the worker for {took:?}"
    );

    // And it ended because the *total* budget ran out, not because the body was
    // read: a handful of bytes arrived out of the 2048 the cap would have allowed.
    let read = sender.response_bytes_read();
    assert!(
        read < 100,
        "expected the total timeout to cut the read short, got {read} bytes"
    );

    // Recorded as delivered, and deliberately so. The endpoint answered `200` before
    // it started misbehaving, and for a webhook the status *is* the acknowledgement —
    // whatever it chooses to dribble afterwards does not un-receive the payload.
    // Retrying here would send a second copy to a receiver that already has one.
    assert!(
        matches!(outcome, Some(Outcome::Succeeded { status: 200 })),
        "got {outcome:?}"
    );
    let attempt = &store.attempt_history(id).await.expect("history")[0];
    assert_eq!(attempt.outcome_class, "success");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_silent_endpoint_is_abandoned_without_waiting_out_the_whole_budget(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_limits_test");
    let addr = receiver.spawn().await;
    // Accepts, then says nothing for thirty seconds.
    let id = queue(&store, &format!("http://{addr}/slow?ms=30000"), b"{}").await;

    // A long total and a short read timeout. The read timeout is doing the work
    // here, which is the case it exists for: a connection that has gone quiet is
    // abandoned in a fraction of the budget rather than occupying a worker for all
    // of it.
    let sender = Sender::with_config(
        store.clone(),
        config(RequestLimits {
            connect: Duration::from_secs(2),
            read: Duration::from_millis(300),
            total: Duration::from_secs(60),
            ..RequestLimits::default()
        }),
    );

    let started = Instant::now();
    sender.deliver_by_id(id).await.expect("deliver");
    let took = started.elapsed();

    assert!(
        took < Duration::from_secs(20),
        "the read timeout did not fire; waited {took:?} of a 60s budget"
    );
    assert_eq!(
        store.attempt_history(id).await.unwrap()[0].outcome_class,
        "retryable"
    );
}

// -------------------------------------------------------------------- the body

#[sqlx::test(migrations = "../store/migrations")]
async fn a_huge_response_body_is_not_read_into_memory(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_limits_test");
    let addr = receiver.spawn().await;
    // Eight megabytes of error page, which is not an unusual thing for a framework
    // to produce when it panics.
    let id = queue(&store, &format!("http://{addr}/bigbody?kb=8192"), b"{}").await;

    let cap = 2048;
    let sender = Sender::with_config(
        store.clone(),
        config(RequestLimits {
            max_response_bytes: cap,
            ..RequestLimits::default()
        }),
    );
    sender.deliver_by_id(id).await.expect("deliver");

    // The claim "a large response body does not increase the memory footprint",
    // asserted as a number rather than argued for. Reading to the end and truncating
    // afterwards would produce the identical stored snippet and cost all eight
    // megabytes, so checking the snippet alone would prove nothing.
    let read = sender.response_bytes_read();
    assert!(
        read <= (cap as u64) + 64 * 1024,
        "read {read} bytes of an 8MB body with a {cap}-byte cap"
    );

    let attempt = &store.attempt_history(id).await.expect("history")[0];
    let snippet = attempt.response_snippet.as_deref().expect("a snippet");
    assert!(snippet.len() <= cap, "stored {} bytes", snippet.len());
    assert!(!snippet.is_empty(), "enough to debug with, not nothing");
    // The status still made it, which is the part that decides what happens next.
    assert_eq!(attempt.http_status, Some(500));
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_response_cap_is_configurable(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_limits_test");
    let addr = receiver.spawn().await;
    let id = queue(&store, &format!("http://{addr}/bigbody?kb=64"), b"{}").await;

    let sender = Sender::with_config(
        store.clone(),
        config(RequestLimits {
            max_response_bytes: 100,
            ..RequestLimits::default()
        }),
    );
    sender.deliver_by_id(id).await.expect("deliver");

    let snippet = store.attempt_history(id).await.unwrap()[0]
        .response_snippet
        .clone()
        .expect("a snippet");
    assert_eq!(snippet.len(), 100);
}

// ----------------------------------------------------------------- the payload

#[sqlx::test(migrations = "../store/migrations")]
async fn an_oversized_payload_is_never_sent(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_limits_test");
    let addr = receiver.spawn().await;

    // Stored under a larger cap and then delivered under a smaller one — the only
    // way this can happen, since ingest refuses anything over the limit. Lowering a
    // cap must not turn old rows into an endless retry loop.
    let payload = vec![b'x'; 4096];
    let id = queue(&store, &format!("http://{addr}/verify"), &payload).await;

    let sender = Sender::with_config(
        store.clone(),
        config(RequestLimits {
            max_payload_bytes: 1024,
            ..RequestLimits::default()
        }),
    );
    sender.deliver_by_id(id).await.expect("deliver");

    assert_eq!(receiver.hits(), 0, "the request must not have been made");

    let d = store.get_delivery(id).await.unwrap().unwrap();
    assert_eq!(d.status, "dead");
    // Permanent, not retryable: the payload is stored and will not shrink, so every
    // retry would fail identically while the endpoint waits for something that is
    // never coming.
    assert_eq!(d.dead_reason.as_deref(), Some("permanent_failure"));

    let attempt = &store.attempt_history(id).await.unwrap()[0];
    assert_eq!(attempt.outcome_class, "permanent");
    assert!(
        attempt.error.as_deref().unwrap().contains("4096 bytes"),
        "the error should name the size, got {:?}",
        attempt.error
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_payload_inside_the_cap_still_goes_out(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_limits_test");
    let addr = receiver.spawn().await;
    let payload = vec![b'x'; 1000];
    let id = queue(&store, &format!("http://{addr}/verify"), &payload).await;

    let sender = Sender::with_config(
        store.clone(),
        config(RequestLimits {
            max_payload_bytes: 1024,
            ..RequestLimits::default()
        }),
    );
    // A cap that refuses everything is not a cap, it is an outage.
    let outcome = sender.deliver_by_id(id).await.expect("deliver");
    assert!(
        matches!(outcome, Some(Outcome::Succeeded { .. })),
        "got {outcome:?}"
    );
    assert_eq!(receiver.bodies()[0].len(), 1000);
}
