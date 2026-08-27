//! Changing a signing secret without dropping a single delivery.
//!
//! A single-secret rotation is a cutover, and there is no ordering of the two
//! changes that avoids failed deliveries: if Relay switches first, every receiver
//! still checking the old secret rejects us; if the receiver switches first, they
//! reject us until we catch up. The customer cannot fix that by deploying faster.
//!
//! So both secrets sign during an overlap window and both signatures go out. The
//! receiver matches on any entry in the list, so it may switch at any moment inside
//! the window and nothing fails on either side of it.
//!
//! Requires Postgres: `docker compose up -d`.

use std::time::Duration;

use relay_api::{AppState, router};
use relay_dispatcher::{Limits, Outcome, Pool, PoolConfig, RequestLimits, Sender, SenderConfig};
use relay_domain::{backoff::Backoff, url_guard::Policy};
use relay_store::Store;
use relay_testkit::Receiver;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

const OLD: &str = "whsec_old_secret_aaaaaaaaaaaaaaaaaaaaaaaa";

fn config() -> SenderConfig {
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
        request: RequestLimits::default(),
        transports: Default::default(),
        breaker: None,
    }
}

fn pool_config() -> PoolConfig {
    PoolConfig {
        workers: 8,
        batch_size: 8,
        idle_poll: Duration::from_millis(5),
        shutdown_deadline: Duration::from_secs(5),
    }
}

/// Serve the admin API so rotation happens the way a customer would do it.
async fn serve(store: Store) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(AppState::permissive(store));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

async fn rotate(api: std::net::SocketAddr, endpoint: Uuid) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(format!(
            "http://{api}/v1/endpoints/{endpoint}/rotate-secret"
        ))
        .send()
        .await
        .expect("rotate");
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap_or(Value::Null))
}

/// An endpoint pointed at `receiver`, plus the event type only it subscribes to.
async fn endpoint(store: &Store, receiver: &Receiver, path: &str) -> (Uuid, String) {
    let addr = receiver.spawn().await;
    let event_type = format!("rot.{}", Uuid::new_v4());
    let ep = store
        .create_endpoint(
            &format!("http://{addr}{path}"),
            OLD,
            std::slice::from_ref(&event_type),
        )
        .await
        .expect("endpoint");
    (ep.id, event_type)
}

async fn send(store: &Store, event_type: &str) -> Uuid {
    store
        .insert_event_and_fan_out(event_type, br#"{"hello":"world"}"#)
        .await
        .expect("insert")
        .delivery_ids[0]
}

// ------------------------------------------------------------------- the window

#[sqlx::test(migrations = "../store/migrations")]
async fn a_receiver_on_either_secret_succeeds_during_the_overlap(pool: PgPool) {
    let store = Store::from_pool(pool);
    // Still verifying against the old secret, as a customer who has not deployed yet
    // would be.
    let receiver = Receiver::new(OLD);
    let (ep, event_type) = endpoint(&store, &receiver, "/verify").await;
    let api = serve(store.clone()).await;

    let (status, body) = rotate(api, ep).await;
    assert_eq!(status, 200);
    let new_secret = body["secret"].as_str().expect("a new secret").to_string();
    assert_ne!(new_secret, OLD);
    assert!(body["previous_secret_expires_at"].is_string());

    // The old receiver keeps working.
    let id = send(&store, &event_type).await;
    let outcome = Sender::with_config(store.clone(), config())
        .deliver_by_id(id)
        .await
        .expect("deliver");
    assert!(
        matches!(outcome, Some(Outcome::Succeeded { .. })),
        "a receiver still on the old secret was rejected: {outcome:?}"
    );

    // The customer deploys mid-window. The new receiver works too, with no second
    // rotation and no coordination about when.
    let updated = Receiver::new(&new_secret);
    let (ep2, type2) = endpoint(&store, &updated, "/verify").await;
    store
        .rotate_secret(ep2, &new_secret, Duration::from_secs(3600))
        .await
        .expect("rotate the second endpoint onto the same new secret");
    let id = send(&store, &type2).await;
    let outcome = Sender::with_config(store.clone(), config())
        .deliver_by_id(id)
        .await
        .expect("deliver");
    assert!(
        matches!(outcome, Some(Outcome::Succeeded { .. })),
        "a receiver on the new secret was rejected: {outcome:?}"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn both_signatures_are_sent_and_neither_is_the_other(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new(OLD);
    let (ep, event_type) = endpoint(&store, &receiver, "/verify").await;
    store
        .rotate_secret(
            ep,
            "whsec_new_secret_bbbbbbbbbbbbbbbbbbbb",
            Duration::from_secs(3600),
        )
        .await
        .expect("rotate");

    let id = send(&store, &event_type).await;
    let claimed = store
        .pending_delivery_by_id(id)
        .await
        .unwrap()
        .expect("a claimable row");

    // The claim carries both, which is what lets the sender build the header without
    // a second query.
    assert!(
        claimed.previous_secret.is_some(),
        "the old secret is still live"
    );

    Sender::with_config(store.clone(), config())
        .deliver_by_id(id)
        .await
        .expect("deliver");

    let header = receiver
        .last_signature_header()
        .expect("a signature header");
    let parts: Vec<&str> = header.split(',').collect();
    assert_eq!(parts.len(), 2, "expected two signatures, got {header:?}");
    assert!(parts.iter().all(|p| p.starts_with("v1=")));
    // Two different keys over the same bytes. If these matched, the rotation had not
    // actually changed anything.
    assert_ne!(parts[0], parts[1]);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn once_the_window_closes_only_the_new_secret_signs(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new(OLD);
    let (ep, event_type) = endpoint(&store, &receiver, "/verify").await;

    // A window that has already elapsed. Expressed as a past expiry rather than by
    // waiting, because the property is "the query decides", not "time passes".
    store
        .rotate_secret(
            ep,
            "whsec_new_secret_bbbbbbbbbbbbbbbbbbbb",
            Duration::from_secs(3600),
        )
        .await
        .expect("rotate");
    sqlx::query("UPDATE endpoints SET previous_secret_expires_at = now() - interval '1 minute' WHERE id = $1")
        .bind(ep)
        .execute(store.pool())
        .await
        .expect("close the window");

    let id = send(&store, &event_type).await;
    let claimed = store.pending_delivery_by_id(id).await.unwrap().unwrap();
    // Not swept, simply not selected. A cleanup job that stopped running must not be
    // able to keep an old secret alive indefinitely.
    assert!(claimed.previous_secret.is_none());

    let outcome = Sender::with_config(store.clone(), config())
        .deliver_by_id(id)
        .await
        .expect("deliver");
    // The receiver never moved, so now it fails — which is the correct end of a
    // rotation, and the reason the window has to be long enough.
    assert!(
        matches!(
            outcome,
            Some(Outcome::Failed {
                status: Some(401),
                ..
            })
        ),
        "got {outcome:?}"
    );
    assert_eq!(
        receiver.last_signature_header().unwrap().split(',').count(),
        1
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn rotating_twice_does_not_leave_three_live_secrets(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new(OLD);
    let (ep, event_type) = endpoint(&store, &receiver, "/verify").await;

    store
        .rotate_secret(
            ep,
            "whsec_second_bbbbbbbbbbbbbbbbbbbbbbbb",
            Duration::from_secs(3600),
        )
        .await
        .expect("first rotation");
    store
        .rotate_secret(
            ep,
            "whsec_third_cccccccccccccccccccccccc",
            Duration::from_secs(3600),
        )
        .await
        .expect("second rotation");

    let id = send(&store, &event_type).await;
    Sender::with_config(store.clone(), config())
        .deliver_by_id(id)
        .await
        .expect("deliver");

    // Two, never three. "How many keys can sign as you" is exactly the number a
    // rotation exists to keep at one, and a chain of previous secrets would let it
    // grow without limit.
    let header = receiver.last_signature_header().expect("a header");
    assert_eq!(header.split(',').count(), 2, "got {header:?}");
    // And the one that was rotated away from twice is gone: the original secret can
    // no longer sign.
    let outcome = store.get_endpoint(ep).await.expect("endpoint");
    assert_ne!(outcome.secret.reveal(), OLD);
}

// -------------------------------------------------------------- zero failures

#[sqlx::test(migrations = "../store/migrations")]
async fn a_rotation_under_load_fails_nothing(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new(OLD);
    let (ep, event_type) = endpoint(&store, &receiver, "/verify").await;
    let api = serve(store.clone()).await;

    // Twenty deliveries queued before the rotation and twenty after, all to a
    // receiver that never changes its secret. Every one of them must succeed: the
    // acceptance criterion is zero failed deliveries, not "few".
    for _ in 0..20 {
        send(&store, &event_type).await;
    }
    assert_eq!(rotate(api, ep).await.0, 200);
    for _ in 0..20 {
        send(&store, &event_type).await;
    }

    // Drained a batch at a time until the queue is empty, rather than a fixed number
    // of passes: `run_once` claims at most `batch_size`, so two calls would leave
    // twenty-four rows behind and the assertion below would pass for the wrong
    // reason.
    let pool_ = Pool::with_config(store.clone(), pool_config(), config());
    for _ in 0..20 {
        if pool_.run_once().await.expect("run") == 0 {
            break;
        }
    }

    let failed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM delivery_attempts WHERE outcome_class <> 'success'",
    )
    .fetch_one(store.pool())
    .await
    .expect("count");
    assert_eq!(failed, 0, "a rotation cost {failed} failed attempts");

    let delivered: i64 =
        sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE status = 'succeeded'")
            .fetch_one(store.pool())
            .await
            .expect("count");
    assert_eq!(delivered, 40);
}

// ----------------------------------------------------------------- the secret

#[sqlx::test(migrations = "../store/migrations")]
async fn a_secret_cannot_be_printed_by_accident(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new(OLD);
    let (ep, event_type) = endpoint(&store, &receiver, "/verify").await;
    store
        .rotate_secret(
            ep,
            "whsec_new_secret_bbbbbbbbbbbbbbbbbbbb",
            Duration::from_secs(3600),
        )
        .await
        .expect("rotate");
    send(&store, &event_type).await;

    let claimed = store.next_pending_delivery().await.unwrap().unwrap();

    // The type refuses, rather than a rule somebody has to remember. `Secret` has no
    // `Display` at all, so `{secret}` does not compile; reading the bytes takes an
    // explicitly-named method that is trivial to grep for at review time.
    let printed = format!("{:?}", claimed.secret);
    assert_eq!(printed, "Secret(<redacted>)");
    assert_eq!(
        format!("{:?}", claimed.previous_secret),
        "Some(Secret(<redacted>))"
    );

    // And the row that carries them prints neither, on the path where it matters:
    // one `?pending` added during an incident.
    let printed = format!("{claimed:?}");
    assert!(!printed.contains(OLD));
    assert!(!printed.contains("whsec_new_secret"));

    let printed = format!("{:?}", store.get_endpoint(ep).await.unwrap());
    assert!(!printed.contains("whsec_new_secret"), "got {printed}");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn rotating_an_unknown_endpoint_is_a_404(pool: PgPool) {
    let api = serve(Store::from_pool(pool)).await;
    assert_eq!(rotate(api, Uuid::new_v4()).await.0, 404);
}
