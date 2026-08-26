//! End-to-end: an event posted to the API arrives at a receiver, signed and
//! verifiable.
//!
//! Four pieces that each pass their own unit tests will still fail when connected
//! — the bytes signed are not the bytes sent, the timestamp in the header is not
//! the one that was signed, the secret differs between the two sides. "All parts
//! work" is not the same claim as "the system works", and this file asserts the
//! second one.
//!
//! Requires Postgres. `docker compose up -d` first, or point `DATABASE_URL`
//! somewhere else.
//!
//! Each test uses a unique event type and subscribes its endpoint only to that
//! type, so concurrently running tests never fan out into each other's endpoints.

use std::net::SocketAddr;

use relay_api::{AppState, router};
use relay_dispatcher::{Outcome, Sender, SenderConfig};
use relay_domain::url_guard::Policy;
use relay_store::Store;
use relay_testkit::Receiver;
use uuid::Uuid;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://relay:relay@localhost:5433/relay".into())
}

async fn store() -> Store {
    let store = Store::connect(&database_url(), 5).await.expect("connect");
    store.migrate().await.expect("migrate");
    store
}

/// Serve the real API router on an ephemeral port.
async fn spawn_api(store: Store) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(AppState::permissive(store));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

struct Fixture {
    store: Store,
    api: SocketAddr,
    receiver: Receiver,
    event_type: String,
}

/// Wire up an isolated API + receiver + endpoint.
///
/// `endpoint_secret` is what Relay will sign with; `receiver_secret` is what the
/// receiver will verify with. They are separate arguments so a test can make them
/// disagree and prove that verification actually fails.
async fn fixture(path: &str, endpoint_secret: &str, receiver_secret: &str) -> Fixture {
    let store = store().await;
    let api = spawn_api(store.clone()).await;

    let receiver = Receiver::new(receiver_secret);
    let recv_addr = receiver.spawn().await;

    let event_type = format!("test.{}", Uuid::new_v4());
    store
        .create_endpoint(
            &format!("http://{recv_addr}{path}"),
            endpoint_secret,
            std::slice::from_ref(&event_type),
        )
        .await
        .expect("create endpoint");

    Fixture {
        store,
        api,
        receiver,
        event_type,
    }
}

/// POST an event through the real HTTP surface and return its delivery ids.
async fn ingest(api: SocketAddr, event_type: &str, body: &'static str) -> Vec<Uuid> {
    let resp = reqwest::Client::new()
        .post(format!("http://{api}/v1/events"))
        .header("content-type", "application/json")
        .header("relay-event-type", event_type)
        .body(body)
        .send()
        .await
        .expect("ingest");

    assert_eq!(
        resp.status(),
        202,
        "ingest must accept without waiting for delivery"
    );

    let json: serde_json::Value = resp.json().await.unwrap();
    json["delivery_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| Uuid::parse_str(v.as_str().unwrap()).unwrap())
        .collect()
}

#[tokio::test]
async fn signed_delivery_is_verified_by_the_receiver() {
    let f = fixture("/verify", "whsec_e2e_ok", "whsec_e2e_ok").await;
    let body = r#"{"type":"order.paid","amount":4999}"#;

    let deliveries = ingest(f.api, &f.event_type, body).await;
    assert_eq!(deliveries.len(), 1, "one subscribed endpoint, one delivery");

    let sender = Sender::with_config(f.store.clone(), local());
    let outcome = sender.deliver_by_id(deliveries[0]).await.unwrap().unwrap();

    assert_eq!(outcome, Outcome::Succeeded { status: 200 });

    let d = f.store.get_delivery(deliveries[0]).await.unwrap().unwrap();
    assert_eq!(d.status, "succeeded");

    // Every attempt appends a row, so the history is complete from the first send.
    assert_eq!(f.store.attempts_for(deliveries[0]).await.unwrap(), 1);

    // The id the receiver saw is the delivery id, which stays stable across the
    // retries that arrive in M3.
    assert_eq!(f.receiver.received_ids(), vec![deliveries[0].to_string()]);
}

#[tokio::test]
async fn a_wrong_secret_is_rejected_by_the_receiver() {
    // Relay signs with one secret, the receiver verifies with another. This is the
    // same failure an attacker would produce by forging a request.
    let f = fixture("/verify", "whsec_relay_side", "whsec_receiver_side").await;

    let deliveries = ingest(f.api, &f.event_type, r#"{"type":"order.paid"}"#).await;
    let sender = Sender::with_config(f.store.clone(), local());
    let outcome = sender.deliver_by_id(deliveries[0]).await.unwrap().unwrap();

    match outcome {
        Outcome::Failed { status, .. } => assert_eq!(status, Some(401)),
        other => panic!("expected rejection, got {other:?}"),
    }

    let d = f.store.get_delivery(deliveries[0]).await.unwrap().unwrap();
    assert_eq!(d.status, "dead", "M1 has no retry policy yet");
}

#[tokio::test]
async fn payload_bytes_are_stored_and_sent_verbatim() {
    let f = fixture("/verify", "whsec_bytes", "whsec_bytes").await;

    // Deliberately awkward: keys out of alphabetical order, odd whitespace. If
    // anything in the path parses and re-serialises the payload, these bytes change
    // and the signature stops matching what the receiver computes.
    let body = r#"{"zebra":1,  "apple":2,"nested":{"b":1,"a":2}}"#;

    let deliveries = ingest(f.api, &f.event_type, body).await;
    let sender = Sender::with_config(f.store.clone(), local());
    let outcome = sender.deliver_by_id(deliveries[0]).await.unwrap().unwrap();

    assert_eq!(
        outcome,
        Outcome::Succeeded { status: 200 },
        "verification proves the bytes survived the round trip unchanged"
    );

    let seen = f.receiver.bodies();
    assert_eq!(seen.len(), 1);
    assert_eq!(
        seen[0],
        body.as_bytes(),
        "the receiver must see byte-for-byte what was posted"
    );
}

#[tokio::test]
async fn a_tampered_body_fails_verification() {
    // Sign one body, deliver a different one. Simulates modification in transit.
    let secret = "whsec_tamper";
    let _f = fixture("/verify", secret, secret).await;

    let original = r#"{"amount":10}"#;
    let timestamp = 1_700_000_000_i64;
    let signature =
        relay_domain::signature::sign(secret.as_bytes(), timestamp, original.as_bytes());

    let recv = Receiver::new(secret);
    let addr = recv.spawn().await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/verify"))
        .header("relay-timestamp", timestamp.to_string())
        .header("relay-signature", format!("v1={signature}"))
        .body(r#"{"amount":99999}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        401,
        "a body that does not match its signature must be refused"
    );
}

#[tokio::test]
async fn an_oversized_body_is_refused() {
    let f = fixture("/verify", "whsec_big", "whsec_big").await;

    // Larger than the extractor's cap. Without a cap, one request can exhaust
    // memory, because the body is buffered before the handler runs.
    let big = "x".repeat(relay_api::extract::MAX_BODY_BYTES + 1);

    let resp = reqwest::Client::new()
        .post(format!("http://{}/v1/events", f.api))
        .header("relay-event-type", &f.event_type)
        .body(big)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 413);
}

#[tokio::test]
async fn ingest_returns_immediately_even_when_the_endpoint_hangs() {
    // The receiver sleeps for 30 seconds. Ingest must not care: it writes one row
    // and returns. This is the property that lets Relay survive bursts.
    let f = fixture("/slow?ms=30000", "whsec_slow", "whsec_slow").await;

    let started = std::time::Instant::now();
    let deliveries = ingest(f.api, &f.event_type, r#"{"type":"x"}"#).await;
    let elapsed = started.elapsed();

    assert_eq!(deliveries.len(), 1);
    assert!(
        elapsed.as_millis() < 1000,
        "ingest took {elapsed:?}; it must never wait on a customer endpoint"
    );
}

#[tokio::test]
async fn a_delivery_is_never_sent_twice_by_the_same_loop() {
    // Regression test for a real defect: the sender used to claim nothing before
    // sending, so any failure to persist the outcome left the row `pending`, the
    // loop picked it up again, and the endpoint received the same webhook
    // repeatedly for as long as the write kept failing.
    //
    // Claiming first makes the second pass find nothing to do.
    let f = fixture("/verify", "whsec_once", "whsec_once").await;
    let deliveries = ingest(f.api, &f.event_type, r#"{"type":"order.paid"}"#).await;
    let sender = Sender::with_config(f.store.clone(), local());

    let first = sender.deliver_by_id(deliveries[0]).await.unwrap();
    assert_eq!(first, Some(Outcome::Succeeded { status: 200 }));

    // Same delivery, second pass: already claimed and finished, so nothing happens.
    let second = sender.deliver_by_id(deliveries[0]).await.unwrap();
    assert_eq!(second, None, "a finished delivery must not be sent again");

    assert_eq!(
        f.receiver.hits(),
        1,
        "the endpoint must be contacted exactly once"
    );
    assert_eq!(f.store.attempts_for(deliveries[0]).await.unwrap(), 1);
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
