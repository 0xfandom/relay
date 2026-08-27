//! The delivery id is stable, and that is the whole deduplication contract.
//!
//! Exactly-once delivery over a network we do not control is not achievable. We
//! send a request and no reply comes back; the endpoint either never received it or
//! received it, processed it, and lost the acknowledgement. Those two are
//! indistinguishable from here, forever, and no design fixes that.
//!
//! So Relay retries, and a receiver will sometimes see the same webhook twice. The
//! only thing that makes that safe is `Relay-Delivery-Id` being fixed at creation
//! and repeated on every attempt: the receiver stores the ids it has handled and
//! ignores one it recognises. If the id changed per attempt, every retry would look
//! like a new event and there would be no way to tell.
//!
//! Duplicates are not prevented here. They are made detectable.
//!
//! Requires Postgres: `docker compose up -d`.

use std::{collections::HashSet, net::SocketAddr, time::Duration};

use relay_api::{AppState, router};
use relay_dispatcher::{Pool, PoolConfig, RequestLimits, SenderConfig};
use relay_domain::{backoff::Backoff, url_guard::Policy};
use relay_store::Store;
use relay_testkit::Receiver;
use sqlx::PgPool;
use uuid::Uuid;

fn local(max_attempts: u32) -> SenderConfig {
    SenderConfig {
        backoff: Backoff {
            base: Duration::from_millis(5),
            cap: Duration::from_millis(20),
            max_attempts,
            retry_after_cap: Duration::from_secs(300),
        },
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
        limits: Default::default(),
    }
}

fn pool_config() -> PoolConfig {
    PoolConfig {
        workers: 4,
        batch_size: 8,
        idle_poll: Duration::from_millis(5),
        shutdown_deadline: Duration::from_secs(5),
    }
}

async fn spawn_api(store: Store) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(AppState::permissive(store));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

/// Drive the pool until every listed delivery is terminal.
///
/// Settle-based rather than a fixed number of passes: a delivery waiting out its
/// backoff makes `run_once` return zero without being finished, so "nothing claimed"
/// is not a stopping condition.
async fn drain_until_settled(pool: &Pool, store: &Store, ids: &[Uuid]) {
    for _ in 0..400 {
        pool.run_once().await.expect("run");
        let mut all_done = true;
        for id in ids {
            let status = store.get_delivery(*id).await.unwrap().unwrap().status;
            all_done &= status == "succeeded" || status == "dead";
        }
        if all_done {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("deliveries never settled");
}

/// Register one endpoint pointed at `path` and queue one delivery to it.
async fn seed(store: &Store, receiver: &Receiver, path: &str) -> Uuid {
    let addr = receiver.spawn().await;
    let event_type = format!("id.{}", Uuid::new_v4());
    store
        .create_endpoint(
            &format!("http://{addr}{path}"),
            "whsec_id_test",
            std::slice::from_ref(&event_type),
        )
        .await
        .expect("endpoint");
    store
        .insert_event_and_fan_out(&event_type, br#"{"hello":"world"}"#)
        .await
        .expect("insert")
        .delivery_ids[0]
}

#[sqlx::test(migrations = "../store/migrations")]
async fn every_attempt_of_one_delivery_carries_the_same_id(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_id_test");
    // Fails twice, succeeds on the third: three attempts of one delivery, which is
    // exactly the case a receiver has to be able to recognise.
    let id = seed(&store, &receiver, "/flaky?pct=3").await;

    let sender = Pool::with_config(store.clone(), pool_config(), local(12));
    drain_until_settled(&sender, &store, &[id]).await;

    assert_eq!(
        store.get_delivery(id).await.unwrap().unwrap().status,
        "succeeded"
    );

    let ids = receiver.received_ids();
    assert!(
        ids.len() >= 3,
        "expected several attempts, saw {}: {ids:?}",
        ids.len()
    );

    // The assertion the contract rests on.
    let distinct: HashSet<&String> = ids.iter().collect();
    assert_eq!(
        distinct.len(),
        1,
        "the receiver saw more than one id for one delivery: {distinct:?}"
    );
    assert_eq!(ids[0], id.to_string(), "the id is the delivery row's id");

    // Every attempt, not merely several. If the sender skipped the header on some
    // path, the receiver would record fewer ids than there were requests and the
    // distinct-count check above would still pass.
    let attempts = store.attempt_history(id).await.unwrap();
    assert_eq!(
        ids.len(),
        attempts.len(),
        "an attempt was made with no delivery id header"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_delivery_that_dies_carries_one_id_across_every_failed_attempt(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_id_test");
    let id = seed(&store, &receiver, "/always500").await;

    // Four attempts, all failing, then the delivery is parked. The receiver may well
    // have processed one of them and only lost the reply, so it needs the id on the
    // failures just as much as on the success.
    let sender = Pool::with_config(store.clone(), pool_config(), local(4));
    drain_until_settled(&sender, &store, &[id]).await;

    assert_eq!(
        store.get_delivery(id).await.unwrap().unwrap().status,
        "dead"
    );

    let ids = receiver.received_ids();
    assert!(ids.len() >= 2, "expected retries, saw {}", ids.len());
    assert!(
        ids.iter().all(|got| got == &id.to_string()),
        "a failed attempt carried a different id: {ids:?}"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_replayed_delivery_keeps_its_id(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_id_test");
    let id = seed(&store, &receiver, "/always500").await;
    let api = spawn_api(store.clone()).await;

    let sender = Pool::with_config(store.clone(), pool_config(), local(2));
    drain_until_settled(&sender, &store, &[id]).await;
    assert_eq!(
        store.get_delivery(id).await.unwrap().unwrap().status,
        "dead"
    );

    let before = receiver.received_ids().len();
    reqwest::Client::new()
        .post(format!("http://{api}/v1/deliveries/{id}/replay"))
        .send()
        .await
        .expect("replay");
    drain_until_settled(&sender, &store, &[id]).await;

    let ids = receiver.received_ids();
    assert!(ids.len() > before, "the replay was never attempted");

    // Deliberate, and worth being explicit about: an operator draining the dead
    // letter queue is retrying *this* delivery, not creating a new one. A receiver
    // that already processed it will recognise the id and ignore the replay, which
    // is the correct outcome — and the reason replay bumps the generation rather
    // than the id.
    let distinct: HashSet<&String> = ids.iter().collect();
    assert_eq!(
        distinct.len(),
        1,
        "a replay must not look like a new event: {distinct:?}"
    );
    assert!(store.get_delivery(id).await.unwrap().unwrap().generation > 0);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn each_endpoint_gets_its_own_id_for_the_same_event(pool: PgPool) {
    let store = Store::from_pool(pool);

    // One event, two subscribers. The id identifies a delivery, not an event, so the
    // two receivers must not be handed the same id — if they were, and both fed a
    // shared deduplication store, the second endpoint's webhook would be discarded
    // as a duplicate of the first's.
    let event_type = format!("id.{}", Uuid::new_v4());
    let mut receivers = Vec::new();
    for _ in 0..2 {
        let receiver = Receiver::new("whsec_id_test");
        let addr = receiver.spawn().await;
        store
            .create_endpoint(
                &format!("http://{addr}/verify"),
                "whsec_id_test",
                std::slice::from_ref(&event_type),
            )
            .await
            .expect("endpoint");
        receivers.push(receiver);
    }

    let accepted = store
        .insert_event_and_fan_out(&event_type, br#"{"hello":"world"}"#)
        .await
        .expect("insert");
    assert_eq!(accepted.delivery_ids.len(), 2);

    let sender = Pool::with_config(store.clone(), pool_config(), local(12));
    drain_until_settled(&sender, &store, &accepted.delivery_ids).await;

    let seen: Vec<String> = receivers
        .iter()
        .map(|r| {
            let ids = r.received_ids();
            assert_eq!(ids.len(), 1, "one attempt each");
            ids[0].clone()
        })
        .collect();

    assert_ne!(seen[0], seen[1], "two deliveries shared one id");
    let expected: HashSet<String> = accepted
        .delivery_ids
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(seen.into_iter().collect::<HashSet<_>>(), expected);
}
