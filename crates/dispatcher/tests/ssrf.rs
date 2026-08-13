//! Deliveries to internal addresses are refused.
//!
//! Relay's product is "give us a URL and we will send an HTTP request to it", from a
//! machine inside a private network. Without a guard that is a server-side request
//! forgery engine: a customer registers the cloud metadata address, we fetch it from
//! inside the instance where it answers without authentication, and the stored
//! response snippet carries the credentials back out through the delivery history.
//!
//! The tests below use the *strict* policy — the production default — while every
//! other test file opts into the permissive one, because every receiver there lives
//! on loopback. That inversion is the point: loopback is exactly what an attacker
//! would aim at.
//!
//! Requires Postgres: `docker compose up -d`.

use std::time::Duration;

use relay_dispatcher::{Outcome, Pool, PoolConfig, Sender, SenderConfig};
use relay_domain::{outcome::Class, url_guard::Policy};
use relay_store::Store;
use relay_testkit::Receiver;
use sqlx::PgPool;
use uuid::Uuid;

fn strict() -> SenderConfig {
    SenderConfig::default()
}

fn pool_config() -> PoolConfig {
    PoolConfig {
        workers: 2,
        batch_size: 4,
        idle_poll: Duration::from_millis(10),
        shutdown_deadline: Duration::from_secs(5),
    }
}

/// Queue one delivery to `url` and return its id.
async fn queue(store: &Store, url: &str) -> Uuid {
    let event_type = format!("ssrf.{}", Uuid::new_v4());
    store
        .create_endpoint(url, "whsec_ssrf_test", std::slice::from_ref(&event_type))
        .await
        .expect("endpoint");
    store
        .insert_event_and_fan_out(&event_type, br#"{"hello":"world"}"#)
        .await
        .expect("insert")
        .delivery_ids[0]
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_cloud_metadata_address_is_refused(pool: PgPool) {
    let store = Store::from_pool(pool);
    // The address behind the 2019 Capital One breach. It answers without
    // authentication to anything running on the instance, and hands out the
    // machine's cloud credentials.
    let id = queue(
        &store,
        "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
    )
    .await;

    let sender = Pool::with_config(store.clone(), pool_config(), strict());
    assert_eq!(sender.run_once().await.expect("run"), 1);

    let d = store.get_delivery(id).await.unwrap().unwrap();
    assert_eq!(d.status, "dead", "the request must not have been made");
    assert_eq!(d.dead_reason.as_deref(), Some("permanent_failure"));

    let attempt = &store.attempt_history(id).await.unwrap()[0];
    assert_eq!(attempt.outcome_class, "permanent");
    assert!(attempt.http_status.is_none(), "no response should exist");
    assert!(
        attempt
            .error
            .as_deref()
            .unwrap()
            .contains("not a public address"),
        "got: {:?}",
        attempt.error
    );
    assert!(
        attempt.response_snippet.is_none(),
        "a refused delivery must store no response body: handing it back to the \
         party who chose the address is the second half of the vulnerability"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_real_receiver_on_loopback_is_refused_under_the_strict_policy(pool: PgPool) {
    let store = Store::from_pool(pool);
    // A receiver that genuinely works. Under the permissive policy every other test
    // file delivers to exactly this; under the strict one it must not be reachable,
    // which is what proves the guard is actually applied rather than merely present.
    let receiver = Receiver::new("whsec_ssrf_test");
    let addr = receiver.spawn().await;
    let id = queue(&store, &format!("http://{addr}/verify")).await;

    let sender = Pool::with_config(store.clone(), pool_config(), strict());
    assert_eq!(sender.run_once().await.expect("run"), 1);

    assert_eq!(receiver.hits(), 0, "the request reached the receiver");
    assert_eq!(
        store.get_delivery(id).await.unwrap().unwrap().status,
        "dead"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn loopback_is_refused_however_the_url_spells_it(pool: PgPool) {
    let store = Store::from_pool(pool);

    // Every one of these reaches this machine. A blocklist matching URL text would
    // have to know all of these spellings; resolving first and judging the address
    // means there is only ever one thing to check.
    let spellings = [
        "http://127.0.0.1:9/x",
        "http://127.1:9/x",
        "http://0.0.0.0:9/x",
        "http://2130706433:9/x",
        "http://0x7f000001:9/x",
        "http://[::1]:9/x",
        "http://[::ffff:127.0.0.1]:9/x",
        "http://10.0.0.1:9/x",
        "http://192.168.1.1:9/x",
        "http://172.16.0.1:9/x",
        "http://169.254.169.254:9/x",
    ];

    let mut ids = Vec::new();
    for url in spellings {
        ids.push((url, queue(&store, url).await));
    }

    let sender = Pool::with_config(store.clone(), pool_config(), strict());
    for _ in 0..10 {
        sender.run_once().await.expect("run");
    }

    for (url, id) in ids {
        let d = store.get_delivery(id).await.unwrap().unwrap();
        assert_eq!(d.status, "dead", "{url} was not refused");
        let attempt = &store.attempt_history(id).await.unwrap()[0];
        assert!(
            attempt.error.as_deref().unwrap().contains("refused"),
            "{url} failed for the wrong reason: {:?}",
            attempt.error
        );
    }
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_refusal_is_permanent_and_does_not_burn_the_retry_budget(pool: PgPool) {
    let store = Store::from_pool(pool);
    let id = queue(&store, "http://169.254.169.254/latest/meta-data/").await;

    let sender = Sender::with_config(store.clone(), strict());
    let outcome = sender
        .deliver_by_id(id)
        .await
        .expect("deliver")
        .expect("attempted");

    match outcome {
        Outcome::Failed { class, status, .. } => {
            // No amount of waiting makes an internal address public. Retrying it
            // eleven more times would only be a slow port scan of our own network.
            assert_eq!(class, Class::Permanent);
            assert_eq!(status, None);
        }
        other => panic!("expected a refusal, got {other:?}"),
    }

    let d = store.get_delivery(id).await.unwrap().unwrap();
    assert_eq!(d.attempt, 1, "it must stop after one attempt");
    assert_eq!(d.status, "dead");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn non_http_schemes_are_refused(pool: PgPool) {
    let store = Store::from_pool(pool);
    let id = queue(&store, "file:///etc/passwd").await;

    let sender = Pool::with_config(store.clone(), pool_config(), strict());
    assert_eq!(sender.run_once().await.expect("run"), 1);

    let attempt = &store.attempt_history(id).await.unwrap()[0];
    assert!(
        attempt
            .error
            .as_deref()
            .unwrap()
            .contains("not http or https"),
        "got: {:?}",
        attempt.error
    );
    assert_eq!(
        store.get_delivery(id).await.unwrap().unwrap().status,
        "dead"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_public_endpoint_still_works_under_the_strict_policy(pool: PgPool) {
    let store = Store::from_pool(pool);
    // The other half of the guard. One that refuses everything is not a guard, it is
    // an outage. There is nothing listening at this public address, so the delivery
    // fails — but it must fail as a *connection* problem, retryable, rather than as
    // a refusal.
    let id = queue(&store, "http://93.184.216.34:9/never-answers").await;

    let sender = Pool::with_config(store.clone(), pool_config(), strict());
    assert_eq!(sender.run_once().await.expect("run"), 1);

    let attempt = &store.attempt_history(id).await.unwrap()[0];
    assert!(
        !attempt.error.as_deref().unwrap().contains("refused:"),
        "a public address was blocked by the guard: {:?}",
        attempt.error
    );
    assert_eq!(
        attempt.outcome_class, "retryable",
        "an unreachable public host is a transient failure, not a refusal"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_permissive_policy_is_what_makes_loopback_work(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_ssrf_test");
    let addr = receiver.spawn().await;
    let id = queue(&store, &format!("http://{addr}/verify")).await;

    let permissive = SenderConfig {
        policy: Policy::permissive(),
        ..Default::default()
    };
    let sender = Pool::with_config(store.clone(), pool_config(), permissive);
    assert_eq!(sender.run_once().await.expect("run"), 1);

    // Local development has to remain possible, and the escape hatch has to be the
    // thing that opens it — not an accident of how the address happens to be written.
    assert_eq!(receiver.hits(), 1);
    assert_eq!(
        store.get_delivery(id).await.unwrap().unwrap().status,
        "succeeded"
    );
}
