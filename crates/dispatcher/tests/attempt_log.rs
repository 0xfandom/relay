//! The attempt log.
//!
//! This table is three things at once: the audit ledger ("did we really send it?"),
//! the retry history ("what went wrong the first five times?") and the latency
//! dataset behind M7's dashboards. All three depend on the same property — that a
//! delivery's whole story can be reconstructed from these rows alone, without the
//! deliveries table, which only ever holds the current state.
//!
//! Requires Postgres: `docker compose up -d`.

use std::time::Duration;

use relay_dispatcher::{Pool, PoolConfig, RequestLimits, SenderConfig};
use relay_domain::{backoff::Backoff, url_guard::Policy};
use relay_store::Store;
use relay_testkit::Receiver;
use sqlx::PgPool;
use uuid::Uuid;

fn fast_backoff(max_attempts: u32) -> Backoff {
    Backoff {
        base: Duration::from_millis(10),
        cap: Duration::from_millis(30),
        max_attempts,
        retry_after_cap: Duration::from_secs(300),
    }
}

fn pool_config() -> PoolConfig {
    PoolConfig {
        workers: 2,
        batch_size: 2,
        idle_poll: Duration::from_millis(10),
        shutdown_deadline: Duration::from_secs(5),
    }
}

async fn endpoint(store: &Store, receiver: &Receiver, path: &str) -> Uuid {
    let addr = receiver.spawn().await;
    let event_type = format!("log.{}", Uuid::new_v4());
    store
        .create_endpoint(
            &format!("http://{addr}{path}"),
            "whsec_log_test",
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

async fn drain(pool: &Pool, store: &Store, id: Uuid) -> String {
    for _ in 0..200 {
        pool.run_once().await.expect("run");
        let status = store.get_delivery(id).await.unwrap().unwrap().status;
        if status == "succeeded" || status == "dead" {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("delivery never settled");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_full_history_is_reconstructable_from_the_log_alone(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_log_test");
    let id = endpoint(&store, &receiver, "/always500").await;

    let max_attempts = 4;
    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(max_attempts),
            ..local()
        },
    );
    assert_eq!(drain(&sender, &store, id).await, "dead");

    let history = store.attempt_history(id).await.unwrap();
    assert_eq!(history.len(), max_attempts as usize);

    for (i, a) in history.iter().enumerate() {
        assert_eq!(
            a.attempt_no, i as i32,
            "attempts must be in order with no gaps"
        );
        assert_eq!(a.http_status, Some(500));
        assert!(a.error.is_some());
        assert!(
            a.worker_id.is_some(),
            "an attempt with no worker id cannot be traced back to the process that made it"
        );
        assert!(a.latency_ms >= 0);
    }

    // The distinction the deliveries table cannot make. Every attempt here is
    // classified `retryable`, but only the first three actually were retried — the
    // last one exhausted the budget. Without the scheduled time on each row, those
    // are indistinguishable after the fact.
    let scheduled: Vec<bool> = history
        .iter()
        .map(|a| a.next_attempt_at.is_some())
        .collect();
    assert_eq!(
        scheduled,
        vec![true, true, true, false],
        "the final attempt must record no next attempt, and the earlier ones must record one"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_deferral_is_distinguishable_from_a_failure(pool: PgPool) {
    let store = Store::from_pool(pool);

    let limited = Receiver::new("whsec_log_test");
    let limited_id = endpoint(&store, &limited, "/429?retry_after=1").await;

    let broken = Receiver::new("whsec_log_test");
    let broken_id = endpoint(&store, &broken, "/always500").await;

    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(12),
            ..local()
        },
    );
    assert_eq!(sender.run_once().await.expect("run"), 2);

    // Both are retryable and both came back. But one endpoint is working correctly
    // and asking us to slow down, and the other is broken. Recording them the same
    // way makes a rate-limited customer look like a failing one, which is the
    // difference between an alert worth waking someone for and noise.
    assert_eq!(
        store.attempt_history(limited_id).await.unwrap()[0].outcome_class,
        "deferred"
    );
    assert_eq!(
        store.attempt_history(broken_id).await.unwrap()[0].outcome_class,
        "retryable"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_success_and_a_permanent_failure_are_recorded_as_such(pool: PgPool) {
    let store = Store::from_pool(pool);

    let ok = Receiver::new("whsec_log_test");
    let ok_id = endpoint(&store, &ok, "/verify").await;

    let gone = Receiver::new("whsec_log_test");
    let gone_id = endpoint(&store, &gone, "/no-such-route").await;

    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(12),
            ..local()
        },
    );
    assert_eq!(sender.run_once().await.expect("run"), 2);

    let ok_attempt = &store.attempt_history(ok_id).await.unwrap()[0];
    assert_eq!(ok_attempt.outcome_class, "success");
    assert_eq!(ok_attempt.http_status, Some(200));
    assert!(ok_attempt.next_attempt_at.is_none());
    assert!(ok_attempt.error.is_none());

    let gone_attempt = &store.attempt_history(gone_id).await.unwrap()[0];
    assert_eq!(gone_attempt.outcome_class, "permanent");
    assert_eq!(gone_attempt.http_status, Some(404));
    assert!(gone_attempt.next_attempt_at.is_none());
}

#[sqlx::test(migrations = "../store/migrations")]
async fn an_enormous_response_body_does_not_produce_an_enormous_row(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_log_test");
    // Four megabytes of error page. Framework debug pages really are this size.
    let id = endpoint(&store, &receiver, "/bigbody?kb=4096").await;

    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(1),
            ..local()
        },
    );
    assert_eq!(sender.run_once().await.expect("run"), 1);

    let snippet = store.attempt_history(id).await.unwrap()[0]
        .response_snippet
        .clone()
        .expect("snippet");

    assert!(
        snippet.len() <= 2048,
        "stored {} bytes of a 4MB error page",
        snippet.len()
    );
    assert!(!snippet.is_empty(), "the snippet should still be useful");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn an_attempt_row_cannot_be_rewritten(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_log_test");
    let id = endpoint(&store, &receiver, "/verify").await;

    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(1),
            ..local()
        },
    );
    assert_eq!(sender.run_once().await.expect("run"), 1);

    // Enforced by the database, not by convention. An attempt that can be edited
    // afterwards is not evidence of anything, and "we definitely sent it, look at the
    // log" is most of what this table is for.
    let rewrite =
        sqlx::query("UPDATE delivery_attempts SET http_status = 200 WHERE delivery_id = $1")
            .bind(id)
            .execute(store.pool())
            .await;

    let err = rewrite.expect_err("an attempt row was silently rewritten");
    assert!(
        err.to_string().contains("append-only"),
        "expected the append-only trigger to fire, got: {err}"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn an_unknown_outcome_class_is_rejected_by_the_database(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_log_test");
    let id = endpoint(&store, &receiver, "/verify").await;

    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(1),
            ..local()
        },
    );
    sender.run_once().await.expect("run");

    // A typo in this column should be a failed write, not a row that quietly breaks
    // every dashboard grouping by it.
    let bad = sqlx::query(
        "INSERT INTO delivery_attempts (delivery_id, attempt_no, latency_ms, outcome_class)
         VALUES ($1, 99, 1, 'sucess')",
    )
    .bind(id)
    .execute(store.pool())
    .await;

    assert!(bad.is_err(), "a misspelled outcome class was accepted");
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
