//! The dead letter queue, and draining it.
//!
//! A dead letter queue that cannot be drained is only a log file. Parking a delivery
//! instead of dropping it is only worth doing because the underlying problem usually
//! gets fixed — the endpoint comes back, the URL is corrected — and the deliveries
//! that failed meanwhile are still owed.
//!
//! Driven through the real HTTP router rather than the store, because the listing
//! and the replay are the operator's interface and their shape is the deliverable.
//!
//! Requires Postgres: `docker compose up -d`.

use std::{net::SocketAddr, time::Duration};

use relay_api::{AppState, router};
use relay_dispatcher::{Pool, PoolConfig, SenderConfig};
use relay_domain::{backoff::Backoff, url_guard::Policy};
use relay_store::Store;
use relay_testkit::Receiver;
use sqlx::PgPool;
use uuid::Uuid;

fn fast_backoff(max_attempts: u32) -> Backoff {
    Backoff {
        base: Duration::from_millis(5),
        cap: Duration::from_millis(20),
        max_attempts,
        retry_after_cap: Duration::from_secs(300),
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
    let app = router(AppState { store });
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

/// Register an endpoint and queue `n` deliveries to it. Returns the endpoint id.
async fn seed(store: &Store, receiver: &Receiver, path: &str, n: usize) -> (Uuid, String) {
    let addr = receiver.spawn().await;
    let event_type = format!("dlq.{}", Uuid::new_v4());
    let ep = store
        .create_endpoint(
            &format!("http://{addr}{path}"),
            "whsec_dlq_test",
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
    (ep.id, event_type)
}

/// Run until nothing is left pending.
async fn drain_all(pool: &Pool) {
    for _ in 0..300 {
        if pool.run_once().await.expect("run") == 0 {
            tokio::time::sleep(Duration::from_millis(8)).await;
        }
    }
}

async fn get_json(url: &str) -> serde_json::Value {
    reqwest::get(url)
        .await
        .expect("get")
        .json()
        .await
        .expect("json")
}

async fn post_json(url: &str) -> (reqwest::StatusCode, serde_json::Value) {
    let resp = reqwest::Client::new().post(url).send().await.expect("post");
    let status = resp.status();
    (status, resp.json().await.unwrap_or(serde_json::Value::Null))
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_permanent_failure_is_parked_on_the_first_attempt_with_a_reason(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_dlq_test");
    seed(&store, &receiver, "/no-such-route", 1).await;
    let api = spawn_api(store.clone()).await;

    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(12),
            ..local()
        },
    );
    assert_eq!(sender.run_once().await.expect("run"), 1);

    let body = get_json(&format!("http://{api}/v1/dlq")).await;
    assert_eq!(body["count"], 1);
    assert_eq!(body["items"][0]["dead_reason"], "permanent_failure");
    // One attempt, not twelve. The budget is not spent proving that a 404 is still a
    // 404. (`hits` stays at zero here because the receiver's fallback answers the
    // unrouted path without going through its recording handler.)
    assert_eq!(
        body["items"][0]["attempt"], 1,
        "a 404 must be parked immediately rather than after the whole budget"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn exhaustion_and_permanent_failure_are_recorded_differently(pool: PgPool) {
    let store = Store::from_pool(pool);

    let broken = Receiver::new("whsec_dlq_test");
    seed(&store, &broken, "/always500", 1).await;

    let gone = Receiver::new("whsec_dlq_test");
    seed(&store, &gone, "/no-such-route", 1).await;

    let api = spawn_api(store.clone()).await;
    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(3),
            ..local()
        },
    );
    drain_all(&sender).await;

    // The two need different responses. One needs someone to fix a URL; the other
    // will probably just work once the endpoint is back. Recording only "dead" makes
    // the queue untriageable.
    let exhausted = get_json(&format!("http://{api}/v1/dlq?reason=attempts_exhausted")).await;
    assert_eq!(exhausted["count"], 1);

    let permanent = get_json(&format!("http://{api}/v1/dlq?reason=permanent_failure")).await;
    assert_eq!(permanent["count"], 1);

    let all = get_json(&format!("http://{api}/v1/dlq")).await;
    assert_eq!(all["count"], 2);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_replayed_delivery_is_attempted_again_and_can_succeed(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_dlq_test");
    // Fails the first two requests, then succeeds — so a delivery given only two
    // attempts dies, and the same delivery replayed afterwards goes through.
    let (_, _) = seed(&store, &receiver, "/flaky?pct=3", 1).await;
    let api = spawn_api(store.clone()).await;

    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(2),
            ..local()
        },
    );
    drain_all(&sender).await;

    let listed = get_json(&format!("http://{api}/v1/dlq")).await;
    assert_eq!(
        listed["count"], 1,
        "the delivery should have run out of attempts"
    );
    let id = listed["items"][0]["delivery_id"]
        .as_str()
        .unwrap()
        .to_string();

    let (status, body) = post_json(&format!("http://{api}/v1/deliveries/{id}/replay")).await;
    assert_eq!(status, 202);
    assert_eq!(body["replayed"], 1);

    drain_all(&sender).await;

    let delivery = store
        .get_delivery(Uuid::parse_str(&id).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        delivery.status, "succeeded",
        "the replay should have gone through"
    );

    assert_eq!(
        get_json(&format!("http://{api}/v1/dlq")).await["count"],
        0,
        "a replayed delivery must leave the queue"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_replay_keeps_the_earlier_attempts_but_starts_a_new_generation(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_dlq_test");
    seed(&store, &receiver, "/always500", 1).await;
    let api = spawn_api(store.clone()).await;

    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(2),
            ..local()
        },
    );
    drain_all(&sender).await;

    let listed = get_json(&format!("http://{api}/v1/dlq")).await;
    let id: Uuid = listed["items"][0]["delivery_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    post_json(&format!("http://{api}/v1/deliveries/{id}/replay")).await;
    drain_all(&sender).await;

    let history = store.attempt_history(id).await.unwrap();
    assert_eq!(history.len(), 4, "the earlier attempts must not be erased");

    // Replay resets the attempt counter, so without the generation there would be
    // two attempt 0s and no way to say which run either belonged to. The log is
    // append-only precisely so this history survives.
    let pairs: Vec<(i32, i32)> = history
        .iter()
        .map(|a| (a.generation, a.attempt_no))
        .collect();
    assert_eq!(pairs, vec![(0, 0), (0, 1), (1, 0), (1, 1)]);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn bulk_replay_is_filtered_and_bounded(pool: PgPool) {
    let store = Store::from_pool(pool);

    let broken = Receiver::new("whsec_dlq_test");
    let (broken_ep, _) = seed(&store, &broken, "/always500", 6).await;

    let gone = Receiver::new("whsec_dlq_test");
    seed(&store, &gone, "/no-such-route", 3).await;

    let api = spawn_api(store.clone()).await;
    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(2),
            ..local()
        },
    );
    drain_all(&sender).await;
    assert_eq!(get_json(&format!("http://{api}/v1/dlq")).await["count"], 9);

    // Filtered by endpoint, and capped. Replaying the whole queue at once would aim
    // every parked delivery at an endpoint that has only just recovered.
    let (status, body) = post_json(&format!(
        "http://{api}/v1/dlq/replay?endpoint_id={broken_ep}&limit=4"
    ))
    .await;
    assert_eq!(status, 202);
    assert_eq!(body["replayed"], 4);

    let left = get_json(&format!("http://{api}/v1/dlq")).await;
    assert_eq!(
        left["count"], 5,
        "only the four requested should have moved"
    );

    // The other endpoint's dead letters were not touched.
    let untouched = get_json(&format!("http://{api}/v1/dlq?reason=permanent_failure")).await;
    assert_eq!(untouched["count"], 3);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn replaying_something_that_is_not_dead_is_refused(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_dlq_test");
    seed(&store, &receiver, "/verify", 1).await;
    let api = spawn_api(store.clone()).await;

    let sender = Pool::with_config(
        store.clone(),
        pool_config(),
        SenderConfig {
            backoff: fast_backoff(12),
            ..local()
        },
    );
    assert_eq!(sender.run_once().await.expect("run"), 1);

    // The delivery succeeded, so it is not in the queue at all.
    let succeeded_id = store
        .dead_letters(&Default::default(), 10)
        .await
        .unwrap()
        .len();
    assert_eq!(succeeded_id, 0);

    // Replaying a delivery that is not dead is refused. It would hand a second
    // worker something the first may still be sending, and for a succeeded delivery
    // it would send the endpoint a webhook it already has.
    let live = sqlx::query_scalar::<_, Uuid>("SELECT id FROM deliveries LIMIT 1")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let (status, _) = post_json(&format!("http://{api}/v1/deliveries/{live}/replay")).await;
    assert_eq!(status, 404, "a succeeded delivery must not be replayable");

    // And neither is one that does not exist.
    let (status, _) = post_json(&format!(
        "http://{api}/v1/deliveries/{}/replay",
        Uuid::new_v4()
    ))
    .await;
    assert_eq!(status, 404);

    assert_eq!(receiver.hits(), 1, "nothing should have been re-sent");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn an_unknown_reason_filter_is_rejected(pool: PgPool) {
    let store = Store::from_pool(pool);
    let api = spawn_api(store).await;

    let resp = reqwest::get(format!("http://{api}/v1/dlq?reason=probably"))
        .await
        .expect("get");
    assert_eq!(
        resp.status(),
        400,
        "a typo in a filter must be an error, not a silently empty listing"
    );
}

/// Every receiver in these tests runs on loopback, which the strict policy refuses.
///
/// Opted into explicitly rather than making permissive the default. A default that
/// allows internal addresses is a vulnerability that ships whenever somebody forgets
/// to configure it, and the tests are exactly where that forgetting would hide.
fn local() -> SenderConfig {
    SenderConfig {
        policy: Policy::permissive(),
        ..Default::default()
    }
}
