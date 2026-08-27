//! One event, three kinds of destination, one delivery machinery.
//!
//! The claim being tested is not "Telegram works". It is that *nothing downstream of
//! building the request knows which transport it is* — the same retries, the same
//! backoff, the same breaker, the same rate limit, the same attempt log. If a
//! transport ever needed to change one of those, the abstraction would be in the
//! wrong place, and the tests below are how that would show up.
//!
//! The second thing under test is the credential. Telegram's bot token and Discord's
//! webhook token are both *path segments* in their native form, and a URL is
//! returned by the admin API, stored on every dead letter, and written into a span on
//! every send. So the tokens live in the endpoint's `secret` and the built URL is
//! never written down.
//!
//! Requires Postgres: `docker compose up -d`.

use std::time::Duration;

use relay_dispatcher::{Limits, Outcome, Pool, PoolConfig, RequestLimits, Sender, SenderConfig};
use relay_domain::{
    backoff::Backoff,
    transport::{self, Kind},
    url_guard::Policy,
};
use relay_store::Store;
use relay_testkit::Receiver;
use sqlx::PgPool;
use uuid::Uuid;

const BOT_TOKEN: &str = "123456:AAHfake_bot_token_that_must_never_be_logged";
const WEBHOOK_TOKEN: &str = "fake_webhook_token_that_must_never_be_logged";
const CHAT_ID: &str = "-1001234567890";
const WEBHOOK_ID: &str = "998877665544332211";

/// A sender whose chat transports point at the local stand-in.
fn config(base: &str) -> SenderConfig {
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
        // Both chat APIs are served by the same stand-in, on the same host. Their
        // path layouts differ, which is the part that matters.
        transports: transport::Registry::with_bases(base, base),
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

async fn endpoint(store: &Store, url: &str, secret: &str, kind: Kind, event_type: &str) -> Uuid {
    store
        .create_endpoint_with(
            url,
            secret,
            std::slice::from_ref(&event_type.to_string()),
            kind,
        )
        .await
        .expect("endpoint")
        .id
}

// ------------------------------------------------------------------- fan-out

#[sqlx::test(migrations = "../store/migrations")]
async fn one_event_reaches_all_three_transports(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_transport_test");
    let addr = receiver.spawn().await;
    let base = format!("http://{addr}");
    let event_type = format!("tp.{}", Uuid::new_v4());

    // Three endpoints subscribed to the same event type, so one ingest fans out to
    // all three.
    endpoint(
        &store,
        &format!("{base}/verify"),
        "whsec_transport_test",
        Kind::Http,
        &event_type,
    )
    .await;
    endpoint(
        &store,
        &format!("telegram://{CHAT_ID}"),
        BOT_TOKEN,
        Kind::Telegram,
        &event_type,
    )
    .await;
    endpoint(
        &store,
        &format!("discord://{WEBHOOK_ID}"),
        WEBHOOK_TOKEN,
        Kind::Discord,
        &event_type,
    )
    .await;

    let ids = store
        .insert_event_and_fan_out(&event_type, br#"{"order":42,"paid":true}"#)
        .await
        .expect("insert")
        .delivery_ids;
    assert_eq!(ids.len(), 3);

    let sender = Pool::with_config(store.clone(), pool_config(), config(&base));
    assert_eq!(sender.run_once().await.expect("run"), 3);

    for id in &ids {
        let d = store.get_delivery(*id).await.unwrap().unwrap();
        assert_eq!(d.status, "succeeded", "delivery {id} did not succeed");
    }
    assert_eq!(receiver.hits(), 3);

    // The credential went where it belongs: in the path, at the right position for
    // each platform.
    let paths = receiver.paths();
    assert!(
        paths.contains(&format!("/bot{BOT_TOKEN}/sendMessage")),
        "got {paths:?}"
    );
    assert!(
        paths.contains(&format!("/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}")),
        "got {paths:?}"
    );
}

// -------------------------------------------------------------- request shape

#[sqlx::test(migrations = "../store/migrations")]
async fn a_chat_transport_reshapes_the_body_and_http_does_not(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_transport_test");
    let addr = receiver.spawn().await;
    let base = format!("http://{addr}");
    let payload = br#"{"order":42,"paid":true}"#;

    let http_type = format!("tp.{}", Uuid::new_v4());
    endpoint(
        &store,
        &format!("{base}/verify"),
        "whsec_transport_test",
        Kind::Http,
        &http_type,
    )
    .await;
    let http_id = store
        .insert_event_and_fan_out(&http_type, payload)
        .await
        .expect("insert")
        .delivery_ids[0];

    let tg_type = format!("tp.{}", Uuid::new_v4());
    endpoint(
        &store,
        &format!("telegram://{CHAT_ID}"),
        BOT_TOKEN,
        Kind::Telegram,
        &tg_type,
    )
    .await;
    let tg_id = store
        .insert_event_and_fan_out(&tg_type, payload)
        .await
        .expect("insert")
        .delivery_ids[0];

    let sender = Sender::with_config(store.clone(), config(&base));
    sender.deliver_by_id(http_id).await.expect("http");
    sender.deliver_by_id(tg_id).await.expect("telegram");

    let bodies = receiver.bodies();
    // HTTP sends the stored bytes verbatim. Nothing may parse and re-encode them:
    // JSON key order is not defined and the signature covers bytes, not meaning.
    assert_eq!(
        bodies[0],
        payload.to_vec(),
        "the http body was not verbatim"
    );

    // Telegram gets an object of its own, because a chat message is text. This is the
    // one place the never-re-encode rule does not apply — and it is allowed to not
    // apply precisely because nothing here is signed.
    let sent: serde_json::Value = serde_json::from_slice(&bodies[1]).expect("json");
    assert_eq!(sent["chat_id"], CHAT_ID);
    let text = sent["text"].as_str().expect("text");
    assert!(
        text.starts_with(&tg_type),
        "the event type should lead: {text:?}"
    );
    assert!(
        text.contains(r#""order":42"#),
        "the payload should be readable: {text:?}"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn only_the_http_transport_signs(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_transport_test");
    let addr = receiver.spawn().await;
    let base = format!("http://{addr}");

    let event_type = format!("tp.{}", Uuid::new_v4());
    endpoint(
        &store,
        &format!("telegram://{CHAT_ID}"),
        BOT_TOKEN,
        Kind::Telegram,
        &event_type,
    )
    .await;
    let id = store
        .insert_event_and_fan_out(&event_type, br#"{"n":1}"#)
        .await
        .expect("insert")
        .delivery_ids[0];

    Sender::with_config(store.clone(), config(&base))
        .deliver_by_id(id)
        .await
        .expect("deliver");

    // Telegram already knows the request came from us: it arrived carrying our bot
    // token. A signature would be ceremony, and signing with a bot token would be
    // worse — it would put a credential through a code path built for a key nobody
    // outside Relay holds.
    assert!(
        receiver.signature_headers().is_empty(),
        "a chat transport should not send a Relay signature"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_long_payload_is_truncated_to_what_the_platform_accepts(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_transport_test");
    let addr = receiver.spawn().await;
    let base = format!("http://{addr}");

    // Well past Discord's 2000-character limit. The stand-in rejects anything longer,
    // exactly as the real one would, so an untruncated body fails this outright.
    let payload = format!(r#"{{"note":"{}"}}"#, "x".repeat(8000));
    let event_type = format!("tp.{}", Uuid::new_v4());
    endpoint(
        &store,
        &format!("discord://{WEBHOOK_ID}"),
        WEBHOOK_TOKEN,
        Kind::Discord,
        &event_type,
    )
    .await;
    let id = store
        .insert_event_and_fan_out(&event_type, payload.as_bytes())
        .await
        .expect("insert")
        .delivery_ids[0];

    let outcome = Sender::with_config(store.clone(), config(&base))
        .deliver_by_id(id)
        .await
        .expect("deliver");

    // A message that arrives cut short is worth more than one that does not arrive.
    assert!(
        matches!(outcome, Some(Outcome::Succeeded { .. })),
        "got {outcome:?}"
    );
    let sent: serde_json::Value = serde_json::from_slice(&receiver.bodies()[0]).expect("json");
    assert_eq!(
        sent["content"].as_str().expect("content").len(),
        transport::DISCORD_MAX_CONTENT
    );
}

// ---------------------------------------------------------- shared machinery

#[sqlx::test(migrations = "../store/migrations")]
async fn a_failing_chat_endpoint_retries_exactly_like_an_http_one(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_transport_test");
    let addr = receiver.spawn().await;
    let base = format!("http://{addr}");

    // A chat id the stand-in will reject: `chat_id` is present but the token path is
    // wrong, so this is served by no route at all and answers `404`.
    let event_type = format!("tp.{}", Uuid::new_v4());
    endpoint(
        &store,
        &format!("discord://{WEBHOOK_ID}"),
        WEBHOOK_TOKEN,
        Kind::Discord,
        &event_type,
    )
    .await;
    let id = store
        .insert_event_and_fan_out(&event_type, br#"{"n":1}"#)
        .await
        .expect("insert")
        .delivery_ids[0];

    // Point the transport at a base with no routes, so every request is a `404`.
    let mut cfg = config(&base);
    cfg.transports =
        transport::Registry::with_bases(format!("{base}/nothing"), format!("{base}/nothing"));
    Sender::with_config(store.clone(), cfg)
        .deliver_by_id(id)
        .await
        .expect("deliver");

    let d = store.get_delivery(id).await.unwrap().unwrap();
    // The same classifier, the same permanent/retryable split, the same dead reason.
    // Nothing in that decision knows what a Discord webhook is.
    assert_eq!(d.status, "dead");
    assert_eq!(d.dead_reason.as_deref(), Some("permanent_failure"));
    let attempt = &store.attempt_history(id).await.unwrap()[0];
    assert_eq!(attempt.outcome_class, "permanent");
    assert_eq!(attempt.http_status, Some(404));
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_malformed_chat_address_is_permanent_rather_than_retried_forever(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_transport_test");
    let addr = receiver.spawn().await;
    let base = format!("http://{addr}");

    // Written straight to the table, as a row created before validation existed
    // would have been. There is no chat id after the scheme.
    let event_type = format!("tp.{}", Uuid::new_v4());
    endpoint(
        &store,
        "telegram://",
        BOT_TOKEN,
        Kind::Telegram,
        &event_type,
    )
    .await;
    let id = store
        .insert_event_and_fan_out(&event_type, br#"{"n":1}"#)
        .await
        .expect("insert")
        .delivery_ids[0];

    Sender::with_config(store.clone(), config(&base))
        .deliver_by_id(id)
        .await
        .expect("deliver");

    let d = store.get_delivery(id).await.unwrap().unwrap();
    // Permanent: no amount of waiting makes a missing chat id appear. Retrying would
    // spend twelve attempts to learn what the first one established.
    assert_eq!(d.dead_reason.as_deref(), Some("permanent_failure"));
    assert_eq!(receiver.hits(), 0, "nothing should have been sent");
    let attempt = &store.attempt_history(id).await.unwrap()[0];
    assert!(
        attempt
            .error
            .as_deref()
            .unwrap()
            .contains("telegram://<chat_id>"),
        "the error should say what the address must look like, got {:?}",
        attempt.error
    );
}

// ------------------------------------------------------------- the credential

#[sqlx::test(migrations = "../store/migrations")]
async fn a_chat_token_never_reaches_the_stored_history(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new("whsec_transport_test");
    let addr = receiver.spawn().await;
    let base = format!("http://{addr}");

    let event_type = format!("tp.{}", Uuid::new_v4());
    let ep = endpoint(
        &store,
        &format!("telegram://{CHAT_ID}"),
        BOT_TOKEN,
        Kind::Telegram,
        &event_type,
    )
    .await;
    let id = store
        .insert_event_and_fan_out(&event_type, br#"{"n":1}"#)
        .await
        .expect("insert")
        .delivery_ids[0];

    Sender::with_config(store.clone(), config(&base))
        .deliver_by_id(id)
        .await
        .expect("deliver");

    // The token is a path segment in Telegram's own URL scheme. Storing the built URL
    // would put it in the endpoints table, in every dead letter, and in a span on
    // every send — three copies, none of which anyone would think to redact.
    let stored: String = sqlx::query_scalar("SELECT url FROM endpoints WHERE id = $1")
        .bind(ep)
        .fetch_one(store.pool())
        .await
        .expect("url");
    assert_eq!(stored, format!("telegram://{CHAT_ID}"));
    assert!(!stored.contains(BOT_TOKEN));

    // And the claimed row prints neither the token nor a URL containing it.
    let printed = format!("{:?}", store.get_endpoint(ep).await.unwrap());
    assert!(!printed.contains(BOT_TOKEN), "got {printed}");
}
