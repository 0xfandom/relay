//! What registration will accept.
//!
//! Registration is a courtesy check — the authority is the send path, which resolves
//! the address at the moment it connects, because a domain that is public today can
//! be repointed tomorrow. But a courtesy check that disagrees with the authority is
//! worse than none: it accepts URLs that will never deliver, and the caller finds
//! out from the dead letter queue instead of from the request they could have fixed.
//!
//! So both processes build the policy from the same variables, and these tests are
//! about the half a URL can be judged on immediately: its scheme and its port.
//!
//! Requires Postgres: `docker compose up -d`.

use std::net::SocketAddr;

use relay_api::{AppState, router};
use relay_store::Store;
use serde_json::Value;
use sqlx::PgPool;

async fn serve(state: AppState) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

async fn register(addr: SocketAddr, url: &str) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/endpoints"))
        .json(&serde_json::json!({ "url": url }))
        .send()
        .await
        .expect("register");
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap_or(Value::Null))
}

fn error(body: &Value) -> String {
    body["error"].as_str().unwrap_or_default().to_string()
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_production_policy_requires_tls(pool: PgPool) {
    let addr = serve(AppState::new(Store::from_pool(pool))).await;

    let (status, body) = register(addr, "http://example.com/hook").await;
    assert_eq!(status, 400);
    // Refused now, at the point the caller can still fix it, rather than accepted
    // and then dead-lettered by a process they cannot see.
    assert!(error(&body).contains("https is required"), "got {body}");

    let (status, _) = register(addr, "https://example.com/hook").await;
    assert_eq!(status, 201);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_production_policy_restricts_the_port(pool: PgPool) {
    let addr = serve(AppState::new(Store::from_pool(pool))).await;

    let (status, body) = register(addr, "https://example.com:6379/hook").await;
    assert_eq!(status, 400);
    assert!(error(&body).contains("port 6379"), "got {body}");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_scheme_no_receiver_speaks_is_refused_either_way(pool: PgPool) {
    // `file://` is not a webhook receiver and does read local files, so it is
    // refused under every policy — the development switch relaxes where deliveries
    // may go, not what they may be.
    for state in [
        AppState::new(Store::from_pool(pool.clone())),
        AppState::permissive(Store::from_pool(pool.clone())),
    ] {
        let addr = serve(state).await;
        let (status, body) = register(addr, "file:///etc/passwd").await;
        assert_eq!(status, 400);
        assert!(error(&body).contains("not http or https"), "got {body}");
    }
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_development_policy_accepts_a_loopback_receiver(pool: PgPool) {
    let addr = serve(AppState::permissive(Store::from_pool(pool))).await;

    // The local demo: plain HTTP, loopback, and whatever port the testkit was given.
    // One switch has to make all three work, or somebody sets it in production to
    // make an error go away.
    let (status, _) = register(addr, "http://127.0.0.1:9099/verify").await;
    assert_eq!(status, 201);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_url_with_no_authority_is_refused(pool: PgPool) {
    let addr = serve(AppState::permissive(Store::from_pool(pool))).await;

    // A port and no host at all. The parser rejects it, and that rejection has to
    // reach the caller as a `400` rather than as a `500` from somewhere deeper.
    let (status, body) = register(addr, "https://:443/x").await;
    assert_eq!(status, 400);
    assert!(error(&body).contains("invalid url"), "got {body}");

    // The trap next door, asserted so nobody "fixes" it later: `https:///hook` looks
    // hostless and is not. The parser collapses the slashes and reads `hook` as the
    // host, so this is a well-formed URL naming a host that happens not to exist. It
    // is accepted here and refused at send time, when it resolves to nothing — which
    // is the correct division of labour, since only the send path can know.
    let (status, _) = register(addr, "https:///hook").await;
    assert_eq!(status, 201);
}

// ------------------------------------------------------------------ body cap

#[sqlx::test(migrations = "../store/migrations")]
async fn a_body_over_the_cap_is_refused_before_it_is_buffered(pool: PgPool) {
    let mut state = AppState::permissive(Store::from_pool(pool));
    state.max_body_bytes = 1024;
    let addr = serve(state).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/events"))
        .header("content-type", "application/json")
        .header("relay-event-type", "order.paid")
        .body(vec![b'x'; 4096])
        .send()
        .await
        .expect("post");

    assert_eq!(resp.status(), 413);
    // The cap is configurable, so the message has to carry the number in force
    // rather than a constant that stopped being true.
    assert!(
        resp.text().await.unwrap_or_default().contains("1024"),
        "the rejection should name the limit"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_lying_content_length_does_not_get_past_the_cap(pool: PgPool) {
    let mut state = AppState::permissive(Store::from_pool(pool));
    state.max_body_bytes = 1024;
    let addr = serve(state).await;

    // Chunked, so there is no `Content-Length` to check up front. The declared-length
    // check is an optimisation — it refuses without buffering — and cannot be the
    // only one, because the sender chooses what to declare.
    let big = vec![b'x'; 4096];
    let stream = futures_util::stream::once(async move { Ok::<_, std::io::Error>(big) });
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/events"))
        .header("content-type", "application/json")
        .header("relay-event-type", "order.paid")
        .body(reqwest::Body::wrap_stream(stream))
        .send()
        .await
        .expect("post");

    assert_eq!(resp.status(), 413);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_body_inside_the_cap_is_accepted(pool: PgPool) {
    let mut state = AppState::permissive(Store::from_pool(pool));
    state.max_body_bytes = 1024;
    let addr = serve(state).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/events"))
        .header("content-type", "application/json")
        .header("relay-event-type", "order.paid")
        .body(vec![b'x'; 512])
        .send()
        .await
        .expect("post");

    assert_eq!(resp.status(), 202);
}

// ------------------------------------------------------------------ transports

async fn register_full(addr: SocketAddr, body: serde_json::Value) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/endpoints"))
        .json(&body)
        .send()
        .await
        .expect("register");
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap_or(Value::Null))
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_chat_endpoint_is_registered_by_address_and_token(pool: PgPool) {
    let addr = serve(AppState::new(Store::from_pool(pool))).await;

    let (status, body) = register_full(
        addr,
        serde_json::json!({
            "url": "telegram://-1001234567890",
            "transport": "telegram",
            "secret": "123456:AAHfake_bot_token",
        }),
    )
    .await;

    assert_eq!(status, 201, "got {body}");
    assert_eq!(body["transport"], "telegram");
    // The stored address, not the built URL. The bot token is a path segment in
    // Telegram's own scheme, and a URL is returned here, stored on every dead letter
    // and written into a span on every send.
    assert_eq!(body["url"], "telegram://-1001234567890");
    // And the token is not echoed back. The caller sent it; returning it would put
    // their own credential in a response body for no gain.
    assert!(body["secret"].is_null(), "got {body}");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_chat_endpoint_without_a_token_is_refused(pool: PgPool) {
    let addr = serve(AppState::new(Store::from_pool(pool))).await;

    let (status, body) = register_full(
        addr,
        serde_json::json!({ "url": "discord://998877665544", "transport": "discord" }),
    )
    .await;

    // Refused now rather than accepted and then permanently failing at send time for
    // a reason the caller never sees.
    assert_eq!(status, 400);
    assert!(error(&body).contains("token"), "got {body}");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_malformed_chat_address_is_refused_at_registration(pool: PgPool) {
    let addr = serve(AppState::new(Store::from_pool(pool))).await;

    for url in ["telegram://", "-1001234567890", "https://example.com"] {
        let (status, body) = register_full(
            addr,
            serde_json::json!({ "url": url, "transport": "telegram", "secret": "tok" }),
        )
        .await;
        assert_eq!(status, 400, "{url:?} should be refused");
        // The rejection names the shape that would work, because "invalid" alone
        // sends the caller to the source.
        assert!(error(&body).contains("telegram://<chat_id>"), "got {body}");
    }
}

#[sqlx::test(migrations = "../store/migrations")]
async fn an_unknown_transport_is_refused(pool: PgPool) {
    let addr = serve(AppState::new(Store::from_pool(pool))).await;
    let (status, body) = register_full(
        addr,
        serde_json::json!({ "url": "slack://general", "transport": "slack", "secret": "t" }),
    )
    .await;
    assert_eq!(status, 400);
    assert!(error(&body).contains("telegram"), "got {body}");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_chat_endpoint_is_checked_against_the_url_it_will_actually_use(pool: PgPool) {
    // The strict policy requires HTTPS on port 443. A chat address is neither a URL
    // nor a port, so the only way this check means anything is if it runs against the
    // *built* destination — which for Telegram is `https://api.telegram.org/...` and
    // passes, where the stored `telegram://…` would have failed for the wrong reason.
    let addr = serve(AppState::new(Store::from_pool(pool))).await;
    let (status, body) = register_full(
        addr,
        serde_json::json!({
            "url": "telegram://-1001234567890",
            "transport": "telegram",
            "secret": "tok",
        }),
    )
    .await;
    assert_eq!(status, 201, "got {body}");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn an_http_endpoint_still_gets_a_generated_secret(pool: PgPool) {
    let addr = serve(AppState::new(Store::from_pool(pool))).await;
    let (status, body) = register_full(
        addr,
        serde_json::json!({ "url": "https://example.com/hook" }),
    )
    .await;

    assert_eq!(status, 201, "got {body}");
    assert_eq!(body["transport"], "http", "http is the default");
    // Relay generates the signing key rather than accepting one, so a weak secret is
    // never the caller's decision to get wrong.
    let secret = body["secret"].as_str().expect("a generated secret");
    assert!(secret.starts_with("whsec_"));
}
